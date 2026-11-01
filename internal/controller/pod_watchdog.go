// Package controller implements the zombie-pod watchdog.
//
// This is a watchdog, not a declarative reconciler: there is no desired state
// to converge to. Each pass is a time-based health check (how long since the
// pod last logged?) that counts strikes and, at the limit, triggers a one-shot
// action in AWS. controller-runtime is used for its event plumbing, caching,
// leader election and requeue/backoff; the Reconcile method is only the entry
// point it requires. Because the check depends on the clock, strikes are
// rate-limited by StrikeInterval and their state is kept in pod annotations.
package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/lucabem/emr-zombie-killer-k8s/api/v1alpha1"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/emr"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/probe"
)

const (
	// Labels set by EMR on EKS on the pods of a job run.
	LabelVirtualCluster = "emr-containers.amazonaws.com/virtual-cluster-id"
	LabelJobID          = "emr-containers.amazonaws.com/job.id"

	// Annotations written by the watchdog (state survives restarts).
	AnnStrikes         = "zombie.lucabem.io/strikes"
	AnnLastStrike      = "zombie.lucabem.io/last-strike-time"
	AnnCancelRequested = "zombie.lucabem.io/cancel-requested-at"

	minRequeue = 30 * time.Second
)

// PodWatchdog counts "strikes" against EMR pods that stop logging and
// cancels the owning EMR job run once MaxStrikes is reached.
type PodWatchdog struct {
	client.Client
	Probe    probe.LogProbe
	Canceler emr.JobCanceller
	Recorder record.EventRecorder
	Now      func() time.Time // injectable for tests
}

func (r *PodWatchdog) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile is the controller-runtime entry point. Despite the name it runs one
// watchdog check for the pod in req; it is not a desired-vs-observed diff.
func (r *PodWatchdog) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return ctrl.Result{}, nil
	}
	if _, done := pod.Annotations[AnnCancelRequested]; done {
		return ctrl.Result{}, nil
	}

	policy, err := r.policyFor(ctx, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if policy == nil {
		return ctrl.Result{}, nil
	}
	spec := withDefaults(policy.Spec)
	threshold, interval := spec.InactivityThreshold.Duration, spec.StrikeInterval.Duration

	vc, jobID := pod.Labels[LabelVirtualCluster], pod.Labels[LabelJobID]
	if vc == "" || jobID == "" {
		log.V(1).Info("pod lacks EMR labels, skipping", "pod", pod.Name)
		return ctrl.Result{}, nil
	}

	// 1. When did this pod last show signs of life?
	last, ok, err := r.Probe.LastLogTime(ctx, pod.Namespace, pod.Name, spec.Container)
	if err != nil {
		// Can't read logs (container starting, kubelet hiccup): never count a strike.
		log.Info("cannot read logs, will retry", "pod", pod.Name, "err", err.Error())
		return ctrl.Result{RequeueAfter: minRequeue}, nil
	}
	if !ok {
		last = startedAt(&pod, spec.Container)
	}
	idle := r.now().Sub(last)

	strikes := intAnn(pod.Annotations, AnnStrikes)

	// 2. Healthy: reset strikes if the pod woke up, check again when the
	//    threshold would be crossed.
	if idle < threshold {
		if strikes > 0 {
			if err := r.patchAnnotations(ctx, &pod, map[string]*string{AnnStrikes: nil, AnnLastStrike: nil}); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(&pod, corev1.EventTypeNormal, "ZombieRecovered",
				"pod logged again after %d strike(s); counter reset", strikes)
		}
		return ctrl.Result{RequeueAfter: clamp(threshold-idle, minRequeue, interval)}, nil
	}

	// 3. Idle. A strike is at most one per StrikeInterval, however often
	//    this reconcile is triggered by pod events.
	if ls, ok := timeAnn(pod.Annotations, AnnLastStrike); ok {
		if since := r.now().Sub(ls); since < interval {
			return ctrl.Result{RequeueAfter: clamp(interval-since, minRequeue, interval)}, nil
		}
	}
	if strikes < int(spec.MaxStrikes) {
		strikes++
	}
	if err := r.patchAnnotations(ctx, &pod, map[string]*string{
		AnnStrikes:    ptr(strconv.Itoa(strikes)),
		AnnLastStrike: ptr(r.now().UTC().Format(time.RFC3339)),
	}); err != nil {
		return ctrl.Result{}, err
	}
	strikesTotal.WithLabelValues(pod.Namespace).Inc()
	r.Recorder.Eventf(&pod, corev1.EventTypeWarning, "ZombieSuspected",
		"no log output for %s (threshold %s): strike %d/%d", idle.Round(time.Second), threshold, strikes, spec.MaxStrikes)

	if strikes < int(spec.MaxStrikes) {
		return ctrl.Result{RequeueAfter: interval}, nil
	}

	// 4. Out of strikes: cancel the EMR job run.
	if spec.DryRun {
		r.Recorder.Eventf(&pod, corev1.EventTypeWarning, "DryRunWouldCancel",
			"dry-run: would cancel EMR job run %s on virtual cluster %s", jobID, vc)
		return ctrl.Result{RequeueAfter: interval}, nil
	}
	res, err := r.Canceler.Cancel(ctx, vc, jobID)
	if err != nil {
		r.Recorder.Eventf(&pod, corev1.EventTypeWarning, "CancelFailed", "%v", err)
		cancelErrorsTotal.WithLabelValues(pod.Namespace).Inc()
		return ctrl.Result{}, err // controller-runtime backs off and retries
	}
	if err := r.patchAnnotations(ctx, &pod, map[string]*string{
		AnnCancelRequested: ptr(r.now().UTC().Format(time.RFC3339)),
	}); err != nil {
		return ctrl.Result{}, err
	}
	cancelsTotal.WithLabelValues(pod.Namespace, string(res)).Inc()
	r.Recorder.Eventf(&pod, corev1.EventTypeWarning, "ZombieKilled",
		"EMR job run %s on virtual cluster %s: %s after %d strikes", jobID, vc, res, strikes)
	log.Info("zombie handled", "pod", pod.Name, "jobId", jobID, "virtualCluster", vc, "result", res)
	return ctrl.Result{}, nil
}

// policyFor returns the first (by name) ZombiePolicy in the pod's namespace
// whose selector matches the pod.
func (r *PodWatchdog) policyFor(ctx context.Context, pod *corev1.Pod) (*v1alpha1.ZombiePolicy, error) {
	var list v1alpha1.ZombiePolicyList
	if err := r.List(ctx, &list, client.InNamespace(pod.Namespace)); err != nil {
		return nil, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	for i := range list.Items {
		sel, err := metav1.LabelSelectorAsSelector(withDefaults(list.Items[i].Spec).Selector)
		if err != nil {
			logf.FromContext(ctx).Error(err, "invalid selector", "policy", list.Items[i].Name)
			continue
		}
		if sel.Matches(labels.Set(pod.Labels)) {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

func (r *PodWatchdog) patchAnnotations(ctx context.Context, pod *corev1.Pod, kv map[string]*string) error {
	base := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	for k, v := range kv {
		if v == nil {
			delete(pod.Annotations, k)
		} else {
			pod.Annotations[k] = *v
		}
	}
	if err := r.Patch(ctx, pod, client.MergeFrom(base)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("patching annotations on %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// SetupWithManager wires the watchdog. Only running EMR pods are reconciled;
// a ZombiePolicy change re-queues every EMR pod in its namespace.
func (r *PodWatchdog) SetupWithManager(mgr ctrl.Manager) error {
	emrPod := predicate.NewPredicateFuncs(func(o client.Object) bool {
		p, ok := o.(*corev1.Pod)
		return ok && p.Status.Phase == corev1.PodRunning && p.Labels[LabelJobID] != ""
	})
	policyToPods := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		var pods corev1.PodList
		if err := r.List(ctx, &pods, client.InNamespace(o.GetNamespace()), client.HasLabels{LabelJobID}); err != nil {
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(pods.Items))
		for _, p := range pods.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: p.Namespace, Name: p.Name}})
		}
		return reqs
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("zombie-pod").
		For(&corev1.Pod{}, builder.WithPredicates(emrPod)).
		Watches(&v1alpha1.ZombiePolicy{}, policyToPods).
		Complete(r)
}

// ---- helpers ----------------------------------------------------------------

func withDefaults(s v1alpha1.ZombiePolicySpec) v1alpha1.ZombiePolicySpec {
	if s.Selector == nil {
		s.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{"spark-role": "driver"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: LabelJobID, Operator: metav1.LabelSelectorOpExists},
			},
		}
	}
	if s.Container == "" {
		s.Container = "spark-kubernetes-driver"
	}
	if s.InactivityThreshold.Duration <= 0 {
		s.InactivityThreshold.Duration = 20 * time.Minute
	}
	if s.StrikeInterval.Duration <= 0 {
		s.StrikeInterval.Duration = 5 * time.Minute
	}
	if s.MaxStrikes <= 0 {
		s.MaxStrikes = 3
	}
	return s
}

// startedAt is the fallback "last activity" for a container that never logged.
func startedAt(pod *corev1.Pod, container string) time.Time {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == container && cs.State.Running != nil {
			return cs.State.Running.StartedAt.Time
		}
	}
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return pod.CreationTimestamp.Time
}

func intAnn(a map[string]string, k string) int {
	n, _ := strconv.Atoi(a[k])
	return n
}

func timeAnn(a map[string]string, k string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, a[k])
	return t, err == nil
}

func clamp(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

func ptr[T any](v T) *T { return &v }
