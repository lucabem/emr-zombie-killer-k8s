package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/lucabem/emr-zombie-killer-k8s/api/v1alpha1"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/emr"
)

type fakeProbe struct {
	t   time.Time
	ok  bool
	err error
}

func (f *fakeProbe) LastLogTime(context.Context, string, string, string) (time.Time, bool, error) {
	return f.t, f.ok, f.err
}

type fakeCanceler struct {
	calls []string
	err   error
}

func (f *fakeCanceler) Cancel(_ context.Context, vc, job string) (emr.Result, error) {
	f.calls = append(f.calls, vc+"/"+job)
	if f.err != nil {
		return "", f.err
	}
	return emr.Cancelled, nil
}

type harness struct {
	r      *PodWatchdog
	probe  *fakeProbe
	cancel *fakeCanceler
	clock  time.Time
	key    types.NamespacedName
}

func newHarness(t *testing.T, mutate func(*v1alpha1.ZombiePolicy, *corev1.Pod)) *harness {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = v1alpha1.AddToScheme(s)

	policy := &v1alpha1.ZombiePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "spark"},
		Spec: v1alpha1.ZombiePolicySpec{
			InactivityThreshold: metav1.Duration{Duration: 20 * time.Minute},
			StrikeInterval:      metav1.Duration{Duration: 5 * time.Minute},
			MaxStrikes:          3,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "driver", Namespace: "spark",
			Labels: map[string]string{
				"spark-role":        "driver",
				LabelJobID:          "job-1",
				LabelVirtualCluster: "vc-1",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if mutate != nil {
		mutate(policy, pod)
	}

	h := &harness{
		probe:  &fakeProbe{ok: true},
		cancel: &fakeCanceler{},
		clock:  time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		key:    types.NamespacedName{Namespace: "spark", Name: "driver"},
	}
	h.probe.t = h.clock // fresh log
	h.r = &PodWatchdog{
		Client:   fake.NewClientBuilder().WithScheme(s).WithObjects(policy, pod).Build(),
		Probe:    h.probe,
		Canceler: h.cancel,
		Recorder: record.NewFakeRecorder(100),
		Now:      func() time.Time { return h.clock },
	}
	return h
}

func (h *harness) reconcile(t *testing.T) ctrl.Result {
	t.Helper()
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (h *harness) pod(t *testing.T) corev1.Pod {
	t.Helper()
	var p corev1.Pod
	if err := h.r.Get(context.Background(), h.key, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func (h *harness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

func TestHealthyPodGetsNoStrike(t *testing.T) {
	h := newHarness(t, nil)
	h.probe.t = h.clock.Add(-5 * time.Minute)
	res := h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "" {
		t.Fatalf("unexpected strikes %q", got)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("healthy pod must be requeued")
	}
}

func TestThreeStrikesCancelsJob(t *testing.T) {
	h := newHarness(t, nil)
	h.probe.t = h.clock.Add(-25 * time.Minute) // idle past the threshold

	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "1" {
		t.Fatalf("want strike 1, got %q", got)
	}

	// Extra reconciles (pod events) within the strike interval must not count.
	h.advance(1 * time.Minute)
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "1" {
		t.Fatalf("event storm double-counted: %q", got)
	}

	h.advance(5 * time.Minute)
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "2" {
		t.Fatalf("want strike 2, got %q", got)
	}
	if len(h.cancel.calls) != 0 {
		t.Fatal("cancelled too early")
	}

	h.advance(5 * time.Minute)
	h.reconcile(t)
	if len(h.cancel.calls) != 1 || h.cancel.calls[0] != "vc-1/job-1" {
		t.Fatalf("want one cancel of vc-1/job-1, got %v", h.cancel.calls)
	}
	if _, ok := h.pod(t).Annotations[AnnCancelRequested]; !ok {
		t.Fatal("cancel-requested annotation missing")
	}

	// Once cancelled, further reconciles are no-ops.
	h.advance(10 * time.Minute)
	h.reconcile(t)
	if len(h.cancel.calls) != 1 {
		t.Fatalf("cancelled twice: %v", h.cancel.calls)
	}
}

func TestRecoveryResetsStrikes(t *testing.T) {
	h := newHarness(t, nil)
	h.probe.t = h.clock.Add(-25 * time.Minute)
	h.reconcile(t)
	h.advance(5 * time.Minute)
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "2" {
		t.Fatalf("setup: want 2 strikes, got %q", got)
	}

	h.probe.t = h.clock.Add(-1 * time.Minute) // pod logs again
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "" {
		t.Fatalf("strikes not reset: %q", got)
	}
}

func TestDryRunNeverCancels(t *testing.T) {
	h := newHarness(t, func(p *v1alpha1.ZombiePolicy, _ *corev1.Pod) { p.Spec.DryRun = true })
	h.probe.t = h.clock.Add(-30 * time.Minute)
	for i := 0; i < 5; i++ {
		h.reconcile(t)
		h.advance(5 * time.Minute)
	}
	if len(h.cancel.calls) != 0 {
		t.Fatalf("dry run cancelled: %v", h.cancel.calls)
	}
}

func TestLogReadErrorIsNotAStrike(t *testing.T) {
	h := newHarness(t, nil)
	h.probe.err = errors.New("container not ready")
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "" {
		t.Fatalf("strike on log error: %q", got)
	}
}

func TestSilentPodFallsBackToStartTime(t *testing.T) {
	h := newHarness(t, func(_ *v1alpha1.ZombiePolicy, p *corev1.Pod) {
		st := metav1.NewTime(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)) // started 1h ago
		p.Status.StartTime = &st
	})
	h.probe.ok = false // never logged anything
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "1" {
		t.Fatalf("want strike 1 for silent pod, got %q", got)
	}
}

func TestExecutorsAndUnlabelledPodsIgnored(t *testing.T) {
	h := newHarness(t, func(_ *v1alpha1.ZombiePolicy, p *corev1.Pod) { p.Labels["spark-role"] = "executor" })
	h.probe.t = h.clock.Add(-1 * time.Hour)
	h.reconcile(t)
	if got := h.pod(t).Annotations[AnnStrikes]; got != "" {
		t.Fatalf("executor was struck: %q", got)
	}
}

func TestCancelFailureRetriesWithoutResettingStrikes(t *testing.T) {
	h := newHarness(t, nil)
	h.probe.t = h.clock.Add(-30 * time.Minute)
	h.reconcile(t)
	h.advance(5 * time.Minute)
	h.reconcile(t)
	h.advance(5 * time.Minute)

	h.cancel.err = errors.New("throttled")
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err == nil {
		t.Fatal("expected cancel error to be returned for backoff")
	}
	if _, ok := h.pod(t).Annotations[AnnCancelRequested]; ok {
		t.Fatal("must not mark cancelled on failure")
	}

	h.cancel.err = nil
	h.advance(5 * time.Minute)
	h.reconcile(t)
	if _, ok := h.pod(t).Annotations[AnnCancelRequested]; !ok {
		t.Fatal("retry should cancel")
	}
}
