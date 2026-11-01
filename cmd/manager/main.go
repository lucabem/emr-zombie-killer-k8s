package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1alpha1 "github.com/lucabem/emr-zombie-killer-k8s/api/v1alpha1"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/controller"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/emr"
	"github.com/lucabem/emr-zombie-killer-k8s/internal/probe"
)

func main() {
	var (
		metricsAddr string
		probeAddr   string
		leaderElect bool
		leaderNS    string
		watchNS     string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Prometheus metrics address.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Health probe address.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Enable leader election (needed with >1 replica).")
	flag.StringVar(&leaderNS, "leader-election-namespace", "", "Namespace for the leader Lease (defaults to the pod's namespace).")
	flag.StringVar(&watchNS, "watch-namespace", "", "Restrict to one namespace (default: all).")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	// Only cache pods that belong to an EMR job run.
	req, _ := labels.NewRequirement(controller.LabelJobID, selection.Exists, nil)
	cacheOpts := cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {Label: labels.NewSelector().Add(*req)},
		},
	}
	if watchNS != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{watchNS: {}}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                     scheme,
		Cache:                      cacheOpts,
		Metrics:                    metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:     probeAddr,
		LeaderElection:             leaderElect,
		LeaderElectionID:           "emr-zombie-killer.zombie.lucabem.io",
		LeaderElectionNamespace:    leaderNS,
		LeaderElectionResourceLock: "leases",
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to create clientset")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	emrClient, err := emr.New(ctx)
	if err != nil {
		setupLog.Error(err, "unable to create EMR client")
		os.Exit(1)
	}

	if err := (&controller.PodWatchdog{
		Client:   mgr.GetClient(),
		Probe:    &probe.KubeLogProbe{Client: cs},
		Canceler: emrClient,
		Recorder: mgr.GetEventRecorderFor("emr-zombie-killer"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add healthz check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to add readyz check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
