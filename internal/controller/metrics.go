package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	strikesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zombie_killer_strikes_total",
		Help: "Strikes recorded against pods that stopped logging.",
	}, []string{"namespace"})

	cancelsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zombie_killer_cancels_total",
		Help: "EMR job runs handled after reaching max strikes.",
	}, []string{"namespace", "result"})

	cancelErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zombie_killer_cancel_errors_total",
		Help: "Failed attempts to cancel an EMR job run.",
	}, []string{"namespace"})
)

func init() {
	metrics.Registry.MustRegister(strikesTotal, cancelsTotal, cancelErrorsTotal)
}
