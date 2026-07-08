// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	uninstallPhaseGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mch_uninstall_phase",
			Help: "Current uninstall escalation phase (1 when active for the given phase label)",
		},
		[]string{"phase"},
	)

	escalationTriggeredCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "mch_uninstall_escalation_triggered_total",
			Help: "Total number of times escalation has been triggered",
		},
	)

	escalationDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "mch_uninstall_escalation_duration_seconds",
			Help:    "Duration of escalated cleanup in seconds",
			Buckets: []float64{60, 300, 600, 1200, 1800, 3600},
		},
	)

	resourcesRemainingGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "mch_uninstall_resources_remaining",
			Help: "Number of ACM resources remaining during uninstall",
		},
	)
)

func init() {
	metrics.Registry.MustRegister(
		uninstallPhaseGauge,
		escalationTriggeredCounter,
		escalationDurationHistogram,
		resourcesRemainingGauge,
	)
}

func recordEscalationTriggered() {
	escalationTriggeredCounter.Inc()
}

func recordEscalationDuration(seconds float64) {
	escalationDurationHistogram.Observe(seconds)
}

func updateUninstallPhaseMetric(phase string) {
	uninstallPhaseGauge.Reset()
	uninstallPhaseGauge.WithLabelValues(phase).Set(1)
}

func updateResourcesRemainingMetric(count int) {
	resourcesRemainingGauge.Set(float64(count))
}
