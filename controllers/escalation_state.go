// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/cleanup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EscalationConfig holds timeout thresholds for escalation detection
type EscalationConfig struct {
	UninstallTimeout       time.Duration
	StuckFinalizerTimeout  time.Duration
	ResourcePlateauTimeout time.Duration
	EscalationEnabled      bool
}

// EscalationTracker tracks deletion progress across reconcile loops
type EscalationTracker struct {
	mu                sync.Mutex
	FirstSeenTime     time.Time
	LastProgressTime  time.Time
	LastResourceCount int
	Initialized       bool
	StuckFinalizers   map[string]time.Time // resource key -> first seen stuck time
}

// LoadEscalationConfig reads configuration from environment variables with defaults
func LoadEscalationConfig() EscalationConfig {
	return EscalationConfig{
		UninstallTimeout:       parseDurationMinutes("UNINSTALL_TIMEOUT_MINUTES", 15),
		StuckFinalizerTimeout:  parseDurationMinutes("STUCK_FINALIZER_TIMEOUT_MINUTES", 5),
		ResourcePlateauTimeout: parseDurationMinutes("RESOURCE_PLATEAU_TIMEOUT_MINUTES", 5),
		EscalationEnabled:      parseBoolEnv("ESCALATION_ENABLED", true),
	}
}

func parseDurationMinutes(envVar string, defaultMinutes int) time.Duration {
	val := os.Getenv(envVar)
	if val == "" {
		return time.Duration(defaultMinutes) * time.Minute
	}
	minutes, err := strconv.Atoi(val)
	if err != nil {
		return time.Duration(defaultMinutes) * time.Minute
	}
	return time.Duration(minutes) * time.Minute
}

func parseBoolEnv(envVar string, defaultVal bool) bool {
	val := os.Getenv(envVar)
	if val == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal
	}
	return b
}

// initializeUninstallPhase sets the initial phase when deletion timestamp first appears
func (r *MultiClusterHubReconciler) initializeUninstallPhase(m *operatorv1.MultiClusterHub) {
	m.Status.UninstallEscalationPhase = operatorv1.UninstallNotRequired

	r.EscalationTracker.mu.Lock()
	defer r.EscalationTracker.mu.Unlock()

	if !r.EscalationTracker.Initialized {
		now := time.Now()
		r.EscalationTracker.FirstSeenTime = now
		r.EscalationTracker.LastProgressTime = now
		r.EscalationTracker.LastResourceCount = -1
		r.EscalationTracker.Initialized = true
		r.EscalationTracker.StuckFinalizers = make(map[string]time.Time)

		// Log escalation configuration for debugging
		config := LoadEscalationConfig()
		r.Log.Info("Escalation tracking initialized",
			"mch", m.Name,
			"namespace", m.Namespace,
			"escalationEnabled", config.EscalationEnabled,
			"uninstallTimeout", config.UninstallTimeout.String(),
			"stuckFinalizerTimeout", config.StuckFinalizerTimeout.String(),
			"resourcePlateauTimeout", config.ResourcePlateauTimeout.String())
	} else {
		r.Log.Info("Escalation tracking re-initialized after operator restart",
			"mch", m.Name, "namespace", m.Namespace)
	}
}

// shouldTriggerEscalation checks if stuck conditions warrant escalation
func (r *MultiClusterHubReconciler) shouldTriggerEscalation(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
	config EscalationConfig,
) (bool, string) {
	if !config.EscalationEnabled {
		return false, ""
	}

	// Don't escalate if already escalated or completed
	if m.Status.UninstallEscalationPhase == operatorv1.UninstallEscalated ||
		m.Status.UninstallEscalationPhase == operatorv1.UninstallCompleted {
		return false, ""
	}

	r.EscalationTracker.mu.Lock()
	defer r.EscalationTracker.mu.Unlock()

	if !r.EscalationTracker.Initialized {
		r.Log.V(1).Info("Escalation check skipped: tracker not initialized",
			"mch", m.Name, "namespace", m.Namespace)
		return false, ""
	}

	now := time.Now()

	// Check 1: Time-based threshold
	if now.Sub(r.EscalationTracker.FirstSeenTime) > config.UninstallTimeout {
		r.Log.Info("Uninstall timeout exceeded",
			"elapsed", now.Sub(r.EscalationTracker.FirstSeenTime).String(),
			"threshold", config.UninstallTimeout.String())
		return true, EscalationTimeoutReason
	}

	// Check 2: Resource count plateau
	filter := cleanup.NewACMResourceFilter(m)
	currentCount, err := filter.CountRemainingResources(ctx, r.Client)
	if err != nil {
		r.Log.Info("Failed to count remaining resources during escalation check", "error", err)
		return false, ""
	}

	if r.EscalationTracker.LastResourceCount == -1 {
		r.EscalationTracker.LastResourceCount = currentCount
		r.EscalationTracker.LastProgressTime = now
	} else if currentCount < r.EscalationTracker.LastResourceCount {
		// Progress made
		r.EscalationTracker.LastResourceCount = currentCount
		r.EscalationTracker.LastProgressTime = now
	} else if currentCount > 0 && now.Sub(r.EscalationTracker.LastProgressTime) > config.ResourcePlateauTimeout {
		r.Log.Info("Resource count plateau detected",
			"resourceCount", currentCount,
			"staleDuration", now.Sub(r.EscalationTracker.LastProgressTime).String(),
			"threshold", config.ResourcePlateauTimeout.String())
		return true, EscalationResourcePlateauReason
	}

	// Check 3: Stuck finalizer detection
	stuckResources, err := filter.GetResourcesWithFinalizers(ctx, r.Client)
	if err != nil {
		r.Log.Info("Failed to get resources with finalizers", "error", err)
		return false, ""
	}

	// Track which resources have had finalizers and for how long
	currentStuckKeys := make(map[string]bool)
	for _, res := range stuckResources {
		key := res.GetKind() + "/" + res.GetNamespace() + "/" + res.GetName()
		currentStuckKeys[key] = true

		if firstSeen, exists := r.EscalationTracker.StuckFinalizers[key]; exists {
			if now.Sub(firstSeen) > config.StuckFinalizerTimeout {
				r.Log.Info("Stuck finalizer detected",
					"resource", key,
					"staleDuration", now.Sub(firstSeen).String(),
					"threshold", config.StuckFinalizerTimeout.String(),
					"finalizers", res.GetFinalizers())
				return true, EscalationStuckFinalizerReason
			}
		} else {
			r.EscalationTracker.StuckFinalizers[key] = now
		}
	}

	// Clean up entries for resources that are no longer stuck
	for key := range r.EscalationTracker.StuckFinalizers {
		if !currentStuckKeys[key] {
			delete(r.EscalationTracker.StuckFinalizers, key)
		}
	}

	return false, ""
}

// triggerEscalation activates escalated cleanup mode
func (r *MultiClusterHubReconciler) triggerEscalation(
	m *operatorv1.MultiClusterHub,
	reason string,
	resourceCount int,
) {
	now := metav1.Now()
	m.Status.UninstallEscalationPhase = operatorv1.UninstallEscalated
	m.Status.UninstallEscalation = &operatorv1.UninstallEscalationStatus{
		Triggered:          true,
		TriggeredTime:      &now,
		Reason:             reason,
		ResourcesRemaining: resourceCount,
		LastAttemptTime:    &now,
		CurrentPass:        0,
	}

	updateUninstallProgressingCondition(&m.Status, operatorv1.UninstallEscalated,
		EscalationCleanupReason,
		"Escalation triggered due to "+reason)

	recordEscalationTriggered()
	updateUninstallPhaseMetric(string(operatorv1.UninstallEscalated))
	updateResourcesRemainingMetric(resourceCount)

	r.Log.Info("Escalation triggered",
		"reason", reason,
		"resourcesRemaining", resourceCount,
		"mch", m.Name, "namespace", m.Namespace)
}

// updateEscalationProgress updates status during multi-pass cleanup
func (r *MultiClusterHubReconciler) updateEscalationProgress(
	m *operatorv1.MultiClusterHub,
	resourceCount int,
	currentPass int,
) {
	if m.Status.UninstallEscalation == nil {
		return
	}
	now := metav1.Now()
	m.Status.UninstallEscalation.ResourcesRemaining = resourceCount
	m.Status.UninstallEscalation.LastAttemptTime = &now
	m.Status.UninstallEscalation.CurrentPass = currentPass

	updateResourcesRemainingMetric(resourceCount)
}
