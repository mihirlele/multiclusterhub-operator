// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"testing"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	clog "sigs.k8s.io/controller-runtime/pkg/log"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = operatorv1.AddToScheme(s)
	return s
}

func newTestReconciler(objects ...runtime.Object) *MultiClusterHubReconciler {
	s := newTestScheme()
	clientObjects := make([]runtime.Object, len(objects))
	copy(clientObjects, objects)

	c := fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(clientObjects...).Build()
	return &MultiClusterHubReconciler{
		Client: c,
		Scheme: s,
		Log:    clog.Log.WithName("test"),
		EscalationTracker: &EscalationTracker{
			StuckFinalizers: make(map[string]time.Time),
		},
	}
}

// --- LoadEscalationConfig tests ---

func TestLoadEscalationConfig_Defaults(t *testing.T) {
	config := LoadEscalationConfig()
	if config.UninstallTimeout != 15*time.Minute {
		t.Errorf("UninstallTimeout = %v, want 15m", config.UninstallTimeout)
	}
	if config.StuckFinalizerTimeout != 5*time.Minute {
		t.Errorf("StuckFinalizerTimeout = %v, want 5m", config.StuckFinalizerTimeout)
	}
	if config.ResourcePlateauTimeout != 5*time.Minute {
		t.Errorf("ResourcePlateauTimeout = %v, want 5m", config.ResourcePlateauTimeout)
	}
	if !config.EscalationEnabled {
		t.Error("EscalationEnabled should default to true")
	}
}

func TestLoadEscalationConfig_EnvOverrides(t *testing.T) {
	t.Setenv("UNINSTALL_TIMEOUT_MINUTES", "30")
	t.Setenv("STUCK_FINALIZER_TIMEOUT_MINUTES", "10")
	t.Setenv("RESOURCE_PLATEAU_TIMEOUT_MINUTES", "8")
	t.Setenv("ESCALATION_ENABLED", "false")

	config := LoadEscalationConfig()
	if config.UninstallTimeout != 30*time.Minute {
		t.Errorf("UninstallTimeout = %v, want 30m", config.UninstallTimeout)
	}
	if config.StuckFinalizerTimeout != 10*time.Minute {
		t.Errorf("StuckFinalizerTimeout = %v, want 10m", config.StuckFinalizerTimeout)
	}
	if config.ResourcePlateauTimeout != 8*time.Minute {
		t.Errorf("ResourcePlateauTimeout = %v, want 8m", config.ResourcePlateauTimeout)
	}
	if config.EscalationEnabled {
		t.Error("EscalationEnabled should be false")
	}
}

func TestParseDurationMinutes_InvalidValue(t *testing.T) {
	t.Setenv("TEST_TIMEOUT", "abc")
	got := parseDurationMinutes("TEST_TIMEOUT", 15)
	if got != 15*time.Minute {
		t.Errorf("parseDurationMinutes() = %v, want 15m for invalid input", got)
	}
}

func TestParseBoolEnv_InvalidValue(t *testing.T) {
	t.Setenv("TEST_BOOL", "notabool")
	got := parseBoolEnv("TEST_BOOL", true)
	if !got {
		t.Error("parseBoolEnv() should return default for invalid input")
	}
}

// --- initializeUninstallPhase tests ---

func TestInitializeUninstallPhase(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
	}

	r.initializeUninstallPhase(m)

	if m.Status.UninstallEscalationPhase != operatorv1.UninstallNotRequired {
		t.Errorf("UninstallPhase = %v, want NotRequired", m.Status.UninstallEscalationPhase)
	}
	if !r.EscalationTracker.Initialized {
		t.Error("EscalationTracker should be initialized")
	}
	if r.EscalationTracker.LastResourceCount != -1 {
		t.Errorf("LastResourceCount = %d, want -1", r.EscalationTracker.LastResourceCount)
	}
}

func TestInitializeUninstallPhase_Idempotent(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
	}

	r.initializeUninstallPhase(m)
	firstTime := r.EscalationTracker.FirstSeenTime

	time.Sleep(1 * time.Millisecond)
	r.initializeUninstallPhase(m)

	if r.EscalationTracker.FirstSeenTime != firstTime {
		t.Error("FirstSeenTime should not change on second initialization")
	}
}

// --- shouldTriggerEscalation tests ---

func TestShouldTriggerEscalation_Disabled(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	config := EscalationConfig{EscalationEnabled: false}
	shouldEscalate, _ := r.shouldTriggerEscalation(context.TODO(), m, config)
	if shouldEscalate {
		t.Error("shouldTriggerEscalation() should return false when disabled")
	}
}

func TestShouldTriggerEscalation_AlreadyEscalated(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallEscalated},
	}

	config := EscalationConfig{EscalationEnabled: true, UninstallTimeout: 15 * time.Minute}
	shouldEscalate, _ := r.shouldTriggerEscalation(context.TODO(), m, config)
	if shouldEscalate {
		t.Error("shouldTriggerEscalation() should return false when already escalated")
	}
}

func TestShouldTriggerEscalation_AlreadyCompleted(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallCompleted},
	}

	config := EscalationConfig{EscalationEnabled: true, UninstallTimeout: 15 * time.Minute}
	shouldEscalate, _ := r.shouldTriggerEscalation(context.TODO(), m, config)
	if shouldEscalate {
		t.Error("shouldTriggerEscalation() should return false when completed")
	}
}

func TestShouldTriggerEscalation_NotInitialized(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	config := EscalationConfig{EscalationEnabled: true, UninstallTimeout: 15 * time.Minute}
	shouldEscalate, _ := r.shouldTriggerEscalation(context.TODO(), m, config)
	if shouldEscalate {
		t.Error("shouldTriggerEscalation() should return false when tracker not initialized")
	}
}

func TestShouldTriggerEscalation_Timeout(t *testing.T) {
	r := newTestReconciler()
	r.EscalationTracker.Initialized = true
	r.EscalationTracker.FirstSeenTime = time.Now().Add(-20 * time.Minute)
	r.EscalationTracker.LastProgressTime = time.Now()
	r.EscalationTracker.LastResourceCount = -1

	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	config := EscalationConfig{
		EscalationEnabled:      true,
		UninstallTimeout:       15 * time.Minute,
		StuckFinalizerTimeout:  5 * time.Minute,
		ResourcePlateauTimeout: 5 * time.Minute,
	}
	shouldEscalate, reason := r.shouldTriggerEscalation(context.TODO(), m, config)
	if !shouldEscalate {
		t.Error("shouldTriggerEscalation() should return true after timeout")
	}
	if reason != EscalationTimeoutReason {
		t.Errorf("reason = %v, want %v", reason, EscalationTimeoutReason)
	}
}

func TestShouldTriggerEscalation_NoEscalationWithinTimeout(t *testing.T) {
	r := newTestReconciler()
	r.EscalationTracker.Initialized = true
	r.EscalationTracker.FirstSeenTime = time.Now().Add(-5 * time.Minute)
	r.EscalationTracker.LastProgressTime = time.Now()
	r.EscalationTracker.LastResourceCount = -1

	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	config := EscalationConfig{
		EscalationEnabled:      true,
		UninstallTimeout:       15 * time.Minute,
		StuckFinalizerTimeout:  5 * time.Minute,
		ResourcePlateauTimeout: 5 * time.Minute,
	}
	shouldEscalate, _ := r.shouldTriggerEscalation(context.TODO(), m, config)
	if shouldEscalate {
		t.Error("shouldTriggerEscalation() should return false within timeout")
	}
}

func TestShouldTriggerEscalation_ResourcePlateau(t *testing.T) {
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "stuck",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "test", "installer.namespace": "ocm"},
			},
		},
	).Build()

	r := &MultiClusterHubReconciler{
		Client: c,
		Scheme: s,
		Log:    clog.Log.WithName("test"),
		EscalationTracker: &EscalationTracker{
			Initialized:       true,
			FirstSeenTime:     time.Now().Add(-3 * time.Minute),
			LastProgressTime:  time.Now().Add(-6 * time.Minute),
			LastResourceCount: 1,
			StuckFinalizers:   make(map[string]time.Time),
		},
	}

	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	config := EscalationConfig{
		EscalationEnabled:      true,
		UninstallTimeout:       15 * time.Minute,
		StuckFinalizerTimeout:  5 * time.Minute,
		ResourcePlateauTimeout: 5 * time.Minute,
	}
	shouldEscalate, reason := r.shouldTriggerEscalation(context.TODO(), m, config)
	if !shouldEscalate {
		t.Error("shouldTriggerEscalation() should return true for resource plateau")
	}
	if reason != EscalationResourcePlateauReason {
		t.Errorf("reason = %v, want %v", reason, EscalationResourcePlateauReason)
	}
}

// --- triggerEscalation tests ---

func TestTriggerEscalation(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status:     operatorv1.MultiClusterHubStatus{UninstallEscalationPhase: operatorv1.UninstallNotRequired},
	}

	r.triggerEscalation(m, EscalationTimeoutReason, 5)

	if m.Status.UninstallEscalationPhase != operatorv1.UninstallEscalated {
		t.Errorf("UninstallPhase = %v, want Escalated", m.Status.UninstallEscalationPhase)
	}
	if m.Status.UninstallEscalation == nil {
		t.Fatal("UninstallEscalation should not be nil")
	}
	if !m.Status.UninstallEscalation.Triggered {
		t.Error("Triggered should be true")
	}
	if m.Status.UninstallEscalation.Reason != EscalationTimeoutReason {
		t.Errorf("Reason = %v, want %v", m.Status.UninstallEscalation.Reason, EscalationTimeoutReason)
	}
	if m.Status.UninstallEscalation.ResourcesRemaining != 5 {
		t.Errorf("ResourcesRemaining = %d, want 5", m.Status.UninstallEscalation.ResourcesRemaining)
	}
	if m.Status.UninstallEscalation.TriggeredTime == nil {
		t.Error("TriggeredTime should be set")
	}
}

// --- updateEscalationProgress tests ---

func TestUpdateEscalationProgress(t *testing.T) {
	r := newTestReconciler()
	now := metav1.Now()
	m := &operatorv1.MultiClusterHub{
		Status: operatorv1.MultiClusterHubStatus{
			UninstallEscalation: &operatorv1.UninstallEscalationStatus{
				Triggered:          true,
				TriggeredTime:      &now,
				ResourcesRemaining: 10,
				CurrentPass:        1,
			},
		},
	}

	r.updateEscalationProgress(m, 3, 2)

	if m.Status.UninstallEscalation.ResourcesRemaining != 3 {
		t.Errorf("ResourcesRemaining = %d, want 3", m.Status.UninstallEscalation.ResourcesRemaining)
	}
	if m.Status.UninstallEscalation.CurrentPass != 2 {
		t.Errorf("CurrentPass = %d, want 2", m.Status.UninstallEscalation.CurrentPass)
	}
	if m.Status.UninstallEscalation.LastAttemptTime == nil {
		t.Error("LastAttemptTime should be updated")
	}
}

func TestUpdateEscalationProgress_NilEscalation(t *testing.T) {
	r := newTestReconciler()
	m := &operatorv1.MultiClusterHub{}

	// Should not panic
	r.updateEscalationProgress(m, 3, 2)
}

// --- isACMCRD tests ---

func TestIsACMCRD(t *testing.T) {
	tests := []struct {
		name string
		crd  string
		want bool
	}{
		{"OCM CRD", "managedclusters.cluster.open-cluster-management.io", true},
		{"multicluster CRD", "multiclusterengines.multicluster.openshift.io", true},
		{"non-ACM CRD", "deployments.apps", false},
		{"partial match", "open-cluster-management.io", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isACMCRD(tt.crd); got != tt.want {
				t.Errorf("isACMCRD(%q) = %v, want %v", tt.crd, got, tt.want)
			}
		})
	}
}

// --- executeEscalatedCleanup routing tests ---

func TestExecuteEscalatedCleanup_PassRouting(t *testing.T) {
	tests := []struct {
		name        string
		currentPass int
	}{
		{"pass 0 routes to pass 1", 0},
		{"pass 1 routes to pass 1", 1},
		{"pass 2 routes to pass 2", 2},
		{"pass 3 routes to pass 3", 3},
		{"pass 4 routes to complete", 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReconciler()
			m := &operatorv1.MultiClusterHub{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
				Status: operatorv1.MultiClusterHubStatus{
					UninstallEscalationPhase: operatorv1.UninstallEscalated,
					UninstallEscalation: &operatorv1.UninstallEscalationStatus{
						Triggered:   true,
						CurrentPass: tt.currentPass,
					},
				},
			}

			result, err := r.executeEscalatedCleanup(context.TODO(), m)
			if err != nil {
				t.Errorf("executeEscalatedCleanup() error = %v", err)
			}

			if tt.currentPass >= 4 {
				if m.Status.UninstallEscalationPhase != operatorv1.UninstallCompleted {
					t.Errorf("phase = %v, want Completed for pass >= 4", m.Status.UninstallEscalationPhase)
				}
			} else if tt.currentPass == 3 {
				// Pass 3 completes escalation
				if m.Status.UninstallEscalationPhase != operatorv1.UninstallCompleted {
					t.Errorf("phase = %v, want Completed after pass 3", m.Status.UninstallEscalationPhase)
				}
			} else {
				// Passes 1-2 should requeue
				if result.RequeueAfter == 0 {
					t.Error("expected RequeueAfter > 0 for active pass")
				}
			}
		})
	}
}

// --- completeEscalation tests ---

func TestCompleteEscalation(t *testing.T) {
	r := newTestReconciler()
	now := metav1.Now()
	triggered := metav1.NewTime(now.Add(-5 * time.Minute))
	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ocm"},
		Status: operatorv1.MultiClusterHubStatus{
			UninstallEscalationPhase: operatorv1.UninstallEscalated,
			UninstallEscalation: &operatorv1.UninstallEscalationStatus{
				Triggered:     true,
				TriggeredTime: &triggered,
				CurrentPass:   3,
			},
		},
	}

	result, err := r.completeEscalation(context.TODO(), m)
	if err != nil {
		t.Fatalf("completeEscalation() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("completeEscalation() should not requeue, got %v", result.RequeueAfter)
	}
	if m.Status.UninstallEscalationPhase != operatorv1.UninstallCompleted {
		t.Errorf("UninstallPhase = %v, want Completed", m.Status.UninstallEscalationPhase)
	}

	cond := GetHubCondition(m.Status, operatorv1.UninstallProgressing)
	if cond == nil {
		t.Fatal("UninstallProgressing condition should be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("UninstallProgressing status = %v, want False", cond.Status)
	}
}

// --- updateUninstallProgressingCondition tests ---

func TestUpdateUninstallProgressingCondition(t *testing.T) {
	tests := []struct {
		name           string
		phase          operatorv1.UninstallPhaseType
		wantStatus     metav1.ConditionStatus
		wantCondExists bool
	}{
		{"NotRequired", operatorv1.UninstallNotRequired, metav1.ConditionFalse, true},
		{"Escalated", operatorv1.UninstallEscalated, metav1.ConditionTrue, true},
		{"Completed", operatorv1.UninstallCompleted, metav1.ConditionFalse, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &operatorv1.MultiClusterHubStatus{}
			updateUninstallProgressingCondition(status, tt.phase, "TestReason", "test message")

			cond := GetHubCondition(*status, operatorv1.UninstallProgressing)
			if tt.wantCondExists && cond == nil {
				t.Fatal("condition should exist")
			}
			if cond != nil && cond.Status != tt.wantStatus {
				t.Errorf("condition status = %v, want %v", cond.Status, tt.wantStatus)
			}
		})
	}
}

// --- emitEscalationEvent tests ---

func TestEmitEscalationEvent(t *testing.T) {
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &MultiClusterHubReconciler{
		Client: c,
		Scheme: s,
		Log:    clog.Log.WithName("test"),
	}

	m := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "ocm",
			UID:       "test-uid",
		},
	}

	// Should not panic even if event creation fails (fake client may not support events)
	r.emitEscalationEvent(m, "TestReason", "Test message")
}

// --- metrics tests ---

func TestMetricFunctions(t *testing.T) {
	// These should not panic
	recordEscalationTriggered()
	recordEscalationDuration(120.5)
	updateUninstallPhaseMetric("NotRequired")
	updateUninstallPhaseMetric("Escalated")
	updateResourcesRemainingMetric(5)
	updateResourcesRemainingMetric(0)
}
