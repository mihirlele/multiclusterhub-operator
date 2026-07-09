# Implementation Plan: Automated Cleanup Escalation for MultiClusterHub

## Context

When users delete a MultiClusterHub (MCH) CR to uninstall ACM, the operator's `finalizeHub()` function performs sequential cleanup of dependent operators (MCE, cluster-manager) and components. However, if this process gets stuck due to finalizers, webhook dependencies, or orphaned resources, deletion can hang indefinitely—currently requeuing every 20 seconds with no timeout or escalation path.

This feature adds a **dormant escalation mechanism** that monitors normal deletion and only activates when stuck conditions are detected. It remains in `NotRequired` state during healthy deletion and only transitions to `Escalated` when timeout, stuck finalizers, or resource count plateau are detected.

The escalated cleanup uses aggressive techniques modeled after the `nuke-acm-mce.sh` script:
- Finalizer stripping
- Webhook disabling
- Multi-pass deletion (3 passes with increasing force)
- Namespace force-deletion

## Current State Analysis

**Deletion Flow** (`controllers/lifecycle.go:42-79`):
- `finalizeHub()` orchestrates cleanup: AppSubscriptions → Components → Namespaces → RBAC → MCE
- Returns error when cleanup incomplete, triggering 20-second requeue (`resyncPeriod`)
- No timeout tracking or escalation path exists

**Status Structure** (`api/v1/multiclusterhub_types.go:163-184`):
- Status updated via deferred `syncHubStatus()` at end of each reconcile
- Current phase during deletion: `HubUninstalling`
- To add fields: modify types → `make generate` → `make manifests`

**Resource Identification**:
- Installer labels: `installer.name`, `installer.namespace` (via `pkg/utils/utils.go:AddInstallerLabel()`)
- RBAC cleanup uses `DeleteAllOf()` with label selectors
- **No existing force-delete utilities** — need to create new ones

## Architecture Decision

**Integrate escalation within existing `finalizeHub()` flow** rather than separate controller:
- Reuses existing cleanup orchestration and resource discovery
- Shares status update infrastructure  
- Natural integration with 20-second requeue pattern
- Simpler state management (no cross-controller coordination)

**State management:** Hybrid approach
- In-memory `EscalationTracker` tracks transient state (resource counts, timestamps) between reconciles
- Status fields provide durable state for phase, trigger reason, and timestamps

---

## Implementation Plan

### Phase 1: API Changes (Foundation)

#### 1.1 Add Status Fields

**File:** `api/v1/multiclusterhub_types.go`

Add to `MultiClusterHubStatus` struct (after line 184):

```go
// UninstallPhase tracks the progression of MCH uninstallation
// +optional
UninstallPhase UninstallPhaseType `json:"uninstallPhase,omitempty"`

// UninstallEscalation tracks escalated cleanup state when normal deletion is stuck
// +optional
UninstallEscalation *UninstallEscalationStatus `json:"uninstallEscalation,omitempty"`
```

Add new types before `MultiClusterHubStatus` (around line 135):

```go
// UninstallPhaseType represents the phase of MCH uninstallation
type UninstallPhaseType string

const (
    UninstallNotRequired UninstallPhaseType = "NotRequired"
    UninstallEscalated   UninstallPhaseType = "Escalated"
    UninstallCompleted   UninstallPhaseType = "Completed"
)

// UninstallEscalationStatus tracks the state of escalated cleanup
type UninstallEscalationStatus struct {
    Triggered           bool        `json:"triggered"`
    TriggeredTime       *metav1.Time `json:"triggeredTime,omitempty"`
    Reason              string      `json:"reason,omitempty"`
    ResourcesRemaining  int         `json:"resourcesRemaining,omitempty"`
    LastAttemptTime     *metav1.Time `json:"lastAttemptTime,omitempty"`
    CurrentPass         int         `json:"currentPass,omitempty"`
}
```

Add condition type constant (around line 230):

```go
UninstallProgressing HubConditionType = "UninstallProgressing"
```

**Run:**
```bash
make generate   # Regenerate DeepCopy methods
make manifests  # Regenerate CRD YAML
```

#### 1.2 Add Escalation Constants

**File:** `controllers/status.go`

Add after existing reason constants (around line 80):

```go
// Escalation reasons
const (
    EscalationTimeoutReason         = "UninstallTimeout"
    EscalationStuckFinalizerReason  = "StuckFinalizer"
    EscalationResourcePlateauReason = "ResourcePlateau"
    EscalationNotRequiredReason     = "NotRequired"
    EscalationCompletedReason       = "Completed"
)
```

---

### Phase 2: State Management & Detection

#### 2.1 Create Escalation State Tracker

**File:** `controllers/escalation_state.go` (NEW)

Implements:
- `EscalationConfig` struct (timeout thresholds from env vars)
- `EscalationTracker` struct (in-memory state: first seen time, last progress time, resource counts, stuck finalizers map)
- `LoadEscalationConfig()` — reads env vars with defaults (15min, 5min, 5min)
- `(r *MultiClusterHubReconciler) initializeUninstallPhase(m *operatorv1.MultiClusterHub)` — sets phase to `NotRequired` on first deletion reconcile
- `(r *MultiClusterHubReconciler) shouldTriggerEscalation(ctx, m, tracker, config) (bool, string)` — checks timeout, stuck finalizer, plateau conditions
- `(r *MultiClusterHubReconciler) triggerEscalation(m, reason, resourceCount)` — sets `UninstallPhase = Escalated`, populates `UninstallEscalation` status
- `(r *MultiClusterHubReconciler) updateEscalationProgress(m, resourceCount, currentPass)` — updates status during multi-pass cleanup

**Key logic:**
- **Timeout:** `time.Since(tracker.FirstSeenTime) > config.UninstallTimeoutMinutes`
- **Stuck finalizer:** Track resources with finalizers; trigger if same finalizer blocks for > `StuckFinalizerTimeoutMinutes`
- **Resource plateau:** Trigger if resource count unchanged for > `ResourcePlateauTimeoutMinutes`

#### 2.2 Update Reconciler Struct

**File:** `controllers/multiclusterhub_controller.go`

Add to `MultiClusterHubReconciler` struct (around line 45):

```go
// Track escalation state across reconciles
EscalationTracker *EscalationTracker
```

**File:** `controllers/setup.go`

Initialize in `SetupWithManager()`:

```go
r.EscalationTracker = &EscalationTracker{
    StuckFinalizers: make(map[string]time.Time),
}
```

---

### Phase 3: Force-Cleanup Utilities

#### 3.1 Force-Delete Functions

**File:** `pkg/cleanup/force_delete.go` (NEW)

Implements:
- `StripFinalizers(ctx, client, obj)` — removes all finalizers from typed object
- `StripFinalizersFromUnstructured(ctx, client, u)` — removes finalizers from unstructured
- `ForceDeleteWithGracePeriod(ctx, client, obj, gracePeriod)` — deletes with `deletionGracePeriodSeconds` override
- `DisableWebhookOnCRD(ctx, client, crdName)` — patches CRD to remove `spec.conversion.webhook`
- `ForceDeleteNamespace(ctx, client, namespaceName)` — strips namespace finalizers and deletes

**Critical Kubernetes API calls:**
- `obj.SetFinalizers([]string{})` then `client.Update()`
- `client.Delete(ctx, obj, &client.DeleteOptions{GracePeriodSeconds: &gracePeriod})`
- CRD patch: `client.Patch(ctx, crd, client.Merge, patchData)`

#### 3.2 Resource Discovery

**File:** `pkg/cleanup/resource_discovery.go` (NEW)

Implements:
- `ACMResourceFilter` struct (installer name/namespace, ACM namespaces list)
- `NewACMResourceFilter(m)` — creates filter from MCH instance
- `(f *ACMResourceFilter) DiscoverACMResources(ctx, client)` — lists all ACM resources by:
  - Installer labels (`installer.name`, `installer.namespace`)
  - ACM namespaces (`open-cluster-management*`, `multicluster-engine*`)
  - CRD groups (`*.open-cluster-management.io`, `*.multicluster.openshift.io`)
- `(f *ACMResourceFilter) CountRemainingResources(ctx, client)` — returns count for status
- `(f *ACMResourceFilter) GetResourcesWithFinalizers(ctx, client)` — finds stuck resources

---

### Phase 4: Multi-Pass Deletion Logic

#### 4.1 Escalated Cleanup Orchestration

**File:** `controllers/escalation_cleanup.go` (NEW)

Implements:

**`(r *MultiClusterHubReconciler) executeEscalatedCleanup(ctx, m) (ctrl.Result, error)`**
- Reads `m.Status.UninstallEscalation.CurrentPass` (defaults to 1)
- Routes to `escalationPass1()`, `escalationPass2()`, or `escalationPass3()`
- Calls `completeEscalation()` after pass 3

**Pass 1: Background deletion without finalizer removal**
- Disable webhooks on ACM CRDs via `DisableWebhookOnCRD()`
- Delete API services
- Delete all discovered ACM resources (ignore errors)
- Update status: `CurrentPass = 1`, count resources
- Requeue after **10 seconds**

**Pass 2: Retry stuck resources with grace period override**
- Re-discover remaining ACM resources
- Call `ForceDeleteWithGracePeriod(ctx, client, resource, 0)` for each
- Update status: `CurrentPass = 2`
- Requeue after **30 seconds**

**Pass 3: Strip finalizers and force-delete everything**
- Re-discover remaining ACM resources
- Call `StripFinalizersFromUnstructured()` for each
- Delete again
- Force-delete stuck namespaces via `ForceDeleteNamespace()`
- Update status: `CurrentPass = 3`
- Call `completeEscalation()`

**`(r *MultiClusterHubReconciler) completeEscalation(ctx, m) (ctrl.Result, error)`**
- Set `UninstallPhase = Completed`
- Update `UninstallProgressing` condition to `False` with reason `EscalationCompletedReason`
- Log completion
- Return empty `ctrl.Result{}` (no requeue — finalizer removal happens next)

**Helper functions:**
- `(r *MultiClusterHubReconciler) disableACMWebhooks(ctx)` — iterates ACM CRDs and disables webhooks
- `(r *MultiClusterHubReconciler) deleteACMAPIServices(ctx, filter)` — removes API services
- `(r *MultiClusterHubReconciler) forceDeleteACMNamespaces(ctx, filter)` — force-deletes stuck namespaces

---

### Phase 5: Integration with Existing Flow

#### 5.1 Modify Finalization Logic

**File:** `controllers/lifecycle.go`

Modify `finalizeHub()` (lines 42-79):

**Before:**
```go
func (r *MultiClusterHubReconciler) finalizeHub(reqLogger logr.Logger, m *operatorv1.MultiClusterHub, ocpConsole, isSTSEnabled bool) error {
    if err := r.cleanupAppSubscriptions(reqLogger, m); err != nil {
        return err
    }
    // ... rest of normal cleanup
}
```

**After:**
```go
func (r *MultiClusterHubReconciler) finalizeHub(reqLogger logr.Logger, m *operatorv1.MultiClusterHub, ocpConsole, isSTSEnabled bool) error {
    ctx := context.Background()
    
    // Initialize uninstall phase on first deletion reconcile
    if m.Status.UninstallPhase == "" {
        r.initializeUninstallPhase(m)
    }
    
    // If escalation already triggered, skip normal cleanup and run escalated cleanup
    if m.Status.UninstallPhase == operatorv1.UninstallEscalated {
        result, err := r.executeEscalatedCleanup(ctx, m)
        if err != nil {
            return err
        }
        if result != (ctrl.Result{}) {
            return errors.NewBadRequest("Requeue needed for escalated cleanup")
        }
        // Escalation complete
        reqLogger.Info("Escalated cleanup completed successfully")
        return nil
    }
    
    // Normal cleanup path (unchanged from here)
    if err := r.cleanupAppSubscriptions(reqLogger, m); err != nil {
        return err
    }
    // ... rest of existing cleanup logic unchanged
}
```

#### 5.2 Add Escalation Trigger Check

**File:** `controllers/reconcile.go`

Modify deletion handling (around lines 203-211):

**Before:**
```go
if controllerutil.ContainsFinalizer(multiClusterHub, hubFinalizer) {
    if err := r.finalizeHub(r.Log, multiClusterHub, ocpConsole, stsEnabled); err != nil {
        r.Log.Info(fmt.Sprintf("Finalizing: %s", err.Error()))
        return ctrl.Result{RequeueAfter: resyncPeriod}, nil
    }
    // Remove finalizer...
}
```

**After:**
```go
if controllerutil.ContainsFinalizer(multiClusterHub, hubFinalizer) {
    // Check if escalation should be triggered
    config := LoadEscalationConfig()
    if config.EscalationEnabled && multiClusterHub.Status.UninstallPhase != operatorv1.UninstallEscalated {
        shouldEscalate, reason := r.shouldTriggerEscalation(ctx, multiClusterHub, r.EscalationTracker, config)
        if shouldEscalate {
            r.Log.Info("Triggering escalated cleanup", "reason", reason)
            
            filter := cleanup.NewACMResourceFilter(multiClusterHub)
            resourceCount, _ := filter.CountRemainingResources(ctx, r.Client)
            
            r.triggerEscalation(multiClusterHub, reason, resourceCount)
            // Status update happens in deferred syncHubStatus
        }
    }
    
    // Run finalization logic
    if err := r.finalizeHub(r.Log, multiClusterHub, ocpConsole, stsEnabled); err != nil {
        r.Log.Info(fmt.Sprintf("Finalizing: %s", err.Error()))
        return ctrl.Result{RequeueAfter: resyncPeriod}, nil
    }

    // Remove hubFinalizer after escalation completes
    controllerutil.RemoveFinalizer(multiClusterHub, hubFinalizer)
    
    err := r.Client.Update(context.TODO(), multiClusterHub)
    if err != nil {
        return ctrl.Result{}, err
    }
}
```

---

### Phase 6: Observability

#### 6.1 Prometheus Metrics

**File:** `controllers/metrics_escalation.go` (NEW)

Implements metrics:
- `mch_uninstall_phase{phase="not_required|escalated|completed"}` (gauge)
- `mch_uninstall_escalation_triggered_total` (counter)
- `mch_uninstall_escalation_duration_seconds` (histogram)
- `mch_uninstall_resources_remaining` (gauge)

Exported functions:
- `recordEscalationTriggered()`
- `updateUninstallPhaseMetric(phase string)`
- `updateResourcesRemainingMetric(count int)`

Call from `escalation_state.go` and `escalation_cleanup.go`.

#### 6.2 Status Conditions

**File:** `controllers/status.go`

Add helper function (around line 200):

```go
// updateUninstallProgressingCondition updates the UninstallProgressing condition based on phase
func updateUninstallProgressingCondition(status *operatorv1.MultiClusterHubStatus, phase operatorv1.UninstallPhaseType, reason, message string) {
    var conditionStatus metav1.ConditionStatus
    var conditionMessage string
    
    switch phase {
    case operatorv1.UninstallNotRequired:
        conditionStatus = metav1.ConditionFalse
        conditionMessage = "Normal uninstallation proceeding without issues"
    case operatorv1.UninstallEscalated:
        conditionStatus = metav1.ConditionTrue
        conditionMessage = fmt.Sprintf("Escalation triggered due to %s: %s", reason, message)
    case operatorv1.UninstallCompleted:
        conditionStatus = metav1.ConditionFalse
        conditionMessage = "All ACM resources removed"
    }
    
    condition := NewHubCondition(operatorv1.UninstallProgressing, conditionStatus, reason, conditionMessage)
    SetHubCondition(status, *condition)
}
```

Call from `triggerEscalation()`, `completeEscalation()`, and `initializeUninstallPhase()`.

#### 6.3 Logging

Add structured logging throughout:
- **INFO:** "Monitoring MCH deletion for stuck conditions" (when deletion timestamp first detected)
- **INFO:** Progress updates every 30 seconds with resource counts (only while monitoring)
- **WARN:** "Triggering escalated cleanup" with reason and threshold values
- **INFO:** Each escalation pass start/completion with resource counts
- **WARN:** Finalizer stripping operations (list resources affected)
- **WARN:** Webhook disabling operations
- **ERROR:** Resources that failed to delete even after pass 3
- **INFO:** "Escalated cleanup completed - all ACM resources removed"

All logs include MCH name and namespace.

---

### Phase 7: Configuration & RBAC

#### 7.1 Environment Variables

**File:** `config/manager/manager.yaml`

Add to container env section:

```yaml
env:
  - name: UNINSTALL_TIMEOUT_MINUTES
    value: "15"
  - name: STUCK_FINALIZER_TIMEOUT_MINUTES
    value: "5"
  - name: RESOURCE_PLATEAU_TIMEOUT_MINUTES
    value: "5"
  - name: ESCALATION_ENABLED
    value: "true"
```

#### 7.2 RBAC Permissions

**File:** `controllers/multiclusterhub_controller.go`

Add RBAC markers (around line 72, with existing markers):

```go
//+kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;patch
//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;patch;delete
//+kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;delete
//+kubebuilder:rbac:groups=apiregistration.k8s.io,resources=apiservices,verbs=get;list;delete
```

**Run:**
```bash
make manifests  # Regenerate ClusterRole in config/rbac/
```

Update CSV with warning annotation about force-deletion behavior.

---

### Phase 8: Testing

#### 8.1 Unit Tests

**File:** `controllers/escalation_test.go` (NEW)

Test cases:
1. **TestEscalationNotTriggeredOnNormalDeletion** — verify `UninstallPhase` stays `NotRequired` when cleanup succeeds
2. **TestTimeoutTriggersEscalation** — mock time advancing beyond timeout, verify transition to `Escalated`
3. **TestStuckFinalizerTriggersEscalation** — create resource with persistent finalizer, verify escalation
4. **TestResourcePlateauTriggersEscalation** — mock stuck resources, verify plateau detection
5. **TestFinalizerStrippingOnlyWhenEscalated** — verify finalizers untouched when `NotRequired`
6. **TestMultiPassDeletionRetries** — verify 3-pass logic with increasing aggression
7. **TestStateTransitionEnforcement** — verify no backward transitions (`Escalated` → `NotRequired`)

#### 8.2 Integration Tests

**File:** `test/integration/escalation_integration_test.go` (NEW)

Scenarios:
- Deploy minimal MCH, delete, verify cleanup without escalation
- Deploy MCH, inject stuck finalizer, verify escalation triggers and removes finalizer
- Deploy MCH with webhook, verify escalation disables webhook and completes cleanup

#### 8.3 E2E Tests

**File:** `test/e2e/escalation_e2e_test.go` (NEW)

Scenarios:
- Full ACM deployment with multiple blocking conditions, verify escalation completes within 30 minutes
- Verify escalation metrics in Prometheus
- Verify all ACM namespaces removed, no stuck terminating namespaces

---

## Implementation Sequence

### Step 1: API Foundation
1. Modify `api/v1/multiclusterhub_types.go`
2. Add constants to `controllers/status.go`
3. Run `make generate && make manifests`
4. **Commit:** "Add UninstallEscalation status fields to MCH API"

### Step 2: State Management
1. Create `controllers/escalation_state.go`
2. Modify `controllers/multiclusterhub_controller.go` (add tracker)
3. Modify `controllers/setup.go` (initialize tracker)
4. **Commit:** "Add escalation state tracking and detection"

### Step 3: Force-Cleanup Utilities
1. Create `pkg/cleanup/force_delete.go`
2. Create `pkg/cleanup/resource_discovery.go`
3. Add unit tests for utilities
4. **Commit:** "Add force-cleanup utilities for escalation"

### Step 4: Multi-Pass Deletion
1. Create `controllers/escalation_cleanup.go`
2. Add unit tests for each pass
3. **Commit:** "Implement multi-pass escalated cleanup"

### Step 5: Integration
1. Modify `controllers/lifecycle.go` (integrate into `finalizeHub`)
2. Modify `controllers/reconcile.go` (add escalation check)
3. Add integration tests
4. **Commit:** "Integrate escalation into finalization flow"

### Step 6: Observability
1. Create `controllers/metrics_escalation.go`
2. Update condition logic in `controllers/status.go`
3. Add logging throughout
4. **Commit:** "Add escalation observability (metrics, conditions, logs)"

### Step 7: Configuration & RBAC
1. Add env vars to `config/manager/manager.yaml`
2. Add RBAC markers, run `make manifests`
3. Update CSV
4. **Commit:** "Add escalation configuration and RBAC"

### Step 8: Testing
1. Create `controllers/escalation_test.go`
2. Create integration and E2E tests
3. Verify all tests pass
4. **Commit:** "Add comprehensive escalation tests"

### Step 9: Documentation
1. Update `README.md`
2. Create troubleshooting guide
3. **Commit:** "Document automated cleanup escalation"

---

## Verification Strategy

After implementation, verify the feature works end-to-end:

### 1. Normal Deletion (No Escalation)
```bash
# Deploy minimal MCH
kubectl apply -f examples/minimal-mch.yaml

# Wait for Running status
kubectl get mch -w

# Delete MCH
kubectl delete mch <name>

# Verify escalation NOT triggered
kubectl get mch <name> -o jsonpath='{.status.uninstallPhase}'
# Expected: "" or "NotRequired" (stays dormant)

# Verify complete cleanup
kubectl get mch -A
# Expected: no resources
```

### 2. Stuck Deletion (Escalation Triggered)
```bash
# Deploy MCH
kubectl apply -f examples/minimal-mch.yaml

# Inject stuck finalizer on test resource
kubectl patch deployment -n open-cluster-management <deployment> \
  -p '{"metadata":{"finalizers":["test-stuck-finalizer"]}}'

# Delete MCH
kubectl delete mch <name>

# Wait 15+ minutes or monitor escalation trigger
kubectl get mch <name> -o yaml | grep -A 10 uninstallEscalation

# Verify escalation triggered
# Expected: uninstallPhase: Escalated, triggered: true

# Verify finalizer removed and cleanup completes
kubectl get deployment -n open-cluster-management <deployment>
# Expected: deployment deleted

# Check metrics
kubectl port-forward -n open-cluster-management deployment/multiclusterhub-operator-manager 8383:8383
curl localhost:8383/metrics | grep mch_uninstall
```

### 3. Monitor Logs
```bash
# Watch operator logs for escalation messages
kubectl logs -n open-cluster-management deployment/multiclusterhub-operator-manager -f | grep -i escalation
```

**Expected log sequence:**
1. "Monitoring MCH deletion for stuck conditions"
2. "Triggering escalated cleanup: TimeoutExceeded"
3. "Escalation Pass 1: Disabling webhooks..."
4. "Escalation Pass 2: Retrying stuck resources..."
5. "Escalation Pass 3: Stripping finalizers..."
6. "Escalated cleanup completed - all ACM resources removed"

---

## Critical Files Modified/Created

**Modified:**
- `api/v1/multiclusterhub_types.go` — API changes
- `controllers/lifecycle.go` — integration into `finalizeHub()`
- `controllers/reconcile.go` — escalation trigger check
- `controllers/multiclusterhub_controller.go` — add tracker to reconciler
- `controllers/setup.go` — initialize tracker
- `controllers/status.go` — add constants and condition helper
- `config/manager/manager.yaml` — env vars
- `config/rbac/` — regenerated RBAC (via `make manifests`)

**Created:**
- `controllers/escalation_state.go` — state tracking and detection
- `controllers/escalation_cleanup.go` — multi-pass deletion logic
- `controllers/metrics_escalation.go` — Prometheus metrics
- `pkg/cleanup/force_delete.go` — force-delete utilities
- `pkg/cleanup/resource_discovery.go` — ACM resource identification
- `controllers/escalation_test.go` — unit tests
- `test/integration/escalation_integration_test.go` — integration tests
- `test/e2e/escalation_e2e_test.go` — E2E tests

---

## Design Decisions (Resolved)

1. **Audit trail for force-deleted resources**
   - **Decision:** Option C — Emit Warning events only
   - Provides audit trail without bloating status fields
   - Relies on cluster event retention (typically 1 hour)

2. **Escalation during cluster backup**
   - **Decision:** Add check in `shouldTriggerEscalation()` — skip escalation if `backup.Status.Phase == "Running"`
   - Prevents data loss from interrupted backups
   - Log: "Delaying escalation: cluster backup in progress"

3. **Managed cluster handling during escalation**
   - **Decision:** Option B — Force-delete ManagedCluster CRs without graceful detach
   - Managed clusters will be orphaned (remain registered but disconnected from hub)
   - Document this behavior in troubleshooting guide
   - Future enhancement: Add graceful detach attempt in Pass 1

4. **Resource deletion order**
   - **Decision:** Follow similar order to nuke script, but not exact match
   - Critical first: Webhooks and API services (prevents blocking)
   - Then: ManagedClusters → OLM → Sub-operators → CRD instances → Workloads → RBAC → Namespaces
   - Webhook/API service deletion most critical for unblocking stuck deletions
