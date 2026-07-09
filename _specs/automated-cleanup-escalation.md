# Feature Spec: Automated Cleanup Escalation

**Feature Title**: Automated Cleanup Escalation  
**Feature Slug**: `automated-cleanup-escalation`  
**Branch**: `claude/feature/automated-cleanup-escalation`  
**RHACM Target**: TBD  
**Minimum OpenShift Version**: TBD  
**Status**: Draft

---

## Overview

When a user deletes a MultiClusterHub CR to uninstall ACM, the operator's existing finalizer logic performs an orderly sequential uninstallation of dependent operators (MCE, cluster-manager, etc.) and their resources. This feature **does not interfere with normal deletion**. Instead, it remains dormant (`uninstallPhase: "NotRequired"`) and only activates when the normal uninstallation process becomes stuck due to finalizers, webhook dependencies, or orphaned resources.

When a stuck deletion is detected (via timeout, stuck finalizers, or resource count plateau), the escalation mechanism automatically triggers an aggressive force-cleanup strategy modeled after the existing `nuke-acm-mce.sh` script from rhacm/support-tools. This eliminates the need for manual intervention with external cleanup scripts when normal deletion fails to complete.

---

## RHACM Context

**Component Owner**: MultiClusterHub Operator (hub cluster)

**Target RHACM Release**: TBD

**Minimum OpenShift Version**: TBD (must support webhook conversion CRD patching)

**Related Components**:
- MultiClusterEngine (MCE) operator
- Cluster Manager operator
- All ACM sub-operators (console, grc, search, observability, cluster-backup, etc.)

This feature is scoped to the hub cluster only and handles cleanup of hub-side resources. Managed cluster cleanup is out of scope for this initial implementation.

---

## Kubernetes / OpenShift API

### Existing CRDs

- `MultiClusterHub` (operator.open-cluster-management.io/v1) — the primary CR whose deletion triggers uninstallation
- `MultiClusterEngine` (multicluster.openshift.io/v1) — MCE operator instance
- `ClusterManager` (operator.open-cluster-management.io/v1) — cluster-manager operator instance
- All ACM component CRDs (ManagedCluster, Policy, PlacementRule, ManifestWork, etc.)

### New CRD Fields

Add to `MultiClusterHub.status`:

```yaml
status:
  uninstallPhase: string  # "NotRequired" | "Escalated" | "Completed"
  uninstallEscalation:
    triggered: bool
    triggeredTime: metav1.Time
    reason: string        # Why escalation was triggered (TimeoutExceeded | StuckFinalizer | WebhookFailure | ResourcePlateau)
    resourcesRemaining: int
    lastAttempt: metav1.Time
```

**State meanings**:
- `NotRequired`: Normal deletion is proceeding without issues; escalation is dormant
- `Escalated`: Stuck deletion detected; aggressive cleanup activated
- `Completed`: All ACM resources removed; MCH CR ready for final removal

### New or Modified RBAC Rules

The operator will need additional cluster-scoped permissions to perform aggressive cleanup:

- **CustomResourceDefinitions**: `get`, `list`, `patch`, `delete` (to remove conversion webhooks and finalizers)
- **MutatingWebhookConfigurations**: `get`, `list`, `delete`
- **ValidatingWebhookConfigurations**: `get`, `list`, `delete`
- **APIServices**: `get`, `list`, `delete`
- **Subscriptions** (operators.coreos.com): `get`, `list`, `delete`
- **ClusterServiceVersions**: `get`, `list`, `delete`
- **Namespaces**: `get`, `list`, `delete`, `patch` (to remove finalizers on stuck namespaces)
- **All workload types**: `deployments`, `statefulsets`, `daemonsets`, `jobs`, `pods` (across all namespaces)
- **PrometheusRules**, **ServiceMonitors**: `get`, `list`, `delete` (in openshift-monitoring namespace)
- **ConsolePlugin**: `get`, `list`, `delete`

These permissions extend beyond normal operator RBAC and should be documented as potentially disruptive.

---

## Controller / Reconciler Design

### Normal Deletion Flow (No Escalation)

When a MultiClusterHub CR is deleted:

1. **Existing operator finalizer logic runs** (unchanged by this feature)
   - Delete dependent operators (MCE, cluster-manager) via their CRs
   - Wait for operators to complete their finalizers
   - Delete sub-component resources in dependency order
   - Remove RBAC, webhooks, and API services
   - Clean up namespaces

2. **Escalation controller remains dormant**
   - `status.uninstallPhase` stays `"NotRequired"`
   - No interference with normal deletion
   - Only monitors for stuck conditions

### Escalation Detection (When Normal Deletion Gets Stuck)

The controller monitors deletion progress and triggers escalation if:

- **Time-based threshold**: Deletion has been in progress for > N minutes (configurable, default 15 minutes)
- **Stuck finalizer detection**: Same finalizer has blocked deletion for > M minutes (configurable, default 5 minutes)
- **Webhook failure pattern**: Repeated webhook timeout errors in logs
- **Resource count plateau**: Number of remaining ACM resources hasn't decreased in > K minutes (configurable, default 5 minutes)

### Escalated Cleanup Phase

When escalation is triggered:

1. Update `status.uninstallPhase = "Escalated"`
2. Set `status.uninstallEscalation.triggered = true` with timestamp and reason
3. Switch to aggressive cleanup mode (modeled after nuke-acm-mce.sh):
   - **Finalizer stripping**: Remove `metadata.finalizers` from all ACM-related resources
   - **Webhook disabling**: Patch CRDs to remove conversion webhooks
   - **Multi-pass deletion**: 
     - Pass 1: Delete with `--wait=false` (background cleanup)
     - Pass 2 (after 10s): Retry stuck resources with `deletionGracePeriodSeconds=0`
     - Pass 3 (after 30s): Force-remove by patching finalizers and re-deleting
   - **Order of operations**:
     1. Disable webhooks and API services first
     2. Remove managed clusters and their namespaces
     3. Delete top-level operators (subscriptions, CSVs)
     4. Delete sub-operator instances
     5. Delete CRD instances (with finalizer removal)
     6. Delete workloads (deployments → replicasets → pods)
     7. Delete RBAC resources
     8. Delete monitoring resources (PrometheusRules, ServiceMonitors)
     9. Delete console plugins
     10. Force-delete namespaces with finalizer removal

### Reconcile Loop Behavior

- **Watch**: MultiClusterHub resources with deletion timestamp set
- **Predicate**: Only reconcile MCH instances that are being deleted
- **Requeue Strategy**: 
  - NotRequired phase (monitoring): requeue after 30s to check for stuck conditions
  - Escalated phase: requeue after 10s for aggressive cleanup passes
  - On error: exponential backoff with max 2-minute delay

### State Transitions

```
NotRequired (monitoring normal deletion) → Escalated (on stuck detection) → Completed (all resources removed)
```

**Important**: 
- The controller **never** sets `uninstallPhase` on a healthy deletion - it stays `NotRequired`
- Transition to `Escalated` only occurs when stuck conditions are detected
- No backward transitions allowed (cannot go from `Escalated` back to `NotRequired`)

---

## Operator Lifecycle

### Deployment Changes

No changes to operator deployment structure required. The escalation logic runs within the existing MultiClusterHub controller.

### CSV (ClusterServiceVersion) Updates

The CSV must be updated to include:

1. **New RBAC permissions** listed in the API section above
2. **Description update** noting the aggressive cleanup capability
3. **Warning annotation** about force-deletion behavior during escalation

### Configuration

Add environment variables to the operator deployment:

- `UNINSTALL_TIMEOUT_MINUTES`: Time before escalation (default: 15)
- `STUCK_FINALIZER_TIMEOUT_MINUTES`: Time a single finalizer can block (default: 5)
- `RESOURCE_PLATEAU_TIMEOUT_MINUTES`: Time without progress before escalation (default: 5)
- `ESCALATION_ENABLED`: Boolean flag to disable escalation if needed (default: true)

These should be configurable via the operator's ConfigMap or environment variables.

---

## Observability

### Metrics (Prometheus)

Expose the following metrics:

- `mch_uninstall_phase{phase="not_required|escalated|completed"}` (gauge, 0 or 1)
- `mch_uninstall_escalation_triggered_total` (counter)
- `mch_uninstall_escalation_duration_seconds` (histogram)
- `mch_uninstall_resources_remaining` (gauge)
- `mch_uninstall_finalizer_stuck_duration_seconds{resource_type, finalizer}` (gauge)

### Status Conditions

Add new condition type to `status.conditions`:

- **Type**: `UninstallProgressing`
  - **Status**: `True` when escalated cleanup is active, `False` otherwise
  - **Reason**: `NotRequired` (normal deletion proceeding) | `EscalatedCleanup` (stuck, force-cleanup active) | `Completed` (all resources removed)
  - **Message**: Descriptive text about escalation state
    - When `NotRequired`: "Normal uninstallation proceeding without issues"
    - When `EscalatedCleanup`: "Escalation triggered due to <reason>: <details>"
    - When `Completed`: "All ACM resources removed"

### Logging

Log at key lifecycle events:

- **INFO**: "Monitoring MCH deletion for stuck conditions" (when deletion timestamp detected)
- **INFO**: Progress updates every 30 seconds with resource counts (only while monitoring)
- **WARN**: "Escalation triggered: <reason>" with detailed threshold values and stuck resources
- **INFO**: Each escalation pass (Pass 1, 2, 3) with resources targeted
- **WARN**: Finalizer stripping operations (list resources and finalizers removed)
- **WARN**: Webhook disabling operations
- **ERROR**: Resources that failed to delete even after 3 passes
- **INFO**: "Escalated cleanup completed - all ACM resources removed"

All logs should include the MCH name and namespace for correlation.

**Note**: If deletion completes normally without escalation, only the initial monitoring message should appear in logs.

---

## Error Handling & Requeue Strategy

### Expected Error Cases

1. **Webhook timeout during deletion**: 
   - Log warning, proceed to webhook disabling in escalation
   - Do not requeue immediately; wait for next reconcile cycle

2. **Finalizer blocking deletion beyond threshold**:
   - Trigger escalation if still in `NotRequired` phase
   - Strip finalizer immediately once in `Escalated` phase
   - Requeue after 10s for retry

3. **CRD in terminating state**:
   - Patch CRD to remove conversion webhook
   - Remove finalizers on remaining instances
   - Requeue after 10s

4. **Namespace stuck in terminating**:
   - Identify remaining resources in namespace
   - Force-delete resources with `deletionGracePeriodSeconds=0`
   - Strip namespace finalizers if resources are gone
   - Requeue after 30s

5. **RBAC permission denied during cleanup**:
   - Log error with details about missing permission
   - Continue with other cleanup operations (don't fail entire reconcile)
   - Report in status condition

### Requeue Durations

- **NotRequired phase (monitoring)**: 30 seconds (checking for stuck conditions)
- **Escalated phase, pass 1**: immediate (trigger background deletions)
- **Escalated phase, pass 2**: 10 seconds after pass 1
- **Escalated phase, pass 3**: 30 seconds after pass 2
- **On transient errors**: exponential backoff (10s, 30s, 1m, 2m)

### Backoff Behavior

Use standard controller-runtime exponential backoff with:
- Initial delay: 10 seconds
- Max delay: 2 minutes
- Reset backoff after successful progress (resource count decreases)

---

## Testing Requirements

### Unit Tests (envtest / fake client)

1. **Normal deletion completes without escalation**:
   - Create MCH, delete it, mock successful deletion of all resources
   - Assert `uninstallPhase` remains `"NotRequired"` throughout
   - Assert `status.uninstallEscalation.triggered = false`
   - Verify controller only monitors, never interferes

2. **Timeout triggers escalation**:
   - Create MCH with deletion timestamp
   - Mock clock to advance time beyond timeout threshold while resources remain
   - Verify `uninstallPhase` transitions `NotRequired` → `Escalated`
   - Assert `status.uninstallEscalation.reason = "TimeoutExceeded"`

3. **Stuck finalizer triggers escalation**:
   - Create MCH with ACM resource stuck on persistent finalizer
   - Advance time beyond stuck finalizer timeout
   - Verify `uninstallPhase = "Escalated"`
   - Assert `status.uninstallEscalation.reason = "StuckFinalizer"`
   - Verify finalizer is removed in escalated phase

4. **Resource count plateau triggers escalation**:
   - Create MCH with 10 ACM resources
   - Mock 5 resources deleting successfully, 5 stuck
   - Advance time showing no progress for plateau timeout period
   - Verify `uninstallPhase = "Escalated"`
   - Assert `status.uninstallEscalation.reason = "ResourcePlateau"`

5. **Finalizer stripping only happens when escalated**:
   - Create resources with various finalizer patterns
   - While `uninstallPhase = "NotRequired"`, verify no finalizers are touched
   - After transition to `"Escalated"`, verify finalizers are stripped
   - Assert resources are subsequently deleted

6. **Webhook disabling only happens when escalated**:
   - Mock CRD with conversion webhook
   - While `NotRequired`, verify CRD is not patched
   - After escalation, verify CRD webhook is removed

7. **Multi-pass deletion retries**:
   - Mock resources that fail first deletion attempt
   - Verify 3-pass retry logic with increasing timeout/grace periods
   - Assert resources are force-deleted by pass 3

8. **State transition enforcement**:
   - Verify controller cannot transition `Escalated` → `NotRequired` (backward)
   - Verify only valid transitions: `NotRequired` → `Escalated` → `Completed`

### Integration Tests (kind / local cluster)

1. **End-to-end orderly uninstall**:
   - Deploy minimal ACM (MCH + MCE)
   - Delete MCH CR
   - Verify complete cleanup without escalation

2. **Stuck finalizer scenario**:
   - Deploy ACM, inject stuck finalizer on test resource
   - Delete MCH CR
   - Verify escalation triggers and removes finalizer
   - Verify complete cleanup

3. **Webhook blocking deletion**:
   - Deploy ACM with webhook configuration
   - Introduce webhook failure condition
   - Delete MCH CR
   - Verify escalation disables webhooks and completes cleanup

### E2E Tests (RHACM test suite)

1. **Full ACM uninstall with escalation**:
   - Deploy full ACM with all components enabled
   - Inject multiple blocking conditions (stuck finalizers, webhooks)
   - Delete MCH CR
   - Verify escalation triggers
   - Verify all ACM resources are removed within expected time (< 30 minutes)
   - Verify no ACM-related resources remain on cluster

2. **Escalation metrics validation**:
   - Deploy Prometheus
   - Trigger escalation scenario
   - Query metrics and verify correct values for escalation events, duration, resource counts

3. **Namespace cleanup verification**:
   - Deploy ACM with managed clusters (creates cluster namespaces)
   - Delete MCH CR
   - Verify all ACM-related namespaces are removed
   - Verify no stuck terminating namespaces

---

## Out of Scope

The following are explicitly excluded from this feature:

1. **Managed cluster cleanup**: Escalation only handles hub cluster resources. Detachment of managed clusters during escalation is undefined and may require manual intervention.

2. **Selective escalation**: Users cannot choose which resources to escalate cleanup for; escalation is all-or-nothing once triggered.

3. **Rollback from escalation**: Once escalation is triggered, there is no mechanism to abort and return to orderly uninstall.

4. **Custom escalation triggers**: Users cannot define custom conditions for escalation beyond the built-in timeout/stuck-finalizer detection.

5. **Preservation of user data**: Escalated cleanup is destructive and makes no attempt to preserve user configurations, policies, or other data. Backup/restore is a separate concern.

6. **Cross-cluster coordination**: This feature does not coordinate with MCE or other operators to synchronize escalation timing; each operator handles its own escalation independently.

7. **UI integration**: No changes to the ACM web console to display escalation status; users must inspect the MCH CR status fields via CLI.

8. **Partial uninstallation**: The feature does not support uninstalling only certain ACM components while preserving others.

---

## Open Questions

1. **Should escalation be opt-in or opt-out?**
   - Proposal: Opt-out via `ESCALATION_ENABLED=false` environment variable
   - Rationale: Most users want stuck deletions to resolve automatically; power users can disable if needed
   - **Decision needed**: Product team input required

2. **What should happen to managed clusters during escalation?**
   - Options:
     a. Attempt graceful detach before aggressive hub cleanup
     b. Leave managed clusters in place (orphaned)
     c. Force-delete ManagedCluster CRs without detach
   - **Decision needed**: Clarify expected behavior with product team

3. **Should the operator emit Kubernetes events during escalation?**
   - Proposal: Yes, emit Warning events for escalation trigger and each aggressive cleanup action
   - **Decision needed**: Confirm with observability requirements

4. **Timeout values: should they be user-configurable via MCH CR or only via operator env vars?**
   - Proposal: Start with operator env vars only (simpler); add CR fields if user demand exists
   - **Decision needed**: Product team preference

5. **Should we preserve logs/events from escalation for post-mortem analysis?**
   - Options:
     a. Write escalation report to ConfigMap before deleting namespace
     b. Emit all details via Events (relies on cluster event retention)
     c. Only log to operator stdout (ephemeral)
   - **Decision needed**: How long should evidence of escalation be retained?

6. **Integration with cluster-backup operator**:
   - Should escalation block if cluster-backup is in progress?
   - Should we snapshot ACM state before escalation?
   - **Decision needed**: Coordinate with backup/restore feature owners

7. **What minimum RBAC should ACM admins have to view escalation status?**
   - Proposal: `get` permission on MultiClusterHub CR is sufficient to view status
   - Question: Should there be a separate role for "uninstall observer"?
   - **Decision needed**: RBAC model review

8. **Should escalation be gated by a webhook admission control?**
   - Use case: Prevent accidental escalation by requiring admin annotation or confirmation
   - Trade-off: Adds complexity but prevents runaway cleanup
   - **Decision needed**: Risk tolerance vs. automation level

9. **How should the controller detect "normal deletion is stuck" vs "normal deletion is just slow"?**
   - Current proposal uses multiple signals (timeout, stuck finalizer, resource plateau)
   - Question: Are the default thresholds (15min timeout, 5min stuck finalizer, 5min plateau) appropriate for production?
   - Should thresholds be different for different cluster sizes or component counts?
   - **Decision needed**: Threshold tuning based on real-world deletion timings

10. **Should the escalation controller run in the same reconcile loop as the main MCH controller, or as a separate controller?**
   - Option A: Same controller with conditional logic (simpler, shares context)
   - Option B: Separate escalation controller (cleaner separation, independent requeue)
   - **Decision needed**: Architecture preference

11. **What happens if a user manually fixes the stuck condition while escalation is running?**
   - Example: Admin manually removes stuck finalizer during escalation Pass 1
   - Should escalation continue to completion, or detect the fix and back off?
   - Proposal: Continue escalation once triggered (simpler, more predictable)
   - **Decision needed**: Confirm escalation is "point of no return"

12. **When exactly should the controller start monitoring for stuck conditions?**
   - Should it start monitoring as soon as the MCH deletion timestamp is set?
   - Or should it wait for the existing finalizer to start its work first (e.g., wait 1 minute before starting to monitor)?
   - Proposal: Start monitoring immediately when deletion timestamp appears, so we can detect issues early
   - **Decision needed**: Confirm monitoring start timing

13. **Should escalation clear the MCH finalizer, or leave finalizer removal to existing controller logic?**
   - Option A: Escalation removes the finalizer as its final step (MCH CR disappears immediately after cleanup)
   - Option B: Escalation leaves the finalizer, existing controller logic removes it after escalation completes
   - Proposal: Option A - escalation should remove the finalizer, ensuring MCH CR is deleted once all ACM resources are gone
   - **Decision needed**: Clarify finalizer ownership during escalation

14. **How should we identify "ACM-related resources" for cleanup?**
   - By namespace patterns (e.g., `open-cluster-management*`, `multicluster-engine*`)?
   - By label selectors (e.g., resources with `app.kubernetes.io/part-of: multiclusterhub`)?
   - By hardcoded list of CRD types and namespaces (similar to nuke script)?
   - Proposal: Combination approach - start with known namespaces/labels, then expand to include resources that might be missed by labels (similar to the nuke script's comprehensive approach)
   - **Decision needed**: Define resource identification strategy

15. **Should we maintain a list of resources that were force-deleted during escalation?**
   - Use case: Debugging and audit trails for post-mortem analysis
   - Option A: Store in `status.uninstallEscalation.forcedDeletions: []string` (resource names/types)
   - Option B: Emit as Kubernetes Warning Events only (relies on cluster event retention)
   - Option C: Both - store summary count in status, emit detailed events
   - Proposal: Option C for comprehensive tracking
   - **Decision needed**: Audit trail requirements

16. **What should `status.uninstallEscalation.resourcesRemaining` count?**
   - Option A: Only CR instances (ManagedCluster, Policy, PlacementRule, etc.)?
   - Option B: All ACM-owned resources (including Deployments, Services, ConfigMaps, etc.)?
   - Option C: Only resources blocking deletion (with finalizers or in terminating state)?
   - Proposal: Option B - count all ACM-owned resources to show full cleanup progress
   - Trade-off: Higher counts but more accurate representation of cleanup completeness
   - **Decision needed**: Define resource counting scope

---

## References

- [nuke-acm-mce.sh script](https://github.com/rhacm/support-tools/blob/main/installer/cleanup-tools/nuker/nuke-acm-mce.sh) (source of aggressive cleanup logic)
- [MultiClusterHub CRD](../config/crd/bases/operator.open-cluster-management.io_multiclusterhubs.yaml)
- [MultiClusterHub Controller](../controllers/multiclusterhub_controller.go)

---

## Implementation Checklist

- [ ] Add `uninstallPhase` and `uninstallEscalation` fields to MCH status
- [ ] Implement escalation detection logic (timeout, stuck finalizer, resource plateau)
- [ ] Implement finalizer stripping in escalated phase
- [ ] Implement webhook disabling (CRD patching)
- [ ] Implement multi-pass deletion retries
- [ ] Add escalation metrics to operator
- [ ] Update operator RBAC in CSV
- [ ] Add configuration environment variables
- [ ] Write unit tests for escalation triggers
- [ ] Write unit tests for finalizer stripping
- [ ] Write integration tests for stuck scenarios
- [ ] Write E2E test for full escalation flow
- [ ] Document escalation behavior in operator README
- [ ] Add troubleshooting guide for escalation scenarios
