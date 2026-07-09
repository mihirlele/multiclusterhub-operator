// Copyright Contributors to the Open Cluster Management project

package controllers

import (
	"context"
	"fmt"
	"time"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	"github.com/stolostron/multiclusterhub-operator/pkg/cleanup"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// executeEscalatedCleanup runs aggressive multi-pass deletion
func (r *MultiClusterHubReconciler) executeEscalatedCleanup(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
) (ctrl.Result, error) {
	currentPass := 1
	if m.Status.UninstallEscalation != nil && m.Status.UninstallEscalation.CurrentPass > 0 {
		currentPass = m.Status.UninstallEscalation.CurrentPass
	}

	filter := cleanup.NewACMResourceFilter(m)

	switch {
	case currentPass <= 1:
		return r.escalationPass1(ctx, m, filter)
	case currentPass == 2:
		return r.escalationPass2(ctx, m, filter)
	case currentPass == 3:
		return r.escalationPass3(ctx, m, filter)
	default:
		return r.completeEscalation(ctx, m)
	}
}

// escalationPass1: Disable webhooks, delete API services, initiate background deletion
func (r *MultiClusterHubReconciler) escalationPass1(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
	filter *cleanup.ACMResourceFilter,
) (ctrl.Result, error) {
	r.Log.Info("Escalation Pass 1: Disabling webhooks and initiating background deletion",
		"mch", m.Name, "namespace", m.Namespace)

	// Disable webhooks on ACM CRDs
	if err := r.disableACMWebhooks(ctx); err != nil {
		r.Log.Info("Error disabling webhooks during escalation pass 1", "error", err)
	}

	// Delete validating and mutating webhook configurations
	if err := r.deleteACMWebhookConfigurations(ctx, filter); err != nil {
		r.Log.Info("Error deleting webhook configurations during escalation pass 1", "error", err)
	}

	// Delete API services
	if err := r.deleteACMAPIServices(ctx, filter); err != nil {
		r.Log.Info("Error deleting API services during escalation pass 1", "error", err)
	}

	// Initiate background deletion of labeled resources
	resources, err := filter.DiscoverLabeledResources(ctx, r.Client)
	if err != nil {
		r.Log.Info("Error discovering resources during escalation pass 1", "error", err)
	}
	for i := range resources {
		if delErr := r.Client.Delete(ctx, &resources[i]); delErr != nil && !errors.IsNotFound(delErr) {
			r.Log.Info("Pass 1 delete failed, will retry in pass 2",
				"kind", resources[i].GetKind(), "name", resources[i].GetName())
		}
	}

	resourceCount, _ := filter.CountRemainingResources(ctx, r.Client)
	r.updateEscalationProgress(m, resourceCount, 2)

	r.Log.Info("Escalation Pass 1 complete, advancing to Pass 2",
		"resourcesTargeted", len(resources), "resourcesRemaining", resourceCount)
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// escalationPass2: Retry stuck resources with grace period override
func (r *MultiClusterHubReconciler) escalationPass2(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
	filter *cleanup.ACMResourceFilter,
) (ctrl.Result, error) {
	r.Log.Info("Escalation Pass 2: Retrying stuck resources with grace period override",
		"mch", m.Name, "namespace", m.Namespace)

	resources, err := filter.DiscoverLabeledResources(ctx, r.Client)
	if err != nil {
		r.Log.Info("Error discovering resources during escalation pass 2", "error", err)
	}

	for i := range resources {
		if delErr := cleanup.ForceDeleteWithGracePeriod(ctx, r.Client, &resources[i], 0); delErr != nil && !errors.IsNotFound(delErr) {
			r.Log.Info("Pass 2 delete failed, will strip finalizers in pass 3",
				"kind", resources[i].GetKind(), "name", resources[i].GetName())
		}
	}

	resourceCount, _ := filter.CountRemainingResources(ctx, r.Client)
	r.updateEscalationProgress(m, resourceCount, 3)

	r.Log.Info("Escalation Pass 2 complete, advancing to Pass 3",
		"resourcesRemaining", resourceCount)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// escalationPass3: Strip finalizers and force-delete everything
func (r *MultiClusterHubReconciler) escalationPass3(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
	filter *cleanup.ACMResourceFilter,
) (ctrl.Result, error) {
	r.Log.Info("Escalation Pass 3: Stripping finalizers and force-deleting all resources",
		"mch", m.Name, "namespace", m.Namespace)

	resources, err := filter.DiscoverLabeledResources(ctx, r.Client)
	if err != nil {
		r.Log.Info("Error discovering resources during escalation pass 3", "error", err)
	}

	for i := range resources {
		if err := cleanup.StripFinalizersFromUnstructured(ctx, r.Client, &resources[i]); err != nil && !errors.IsNotFound(err) {
			r.Log.Error(err, "Failed to strip finalizers in pass 3",
				"kind", resources[i].GetKind(), "name", resources[i].GetName())
		}
		if err := r.Client.Delete(ctx, &resources[i]); err != nil && !errors.IsNotFound(err) {
			r.Log.Error(err, "Pass 3 delete failed",
				"kind", resources[i].GetKind(), "name", resources[i].GetName())
		}
	}

	// Force-delete stuck namespaces
	if err := r.forceDeleteACMNamespaces(ctx, filter); err != nil {
		r.Log.Error(err, "Failed to force-delete ACM namespaces in pass 3")
	}

	resourceCount, _ := filter.CountRemainingResources(ctx, r.Client)
	r.updateEscalationProgress(m, resourceCount, 4)

	if resourceCount > 0 {
		r.Log.Error(fmt.Errorf("resources remaining after pass 3"),
			"Some resources could not be force-deleted", "count", resourceCount)
	}

	r.Log.Info("Escalation Pass 3 complete", "resourcesRemaining", resourceCount)
	return r.completeEscalation(ctx, m)
}

// completeEscalation marks escalation as done
func (r *MultiClusterHubReconciler) completeEscalation(
	ctx context.Context,
	m *operatorv1.MultiClusterHub,
) (ctrl.Result, error) {
	r.Log.Info("Escalated cleanup completed - all ACM resources removed",
		"mch", m.Name, "namespace", m.Namespace)

	m.Status.UninstallEscalationPhase = operatorv1.UninstallCompleted

	updateUninstallProgressingCondition(&m.Status, operatorv1.UninstallCompleted,
		EscalationCompletedReason, "All ACM resources removed")
	updateUninstallPhaseMetric(string(operatorv1.UninstallCompleted))

	if m.Status.UninstallEscalation != nil && m.Status.UninstallEscalation.TriggeredTime != nil {
		duration := time.Since(m.Status.UninstallEscalation.TriggeredTime.Time).Seconds()
		recordEscalationDuration(duration)
	}

	return ctrl.Result{}, nil
}

// disableACMWebhooks patches ACM CRDs to remove conversion webhooks
func (r *MultiClusterHubReconciler) disableACMWebhooks(ctx context.Context) error {
	crdList := &apiextensionsv1.CustomResourceDefinitionList{}
	if err := r.Client.List(ctx, crdList); err != nil {
		return err
	}

	for _, crd := range crdList.Items {
		if isACMCRD(crd.Name) && crd.Spec.Conversion != nil && crd.Spec.Conversion.Strategy == apiextensionsv1.WebhookConverter {
			if err := cleanup.DisableWebhookOnCRD(ctx, r.Client, crd.Name); err != nil {
				r.Log.Info("Failed to disable webhook on CRD", "crd", crd.Name, "error", err)
			}
		}
	}
	return nil
}

// deleteACMWebhookConfigurations removes validating and mutating webhook configurations
func (r *MultiClusterHubReconciler) deleteACMWebhookConfigurations(ctx context.Context, filter *cleanup.ACMResourceFilter) error {
	// Validating webhooks
	vwList := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, vwList, filter.InstallerLabels()); err == nil {
		for i := range vwList.Items {
			r.Log.Info("Deleting ValidatingWebhookConfiguration", "name", vwList.Items[i].Name)
			if err := r.Client.Delete(ctx, &vwList.Items[i]); err != nil && !errors.IsNotFound(err) {
				r.Log.Info("Failed to delete ValidatingWebhookConfiguration", "name", vwList.Items[i].Name, "error", err)
			}
		}
	}

	// Mutating webhooks
	mwList := &admissionregistrationv1.MutatingWebhookConfigurationList{}
	if err := r.Client.List(ctx, mwList, filter.InstallerLabels()); err == nil {
		for i := range mwList.Items {
			r.Log.Info("Deleting MutatingWebhookConfiguration", "name", mwList.Items[i].Name)
			if err := r.Client.Delete(ctx, &mwList.Items[i]); err != nil && !errors.IsNotFound(err) {
				r.Log.Info("Failed to delete MutatingWebhookConfiguration", "name", mwList.Items[i].Name, "error", err)
			}
		}
	}

	return nil
}

// deleteACMAPIServices removes ACM-related API services
func (r *MultiClusterHubReconciler) deleteACMAPIServices(ctx context.Context, filter *cleanup.ACMResourceFilter) error {
	apiServiceList := &apiregistrationv1.APIServiceList{}
	if err := r.Client.List(ctx, apiServiceList, filter.InstallerLabels()); err != nil {
		return err
	}

	for i := range apiServiceList.Items {
		r.Log.Info("Deleting APIService", "name", apiServiceList.Items[i].Name)
		if err := r.Client.Delete(ctx, &apiServiceList.Items[i]); err != nil && !errors.IsNotFound(err) {
			r.Log.Info("Failed to delete APIService", "name", apiServiceList.Items[i].Name, "error", err)
		}
	}
	return nil
}

// forceDeleteACMNamespaces force-deletes all ACM-related namespaces
func (r *MultiClusterHubReconciler) forceDeleteACMNamespaces(ctx context.Context, filter *cleanup.ACMResourceFilter) error {
	acmNamespaces, err := filter.DiscoverACMNamespaces(ctx, r.Client)
	if err != nil {
		return err
	}

	for _, ns := range acmNamespaces {
		if err := cleanup.ForceDeleteNamespace(ctx, r.Client, ns.Name); err != nil {
			r.Log.Info("Failed to force-delete namespace", "namespace", ns.Name, "error", err)
		}
	}
	return nil
}

// isACMCRD returns true if the CRD name belongs to an ACM-related API group
func isACMCRD(name string) bool {
	acmSuffixes := []string{
		".open-cluster-management.io",
		".multicluster.openshift.io",
	}
	for _, suffix := range acmSuffixes {
		if len(name) > len(suffix) && name[len(name)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

// Emit a Kubernetes warning event for audit trail
func (r *MultiClusterHubReconciler) emitEscalationEvent(m *operatorv1.MultiClusterHub, reason, message string) {
	ref := &corev1.ObjectReference{
		Kind:       "MultiClusterHub",
		APIVersion: operatorv1.GroupVersion.String(),
		Name:       m.Name,
		Namespace:  m.Namespace,
		UID:        m.UID,
	}

	now := metav1.Now()
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.escalation.%s", m.Name, now.Format("20060102150405")),
			Namespace: m.Namespace,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		Type:           "Warning",
		FirstTimestamp: now,
		LastTimestamp:  now,
	}

	if err := r.Client.Create(context.TODO(), event); err != nil {
		r.Log.Info("Failed to emit escalation event", "error", err)
	}
}
