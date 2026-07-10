// Copyright Contributors to the Open Cluster Management project

package cleanup

import (
	"context"
	"strings"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ACMNamespacePrefixes are namespace prefixes that identify ACM-related namespaces
var ACMNamespacePrefixes = []string{
	"open-cluster-management",
	"multicluster-engine",
	"hive",
}

// ACMResourceFilter identifies resources owned by ACM for cleanup
type ACMResourceFilter struct {
	InstallerName      string
	InstallerNamespace string
	MCHNamespace       string
}

// NewACMResourceFilter creates a filter from a MultiClusterHub instance
func NewACMResourceFilter(m *operatorv1.MultiClusterHub) *ACMResourceFilter {
	return &ACMResourceFilter{
		InstallerName:      m.GetName(),
		InstallerNamespace: m.GetNamespace(),
		MCHNamespace:       m.GetNamespace(),
	}
}

// InstallerLabels returns the label selector for resources managed by this MCH instance
func (f *ACMResourceFilter) InstallerLabels() client.MatchingLabels {
	return client.MatchingLabels{
		"installer.name":      f.InstallerName,
		"installer.namespace": f.InstallerNamespace,
	}
}

// IsACMNamespace returns true if the namespace name matches an ACM-related prefix
func IsACMNamespace(name string) bool {
	for _, prefix := range ACMNamespacePrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// DiscoverACMNamespaces returns all namespaces that are ACM-related, excluding the operator's own namespace
func (f *ACMResourceFilter) DiscoverACMNamespaces(ctx context.Context, c client.Client) ([]corev1.Namespace, error) {
	nsList := &corev1.NamespaceList{}
	if err := c.List(ctx, nsList); err != nil {
		return nil, err
	}

	var acmNamespaces []corev1.Namespace
	for _, ns := range nsList.Items {
		// Skip the operator's own namespace to prevent self-deletion
		if ns.Name == f.MCHNamespace {
			continue
		}
		if IsACMNamespace(ns.Name) {
			acmNamespaces = append(acmNamespaces, ns)
		}
	}
	return acmNamespaces, nil
}

// CountRemainingResources returns the count of ACM-related resources still present on the cluster
func (f *ACMResourceFilter) CountRemainingResources(ctx context.Context, c client.Client) (int, error) {
	count := 0

	// Count resources with installer labels across all namespaces
	deployList := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployList, f.InstallerLabels()); err == nil {
		count += len(deployList.Items)
	}

	// Count ACM namespaces that still exist
	acmNamespaces, err := f.DiscoverACMNamespaces(ctx, c)
	if err == nil {
		count += len(acmNamespaces)
	}

	return count, nil
}

// GetResourcesWithFinalizers returns unstructured representations of ACM resources
// that have deletion timestamps set but still have finalizers blocking deletion
func (f *ACMResourceFilter) GetResourcesWithFinalizers(ctx context.Context, c client.Client) ([]unstructured.Unstructured, error) {
	var stuckResources []unstructured.Unstructured

	// Check deployments with installer labels that have finalizers
	deployList := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployList, f.InstallerLabels()); err == nil {
		for _, d := range deployList.Items {
			if d.DeletionTimestamp != nil && len(d.GetFinalizers()) > 0 {
				u := unstructured.Unstructured{}
				u.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
				u.SetName(d.Name)
				u.SetNamespace(d.Namespace)
				u.SetFinalizers(d.GetFinalizers())
				stuckResources = append(stuckResources, u)
			}
		}
	}

	// Check ACM namespaces that are stuck in Terminating
	nsList := &corev1.NamespaceList{}
	if err := c.List(ctx, nsList); err == nil {
		for _, ns := range nsList.Items {
			if IsACMNamespace(ns.Name) && ns.Status.Phase == corev1.NamespaceTerminating && len(ns.GetFinalizers()) > 0 {
				u := unstructured.Unstructured{}
				u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Namespace"))
				u.SetName(ns.Name)
				u.SetFinalizers(ns.GetFinalizers())
				stuckResources = append(stuckResources, u)
			}
		}
	}

	return stuckResources, nil
}

// DiscoverLabeledResources returns all resources matching installer labels across workload types
func (f *ACMResourceFilter) DiscoverLabeledResources(ctx context.Context, c client.Client) ([]unstructured.Unstructured, error) {
	var resources []unstructured.Unstructured
	selector := labels.SelectorFromSet(labels.Set{
		"installer.name":      f.InstallerName,
		"installer.namespace": f.InstallerNamespace,
	})
	listOpts := &client.ListOptions{LabelSelector: selector}

	// Deployments
	deployList := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployList, listOpts); err == nil {
		for _, d := range deployList.Items {
			u := unstructured.Unstructured{}
			u.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
			u.SetName(d.Name)
			u.SetNamespace(d.Namespace)
			u.SetUID(d.UID)
			u.SetResourceVersion(d.ResourceVersion)
			u.SetFinalizers(d.GetFinalizers())
			resources = append(resources, u)
		}
	}

	// StatefulSets
	stsList := &appsv1.StatefulSetList{}
	if err := c.List(ctx, stsList, listOpts); err == nil {
		for _, s := range stsList.Items {
			u := unstructured.Unstructured{}
			u.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("StatefulSet"))
			u.SetName(s.Name)
			u.SetNamespace(s.Namespace)
			u.SetUID(s.UID)
			u.SetResourceVersion(s.ResourceVersion)
			u.SetFinalizers(s.GetFinalizers())
			resources = append(resources, u)
		}
	}

	return resources, nil
}

// ACMCRDSuffixes are CRD name suffixes that identify ACM-related CRDs and dependencies
var ACMCRDSuffixes = []string{
	".open-cluster-management.io",
	".cluster.open-cluster-management.io",
	".multicluster.openshift.io",
	".agent-install.openshift.io",
	".hive.openshift.io",
	".hiveinternal.openshift.io",
	".extensions.hive.openshift.io",
	".hypershift.openshift.io",
	".cluster.x-k8s.io",
	".infrastructure.cluster.x-k8s.io",
	".ipam.cluster.x-k8s.io",
	".capi-provider.agent-install.openshift.io",
	".scheduling.hypershift.openshift.io",
	".certificates.hypershift.openshift.io",
	".auditlogpersistence.hypershift.openshift.io",
	".observatorium.io",
	".wgpolicyk8s.io",
	".app.k8s.io",
	".k-orc.cloud",
}

// DiscoverACMCRDs returns all ACM-related CRDs
func DiscoverACMCRDs(ctx context.Context, c client.Client) ([]apiextensionsv1.CustomResourceDefinition, error) {
	crdList := &apiextensionsv1.CustomResourceDefinitionList{}
	if err := c.List(ctx, crdList); err != nil {
		return nil, err
	}

	var acmCRDs []apiextensionsv1.CustomResourceDefinition
	for _, crd := range crdList.Items {
		for _, suffix := range ACMCRDSuffixes {
			if strings.HasSuffix(crd.Name, suffix) {
				acmCRDs = append(acmCRDs, crd)
				break
			}
		}
	}
	return acmCRDs, nil
}

// DiscoverCustomResourcesForCRD returns all instances of a given CRD
func DiscoverCustomResourcesForCRD(ctx context.Context, c client.Client, crd apiextensionsv1.CustomResourceDefinition) ([]unstructured.Unstructured, error) {
	// Get the stored version to query
	var version string
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			version = v.Name
			break
		}
	}
	if version == "" && len(crd.Spec.Versions) > 0 {
		version = crd.Spec.Versions[0].Name
	}

	gvr := schema.GroupVersionResource{
		Group:    crd.Spec.Group,
		Version:  version,
		Resource: crd.Spec.Names.Plural,
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvr.Group,
		Version: gvr.Version,
		Kind:    crd.Spec.Names.ListKind,
	})

	if err := c.List(ctx, list); err != nil {
		// Ignore errors - CRD might be in the process of being deleted
		return nil, nil
	}

	return list.Items, nil
}

// DiscoverACMClusterRoles returns all ACM-related ClusterRoles
func DiscoverACMClusterRoles(ctx context.Context, c client.Client) ([]rbacv1.ClusterRole, error) {
	crList := &rbacv1.ClusterRoleList{}
	if err := c.List(ctx, crList); err != nil {
		return nil, err
	}

	var acmRoles []rbacv1.ClusterRole
	for _, cr := range crList.Items {
		if isACMClusterRole(cr.Name) {
			acmRoles = append(acmRoles, cr)
		}
	}
	return acmRoles, nil
}

// DiscoverACMClusterRoleBindings returns all ACM-related ClusterRoleBindings
func DiscoverACMClusterRoleBindings(ctx context.Context, c client.Client) ([]rbacv1.ClusterRoleBinding, error) {
	crbList := &rbacv1.ClusterRoleBindingList{}
	if err := c.List(ctx, crbList); err != nil {
		return nil, err
	}

	var acmBindings []rbacv1.ClusterRoleBinding
	for _, crb := range crbList.Items {
		if isACMClusterRole(crb.Name) || isACMClusterRole(crb.RoleRef.Name) {
			acmBindings = append(acmBindings, crb)
		}
	}
	return acmBindings, nil
}

// DiscoverACMAPIServices returns all ACM-related APIServices
func DiscoverACMAPIServices(ctx context.Context, c client.Client) ([]apiregistrationv1.APIService, error) {
	apiList := &apiregistrationv1.APIServiceList{}
	if err := c.List(ctx, apiList); err != nil {
		return nil, err
	}

	var acmAPIs []apiregistrationv1.APIService
	for _, api := range apiList.Items {
		for _, suffix := range ACMCRDSuffixes {
			if strings.HasSuffix(api.Spec.Group, suffix) {
				acmAPIs = append(acmAPIs, api)
				break
			}
		}
	}
	return acmAPIs, nil
}

// DiscoverACMWebhooks returns all ACM-related ValidatingWebhookConfigurations and MutatingWebhookConfigurations
func DiscoverACMWebhooks(ctx context.Context, c client.Client) ([]client.Object, error) {
	var webhooks []client.Object

	// Validating webhooks
	vwhList := &admissionv1.ValidatingWebhookConfigurationList{}
	if err := c.List(ctx, vwhList); err == nil {
		for i := range vwhList.Items {
			if isACMWebhook(vwhList.Items[i].Name) {
				webhooks = append(webhooks, &vwhList.Items[i])
			}
		}
	}

	// Mutating webhooks
	mwhList := &admissionv1.MutatingWebhookConfigurationList{}
	if err := c.List(ctx, mwhList); err == nil {
		for i := range mwhList.Items {
			if isACMWebhook(mwhList.Items[i].Name) {
				webhooks = append(webhooks, &mwhList.Items[i])
			}
		}
	}

	return webhooks, nil
}

// isACMClusterRole returns true if the ClusterRole name is ACM-related
func isACMClusterRole(name string) bool {
	acmPrefixes := []string{
		"open-cluster-management",
		"multiclusterhubs.operator.open-cluster-management.io",
		"multiclusterengines.multicluster.openshift.io",
		"multicluster-engine:",
		"olm.og.open-cluster-management",
		"system:open-cluster-management",
		"hypershift",
		"server-foundation",
		"klusterlet",
		"global-search-",
		"access-to-brokers-",
		"thanos-ruler-",
	}
	for _, prefix := range acmPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// isACMWebhook returns true if the webhook name is ACM-related
func isACMWebhook(name string) bool {
	acmKeywords := []string{
		"open-cluster-management",
		"multicluster",
		"ocm-webhook",
		"managedcluster",
		"klusterlet",
		"hive",
		"hypershift",
		"application-webhook",
		"channels.apps",
	}
	nameLower := strings.ToLower(name)
	for _, keyword := range acmKeywords {
		if strings.Contains(nameLower, keyword) {
			return true
		}
	}
	return false
}
