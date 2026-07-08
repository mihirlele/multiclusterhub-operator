// Copyright Contributors to the Open Cluster Management project

package cleanup

import (
	"context"
	"strings"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
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

// DiscoverACMNamespaces returns all namespaces that are ACM-related
func (f *ACMResourceFilter) DiscoverACMNamespaces(ctx context.Context, c client.Client) ([]corev1.Namespace, error) {
	nsList := &corev1.NamespaceList{}
	if err := c.List(ctx, nsList); err != nil {
		return nil, err
	}

	var acmNamespaces []corev1.Namespace
	for _, ns := range nsList.Items {
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
