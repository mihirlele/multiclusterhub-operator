// Copyright Contributors to the Open Cluster Management project

package cleanup

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DiscoverACMRolesInNamespace returns ACM Roles and RoleBindings in a specific namespace
func DiscoverACMRolesInNamespace(ctx context.Context, c client.Client, namespace string) ([]client.Object, error) {
	var resources []client.Object

	// Get Roles
	roleList := &rbacv1.RoleList{}
	if err := c.List(ctx, roleList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	for i := range roleList.Items {
		name := roleList.Items[i].Name
		if strings.HasPrefix(name, "open-cluster-management") ||
			strings.HasPrefix(name, "system:") {
			resources = append(resources, &roleList.Items[i])
		}
	}

	// Get RoleBindings
	rbList := &rbacv1.RoleBindingList{}
	if err := c.List(ctx, rbList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	for i := range rbList.Items {
		name := rbList.Items[i].Name
		roleName := rbList.Items[i].RoleRef.Name
		if strings.HasPrefix(name, "open-cluster-management") ||
			strings.HasPrefix(name, "system:") ||
			strings.HasPrefix(roleName, "open-cluster-management") {
			resources = append(resources, &rbList.Items[i])
		}
	}

	return resources, nil
}

// DiscoverLocalClusterNamespace returns the local-cluster namespace if it exists
func DiscoverLocalClusterNamespace(ctx context.Context, c client.Client) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{}
	err := c.Get(ctx, client.ObjectKey{Name: "local-cluster"}, ns)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return ns, nil
}

// DiscoverOLMOperators returns ACM/MCE Operator CRs (operators.operators.coreos.com)
func DiscoverOLMOperators(ctx context.Context, c client.Client) ([]unstructured.Unstructured, error) {
	operatorList := &unstructured.UnstructuredList{}
	operatorList.SetAPIVersion("operators.coreos.com/v1")
	operatorList.SetKind("OperatorList")

	if err := c.List(ctx, operatorList); err != nil {
		// If Operator CRD doesn't exist, return empty list
		if meta.IsNoMatchError(err) {
			return []unstructured.Unstructured{}, nil
		}
		return nil, err
	}

	var acmOperators []unstructured.Unstructured
	for _, op := range operatorList.Items {
		name := op.GetName()
		namespace := op.GetNamespace()

		// Match ACM and MCE operators
		if (name == "advanced-cluster-management" && namespace == "open-cluster-management") ||
			(name == "multicluster-engine" && namespace == "multicluster-engine") {
			acmOperators = append(acmOperators, op)
		}
	}

	return acmOperators, nil
}

// DiscoverHypershiftResources discovers deployments and other resources in hypershift namespace
func DiscoverHypershiftResources(ctx context.Context, c client.Client) ([]unstructured.Unstructured, error) {
	// Get all resources in hypershift namespace with ACM installer labels
	deploymentList := &unstructured.UnstructuredList{}
	deploymentList.SetAPIVersion("apps/v1")
	deploymentList.SetKind("DeploymentList")

	if err := c.List(ctx, deploymentList, client.InNamespace("hypershift")); err != nil {
		if meta.IsNoMatchError(err) {
			return []unstructured.Unstructured{}, nil
		}
		return nil, err
	}

	return deploymentList.Items, nil
}
