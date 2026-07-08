// Copyright Contributors to the Open Cluster Management project

package cleanup

import (
	"context"
	"testing"

	operatorv1 "github.com/stolostron/multiclusterhub-operator/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = apiextensionsv1.AddToScheme(s)
	return s
}

func TestIsACMNamespace(t *testing.T) {
	tests := []struct {
		name string
		ns   string
		want bool
	}{
		{"open-cluster-management", "open-cluster-management", true},
		{"open-cluster-management-addon", "open-cluster-management-addon", true},
		{"multicluster-engine", "multicluster-engine", true},
		{"hive", "hive", true},
		{"hive-backup", "hive-backup", true},
		{"default", "default", false},
		{"kube-system", "kube-system", false},
		{"openshift-monitoring", "openshift-monitoring", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsACMNamespace(tt.ns); got != tt.want {
				t.Errorf("IsACMNamespace(%q) = %v, want %v", tt.ns, got, tt.want)
			}
		})
	}
}

func TestNewACMResourceFilter(t *testing.T) {
	mch := &operatorv1.MultiClusterHub{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multiclusterhub",
			Namespace: "open-cluster-management",
		},
	}
	filter := NewACMResourceFilter(mch)
	if filter.InstallerName != "multiclusterhub" {
		t.Errorf("InstallerName = %v, want multiclusterhub", filter.InstallerName)
	}
	if filter.InstallerNamespace != "open-cluster-management" {
		t.Errorf("InstallerNamespace = %v, want open-cluster-management", filter.InstallerNamespace)
	}
}

func TestInstallerLabels(t *testing.T) {
	filter := &ACMResourceFilter{
		InstallerName:      "mch",
		InstallerNamespace: "ocm",
	}
	labels := filter.InstallerLabels()
	if labels["installer.name"] != "mch" {
		t.Errorf("installer.name = %v, want mch", labels["installer.name"])
	}
	if labels["installer.namespace"] != "ocm" {
		t.Errorf("installer.namespace = %v, want ocm", labels["installer.namespace"])
	}
}

func TestDiscoverACMNamespaces(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "open-cluster-management"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "multicluster-engine"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "hive"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
	).Build()

	filter := &ACMResourceFilter{InstallerName: "mch", InstallerNamespace: "ocm"}
	namespaces, err := filter.DiscoverACMNamespaces(ctx, c)
	if err != nil {
		t.Fatalf("DiscoverACMNamespaces() error = %v", err)
	}
	if len(namespaces) != 3 {
		t.Errorf("DiscoverACMNamespaces() returned %d namespaces, want 3", len(namespaces))
	}
}

func TestCountRemainingResources(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "open-cluster-management"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "deploy1",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "mch", "installer.namespace": "ocm"},
			},
		},
	).Build()

	filter := &ACMResourceFilter{InstallerName: "mch", InstallerNamespace: "ocm"}
	count, err := filter.CountRemainingResources(ctx, c)
	if err != nil {
		t.Fatalf("CountRemainingResources() error = %v", err)
	}
	// 1 deployment + 1 ACM namespace
	if count != 2 {
		t.Errorf("CountRemainingResources() = %d, want 2", count)
	}
}

func TestGetResourcesWithFinalizers(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "open-cluster-management",
				Finalizers: []string{"kubernetes"},
			},
			Status: corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "healthy-deploy",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "mch", "installer.namespace": "ocm"},
			},
		},
	).Build()

	filter := &ACMResourceFilter{InstallerName: "mch", InstallerNamespace: "ocm"}
	stuck, err := filter.GetResourcesWithFinalizers(ctx, c)
	if err != nil {
		t.Fatalf("GetResourcesWithFinalizers() error = %v", err)
	}
	if len(stuck) != 1 {
		t.Errorf("GetResourcesWithFinalizers() returned %d stuck resources, want 1 (terminating namespace)", len(stuck))
	}
	if len(stuck) > 0 && stuck[0].GetKind() != "Namespace" {
		t.Errorf("stuck resource kind = %v, want Namespace", stuck[0].GetKind())
	}
}

func TestDiscoverLabeledResources(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "deploy1",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "mch", "installer.namespace": "ocm"},
			},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sts1",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "mch", "installer.namespace": "ocm"},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "other-deploy",
				Namespace: "ocm",
				Labels:    map[string]string{"installer.name": "other"},
			},
		},
	).Build()

	filter := &ACMResourceFilter{InstallerName: "mch", InstallerNamespace: "ocm"}
	resources, err := filter.DiscoverLabeledResources(ctx, c)
	if err != nil {
		t.Fatalf("DiscoverLabeledResources() error = %v", err)
	}
	if len(resources) != 2 {
		t.Errorf("DiscoverLabeledResources() returned %d resources, want 2", len(resources))
	}
	for _, r := range resources {
		if r.GetResourceVersion() == "" {
			t.Errorf("DiscoverLabeledResources() resource %s missing ResourceVersion", r.GetName())
		}
	}
}

func TestStripFinalizers(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-ns",
			Finalizers: []string{"kubernetes", "test-finalizer"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	if err := StripFinalizers(ctx, c, ns); err != nil {
		t.Fatalf("StripFinalizers() error = %v", err)
	}

	updated := &corev1.Namespace{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-ns"}, updated); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if len(updated.GetFinalizers()) != 0 {
		t.Errorf("StripFinalizers() finalizers = %v, want empty", updated.GetFinalizers())
	}
}

func TestStripFinalizers_NoOp(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ns"},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	if err := StripFinalizers(ctx, c, ns); err != nil {
		t.Fatalf("StripFinalizers() error on no finalizers = %v", err)
	}
}

func TestStripFinalizersFromUnstructured(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-ns",
			Finalizers: []string{"blocking-finalizer"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Namespace"))
	u.SetName("test-ns")
	u.SetFinalizers([]string{"blocking-finalizer"})
	// Need ResourceVersion from the created object
	got := &corev1.Namespace{}
	_ = c.Get(ctx, types.NamespacedName{Name: "test-ns"}, got)
	u.SetResourceVersion(got.ResourceVersion)

	if err := StripFinalizersFromUnstructured(ctx, c, u); err != nil {
		t.Fatalf("StripFinalizersFromUnstructured() error = %v", err)
	}

	updated := &corev1.Namespace{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test-ns"}, updated); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if len(updated.GetFinalizers()) != 0 {
		t.Errorf("StripFinalizersFromUnstructured() finalizers = %v, want empty", updated.GetFinalizers())
	}
}

func TestForceDeleteWithGracePeriod(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ns"},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	if err := ForceDeleteWithGracePeriod(ctx, c, ns, 0); err != nil {
		t.Fatalf("ForceDeleteWithGracePeriod() error = %v", err)
	}

	got := &corev1.Namespace{}
	err := c.Get(ctx, types.NamespacedName{Name: "test-ns"}, got)
	if err == nil {
		t.Errorf("ForceDeleteWithGracePeriod() object still exists after delete")
	}
}

func TestDisableWebhookOnCRD(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "test.open-cluster-management.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "open-cluster-management.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:   "Test",
				Plural: "tests",
			},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
				}},
			},
			Conversion: &apiextensionsv1.CustomResourceConversion{
				Strategy: apiextensionsv1.WebhookConverter,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(crd).Build()

	if err := DisableWebhookOnCRD(ctx, c, "test.open-cluster-management.io"); err != nil {
		t.Fatalf("DisableWebhookOnCRD() error = %v", err)
	}

	updated := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(ctx, types.NamespacedName{Name: "test.open-cluster-management.io"}, updated); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if updated.Spec.Conversion.Strategy != apiextensionsv1.NoneConverter {
		t.Errorf("DisableWebhookOnCRD() strategy = %v, want None", updated.Spec.Conversion.Strategy)
	}
}

func TestDisableWebhookOnCRD_NotFound(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()
	c := fake.NewClientBuilder().WithScheme(s).Build()

	if err := DisableWebhookOnCRD(ctx, c, "nonexistent.crd"); err != nil {
		t.Errorf("DisableWebhookOnCRD() should return nil for not found, got %v", err)
	}
}

func TestDisableWebhookOnCRD_NoWebhook(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "test.crd"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "test",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: "Test", Plural: "tests"},
			Scope: apiextensionsv1.ClusterScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{Name: "v1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
				}},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(crd).Build()

	if err := DisableWebhookOnCRD(ctx, c, "test.crd"); err != nil {
		t.Errorf("DisableWebhookOnCRD() should be no-op without webhook, got %v", err)
	}
}

func TestForceDeleteNamespace(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "acm-ns",
			Finalizers: []string{"kubernetes"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	if err := ForceDeleteNamespace(ctx, c, "acm-ns"); err != nil {
		t.Fatalf("ForceDeleteNamespace() error = %v", err)
	}

	got := &corev1.Namespace{}
	err := c.Get(ctx, types.NamespacedName{Name: "acm-ns"}, got)
	if err == nil {
		t.Errorf("ForceDeleteNamespace() namespace still exists")
	}
}

func TestForceDeleteNamespace_NotFound(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()
	c := fake.NewClientBuilder().WithScheme(s).Build()

	if err := ForceDeleteNamespace(ctx, c, "nonexistent"); err != nil {
		t.Errorf("ForceDeleteNamespace() should return nil for not found, got %v", err)
	}
}

func TestForceDeleteNamespace_AlreadyTerminating(t *testing.T) {
	s := newScheme()
	ctx := context.TODO()

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "term-ns"},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(ns).Build()

	if err := ForceDeleteNamespace(ctx, c, "term-ns"); err != nil {
		t.Errorf("ForceDeleteNamespace() should skip delete for terminating ns, got %v", err)
	}

	got := &corev1.Namespace{}
	if err := c.Get(ctx, types.NamespacedName{Name: "term-ns"}, got); err != nil {
		t.Errorf("ForceDeleteNamespace() should not delete terminating namespace")
	}
}
