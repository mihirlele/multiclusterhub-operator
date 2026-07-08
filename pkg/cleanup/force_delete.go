// Copyright Contributors to the Open Cluster Management project

package cleanup

import (
	"context"
	"encoding/json"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

var log = logf.Log.WithName("cleanup")

// StripFinalizers removes all finalizers from a typed resource
func StripFinalizers(ctx context.Context, c client.Client, obj client.Object) error {
	if len(obj.GetFinalizers()) == 0 {
		return nil
	}
	log.Info("Stripping finalizers",
		"kind", obj.GetObjectKind().GroupVersionKind().Kind,
		"name", obj.GetName(),
		"namespace", obj.GetNamespace(),
		"finalizers", obj.GetFinalizers())
	obj.SetFinalizers([]string{})
	return c.Update(ctx, obj)
}

// StripFinalizersFromUnstructured removes all finalizers from an unstructured resource
func StripFinalizersFromUnstructured(ctx context.Context, c client.Client, u *unstructured.Unstructured) error {
	if len(u.GetFinalizers()) == 0 {
		return nil
	}
	log.Info("Stripping finalizers",
		"kind", u.GetKind(),
		"name", u.GetName(),
		"namespace", u.GetNamespace(),
		"finalizers", u.GetFinalizers())
	u.SetFinalizers([]string{})
	return c.Update(ctx, u)
}

// ForceDeleteWithGracePeriod deletes a resource with a specific grace period override
func ForceDeleteWithGracePeriod(ctx context.Context, c client.Client, obj client.Object, gracePeriod int64) error {
	return c.Delete(ctx, obj, &client.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
}

// DisableWebhookOnCRD patches a CRD to remove conversion webhook configuration
func DisableWebhookOnCRD(ctx context.Context, c client.Client, crdName string) error {
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := c.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if crd.Spec.Conversion == nil || crd.Spec.Conversion.Strategy != apiextensionsv1.WebhookConverter {
		return nil
	}

	log.Info("Disabling conversion webhook on CRD", "crd", crdName)

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"conversion": map[string]interface{}{
				"strategy": "None",
				"webhook":  nil,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	return c.Patch(ctx, crd, client.RawPatch(types.MergePatchType, patchBytes))
}

// ForceDeleteNamespace strips namespace finalizers and deletes the namespace
func ForceDeleteNamespace(ctx context.Context, c client.Client, namespaceName string) error {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, types.NamespacedName{Name: namespaceName}, ns); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if len(ns.GetFinalizers()) > 0 {
		log.Info("Stripping finalizers from namespace", "namespace", namespaceName,
			"finalizers", ns.GetFinalizers())
		ns.SetFinalizers([]string{})
		if err := c.Update(ctx, ns); err != nil {
			return err
		}
	}

	if ns.Status.Phase == corev1.NamespaceTerminating {
		return nil
	}

	log.Info("Force-deleting namespace", "namespace", namespaceName)
	return client.IgnoreNotFound(c.Delete(ctx, ns))
}
