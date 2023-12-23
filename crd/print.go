package crd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/acorn-io/schemer/data/convert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/yaml"
)

var (
	cleanPrefix = []string{
		"kubectl.kubernetes.io/",
		"apply.acorn.io/",
	}
)

func WriteFile(filename string, scheme *runtime.Scheme, crds []CRD) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return Print(f, scheme, crds)
}

func Print(out io.Writer, scheme *runtime.Scheme, crds []CRD) error {
	obj, err := Objects(crds)
	if err != nil {
		return err
	}

	data, err := export(scheme, obj...)
	if err != nil {
		return err
	}

	_, err = out.Write(data)
	return err
}

func Objects(crds []CRD) (result []runtime.Object, err error) {
	for _, crdDef := range crds {
		if crdDef.Override == nil {
			crd, err := crdDef.ToCustomResourceDefinition()
			if err != nil {
				return nil, err
			}
			result = append(result, crd)
		} else {
			result = append(result, crdDef.Override)
		}
	}
	return
}

func Create(ctx context.Context, cfg *rest.Config, scheme *runtime.Scheme, apply ApplyFunc, crds []CRD) error {
	factory, err := NewFactoryFromClient(cfg, scheme, apply)
	if err != nil {
		return err
	}

	return factory.BatchCreateCRDs(ctx, crds...).BatchWait()
}

// export will attempt to clean up the objects a bit before
// rendering to yaml so that they can easily be imported into another
// cluster
func export(scheme *runtime.Scheme, objects ...runtime.Object) ([]byte, error) {
	if len(objects) == 0 {
		return nil, nil
	}

	buffer := &bytes.Buffer{}
	for i, obj := range objects {
		if i > 0 {
			buffer.WriteString("\n---\n")
		}

		obj, err := cleanObjectForExport(scheme, obj)
		if err != nil {
			return nil, err
		}

		bytes, err := yaml.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("failed to encode %s: %w", obj.GetObjectKind().GroupVersionKind(), err)
		}
		buffer.Write(bytes)
	}

	return buffer.Bytes(), nil
}

func cleanObjectForExport(scheme *runtime.Scheme, obj runtime.Object) (runtime.Object, error) {
	obj = obj.DeepCopyObject()
	if obj.GetObjectKind().GroupVersionKind().Kind == "" {
		if gvk, err := apiutil.GVKForObject(obj, scheme); err == nil {
			obj.GetObjectKind().SetGroupVersionKind(gvk)
		} else if err != nil {
			return nil, fmt.Errorf("kind and/or apiVersion is not set on input object: %v: %w", obj, err)
		}
	}

	data, err := convert.EncodeToMap(obj)
	if err != nil {
		return nil, err
	}

	unstr := &unstructured.Unstructured{
		Object: data,
	}

	metadata := map[string]interface{}{}

	if name := unstr.GetName(); len(name) > 0 {
		metadata["name"] = name
	} else if generated := unstr.GetGenerateName(); len(generated) > 0 {
		metadata["generateName"] = generated
	} else {
		return nil, fmt.Errorf("either name or generateName must be set on obj: %v", obj)
	}

	if unstr.GetNamespace() != "" {
		metadata["namespace"] = unstr.GetNamespace()
	}
	if annotations := unstr.GetAnnotations(); len(annotations) > 0 {
		cleanMap(annotations)
		if len(annotations) > 0 {
			metadata["annotations"] = annotations
		} else {
			delete(metadata, "annotations")
		}
	}
	if labels := unstr.GetLabels(); len(labels) > 0 {
		cleanMap(labels)
		if len(labels) > 0 {
			metadata["labels"] = labels
		} else {
			delete(metadata, "labels")
		}
	}

	if spec, ok := data["spec"]; ok {
		if spec == nil {
			delete(data, "spec")
		} else if m, ok := spec.(map[string]interface{}); ok && len(m) == 0 {
			delete(data, "spec")
		}
	}

	data["metadata"] = metadata
	delete(data, "status")

	return unstr, nil
}

func cleanMap(annoLabels map[string]string) {
	for k := range annoLabels {
		for _, prefix := range cleanPrefix {
			if strings.HasPrefix(k, prefix) {
				delete(annoLabels, k)
			}
		}
	}
}
