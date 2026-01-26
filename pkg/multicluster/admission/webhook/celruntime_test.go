package webhook

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/environment"
)

func TestCelRuntime_BaseCompilerCache(t *testing.T) {
	rt := NewCelRuntime()
	c1, err := rt.BaseCompiler()
	if err != nil {
		t.Fatalf("BaseCompiler: %v", err)
	}
	c2, err := rt.BaseCompiler()
	if err != nil {
		t.Fatalf("BaseCompiler (second): %v", err)
	}
	if c1 != c2 {
		t.Fatalf("expected BaseCompiler to be cached")
	}
	if sz := rt.CacheSize(); sz != 1 {
		t.Fatalf("expected cache size 1, got %d", sz)
	}
}

func TestCelRuntime_CompilerForKey(t *testing.T) {
	rt := NewCelRuntime()
	if _, err := rt.BaseCompiler(); err != nil {
		t.Fatalf("BaseCompiler: %v", err)
	}
	key := EnvKey{
		ClusterID: "c1",
		GVK:       schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"},
		EnvType:   environment.NewExpressions,
	}
	c1, err := rt.CompilerFor(key, nil)
	if err != nil {
		t.Fatalf("CompilerFor: %v", err)
	}
	c2, err := rt.CompilerFor(key, nil)
	if err != nil {
		t.Fatalf("CompilerFor (second): %v", err)
	}
	if c1 != c2 {
		t.Fatalf("expected CompilerFor to be cached for the same key")
	}
	if sz := rt.CacheSize(); sz != 2 { // base + key
		t.Fatalf("expected cache size 2, got %d", sz)
	}
}
