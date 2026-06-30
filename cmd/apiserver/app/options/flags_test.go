/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package options

import (
	"testing"

	"github.com/spf13/pflag"
)

// TestStorageBackendFlagRoutesToEtcdConfig guards the backend-selection
// flag wiring. The backend selection comes from upstream EtcdOptions's
// --storage-backend flag, which binds directly to s.Etcd.StorageConfig.Type.
// Options.Complete reads that field to decide which backend to register
// with the fork factory registry.
//
// A previous version of kplane's Flags() registered a SECOND --storage-backend
// pflag pointing at a separate kplane-side field. pflag's NamedFlagSets
// merge silently collapses duplicate registrations, but which destination
// pointer "wins" depends on the order flag-sets are added — so half the
// time the upstream field stayed empty after parse, Complete skipped
// Register, and upstream Validate then rejected the backend as unknown.
//
// This test asserts that after parsing --storage-backend=<name>, the value
// lands in s.Etcd.StorageConfig.Type (the field Complete actually reads).
// Re-introducing a duplicate registration anywhere in Flags() fails the
// test because the new flag's destination wins parsing instead.
func TestStorageBackendFlagRoutesToEtcdConfig(t *testing.T) {
	s := NewServerRunOptions()

	fss := s.Flags()
	merged := pflag.NewFlagSet("merged", pflag.ContinueOnError)
	for _, fs := range fss.FlagSets {
		merged.AddFlagSet(fs)
	}

	if err := merged.Parse([]string{"--storage-backend=spanner"}); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if s.Etcd == nil {
		t.Fatal("s.Etcd is nil after NewServerRunOptions")
	}
	if got := s.Etcd.StorageConfig.Type; got != "spanner" {
		t.Fatalf("after --storage-backend=spanner, s.Etcd.StorageConfig.Type=%q; want \"spanner\"", got)
	}
}
