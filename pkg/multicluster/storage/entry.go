package storage

import (
	extstorage "github.com/kplane-dev/storage"
)

// InternalEntry is a type alias for the canonical ObjectWithClusterIdentity
// from the kplane-dev/storage module. It carries cluster identity metadata
// for storage/cache paths and is never serialized to API clients.
//
// The alias preserves backward compatibility: existing type assertions like
// *mcstorage.InternalEntry continue to work since Go aliases are identical types.
type InternalEntry = extstorage.ObjectWithClusterIdentity
