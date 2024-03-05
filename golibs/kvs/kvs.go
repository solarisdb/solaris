// Copyright 2023 The acquirecloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*
kvs package contains interfaces and structures for working with a key-value storage.
The kvs.Storage can be implemented as a distributed consistent storage like etcd,
Zookeeper etc. For stand-alone or test environments some light-weight implementations
like local file-storage or in-memory implementations could be used.
*/

package kvs

import (
	"context"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"time"
)

type (

	// A record that can be stored in a storage
	Record struct {
		// Key is a key for the record
		Key string
		// Value is a value for the record
		Value []byte

		// A version that identifies the record. It is managed by the Storage, and
		// it is ignored in Create and update operations
		Version string

		// ExpiresAt indicate the record expiration time. If it is not provided
		// the record doesn't have the expiration time
		ExpiresAt *time.Time
	}

	// Storage interface defines some operations over the record storage.
	// The record storage allows to keep key-value pairs, and supports a set
	// of operations that allow to implement some distributed (if supported) primitives
	Storage interface {
		// Create adds a new record into the storage. It returns existing record with
		// ErrExist error if it already exists in the storage.
		// Create returns version of the new record with error=nil
		Create(ctx context.Context, record Record) (string, error)

		// Get retrieves the record by its key. ErrNotExist is returned if the key
		// is not found in the storage
		Get(ctx context.Context, key string) (Record, error)

		// GetMany retrieves many records at a time. It will return only the records it
		// finds, and skip that one, which doesn't exist
		GetMany(ctx context.Context, keys ...string) ([]*Record, error)

		// Put replaces the record if it exists and write the new one if it doesn't
		// The record version will be updated automatically
		Put(ctx context.Context, record Record) (Record, error)

		// PutMany allows to update multiple records in one call
		PutMany(ctx context.Context, records []Record) error

		// CasByVersion compares-and-sets the record Value if the record stored
		// version is same as in the provided record. The record version will be updated,
		// and it be returned as first parameter in the result.
		//
		// The error will contain the reason if the operation was not successful, or
		// the new version will be returned otherwise
		//   ErrConflict - indicates that the version is different than one is expected
		//   ErrNotExist - indicates that the record does not exist
		CasByVersion(ctx context.Context, record Record) (Record, error)

		// Delete removes the record from the storage by its key. It returns
		// an error if the operation was not successful:
		//   ErrNotExist - indicates that the record does not exist
		Delete(ctx context.Context, key string) error

		// WaitForVersionChange blocks the call until the ctx is closed or the key's version
		// becomes different from the ver.
		//
		// The function may return the following results:
		// nil: the key exists, and it's version is different from the ver
		// ctx.Err(): if the context is closed
		// ErrNotExist: the key is not found or was deleted being in the function
		WaitForVersionChange(ctx context.Context, key, ver string) error

		// ListKeys allows to read the keys by the pattern provided. The pattern is a glob-alike
		// matcher (not a regexp). For the pattern matching please refer to the
		// Glob library doc https://github.com/gobwas/glob
		ListKeys(ctx context.Context, pattern string) (iterable.Iterator[string], error)
	}
)

// Copy returns copy of the record r
func (r Record) Copy() Record {
	var res Record
	res.Key = r.Key

	if r.Value != nil {
		res.Value = make([]byte, len(r.Value))
		copy(res.Value, r.Value)
	}
	res.Version = r.Version
	if r.ExpiresAt != nil {
		res.ExpiresAt = cast.Ptr(*r.ExpiresAt)
	}
	return res
}
