// Copyright 2024 The Solaris Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
sss package stays for Simple Storage Service which provides an interface to the cloud services like AWS S3
*/
package sss

import (
	"io"
	"strings"
)

// Storage interface provides access to the Key-Value Simple service.
// "Simple" means consistency model like "eventual" or stronger one.
// The purpose of the key-value storage is persisting values (may be BLOBs),
// which could be found by the keys.
//
// The key represents a file-system alike path and ID to the value. The following
// conventions are applied:
//   - any key starts from '/' which is called delimiter
//   - a key cannot end on '/' delimiter
//   - the prefix of key, which starts from '/' and ends on '/' called path
//   - any key consists of 2 parts <path><valId>, where valId is a value
//     identifier within the path. valId cannot contain delimiters
//
// Examples:
// "/abc" - the key with path="/" and valId="abc"
// "/abc/def/ms.js" - the key with path="/abc/def/" and valId="ms.js"
// "abc.js", "", "/", "/abc/" - are not keys
//
// A Value is an object (may be big one), the limitations can be applied by
// the implementation
type Storage interface {
	// Get allows to read a value by its key. If key is not found the
	// ErrNotFound should be returned
	Get(key string) (io.ReadCloser, error)

	// Put allows to store value represented by reader r by the key
	Put(key string, r io.Reader) error

	// List returns a list of keys and sub-paths (part of an existing path which
	// is a path itself), which have the prefix of the path argument
	//
	// Example:
	// for the keys list: "/abc", "/def/abc", "/def/aa1"
	// List("/") -> "/abc", "/def/"
	// List("/def/") -> "/def/abc", "/def/aa1"
	List(path string) ([]string, error)

	// Delete allows to delete a value by key. If the key doesn't exist, the operation
	// will return no error
	Delete(key string) error
}

// IsKeyValid checks whether the key is valid: <path><keysuffix> where the keysuffix is a string without '/'
func IsKeyValid(key string) bool {
	idx := strings.LastIndex(key, "/")
	if idx == -1 {
		return false
	}

	if strings.Trim(key[idx+1:], " ") == "" {
		return false
	}

	return IsPathValid(key[:idx+1])
}

// IsPathValid checks whether the path is valid: it should start and end on '/'
func IsPathValid(path string) bool {
	if path == "" {
		return false
	}

	strs := strings.Split(path, "/")
	if strs[0] != "" || strs[len(strs)-1] != "" {
		return false
	}

	for i := 1; i < len(strs)-1; i++ {
		if strs[i] == "" {
			return false
		}
	}

	return true
}
