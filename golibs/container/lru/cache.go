// Copyright 2023 The acquirecloud Authors
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
package lru

// Cache implements container with limited size capacity and LRU (Least Recently Used) pull out discipline.
// The elements can be created automatically if they are not found in the pool via the createNewF function call,
// which is provided via the Cache creation (see NewCache)
type Cache[K comparable, V any] struct {
	*ECache[K, K, V]
}

func directT[T any](v T) T { return v }

// NewCache creates new pool object. It expects the maximum pull size (maxSize) and the create new
// element function in the parameters
func NewCache[K comparable, V any](maxSize int, createNewF CreatePoolElemF[K, V], onDeleteF OnDeleteElemF[K, V]) (*Cache[K, V], error) {
	eCache, err := NewECache(maxSize, directT[K], createNewF, onDeleteF)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{eCache}, nil
}
