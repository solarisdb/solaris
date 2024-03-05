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

import (
	"time"
)

type (
	// ExpirableCache - wrapper around cache that checks if value reached expires at timestamp
	// and re-adds it to the cache by calling the createNewF
	ExpirableCache[K comparable, V CacheItem] struct {
		*Cache[K, V]
	}

	// ExpirableItem - a helper struct for expirable cache.
	// Allows to store any value in cache with a expires at timestamp.
	ExpirableItem[V any] struct {
		Value     V
		ExpiresAt time.Time
	}

	// CacheItem - interface for ExpirableItem that needs to be implemented
	// in order to work with ExpirableCache cache
	CacheItem interface {
		GetValue() any
		GetExpiresAt() time.Time
	}
)

func NewExpirableCache[K comparable, V CacheItem](maxSize int, createNewF CreatePoolElemF[K, V], onDeleteF OnDeleteElemF[K, V]) (*ExpirableCache[K, V], error) {
	c, err := NewCache[K, V](maxSize, createNewF, onDeleteF)
	if err != nil {
		return nil, err
	}
	ec := new(ExpirableCache[K, V])
	ec.Cache = c
	return ec, nil
}

func (p *ExpirableCache[K, V]) GetOrCreate(k K) (V, error) {
	now := time.Now()

	// get value from cache
	v, err := p.Cache.GetOrCreate(k)
	if err != nil {
		return v, err
	}

	// if expires at reached
	if v.GetExpiresAt().Before(now) {
		// remove from cache
		p.Remove(k)
		// call get or create again to add new version of the item
		return p.Cache.GetOrCreate(k)
	}

	return v, nil
}

func NewCacheItem[V any](value V, expiresAt time.Time) ExpirableItem[V] {
	return ExpirableItem[V]{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func (i ExpirableItem[V]) GetValue() any {
	return i.Value
}

func (i ExpirableItem[V]) GetExpiresAt() time.Time {
	return i.ExpiresAt
}
