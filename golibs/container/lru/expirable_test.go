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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newItem(v string) (ExpirableItem[string], error) {
	now := time.Now()
	return NewCacheItem[string](v, now.Add(time.Second)), nil
}

func TestNewExpirableCache_GetOrCreate(t *testing.T) {
	cache, err := NewExpirableCache[string, ExpirableItem[string]](
		1,
		newItem,
		nil,
	)
	assert.NoError(t, err)

	// get or create
	v, err := cache.GetOrCreate("a")
	// check that ttl of one second is not reached
	assert.True(t, v.ExpiresAt.After(time.Now()))

	// sleep for 1 second
	time.Sleep(time.Second)

	// get or create
	v, err = cache.GetOrCreate("a")
	// check that ttl of one second is not reached (item was re-added)
	assert.True(t, v.ExpiresAt.After(time.Now()))
}
