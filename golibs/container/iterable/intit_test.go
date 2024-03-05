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
package iterable

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWrapIntSlice(t *testing.T) {
	it := WrapIntSlice(nil)
	assert.False(t, it.HasNext())
	i, v := it.Next()
	assert.Equal(t, 0, i)
	assert.False(t, v)

	it = WrapIntSlice([]int{})
	assert.False(t, it.HasNext())
	i, v = it.Next()
	assert.Equal(t, 0, i)
	assert.False(t, v)
}

func TestIntIterator_HasNext(t *testing.T) {
	i := []int{22, 33, 123, 55, 3234, 22}
	it := WrapIntSlice(i)
	assert.True(t, it.HasNext())
	for _, v := range i {
		assert.True(t, it.HasNext())
		v1, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, v, v1)
	}
}
