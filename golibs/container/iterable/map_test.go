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

func TestNewIterableMap(t *testing.T) {
	im := NewMap[string, string]()
	it := im.Iterator()
	assert.False(t, it.HasNext())
	e, ok := it.Next()
	assert.False(t, ok)
	assert.Equal(t, "", e.Key)
	assert.Equal(t, "", e.Value)
}

func TestIterator(t *testing.T) {
	im := NewMap[int, int]()
	for i := 0; i < 100; i++ {
		im.Add(i, i+1)
	}
	k := 0
	it := im.Iterator()
	for it.HasNext() {
		e, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, k, e.Key)
		assert.Equal(t, k+1, e.Value)
		k++
	}
	assert.Equal(t, 100, k)
}

func TestIteratorDeleted(t *testing.T) {
	im := NewMap[int, int]()
	for i := 0; i < 100; i++ {
		im.Add(i, i+1)
	}
	for i := 1; i < 100; i += 2 {
		im.Remove(i)
	}
	k := 0
	it := im.Iterator()
	for it.HasNext() {
		e, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, k, e.Key)
		assert.Equal(t, k+1, e.Value)
		k += 2
	}
	assert.Equal(t, 100, k)
}

func TestRefCounter(t *testing.T) {
	im := NewMap[int, int]()
	im.Add(0, 1)
	it := im.Iterator().(*mapIterator[int, int])
	assert.Equal(t, im.head, it.ptr)
	assert.Equal(t, rlOk, im.head.state)
	im.Remove(0)
	assert.Equal(t, rlDeleted, im.head.state)
	assert.False(t, it.HasNext())
	assert.Equal(t, rlLast, im.head.state)
	assert.Equal(t, im.head, it.ptr)
	assert.Equal(t, im.last, it.ptr)

	im.Add(10, 20)
	assert.True(t, it.HasNext())
	e, ok := it.Next()
	assert.True(t, ok)
	assert.Equal(t, 10, e.Key)
	assert.Equal(t, 20, e.Value)

	assert.Equal(t, 1, im.last.refCnt)
	assert.Equal(t, 0, im.head.refCnt)
	it.Close()
	assert.Nil(t, it.ptr)
	assert.Equal(t, 0, im.last.refCnt)

	im = NewMap[int, int]()
	im.Add(0, 1)
	im.Add(1, 2)
	it = im.Iterator().(*mapIterator[int, int])
	assert.True(t, it.HasNext())
	im.Remove(0)
	e, ok = it.Next()
	assert.True(t, ok)
	assert.Equal(t, 1, e.Key)
	assert.Equal(t, 2, e.Value)
	assert.Equal(t, 1, im.last.refCnt)
	assert.Equal(t, 0, im.head.refCnt)
	assert.Equal(t, rlOk, im.head.state)
}

func TestGet(t *testing.T) {
	im := NewMap[int, int]()
	im.Add(0, 1)
	_, ok := im.Get(2)
	assert.False(t, ok)
	k, ok := im.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 1, k)
}

func TestLen(t *testing.T) {
	im := NewMap[int, int]()
	assert.Equal(t, 0, im.Len())
	it := im.Iterator()
	im.Add(1, 1)
	assert.Equal(t, 1, im.Len())
	assert.True(t, it.HasNext())
	im.Remove(1)
	assert.Equal(t, 0, im.Len())
	e, ok := it.Next()
	assert.False(t, ok)
	assert.Equal(t, 0, e.Key)
	assert.Equal(t, 0, e.Value)
}

func TestIterableMap_First(t *testing.T) {
	im := NewMap[int, int]()
	k, ok := im.First()
	assert.Equal(t, 0, k)
	assert.False(t, ok)

	im.Add(1, 1)
	k, ok = im.First()
	assert.Equal(t, 1, k)
	assert.True(t, ok)

	im.Add(2, 3)
	k, ok = im.First()
	assert.Equal(t, 1, k)
	assert.True(t, ok)

	im.Remove(1)
	k, ok = im.First()
	assert.Equal(t, 2, k)
	assert.True(t, ok)

	im.Remove(2)
	k, ok = im.First()
	assert.Equal(t, 0, k)
	assert.False(t, ok)
}
