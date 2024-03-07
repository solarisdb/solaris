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

func TestSimpleMix(t *testing.T) {
	testSimpleMix(t, WrapIntSlice(nil), WrapIntSlice(nil), []int{})
	testSimpleMix(t, WrapIntSlice([]int{1, 2, 3}), WrapIntSlice(nil), []int{1, 2, 3})
	testSimpleMix(t, WrapIntSlice([]int{}), WrapIntSlice([]int{1, 2, 3}), []int{1, 2, 3})
	testSimpleMix(t, WrapIntSlice([]int{3}), WrapIntSlice([]int{1, 2}), []int{1, 2, 3})
	testSimpleMix(t, WrapIntSlice([]int{3, 4}), WrapIntSlice([]int{1, 2}), []int{1, 2, 3, 4})
	testSimpleMix(t, WrapIntSlice([]int{1, 2}), WrapIntSlice([]int{3, 4}), []int{1, 2, 3, 4})
}

func TestMixer_Reset(t *testing.T) {
	it1 := WrapIntSlice([]int{5, 6})
	it2 := WrapIntSlice([]int{3, 4})
	m1 := &Mixer[int]{}
	m1.Init(func(a, b int) bool { return a <= b }, it1, it2)
	it3 := WrapIntSlice([]int{1, 2})
	m2 := &Mixer[int]{}
	m2.Init(func(a, b int) bool { return a <= b }, m1, it3)
	assert.True(t, m2.HasNext())
	v, _ := m2.Next()
	assert.Equal(t, 1, v)
	m2.Reset()
	v, _ = m2.Next()
	assert.Equal(t, 1, v)
	v, _ = m2.Next()
	assert.Equal(t, 2, v)
	v, _ = m2.Next()
	assert.Equal(t, 3, v)
	v, _ = m2.Next()
	assert.Equal(t, 4, v)
	v, _ = m2.Next()
	assert.Equal(t, 5, v)
}

func TestMixer_Close(t *testing.T) {
	it1 := WrapIntSlice([]int{5, 6})
	it2 := WrapIntSlice([]int{3, 4})
	mx := &Mixer[int]{}
	mx.Init(func(a, b int) bool { return a <= b }, it1, it2)
	assert.Equal(t, it1, mx.src1.it)
	assert.Equal(t, it2, mx.src2.it)
	assert.Nil(t, mx.Close())
	assert.Nil(t, mx.src1.it)
	assert.Nil(t, mx.src2.it)
}

func testSimpleMix(t *testing.T, it1, it2 Iterator[int], res []int) {
	var m Mixer[int]
	m.Init(func(a, b int) bool { return a <= b }, it1, it2)
	for _, v := range res {
		assert.True(t, m.HasNext())
		v1, ok := m.Next()
		assert.True(t, ok)
		assert.Equal(t, v, v1)
	}
	assert.False(t, m.HasNext())
}
