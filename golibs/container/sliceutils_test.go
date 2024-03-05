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
package container

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func BenchmarkMergeSlicesUnique(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MergeSlicesUnique([]string{"aaa", "bbb", "aaa"})
	}
}

func BenchmarkSliceFill(b *testing.B) {
	s := make([]int, 50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SliceFill(s, 1234)
	}
}

func TestIndexOf(t *testing.T) {
	assert.Equal(t, -1, IndexOf(nil, 1))
	assert.Equal(t, -1, IndexOf([]int{}, 1))
	assert.Equal(t, -1, IndexOf([]int{2}, 1))
	assert.Equal(t, 1, IndexOf([]int{2, 1}, 1))
	assert.Equal(t, 0, IndexOf([]int{2, 1}, 2))
}

func TestIndexOfAny(t *testing.T) {
	intEq := func(v1, v2 int) bool { return v1 == v2 }
	assert.Equal(t, -1, IndexOfAny(nil, 1, intEq))
	assert.Equal(t, -1, IndexOfAny([]int{}, 1, intEq))
	assert.Equal(t, -1, IndexOfAny([]int{2}, 1, intEq))
	assert.Equal(t, 1, IndexOfAny([]int{2, 1}, 1, intEq))
	assert.Equal(t, 0, IndexOfAny([]int{2, 1}, 2, intEq))
}

func TestMergeSlicesUnique(t *testing.T) {
	assert.Equal(t, []int{}, MergeSlicesUnique[int]())
	s := MergeSlicesUnique[int]([]int{1, 2, 3})
	sort.Ints(s)
	assert.Equal(t, []int{1, 2, 3}, s)
	s = MergeSlicesUnique([]int{1, 2, 3}, []int{})
	sort.Ints(s)
	assert.Equal(t, []int{1, 2, 3}, s)
	s = MergeSlicesUnique([]int{1, 2, 3}, []int{-1, 1, 2})
	sort.Ints(s)
	assert.Equal(t, []int{-1, 1, 2, 3}, s)
}

func TestSliceRemoveIdx(t *testing.T) {
	assert.Panics(t, func() {
		SliceRemoveIdx([]string{}, 0)
	})
	s := []string{"a", "b", "c"}
	assert.Equal(t, []string{"c", "b"}, SliceRemoveIdx(s, 0))
	s = []string{"a", "b", "c"}
	assert.Equal(t, []string{"a", "c"}, SliceRemoveIdx(s, 1))
	s = []string{"a", "b", "c"}
	assert.Equal(t, []string{"a", "b"}, SliceRemoveIdx(s, 2))
}

func TestSliceReverse(t *testing.T) {
	assert.Nil(t, SliceReverse[int](nil))
	assert.Equal(t, []string{}, SliceReverse([]string{}))
	assert.Equal(t, []int{1}, SliceReverse([]int{1}))
	assert.Equal(t, []int{2, 1}, SliceReverse([]int{1, 2}))
	assert.Equal(t, []int{3, 2, 1}, SliceReverse([]int{1, 2, 3}))
}

func TestSliceCopy(t *testing.T) {
	assert.Equal(t, []int{}, SliceCopy[int](nil))
	assert.Equal(t, []int{}, SliceCopy([]int{}))
	assert.Equal(t, []int{1, 2}, SliceCopy([]int{1, 2}))
}

func Test_SliceExcludeOverlaps(t *testing.T) {
	mld1 := []string{"a", "b"}
	mld2 := []string{}
	s1, s2 := SliceExcludeOverlaps(mld1, mld2)
	assert.Len(t, s2, 0)
	sort.Strings(s1)
	assert.Equal(t, []string{"a", "b"}, s1)

	s1, s2 = SliceExcludeOverlaps(mld2, mld1)
	assert.Len(t, s1, 0)
	sort.Strings(s2)
	assert.Equal(t, []string{"a", "b"}, s2)

	mld1 = []string{"a", "b", "c"}
	mld2 = []string{"b", "d", "e"}
	s1, s2 = SliceExcludeOverlaps(mld1, mld2)
	sort.Strings(s2)
	sort.Strings(s1)
	assert.Equal(t, []string{"d", "e"}, s2)
	assert.Equal(t, []string{"a", "c"}, s1)
}

func TestSliceExludeUniqueS2(t *testing.T) {
	unqie, same := SliceExludeUniqueS2([]string{"1", "2"}, []string{"2", "3"})
	assert.Equal(t, []string{"2"}, same)
	assert.Equal(t, []string{"1"}, unqie)

	var s1, s2 []string
	unqie, same = SliceExludeUniqueS2(s1, s2)
	assert.Equal(t, []string{}, same)
	assert.Equal(t, []string{}, unqie)

	unqie, same = SliceExludeUniqueS2(nil, []string{"1"})
	assert.Equal(t, []string{}, same)
	assert.Equal(t, []string{}, unqie)

	unqie, same = SliceExludeUniqueS2([]string{"1"}, []string{"1"})
	assert.Equal(t, []string{"1"}, same)
	assert.Equal(t, []string{}, unqie)

	unqie, same = SliceExludeUniqueS2([]string{"2"}, []string{})
	assert.Equal(t, []string{}, same)
	assert.Equal(t, []string{"2"}, unqie)
}

func TestSliceFill(t *testing.T) {
	var s []int
	SliceFill(s, 123)

	s = make([]int, 2)
	SliceFill(s, 123)
	for _, v := range s {
		assert.Equal(t, 123, v)
	}

	s = make([]int, 2000)
	SliceFill(s, 123)
	for _, v := range s {
		assert.Equal(t, 123, v)
	}
}
