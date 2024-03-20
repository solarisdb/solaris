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

// IndexOf returns the position of v in slice and -1 if the v is not in slice.
func IndexOf[V comparable](slice []V, v V) int {
	if len(slice) == 0 {
		return -1
	}
	for idx, v1 := range slice {
		if v == v1 {
			return idx
		}
	}
	return -1
}

// IndexOfAny returns the position of v in slice and -1 if the v is not in slice.
func IndexOfAny[V any](slice []V, v V, equalF func(v1, v2 V) bool) int {
	if len(slice) == 0 {
		return -1
	}
	for idx, v1 := range slice {
		if equalF(v, v1) {
			return idx
		}
	}
	return -1
}

// MergeSlicesUnique receives slices and form one with unique values from all of them
func MergeSlicesUnique[V comparable](slices ...[]V) []V {
	m := make(map[V]bool)
	for _, s := range slices {
		for _, v := range s {
			m[v] = true
		}
	}
	res := make([]V, 0, len(m))
	for v := range m {
		res = append(res, v)
	}
	return res
}

// SliceExcludeOverlaps returns unique elements in each slice of two
// first return param(unique in s1) - present in s1, but not in s2
// second return param(unique in s2) - present in s2, but not in s1,
func SliceExcludeOverlaps[V comparable](s1, s2 []V) ([]V, []V) {
	m := make(map[V]bool)
	for _, v := range s1 {
		m[v] = true
	}
	uniqueS2 := []V{}
	for _, v := range s2 {
		if m[v] {
			delete(m, v)
		} else {
			uniqueS2 = append(uniqueS2, v)
		}
	}
	uniqueS1 := make([]V, 0, len(m))
	for v := range m {
		uniqueS1 = append(uniqueS1, v)
	}
	return uniqueS1, uniqueS2
}

// SliceExludeS2Unique substracts elements s2 from the s1 and returns the following:
// first param - elements that present in s1, but not in s2
// second param - elements that present in s1 and s2 both
func SliceExludeUniqueS2[V comparable](s1, s2 []V) ([]V, []V) {
	m := make(map[V]bool)
	for _, v := range s1 {
		m[v] = true
	}
	common := []V{}
	for _, v := range s2 {
		if m[v] {
			common = append(common, v)
			delete(m, v)
		}
	}
	uniqueS1 := make([]V, 0, len(m))
	for k := range m {
		uniqueS1 = append(uniqueS1, k)
	}
	return uniqueS1, common
}

// SliceRemoveIdx removes the element at idx from the slice and return the updated slice.
// the function does it in place, so the original slice is changed
func SliceRemoveIdx[V any](slice []V, idx int) []V {
	slice[idx] = slice[len(slice)-1]
	slice[len(slice)-1] = *new(V)
	return slice[:len(slice)-1]
}

// SliceReverse the slice in place and returns the slice itself
func SliceReverse[V any](slice []V) []V {
	s, e := 0, len(slice)-1
	for s < e {
		slice[s], slice[e] = slice[e], slice[s]
		s++
		e--
	}
	return slice
}

// SliceCopy makes a copy of slice
func SliceCopy[V any](v []V) []V {
	res := make([]V, len(v))
	copy(res, v)
	return res
}

// SliceFill sets all values of s to v
func SliceFill[V any](s []V, v V) {
	// magic number when copy becomes faster
	if len(s) < 50 {
		for i := range s {
			s[i] = v
		}
		return
	}
	s[0] = v
	for j := 1; j < len(s); j *= 2 {
		copy(s[j:], s[:j])
	}
}
