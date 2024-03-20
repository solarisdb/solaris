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

// CopyMap allows to return a copy of the map provided.
// NOTE: it is NOT a deep copy, but just simply produces the copy of the map
func CopyMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	res := make(map[K]V)
	for k, v := range m {
		res[k] = v
	}
	return res
}

// GetFirst returns the first (pretty random) element in the map m.
// This weird function maybe helpful if we know that the len(m) == 1 and we need just get
// the elements out of there
func GetFirst[K comparable, V any](m map[K]V) (K, V, bool) {
	for k, v := range m {
		return k, v, true
	}
	return *new(K), *new(V), false
}

// Keys returns keys list (slice is created) or nil
func Keys[K comparable, V any](m map[K]V) []K {
	if len(m) == 0 {
		return nil
	}
	keys := make([]K, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values returns values list (slice is created) or nil
func Values[K comparable, V any](m map[K]V) []V {
	if len(m) == 0 {
		return nil
	}
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}
