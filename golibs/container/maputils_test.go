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

func TestCopyMap(t *testing.T) {
	var m map[string]string
	m1 := CopyMap(m)
	assert.Equal(t, m, m1)

	m = map[string]string{}
	m1 = CopyMap(m)
	assert.Equal(t, m, m1)

	m = map[string]string{"a": "bb", "cc": "dd"}
	m1 = CopyMap(m)
	assert.Equal(t, m, m1)
}

func TestGetFist(t *testing.T) {
	var m map[int]int
	k, v, ok := GetFirst(m)
	assert.Equal(t, 0, k)
	assert.Equal(t, 0, v)
	assert.False(t, ok)

	m = map[int]int{}
	k, v, ok = GetFirst(m)
	assert.Equal(t, 0, k)
	assert.Equal(t, 0, v)
	assert.False(t, ok)

	m = map[int]int{111: 222}
	k, v, ok = GetFirst(m)
	assert.Equal(t, 111, k)
	assert.Equal(t, 222, v)
	assert.True(t, ok)
}

func TestKeys(t *testing.T) {
	var m map[string]string
	ks := Keys(m)
	assert.True(t, len(ks) == 0)
	m = map[string]string{"a": "bb", "cc": "dd"}
	ks = Keys(m)
	sort.Strings(ks)
	assert.Equal(t, []string{"a", "cc"}, ks)
	assert.Nil(t, Keys(map[int]int{}))
}

func TestValues(t *testing.T) {
	m := map[string]string{"a": "bb", "cc": "dd"}
	vs := Values(m)
	sort.Strings(vs)
	assert.Equal(t, []string{"bb", "dd"}, vs)
	assert.Nil(t, Values(map[int]int{}))
}
