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
package strutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSumHashes(t *testing.T) {
	h := SumHashes()
	assert.True(t, isBufOkForHash(h.Bytes()))
	h2 := getRandom(t)
	h3 := getRandom(t)
	sum := SumHashes(h2, h3)
	assert.NotEqual(t, h, sum)
	assert.Equal(t, sum, SumHashes(h2, h3))
	assert.NotEqual(t, SumHashes(h2, h3), SumHashes(h3, h2))
}

func TestCreateHash(t *testing.T) {
	_, err := CreateHash([]byte{1, 2, 3})
	assert.NotNil(t, err)
	h2 := getRandom(t)
	h, err := CreateHash(h2.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, h2, h)
	assert.Equal(t, h2.String(), h.String())
}

func TestParseHash(t *testing.T) {
	h := getRandom(t)
	h2, err := ParseHash(h.String())
	assert.Nil(t, err)
	assert.Equal(t, h, h2)
}

func TestEqual(t *testing.T) {
	h := getRandom(t)
	h2, err := CreateHash(h.Bytes())
	assert.Nil(t, err)
	assert.True(t, h.String() == h2.String())
}

func TestRandomHash(t *testing.T) {
	assert.True(t, isBufOkForHash(RandomHash().Bytes()))
}

func getRandom(t *testing.T) Hash {
	return RandomHash()
}
