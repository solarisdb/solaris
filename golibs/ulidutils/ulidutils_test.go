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
package ulidutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextID(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := NewID()
		id2 := NextID(id)
		assert.True(t, id2 > id)
	}
}

func TestNextIDInvalid(t *testing.T) {
	assert.Panics(t, func() {
		NextID("invalid")
	})
}

func TestPrevID(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := NewID()
		id2 := PrevID(id)
		assert.True(t, id2 < id)
	}
}

func TestPrevIDInvalid(t *testing.T) {
	assert.Panics(t, func() {
		PrevID("invalid")
	})
}

func TestNewUUID(t *testing.T) {
	assert.Equal(t, 16, len(NewUUID()))
}
