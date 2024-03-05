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
package chans

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteToManyWithControl(t *testing.T) {
	dones := []chan struct{}{make(chan struct{}), make(chan struct{})}
	wchs := []chan int{make(chan int, 1), make(chan int, 1)}
	descs := []WriteDesc[int]{{dones[0], wchs[0]}, {dones[1], wchs[1]}}

	idx, ok := WriteToManyWithControl(descs, 5)
	assert.True(t, ok)
	v := 0
	if idx == 0 {
		v = <-wchs[0]
	} else {
		v = <-wchs[1]
	}
	assert.Equal(t, 5, v)
	idx, ok = WriteToManyWithControl(descs, 5)
	assert.True(t, ok)
	idx, ok = WriteToManyWithControl(descs, 5)
	assert.True(t, ok)
	go func() {
		close(dones[1])
	}()
	idx, ok = WriteToManyWithControl(descs, 5)
	assert.False(t, ok)
	assert.Equal(t, 1, idx)

	close(dones[0])
	idx, ok = WriteToManyWithControl(descs[:1], 5)
	assert.False(t, ok)
	assert.Equal(t, 0, idx)
}
