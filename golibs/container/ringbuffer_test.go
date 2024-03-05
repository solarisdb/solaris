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
	"io"
	"testing"
)

func TestNewRingBuffer(t *testing.T) {
	r := NewRingBuffer[int](0)
	assert.Equal(t, 0, r.Cap())
	assert.Equal(t, 1, len(r.buf))
	assert.Equal(t, 0, r.Len())

	r = NewRingBuffer[int](10)
	assert.Equal(t, 10, r.Cap())
	assert.Equal(t, 11, len(r.buf))
	assert.Equal(t, 0, r.Len())
}

func TestRingBuffer_Read(t *testing.T) {
	r := NewRingBuffer[int](10)
	_, err := r.Read()
	assert.Equal(t, io.EOF, err)

	for i := 1; i <= 10; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.NotNil(t, r.Write(123))

	for i := 1; i <= 10; i++ {
		v, err := r.Read()
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}
	assert.Equal(t, 0, r.Len())

	r.r, r.w = 7, 7
	for i := 1; i <= 10; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.NotNil(t, r.Write(123))

	for i := 1; i <= 10; i++ {
		v, err := r.Read()
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}
	assert.Equal(t, 0, r.Len())

	assert.Equal(t, 6, r.w)
	assert.Equal(t, 6, r.r)
}

func TestRingBuffer_Write(t *testing.T) {
	r := NewRingBuffer[int](10)
	for i := 1; i <= 5; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 1, r.buf[0]) // before read
	for i := 1; i <= 3; i++ {
		v, err := r.Read()
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}
	assert.Equal(t, 0, r.buf[0]) // after read
	for i := 6; i <= 13; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.NotNil(t, r.Write(123))
	for i := 4; i <= 13; i++ {
		v, err := r.Read()
		assert.Nil(t, err)
		assert.Equal(t, i, v)
	}
}

func TestRingBuffer_ReadNt(t *testing.T) {
	r := NewRingBuffer[int](10)
	buf := make([]int, 5)
	assert.Equal(t, 0, r.ReadN(buf))

	for i := 0; i < 2; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 2, r.ReadN(buf))

	for i := 0; i < 6; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 5, r.ReadN(buf))

	assert.Equal(t, 7, r.r)
	assert.Equal(t, 8, r.w)

	for i := 0; i < 4; i++ {
		assert.Nil(t, r.Write(i))
	}

	assert.Equal(t, 7, r.r)
	assert.Equal(t, 1, r.w)
	assert.Equal(t, 5, r.ReadN(buf))

	assert.Equal(t, 1, r.r)
	assert.Equal(t, 1, r.w)
	assert.Equal(t, []int{5, 0, 1, 2, 3}, buf)
	assert.Equal(t, 0, r.buf[0])
	assert.Equal(t, 0, r.buf[10])
}

func TestRingBuffer_Skip(t *testing.T) {
	r := NewRingBuffer[int](5)
	for i := 0; i < 5; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 5, r.w)
	assert.Equal(t, 5, r.Len())
	assert.Equal(t, 2, r.Skip(2))
	assert.Equal(t, 2, r.r)
	assert.Equal(t, 3, r.Len())
	assert.Equal(t, 0, r.buf[1])

	for i := 0; i < 2; i++ {
		assert.Nil(t, r.Write(i+2))
	}
	assert.Equal(t, 1, r.w)
	assert.Equal(t, 5, r.Len())
	assert.Equal(t, 5, r.Skip(12))
	assert.Equal(t, 1, r.r)
	assert.Equal(t, 1, r.w)
	assert.Equal(t, 0, r.Len())
	assert.Equal(t, 0, r.buf[4])
}

func TestRingBuffer_Clear(t *testing.T) {
	r := NewRingBuffer[int](5)
	for i := 0; i < 5; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 5, r.w)
	assert.Equal(t, 5, r.Len())
	r.Clear()
	assert.Equal(t, 5, r.w)
	assert.Equal(t, 5, r.r)
	assert.Equal(t, 0, r.buf[4])
	assert.Equal(t, 0, r.Len())
}

func TestRingBuffer_At(t *testing.T) {
	r := NewRingBuffer[int](5)
	r.r = 4
	r.w = 4

	assert.Panics(t, func() {
		r.At(0)
	})

	for i := 1; i < 6; i++ {
		assert.Nil(t, r.Write(i))
	}
	assert.Equal(t, 1, r.At(0))
	assert.Equal(t, 5, r.At(4))
	assert.Panics(t, func() {
		r.At(5)
	})
}
