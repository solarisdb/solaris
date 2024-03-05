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
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"io"
)

type (
	// RingBuffer is an interface for working with the FIFO buffer with the fixed size
	RingBuffer[V any] interface {
		// Write allows to write the value v into the buffer. It may return errors.ErrExhausted
		// if the maximum capacity of the buffer is reached
		Write(v V) error
		// Read reads the value from the buffer. It will return io.EOF if the buffer is empty
		Read() (V, error)
		// ReadN allows to read multiple values into the slice dst. It will return number of
		// values read
		ReadN(dst []V) int
		// Skip allows to skip first n values. It returns the actual number of values skipped
		Skip(n int) int
		// At returns the element at the index idx. It doesn't change the buffer values
		At(idx int) V
		// Clear removes all elements from the buffer
		Clear()
		// Len returns the actual number of values can be read
		Len() int
		// Cap returns the maximum buffer capacity
		Cap() int
	}

	ringBuffer[V any] struct {
		size int
		r    int
		w    int
		buf  []V
	}
)

var _ RingBuffer[int] = (*ringBuffer[int])(nil)

// NewRingBuffer returns the new instance of the *ringBuffer (which implements the RingBuffer)
func NewRingBuffer[V any](size uint) *ringBuffer[V] {
	return &ringBuffer[V]{buf: make([]V, size+1)}
}

func (r *ringBuffer[V]) Write(v V) error {
	if r.Len() == r.Cap() {
		return fmt.Errorf("the buffer is full(%d): %w", r.Len(), errors.ErrExhausted)
	}

	r.buf[r.w] = v
	r.w++
	if r.w == len(r.buf) {
		r.w = 0
	}
	return nil
}

func (r *ringBuffer[V]) Read() (V, error) {
	if r.Len() == 0 {
		return *new(V), io.EOF
	}
	v := r.buf[r.r]
	r.buf[r.r] = *new(V)
	r.r++
	if r.r == len(r.buf) {
		r.r = 0
	}
	return v, nil
}

func (r *ringBuffer[V]) ReadN(dst []V) int {
	res := 0
	nilVal := *new(V)
	for len(dst) > 0 && r.Len() > 0 {
		endIdx := len(r.buf)
		if r.r < r.w {
			endIdx = r.w
		}
		cnt := copy(dst, r.buf[r.r:endIdx])
		SliceFill(r.buf[r.r:r.r+cnt], nilVal)
		r.r += cnt
		if r.r == len(r.buf) {
			r.r = 0
		}
		dst = dst[cnt:]
		res += cnt
	}
	return res
}

func (r *ringBuffer[V]) Skip(n int) int {
	res := 0
	nilVal := *new(V)
	for n > 0 && r.Len() > 0 {
		if n > r.Len() {
			n = r.Len()
		}
		endIdx := r.r + n
		if endIdx >= len(r.buf) {
			endIdx = len(r.buf)
		}
		cnt := endIdx - r.r
		SliceFill(r.buf[r.r:endIdx], nilVal)
		res += cnt
		n -= cnt
		r.r = endIdx
		if r.r == len(r.buf) {
			r.r = 0
		}
	}
	return res
}

func (r *ringBuffer[V]) At(idx int) V {
	if idx < 0 || idx >= r.Len() {
		panic(fmt.Sprintf("ringBuffer: index %d is out of bounds(len=%d)", idx, r.Len()))
	}
	d := 0
	if r.r+idx >= len(r.buf) {
		d = len(r.buf)
	}
	return r.buf[r.r+idx-d]
}

func (r *ringBuffer[V]) Clear() {
	r.Skip(r.Len())
}

func (r *ringBuffer[V]) Len() int {
	if r.r <= r.w {
		return r.w - r.r
	}
	return len(r.buf) - (r.r - r.w)
}

func (r *ringBuffer[V]) Cap() int {
	return len(r.buf) - 1
}
