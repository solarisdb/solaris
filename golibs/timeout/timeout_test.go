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
package timeout

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestNilCall(t *testing.T) {
	f := Call(nil, time.Millisecond)
	assert.Equal(t, -1, f.(*future).idx)
	f.Cancel()
	f.Cancel()
}

func TestCall(t *testing.T) {
	cc := newCallControl()
	var called int32
	call(cc, func() { atomic.AddInt32(&called, 1) }, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
	assert.Equal(t, 1, cc.watchers)

	f := call(cc, func() { atomic.AddInt32(&called, 1) }, 10*time.Millisecond)
	f.Cancel()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&called))

	assert.Equal(t, 1, cc.watchers)

	call(cc, func() { atomic.AddInt32(&called, 1) }, 0)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&called))
}

func TestBunch(t *testing.T) {
	cc := newCallControl()
	var called int32
	for i := 0; i < 1000; i++ {
		call(cc, func() { atomic.AddInt32(&called, 1) }, time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1000), atomic.LoadInt32(&called))
	assert.Equal(t, cc.maxWorkers, cc.watchers)
}

func TestBunch2(t *testing.T) {
	cc := newCallControl()
	cc.idleTimeout = 100 * time.Millisecond
	var called int32
	for i := 0; i < 1000; i++ {
		call(cc, func() { atomic.AddInt32(&called, 1) }, time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1000), atomic.LoadInt32(&called))
	assert.Equal(t, cc.maxWorkers, cc.watchers)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, cc.watchers)
}

func TestCancelMany(t *testing.T) {
	cc := newCallControl()
	var called int32
	ff := []Future{}
	for i := 0; i < 100; i++ {
		f := call(cc, func() { atomic.AddInt32(&called, 1) }, (10+time.Duration(i))*time.Millisecond)
		if i&1 == 1 {
			ff = append(ff, f)
		}
	}
	assert.Equal(t, 50, len(ff))
	for _, f := range ff {
		f.Cancel()
	}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(50), atomic.LoadInt32(&called))
	assert.Equal(t, 1, cc.watchers)
}
