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
package dist

import (
	"context"
	"github.com/solarisdb/solaris/golibs/chans"
	"github.com/solarisdb/solaris/golibs/kvs/inmem"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKvLockProvider_Shutdown(t *testing.T) {
	dlp := newKVDLP()
	tearOff(dlp)

	assert.False(t, chans.IsOpened(dlp.done))
}

func TestKvDistLock_Lock(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock := dlp.NewLocker("test").(*kvsLock)

	for i := 0; i < 10; i++ {
		assert.False(t, lock.isLocked())
		_, err := dlp.Storage.Get(context.Background(), lock.key)
		assert.Equal(t, os.ErrNotExist, err)

		lock.Lock()
		assert.True(t, lock.isLocked())
		_, err = dlp.Storage.Get(context.Background(), lock.key)
		assert.Nil(t, err)

		lock.Unlock()
	}
}

func TestKvDistLock_LockAfterShutdown(t *testing.T) {
	dlp := newKVDLP()
	lock := dlp.NewLocker("test").(*kvsLock)
	tearOff(dlp)
	time.Sleep(time.Millisecond)
	err := lock.lockWithCtx(context.Background())
	assert.NotNil(t, err)
	//assert.Panics(t, lock.Lock)
}

func TestKvDistLock_LockMultiple(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock := dlp.NewLocker("test").(*kvsLock)

	lock.Lock()
	var start, end sync.WaitGroup
	start.Add(100)
	for i := 0; i < 100; i++ {
		end.Add(1)
		go func() {
			start.Done()
			lock.Lock()
			lock.Unlock()
			end.Done()
		}()
	}

	start.Wait()
	time.Sleep(10 * time.Millisecond)
	assert.True(t, lock.isLocked())
	assert.Equal(t, int32(100), lock.waiters)
	lock.Unlock()
	end.Wait()

	assert.False(t, lock.isLocked())
	assert.Equal(t, int32(0), lock.waiters)
}

func TestKvDistLock_CancelCtxInLock(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)

	// make record in kv
	lock1.Lock()

	start := time.Now()
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := lock1.lockWithCtx(ctx)
	assert.True(t, time.Now().Sub(start) >= 100*time.Millisecond)
	assert.Equal(t, ctx.Err(), err)
	assert.NotNil(t, err)
	assert.True(t, lock1.isLocked())

	lock1.Unlock()
}

func TestKvDistLock_CancelCtxWithRaise(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)
	lock2 := dlp.NewLocker("test").(*kvsLock)

	lock1.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := lock2.lockWithCtx(ctx)
	assert.Equal(t, ctx.Err(), err)

	assert.False(t, lock2.isLocked())
	assert.True(t, lock1.isLocked())
	lock1.Unlock()
}

func TestKvDistLock_LockExpiredCtx(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)
	lock2 := dlp.NewLocker("test").(*kvsLock)

	// make record in kv
	lock1.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := lock2.lockWithCtx(ctx)
	assert.Equal(t, ctx.Err(), err)
	assert.NotNil(t, err)
	assert.False(t, lock2.isLocked())

	lock1.Unlock()
}

func TestKvDistLock_RaiseLocking(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)
	lock2 := dlp.NewLocker("test").(*kvsLock)

	lock1.Lock()

	var ack int32
	go func() {
		atomic.StoreInt32(&ack, 1)
		lock1.Lock()
		atomic.StoreInt32(&ack, 2)
		lock1.Unlock()
	}()
	for atomic.LoadInt32(&ack) == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
	assert.True(t, lock1.isLocked())
	assert.Equal(t, int32(1), atomic.LoadInt32(&lock1.waiters))

	start := time.Now()
	go func() {
		time.Sleep(10 * time.Millisecond)
		lock1.Unlock()
	}()
	lock2.Lock()
	assert.True(t, time.Now().Sub(start) >= 10*time.Millisecond)
	assert.True(t, lock2.isLocked())
	if lock1.isLocked() {
		assert.Equal(t, int32(1), atomic.LoadInt32(&ack))
	} else {
		assert.Equal(t, int32(2), atomic.LoadInt32(&ack))
	}
	lock2.Unlock()
	assert.False(t, lock2.isLocked())
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(2), atomic.LoadInt32(&ack))
	assert.False(t, lock1.isLocked())
}

func TestKvDistLock_Unlock(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)

	lock1 := dlp.NewLocker("test").(*kvsLock)
	assert.Panics(t, lock1.Unlock)
}

func TestKvDistLock_TryLock(t *testing.T) {
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)
	lock2 := dlp.NewLocker("test").(*kvsLock)

	lock1.Lock()
	assert.False(t, lock2.TryLock(context.Background()))
	lock1.Unlock()
	assert.True(t, lock2.TryLock(context.Background()))
	assert.False(t, lock1.TryLock(context.Background()))
	lock2.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, lock2.TryLock(ctx))
	lock1.Lock()
	lock1.Unlock()
}

func TestKvDistLock_Timeout(t *testing.T) {
	defaultLeaseTimeout = time.Millisecond * 50
	dlp := newKVDLP()
	defer tearOff(dlp)
	lock1 := dlp.NewLocker("test").(*kvsLock)
	lock1.Lock()
	for i := 1; i < 3; i++ {
		r, err := dlp.Storage.Get(context.Background(), lock1.key)
		assert.Nil(t, err)
		time.Sleep(time.Millisecond * 50)
		r1, err := dlp.Storage.Get(context.Background(), lock1.key)
		assert.Nil(t, err)
		assert.NotEqual(t, r1.Version, r.Version)
	}
	lock1.Unlock()
	_, err := dlp.Storage.Get(context.Background(), lock1.key)
	assert.Equal(t, os.ErrNotExist, err)
}

func newKVDLP() *kvsLockProvider {
	st := inmem.New()
	dlp := New("prefix")
	dlp.Storage = st
	return dlp
}

func tearOff(dlp *kvsLockProvider) {
	dlp.Shutdown()
}
