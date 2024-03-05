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
	"fmt"
	"github.com/logrange/linker"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/chans"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/kvs"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/sync"
	"github.com/solarisdb/solaris/golibs/timeout"
	"sync/atomic"
	"time"
)

// kvsLockProvider provides an implementation of LockProvider based on kv.Storage
type kvsLockProvider struct {
	// Storage is an implementation of kv.Storage consistent key-value storage implementation
	Storage kvs.Storage `inject:""`

	path     string
	done     chan struct{}
	leaseTTL time.Duration
	logger   logging.Logger
}

// kvsLock provides an implementation of Locker object, which is returned by
// NewLocker() of kvsLockProvider
type kvsLock struct {
	dlp     *kvsLockProvider // immutable
	key     string           // immutable
	lockCh  chan bool        // immutable
	future  atomic.Value     // timeout.Future
	lckCntr int32
	waiters int32
}

// LockProvider helper interface to indicate that the object has
// a lifecyle, which should be supported if created and used outside of linker
type LockProvider interface {
	sync.LockProvider
	linker.Shutdowner
}

var _ sync.LockProvider = (*kvsLockProvider)(nil)
var _ linker.Shutdowner = (*kvsLockProvider)(nil)
var dummyValue = []byte{0}
var defaultLeaseTimeout = time.Second * 10

// New allows to create new instance of kvsLockProvider.
// It expects instance of kv.Storage and a prefix key for the lockers key-space
func New(path string) *kvsLockProvider {
	dlp := new(kvsLockProvider)
	dlp.path = path
	dlp.leaseTTL = defaultLeaseTimeout
	dlp.done = make(chan struct{})
	dlp.logger = logging.NewLogger("kvs.LockProvider")
	return dlp
}

// NewKvsLockProvider returns implementation of the LockProvider
// The resulted object has lifecycle (Shutdown must be called), so the function
// just indicates this fact. Prefer to use New(), instead of the object and
// let the linker does its job by supporting the object lifecycle.
func NewKvsLockProvider(kvs kvs.Storage, path string) LockProvider {
	kp := New(path)
	kp.Storage = kvs
	return kp
}

// Shutdown frees resources borrowed by the kvsLockProvider, implementation of
// linker.Shutdowner
func (dlp *kvsLockProvider) Shutdown() {
	close(dlp.done)
}

// NewLocker is part of LockProvider. It returns a kvsLock object, which
// implements Locker
func (dlp *kvsLockProvider) NewLocker(name string) sync.Locker {
	lock := new(kvsLock)
	lock.dlp = dlp
	lock.lockCh = make(chan bool, 1)
	lock.lockCh <- true
	lock.key = dlp.path + name
	return lock
}

func (l *kvsLock) TryLock(ctx context.Context) bool {
	atomic.AddInt32(&l.waiters, 1)
	defer atomic.AddInt32(&l.waiters, -1)
	if err := l.tryLockInternal(); err != nil {
		return false
	}
	if ver, err := l.dlp.Storage.Create(ctx, kvs.Record{
		Key:       l.key,
		Value:     cast.StringToByteArray(""),
		ExpiresAt: cast.Ptr(time.Now().Add(l.dlp.leaseTTL)),
	}); err == nil {
		l.future.Store(timeout.Call(func() { l.supportTimeout(ver) }, l.dlp.leaseTTL/2))
		return true
	}
	atomic.StoreInt32(&l.lckCntr, 0)
	l.lockCh <- true
	return false
}

// Lock is part of sync.Locker. It allows to acquire and hold the distributed lock
func (l *kvsLock) Lock() {
	err := l.lockWithCtx(context.Background())
	if err != nil {
		l.dlp.logger.Errorf("kvsLock.Lock(): unexpected lockWithContext behavior lock=%s, err=%s", l.String(), err)
		panic("kvsLock: unhandled error situation while locking err=" + err.Error())
	}
}

func (l *kvsLock) LockWithCtx(ctx context.Context) error {
	return l.lockWithCtx(ctx)
}

// Unlock releases the distributed lock
func (l *kvsLock) Unlock() {
	if !atomic.CompareAndSwapInt32(&l.lckCntr, 1, 0) {
		l.dlp.logger.Errorf("kvsLock.Unlock(): wrong object state: %s", l.String())
		panic("kvsLock: an attempt to unlock not-locked object " + l.String())
	}

	future := l.future.Load().(timeout.Future)
	future.Cancel()
	err := l.dlp.Storage.Delete(context.Background(), l.key)
	if err != nil && !errors.Is(err, errors.ErrNotExist) {
		// some serious situation with the storage corruption or non-availability
		l.dlp.logger.Warnf("kvsLock.Unlock(): could not read the key %s, but will release the lock: %s", l.String(), err)
	}

	// unlock the lock
	l.lockCh <- true
}

// String implements fmt.Stringify
func (l *kvsLock) String() string {
	return fmt.Sprintf("{lckCntr: %d, key: %s, shutdown: %t, waiters: %d}",
		atomic.LoadInt32(&l.lckCntr), l.key, !chans.IsOpened(l.dlp.done), atomic.LoadInt32(&l.waiters))
}

// lockWithCtx is part of Locker.
func (l *kvsLock) lockWithCtx(ctx context.Context) error {
	atomic.AddInt32(&l.waiters, 1)
	defer atomic.AddInt32(&l.waiters, -1)
	if err := l.lockInternal(ctx); err != nil {
		return err
	}

	err := ctx.Err()
	var ver string
	for err == nil {
		ver, err = l.dlp.Storage.Create(ctx, kvs.Record{
			Key:       l.key,
			Value:     cast.StringToByteArray(""),
			ExpiresAt: cast.Ptr(time.Now().Add(l.dlp.leaseTTL)),
		})
		if err == nil {
			l.future.Store(timeout.Call(func() { l.supportTimeout(ver) }, l.dlp.leaseTTL/2))
			return nil
		}

		if errors.Is(err, errors.ErrExist) {
			_ = l.dlp.Storage.WaitForVersionChange(ctx, l.key, ver)
			err = ctx.Err()
		}
	}

	atomic.StoreInt32(&l.lckCntr, 0)
	l.lockCh <- true
	return err
}

// supportTimeout tries to refresh the record timeout if we hold it too long (at least twice of leaseTTL)
// the function is tricky, cause it uses CAS operation to update the record version and if the version
// is updated, it recharges the timeout. This is where the new raise can happen and the new future may
// overwrite the future flag stored in the atomic.
func (l *kvsLock) supportTimeout(ver string) {
	future := l.future.Load().(timeout.Future)
	r, err := l.dlp.Storage.CasByVersion(context.Background(), kvs.Record{
		Key:       l.key,
		Value:     cast.StringToByteArray(""),
		Version:   ver,
		ExpiresAt: cast.Ptr(time.Now().Add(l.dlp.leaseTTL)),
	})
	if err != nil {
		l.dlp.logger.Debugf("supportTimeout raise detected, just do nothing for the key=%s, err=%s", l.key, err)
		return
	}
	newFuture := timeout.Call(func() { l.supportTimeout(r.Version) }, l.dlp.leaseTTL/2)
	if !l.future.CompareAndSwap(future, newFuture) {
		// somebody already started the new timer, so drop this and forget about the incident
		l.dlp.logger.Debugf("supportTimeout raise 2 detected, just cancelling the call timeout")
		newFuture.Cancel()
	}
}

func (l *kvsLock) isLocked() bool {
	return atomic.LoadInt32(&l.lckCntr) == 1
}

func (l *kvsLock) lockInternal(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.dlp.done:
		return fmt.Errorf("kvsLock.lockInternal(): locking mechanism is shutdown: %w", errors.ErrClosed)
	case <-l.lockCh:
		if !chans.IsOpened(l.dlp.done) {
			return fmt.Errorf("kvsLock.lockInternal(): locking mechanism is shutdown: %w", errors.ErrClosed)
		}
		if !atomic.CompareAndSwapInt32(&l.lckCntr, 0, 1) {
			l.dlp.logger.Errorf("kvsLock.lockInternal(): internal error, invalid locker state %s", l.String())
			panic("kvsLock.lockInternal(): internal error, invalid state " + l.String())
		}
		return nil
	}
}

func (l *kvsLock) tryLockInternal() error {
	select {
	case <-l.dlp.done:
		return fmt.Errorf("kvsLock.tryLockInternal(): locking mechanism is shutdown: %w", errors.ErrClosed)
	case <-l.lockCh:
		if !chans.IsOpened(l.dlp.done) {
			return fmt.Errorf("kvsLock.tryLockInternal(): locking mechanism is shutdown: %w", errors.ErrClosed)
		}
		if !atomic.CompareAndSwapInt32(&l.lckCntr, 0, 1) {
			l.dlp.logger.Errorf("kvsLock.tryLockInternal(): internal error, invalid locker state %s", l.String())
			panic("kvsLock.tryLockInternal(): internal error, invalid state " + l.String())
		}
		return nil
	default:
		return fmt.Errorf("could not acquire: %w", errors.ErrConflict)
	}
}
