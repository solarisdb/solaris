// Copyright 2024 The Solaris Authors
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

package lru

import (
	"context"
	"fmt"
	"github.com/solarisdb/solaris/golibs/chans"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/errors"
	"sync"
)

type (
	// ReleasableCache is a cache which allows to keep up to N(maxSize) objects. The cache allows to delete only
	// not used objects with the LRU discipline. To achieve the behavior, any object, retrieved from the cache,
	// must be released as soon as it's not used anymore. The object can be kept in the cache and retrieved again or been
	// deleted if the capacity of the cache is reached its maximum. The retrieved objects will not be deleted until
	// they are released, so the cache clients must follow the protocol and release any object retrieved from the cache.
	ReleasableCache[K comparable, V any] struct {
		lock       sync.Mutex
		maxSize    int
		allKnown   map[K]*rHolder[V]
		lruCache   *iterable.Map[K, V]
		inflight   map[K]chan struct{}
		createNewF CreateCtxPoolElemF[K, V]
		onDeleteF  OnDeleteElemF[K, V]
		waiter     chan struct{}
		closed     bool
	}

	rHolder[V any] struct {
		value      V
		refCounter int
	}

	// Releasable struct represents an object retrieved from the cache. Clients can obtain the object
	// by calling Releasable.Value() function
	Releasable[V any] struct {
		k  any
		rh *rHolder[V]
	}

	// CreateCtxPoolElemF a configuration type used for providing the function which will be called
	// for creating new objects
	CreateCtxPoolElemF[K any, V any] func(ctx context.Context, k K) (V, error)
)

// NewReleasableCache creates the new ReleasableCache object
func NewReleasableCache[K comparable, V any](maxSize int, createNewF CreateCtxPoolElemF[K, V], onDeleteF OnDeleteElemF[K, V]) (*ReleasableCache[K, V], error) {
	if maxSize < 1 {
		return nil, fmt.Errorf("NewReleasableCache(): the maxSize=%d, but it cannot be less than 1", maxSize)
	}
	if createNewF == nil {
		return nil, fmt.Errorf("NewReleasableCache(): createNewF must not be nil")
	}
	r := new(ReleasableCache[K, V])
	r.allKnown = make(map[K]*rHolder[V])
	r.lruCache = iterable.NewMap[K, V]()
	r.inflight = make(map[K]chan struct{})
	r.maxSize = maxSize
	r.createNewF = createNewF
	r.onDeleteF = onDeleteF
	return r, nil
}

// GetOrCreate retrieves an existing object or creates a new one if the object is not in the cache. The function
// accepts ctx and may be blocked until the object is created or the context is expired. If the cache is full
// and the new object cannot be created due to the capacity limits, the function will be blocked until the creation
// of the new object will be available or the context is closed.
func (r *ReleasableCache[K, V]) GetOrCreate(ctx context.Context, k K) (Releasable[V], error) {
	for {
		r.lock.Lock()
		if r.closed {
			r.lock.Unlock()
			return Releasable[V]{}, errors.ErrClosed
		}
		if rh, ok := r.allKnown[k]; ok {
			rh.refCounter++
			if rh.refCounter == 1 {
				r.lruCache.Remove(k)
			}
			r.lock.Unlock()
			return Releasable[V]{k: k, rh: rh}, nil
		}
		ch, watcher := r.inflight[k]
		waiter := false
		if !watcher {
			r.sweep(r.maxSize)
			if r.maxSize <= r.used() {
				// we cannot continue to create the new elements, but has to wait until the size will be adjusted
				if !chans.IsOpened(r.waiter) {
					r.waiter = make(chan struct{})
				}
				ch = r.waiter
				waiter = true
			} else {
				ch = make(chan struct{})
				r.inflight[k] = ch
			}
		}
		r.lock.Unlock()

		// if watcher is true, it means that another goroutine already creating the new item,
		// so it needs to wait for the result instead of requesting new value.
		// if the waiter is true, it means that we hit the maximum capacity, so waiting until
		// someone will release a resource
		if watcher || waiter {
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return Releasable[V]{}, ctx.Err()
			}
		}

		// only creaters may be here
		v, err := r.createNewF(ctx, k)

		r.lock.Lock()
		// if it was closed while we were creating the new object...
		if r.closed {
			if r.onDeleteF != nil {
				r.onDeleteF(k, v)
			}
			r.lock.Unlock()
			return Releasable[V]{}, errors.ErrClosed
		}

		close(ch)
		delete(r.inflight, k)
		var rh *rHolder[V]
		if err == nil {
			rh = &rHolder[V]{refCounter: 1, value: v}
			r.allKnown[k] = rh
		}
		r.lock.Unlock()

		return Releasable[V]{k: k, rh: rh}, err
	}
}

// Release allows to return the object back into the cache and let the cache know that the client is not
// going to use the object. The client MUST NOT use the rlsbl after the call.
func (r *ReleasableCache[K, V]) Release(rlsbl *Releasable[V]) {
	r.lock.Lock()
	defer r.lock.Unlock()
	rlsbl.rh.refCounter--
	if rlsbl.rh.refCounter < 0 {
		panic(fmt.Sprintf("unacceptable usage of Release() for key=%s, v=%v, refCounter is negative", rlsbl.k, rlsbl.rh.value))
	}
	if rlsbl.rh.refCounter == 0 {
		if r.closed {
			if r.onDeleteF != nil {
				r.onDeleteF((rlsbl.k).(K), rlsbl.rh.value)
			}
			return
		}
		r.lruCache.Add((rlsbl.k).(K), rlsbl.rh.value)
		if r.waiter != nil {
			r.sweep(r.maxSize)
			if r.used() < r.maxSize {
				close(r.waiter)
				r.waiter = nil
			}
		}
	}
	rlsbl.rh = nil
	return
}

// Close removes all not borrowed objects. The objects that are not released yet will be deleted after the
// Release() call. After the Close() call the new objects cannot be created
func (r *ReleasableCache[K, V]) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.closed {
		return errors.ErrClosed
	}
	r.sweep(0)
	r.closed = true
	for _, ch := range r.inflight {
		close(ch)
	}
	if r.waiter != nil {
		close(r.waiter)
		r.waiter = nil
	}
	r.inflight = nil
	r.allKnown = nil
	r.lruCache = nil
	return nil
}

// used returns how many keys are created and how many are in flight so far. The function must
// be called under the lock
func (r *ReleasableCache[K, V]) used() int {
	return len(r.allKnown) + len(r.inflight)
}

// sweep allows to remove not borrowed objects
func (r *ReleasableCache[K, V]) sweep(maxAllowed int) {
	for r.lruCache.Len() > 0 && r.used() >= maxAllowed {
		k, _ := r.lruCache.First()
		r.lruCache.Remove(k)
		if r.onDeleteF != nil {
			v := r.allKnown[k].value
			r.onDeleteF(k, v)
		}
		delete(r.allKnown, k)
	}
}

// Value returns the object value associated with Releasable. The function must not be called after the rlsbl
// is released back to the cache
func (rlsbl Releasable[V]) Value() V {
	return rlsbl.rh.value
}
