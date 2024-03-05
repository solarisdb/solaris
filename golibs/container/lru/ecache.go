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
package lru

import (
	"fmt"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"sync"
)

// ECache implements container with limited size capacity and LRU (Least Recently Used) pull out discipline.
// The elements can be created automatically if they are not found in the pool via the createNewF function call,
// which is provided via the Cache creation (see NewECache). ECache allows to operate with not comparable type as
// a primary key. For mapping the type to comparable on MapToInnerKeyF[] should be specified
type ECache[PK any, K comparable, V any] struct {
	lock           sync.Mutex
	maxSize        int
	items          *iterable.Map[K, pair[PK, V]]
	inflight       map[K]chan struct{}
	createNewF     CreatePoolElemF[PK, V]
	onDeleteF      OnDeleteElemF[PK, V]
	mapToInnerKeyF MapToInnerKeyF[PK, K]
}

type pair[PK any, V any] struct {
	pk PK
	v  V
}

// CreatePoolElemF function type for creating new pool elements
type CreatePoolElemF[K any, V any] func(k K) (V, error)
type OnDeleteElemF[K any, V any] func(k K, v V)
type MapToInnerKeyF[V any, K any] func(V) K

// NewECache creates new pool object. It expects the maximum pull size (maxSize) and the create new
// element function in the parameters
func NewECache[PK any, K comparable, V any](maxSize int, toComparableF MapToInnerKeyF[PK, K], createNewF CreatePoolElemF[PK, V], onDeleteF OnDeleteElemF[PK, V]) (*ECache[PK, K, V], error) {
	if maxSize < 1 {
		return nil, fmt.Errorf("NewECache(): the maxSize=%d, but it cannot be less than 1", maxSize)
	}
	if createNewF == nil {
		return nil, fmt.Errorf("NewECache(): createNewF must not be nil")
	}
	p := new(ECache[PK, K, V])
	p.items = iterable.NewMap[K, pair[PK, V]]()
	p.inflight = make(map[K]chan struct{})
	p.maxSize = maxSize
	p.createNewF = createNewF
	p.onDeleteF = onDeleteF
	p.mapToInnerKeyF = toComparableF
	return p, nil
}

// GetOrCreate returns an existing pool element or create the new one by its key
func (p *ECache[PK, K, V]) GetOrCreate(pk PK) (V, error) {
	k := p.mapToInnerKeyF(pk)
	for {
		p.lock.Lock()
		if res, ok := p.items.Get(k); ok {
			// make it recently used, but adding to the end of the list ...
			p.items.Remove(k)
			p.items.Add(k, res)
			p.lock.Unlock()
			return res.v, nil
		}
		ch, watcher := p.inflight[k]
		if !watcher {
			ch = make(chan struct{})
			p.inflight[k] = ch
		}
		p.lock.Unlock()

		// if watcher is true, it means that another goroutine already creating the new item,
		// so it needs to wait for the result instead of requesting new value.
		if watcher {
			<-ch
			continue
		}

		v, err := p.createNewF(pk)

		p.lock.Lock()
		close(ch)
		delete(p.inflight, k)
		if err == nil {
			p.items.Add(k, pair[PK, V]{pk, v})
			if p.maxSize < p.items.Len() {
				k, _ := p.items.First()
				v, _ := p.items.Get(k)
				p.items.Remove(k)
				if p.onDeleteF != nil {
					p.onDeleteF(v.pk, v.v)
				}
			}
		}
		p.lock.Unlock()

		return v, err
	}
}

// Remove deletes the element by key k. It returns true if the element
// was in the collection and false if it was not found
func (p *ECache[PK, K, V]) Remove(pk PK) bool {
	k := p.mapToInnerKeyF(pk)
	p.lock.Lock()
	defer p.lock.Unlock()

	v, ok := p.items.Get(k)
	if !ok {
		return false
	}
	p.items.Remove(k)
	if p.onDeleteF != nil {
		p.onDeleteF(v.pk, v.v)
	}
	return true
}

// Clear cleans up the cache removing all elements. The function will return number of the elements deleted
func (p *ECache[PK, K, V]) Clear() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	it := p.items.Iterator()
	removed := 0
	for it.HasNext() {
		e, ok := it.Next()
		if !ok {
			continue
		}
		p.items.Remove(e.Key)
		if p.onDeleteF != nil {
			p.onDeleteF(e.Value.pk, e.Value.v)
		}
		removed++
	}
	return removed
}
