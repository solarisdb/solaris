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
package iterable

import (
	"fmt"
	"sync"
)

type (
	// Map implements a map with an mapIterator capability. The mapIterator allows considering elements from start
	// to end in the order of the elements were added into the map. It is safe to remove and add new elements into
	// the map while an iterator exists, it will support the order the elements were added into the map
	Map[K comparable, V any] struct {
		vals map[K]*rlItem[K, V]
		head *rlItem[K, V]
		last *rlItem[K, V]
		pool sync.Pool
	}

	// MapEntry implements a record in the Map, which contains Key and the Value for the record
	MapEntry[K comparable, V any] struct {
		Key   K
		Value V
	}

	// mapIterator structure provides an iterator pattern over the Map structure. It
	// implements the Iterator interface, which allows to move through the iterable.Map
	mapIterator[K comparable, V any] struct {
		im  *Map[K, V]
		ptr *rlItem[K, V]
	}

	rlItem[K comparable, V any] struct {
		state  int
		prev   *rlItem[K, V]
		next   *rlItem[K, V]
		refCnt int
		key    K
		val    V
	}
)

const (
	rlLast = iota
	rlOk
	rlDeleted
)

// NewMap creates the new instance of Map[K, V]
func NewMap[K comparable, V any]() *Map[K, V] {
	im := new(Map[K, V])
	im.vals = make(map[K]*rlItem[K, V])
	head := &rlItem[K, V]{state: rlLast}
	im.head, im.last = head, head
	im.pool = sync.Pool{New: func() any { return &rlItem[K, V]{} }}
	return im
}

// Iterator returns &mapIterator[K, V] object, which implements the Iterator[MapEntry[K, V]] interface.
// The iterator must be released (Closed) after usage for the proper resource management
func (im *Map[K, V]) Iterator() Iterator[MapEntry[K, V]] {
	im.head.refCnt++
	return &mapIterator[K, V]{im: im, ptr: im.head}
}

// Add allows adding new key-value pair into the map. The function returns error if the key already
// exists in the map
func (im *Map[K, V]) Add(k K, v V) error {
	if _, ok := im.vals[k]; ok {
		return fmt.Errorf("the Map alredy has value for the key=%v", k)
	}
	rliNew := im.pool.Get().(*rlItem[K, V])
	im.last = im.last.putVal(k, v, rliNew)
	im.vals[k] = im.last.prev
	return nil
}

// Get allows returning value by its key
func (im *Map[K, V]) Get(k K) (V, bool) {
	if rli, ok := im.vals[k]; ok {
		return rli.val, true
	}
	return *new(V), false
}

// Remove removes the value by its key
func (im *Map[K, V]) Remove(k K) {
	if rli, ok := im.vals[k]; ok {
		head := rli.delete()
		if head != nil {
			im.head = head
		}
		if rli.refCnt == 0 {
			im.pool.Put(rli)
		}
		delete(im.vals, k)
	}
}

// Len returns current map size
func (im *Map[K, V]) Len() int {
	return len(im.vals)
}

// First returns the first key and the whether the key exist or not
func (im *Map[K, V]) First() (K, bool) {
	it := im.Iterator()
	defer it.Close()
	e, res := it.Next()
	return e.Key, res
}

func (im *Map[K, V]) getValue(p *rlItem[K, V]) *rlItem[K, V] {
	if p.state == rlDeleted {
		p = im.next(p)
	}
	return p
}

func (im *Map[K, V]) release(p *rlItem[K, V]) {
	p.refCnt--
	if p.state == rlDeleted {
		p.delete()
		if p.refCnt == 0 {
			im.pool.Put(p)
		}
	}
}

func (im *Map[K, V]) next(p *rlItem[K, V]) *rlItem[K, V] {
	for {
		if p.state == rlLast {
			return p
		}
		p.refCnt--
		if p.state == rlDeleted && p.refCnt <= 0 {
			np := p.next
			head := p.delete()
			if head != nil {
				im.head = head
			}
			im.pool.Put(p)
			p = np
			p.refCnt++
		} else {
			p = p.next
			p.refCnt++
		}
		if p.state != rlDeleted {
			break
		}
	}
	return p
}

// delete removes the element and returns new head if it is changed. otherwise it returns nil
func (rli *rlItem[K, V]) delete() *rlItem[K, V] {
	if rli.state == rlLast {
		return nil
	}
	rli.val = *new(V)
	if rli.refCnt == 0 {
		if rli.prev != nil {
			rli.prev.next = rli.next
			rli.next.prev = rli.prev
			rli.next, rli.prev = nil, nil
			return nil
		}
		rli.next.prev = nil
		head := rli.next
		rli.next = nil
		return head
	}
	rli.state = rlDeleted
	return nil
}

func (rli *rlItem[K, V]) putVal(k K, v V, rliNew *rlItem[K, V]) *rlItem[K, V] {
	if rli.state != rlLast {
		panic("only last element can be used for adding new value")
	}
	rliNew.prev = rli
	rliNew.next = nil
	rliNew.state = rlLast
	rli.next = rliNew
	rli.state = rlOk
	rli.key = k
	rli.val = v
	return rli.next
}

// HasNext returns true if the map contains next element for the iterator. Please see Next() function
func (it *mapIterator[K, V]) HasNext() bool {
	it.ptr = it.im.getValue(it.ptr)
	return it.ptr.state != rlLast
}

// Next returns the next element and shifts the iterator to next one if it exists.
// This function may return default values for K and V types, if the Next element does
// not exist.
// An imparity may be observed between HasNext() and Next() functions results if the
// element the iterator was pointing to was removed in between this 2 calls. This case the
// HasNext() will return true, but the Next() will returns default values because the element
// is deleted.
func (it *mapIterator[K, V]) Next() (MapEntry[K, V], bool) {
	it.ptr = it.im.getValue(it.ptr)
	has := it.ptr.state != rlLast
	k, v := it.ptr.key, it.ptr.val
	it.ptr = it.im.next(it.ptr)
	return MapEntry[K, V]{k, v}, has
}

// Close closes the iterator and releases resources. The iterator object must not be used after the call.
// The function must be always called for any iterator to release the resources properly. Violating the
// contract may cause a memory leak.
func (it *mapIterator[K, V]) Close() error {
	it.im.release(it.ptr)
	it.ptr = nil
	return nil
}
