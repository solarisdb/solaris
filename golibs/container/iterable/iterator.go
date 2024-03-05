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

// Iterator interface provides some functions that allow to move over some sorted collection.
// It has three functions: HasNext(), Next() and Close() that can be used for iterating over
// the collection elements and releasing resources after usage.
type Iterator[V any] interface {
	// HasNext returns true if the collection contains next element for the iterator. Please see Next() function
	HasNext() bool

	// Next returns the next element and shifts the iterator to next one if it exists. Second
	// parameter indicates that the iterator returned a value. This function may return default
	// value for the type V, if the Next element does not exist (second parameter is false).
	//
	// The imparity may be observed between HasNext() and Next() functions results if the
	// element the iterator was pointing to was the last element and it was removed in between this 2 calls.
	// This case the HasNext() will return true, but the Next() will returns default values because the element
	// is deleted.
	Next() (V, bool)

	// Close closes the iterator and releases resources. The iterator object must not be used after the call.
	// The function must be always called for any iterator to release the resources properly. Violating the
	// contract may cause a memory leak.
	Close() error
}

type EmptyIterator[V any] struct{}

var _ Iterator[int] = (*EmptyIterator[int])(nil)

func (ei *EmptyIterator[V]) HasNext() bool {
	return false
}

func (ei *EmptyIterator[V]) Next() (V, bool) {
	return *new(V), false
}

func (ei *EmptyIterator[V]) Close() error {
	return nil
}
