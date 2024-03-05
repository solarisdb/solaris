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
	"reflect"
)

// WriteDesc is used by WriteToManyWithControl to describe write channels
type WriteDesc[T any] struct {
	// DonceChan is the done indicator. The channel can be used to interrupt the function
	// for indicating that the WrtChan should not be used for write
	DoneChan <-chan struct{}
	// WrtChan is the channel where a write operation attempt should be made
	WrtChan chan<- T
}

// WriteToManyWithControl tries to write to many channels with control channels (WriteDesc). The value v will be
// written to only one channel and its index will be returned. The WriteDesc.DoneChan cannot read anything, but be closed.
// If the WriteDesc.DoneChan is closed, the second return param will be false. If the value is written the second param
// will be true.
func WriteToManyWithControl[V any](chnsDescs []WriteDesc[V], v V) (int, bool) {
	if len(chnsDescs) == 0 {
		panic("the WriteToManyWithControl cannot be called with no channels")
	}
	cases := make([]reflect.SelectCase, 0, len(chnsDescs)*2)
	for _, ch := range chnsDescs {
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.DoneChan)})
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(ch.WrtChan), Send: reflect.ValueOf(v)})
	}
	idx, _, _ := reflect.Select(cases)
	return idx / 2, idx&1 == 1
}
