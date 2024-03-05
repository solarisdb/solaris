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

type intIterator struct {
	i   []int
	idx int
}

func WrapIntSlice(i []int) Iterator[int] {
	return (Iterator[int])(&intIterator{i, 0})
}

func (ii *intIterator) HasNext() bool {
	return ii.idx < len(ii.i)
}

func (ii *intIterator) Next() (int, bool) {
	if ii.idx < len(ii.i) {
		i := ii.idx
		ii.idx++
		return ii.i[i], true
	}
	return 0, false
}

func (ii *intIterator) Reset() error {
	ii.idx = 0
	return nil
}

func (ii *intIterator) Close() error {
	ii.i = nil
	ii.idx = 0
	return nil
}
