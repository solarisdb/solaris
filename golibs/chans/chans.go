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

// IsOpened checks the channel ch and returns true if it is still opened. The function
// may consume value from the channel if any, and it will be lost.
func IsOpened[V any](ch chan V) bool {
	select {
	case _, ok := <-ch:
		return ok
	default:
	}
	return ch != nil
}
