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
package golibs

type (
	// Reseter is the interface that wraps the Reset method.
	//
	// Some implementations may support reset mechanism, which allows to reset
	// an object to its initial state. The objects may be considered resetable
	// if they support this interface.
	Reseter interface {
		// Reset allows to reset the object to the initial state. Result may
		// indicate about an error during the reset.
		Reset() error
	}
)
