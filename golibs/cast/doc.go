// Copyright 2023 The acquirecloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/*
Package cast contains some utility functions for casting types. The first version
contains the cast of the simple types (int, string, bool, etc.) to cast their scalar
variables to the pointers and pointers to the variables of the simple types to the
variables of the types. The casting maybe useful when it needs to distinguish whether
a value is passed from the default values (in JSON objects, for example). In this
case the target golang structures may use pointers to the types instead of the concrete
types to understand whether a value was not provided (pointer is nil), or what its value
if it is provided.
*/
package cast
