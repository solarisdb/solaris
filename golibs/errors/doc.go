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
Package errors contains some very general class of errors that any service may
use. It is proposed to use the globally defined error variables to describe the
situations that may be transformed into an API response or a class of user-faced
errors.

The package also contains some gRPC helper functions that allows to encode the
general errors to the gRPC code-based errors, so the errors can be passed through
the distributed system.
*/
package errors
