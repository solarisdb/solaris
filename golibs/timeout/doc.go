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
Package timeout contains only one public function, which allows calling functions in the future.
The call request may be canceled if the execution of the function is not started yet.

One of the examples, when it can be used, is an operation timeout or a watchdog functionality -
a special action should be taken if the request for the action is not canceled in the
specific time.
*/
package timeout
