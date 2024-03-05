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
Package context adds some utility functions to work with the standard context.Context objects:
- NewSignalsContext(): creates a context that will be closed when one of some specified
  system signals are sent to the program
- Sleep(): the goroutine sleeping with the context functionality (the goroutine sleep will
  be interrupted if the provided context is closed)
- WrapChannel(): the function allows to wrap of a channel and creates a context object, which
  will be closed if the channel is closed.
*/
package context
