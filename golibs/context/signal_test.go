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
package context

import (
	"github.com/stretchr/testify/assert"
	"syscall"
	"testing"
)

func TestNewSignalsContext(t *testing.T) {
	ctx := NewSignalsContext(syscall.SIGINT)
	assert.Nil(t, ctx.Err())
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	<-ctx.Done()
	assert.NotNil(t, ctx.Err())
}
