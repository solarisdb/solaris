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
	ctxt "context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSleep_DurationFirst(t *testing.T) {
	start := time.Now()
	Sleep(ctxt.Background(), 10*time.Millisecond)
	assert.True(t, time.Now().Sub(start) >= 10*time.Millisecond)
}

func TestSleep_CtxFirst(t *testing.T) {
	start := time.Now()
	ctx, _ := ctxt.WithTimeout(ctxt.Background(), 10*time.Millisecond)
	Sleep(ctx, time.Minute)
	assert.True(t, time.Now().Sub(start) >= 10*time.Millisecond)
	assert.True(t, time.Now().Sub(start) < time.Minute)
}
