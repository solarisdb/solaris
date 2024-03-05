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
package cast

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestString(t *testing.T) {
	assert.Equal(t, "aaa", String(nil, "aaa"))
	assert.Equal(t, "ppp", String(StringPtr("ppp"), "aaa"))
}

func TestBool(t *testing.T) {
	assert.Equal(t, true, Bool(nil, true))
	assert.Equal(t, true, Bool(BoolPtr(true), false))
}

func TestInt(t *testing.T) {
	assert.Equal(t, 11, Int(nil, 11))
	assert.Equal(t, 22, Int(IntPtr(22), 23))
}

func TestInt64(t *testing.T) {
	assert.Equal(t, int64(11), Int64(nil, 11))
	assert.Equal(t, int64(22), Int64(Int64Ptr(22), 23))
}

func TestValue(t *testing.T) {
	now := time.Now()
	assert.Equal(t, time.Time{}, Value[time.Time](nil, time.Time{}))
	assert.Equal(t, now, Value(Ptr(now), time.Time{}))
}
