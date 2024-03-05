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
package kvs

import (
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRecord_Copy(t *testing.T) {
	r := Record{Key: "aa", Version: "333", Value: []byte("ddfd"), ExpiresAt: cast.Ptr(time.Now())}
	assert.Equal(t, r, r.Copy())
}
