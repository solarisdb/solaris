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
package transport

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_Addr(t *testing.T) {
	cfg := Config{Network: "tcp", Address: "hello.com", Port: 1234}
	assert.Equal(t, "hello.com:1234", cfg.Addr())
}

func TestScanAddr(t *testing.T) {
	cfg, err := ScanAddr("abcd:1234")
	assert.Nil(t, err)
	assert.Equal(t, Config{Address: "abcd", Port: 1234}, cfg)

	cfg, err = ScanAddr("abcd.1234")
	assert.Nil(t, err)
	assert.Equal(t, Config{Address: "abcd.1234", Port: 0}, cfg)

	cfg, err = ScanAddr("abcdL:.1234")
	assert.NotNil(t, err)
}
