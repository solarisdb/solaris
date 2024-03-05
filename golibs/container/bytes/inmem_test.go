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
package bytes

import (
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPositioningInMem(t *testing.T) {
	ib := NewInMemBytes(100)
	buf := []byte{1, 2, 3, 4, 5}

	res, err := ib.Buffer(12, len(buf))
	assert.Nil(t, err)
	assert.Equal(t, len(buf), len(res))
	copy(res, buf)

	res, _ = ib.Buffer(13, len(buf)-1)
	assert.Equal(t, buf[1:], res)

	res, err = ib.Buffer(98, 10)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))

	res, err = ib.Buffer(980, 10)
	assert.NotNil(t, err)
}

func TestCloseInMem(t *testing.T) {
	ib := NewInMemBytes(100)
	assert.Nil(t, ib.Close())

	assert.Equal(t, int64(0), ib.Size())

	assert.True(t, errors.Is(ib.Close(), errors.ErrClosed))
}
