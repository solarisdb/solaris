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

package files

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOpenCloseMMFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestOpenCloseMMFile")
	assert.Nil(t, err)
	defer os.RemoveAll(dir) // clean up

	fsz := int64(23451 * 4096)
	fn := path.Join(dir, "testFile")
	mmf, err := NewMMFile(fn, fsz)
	assert.Nil(t, err)
	defer mmf.Close()

	buf := []byte{1, 2, 3, 4, 5}
	res, err := mmf.Buffer(12345, len(buf))
	assert.Nil(t, err)
	copy(res, buf)

	res, err = mmf.Buffer(mmf.Size()-2, len(buf))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(res))
	copy(res, buf)

	mmf.Close()
	assert.NotEqual(t, -1, mmf.Size())

	mmf, err = NewMMFile(fn, -1)
	assert.Nil(t, err)
	defer mmf.Close()
	assert.Equal(t, fsz, mmf.Size())

	res, err = mmf.Buffer(12345, len(buf))
	assert.Nil(t, err)
	assert.Equal(t, buf, res)

	res, err = mmf.Buffer(mmf.Size()-3, len(buf))
	assert.Nil(t, err)
	assert.Equal(t, buf[:2], res[1:3])
}

func TestCreateNotExistingMMFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestCreateNotExistingMMFile")
	assert.Nil(t, err)
	defer os.RemoveAll(dir) // clean up

	_, err = NewMMFile(path.Join(dir, "testFile"), -1)
	assert.NotNil(t, err)
}

func TestGrowMMFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestGrowMMFile")
	assert.Nil(t, err)
	defer os.RemoveAll(dir) // clean up

	fsz := int64(8 * 4096)
	fn := path.Join(dir, "testFile")
	mmf, err := NewMMFile(fn, fsz)
	assert.Nil(t, err)
	defer mmf.Close()

	buf := []byte{1, 2, 3, 4, 5}
	res, err := mmf.Buffer(4093, len(buf))
	n := copy(res, buf)
	assert.Nil(t, err)
	assert.Equal(t, n, len(buf))

	err = mmf.Grow(4 * 4096)
	assert.NotNil(t, err)

	err = mmf.Grow(20*4096 - 1)
	assert.NotNil(t, err)
	assert.Equal(t, fsz, mmf.Size())

	err = mmf.Grow(20 * 4096)
	assert.Nil(t, err)
	assert.Equal(t, int64(20*4096), mmf.Size())

	res, err = mmf.Buffer(4093, len(buf))
	assert.Nil(t, err)
	assert.Equal(t, buf, res)
}

func TestParrallelMMFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestParrallelMMFile")
	assert.Nil(t, err)
	defer os.RemoveAll(dir) // clean up

	ps := os.Getpagesize()

	fsz := int64(ps * 10)
	fn := path.Join(dir, "testFile2")
	mmf, err := NewMMFile(fn, fsz)
	assert.Nil(t, err)
	defer mmf.Close()

	var wg sync.WaitGroup
	var errs int32

	for i := 0; i < 640; i++ {
		wg.Add(1)
		go func(pid int) {
			buf := make([]byte, 64)
			for j, _ := range buf {
				buf[j] = byte(pid)
			}
			for i := 0; i < 500; i++ {
				res, err := mmf.Buffer(int64(pid*64), len(buf))
				if len(res) != len(buf) || err != nil {
					fmt.Println("Error when write n=", len(res), ", err=", err)
					atomic.AddInt32(&errs, 1)
				}
				copy(res, buf)

				res, err = mmf.Buffer(int64(pid*64), len(buf))
				if len(res) != len(buf) || err != nil {
					fmt.Println("Error when read n=", len(res), ", err=", err)
					atomic.AddInt32(&errs, 1)
				}

				if !reflect.DeepEqual(buf, res) {
					fmt.Println("Error buf=", buf, ", res=", res)
					atomic.AddInt32(&errs, 1)
					break
				}
			}

			wg.Done()
		}(i)
	}
	wg.Wait()
	if atomic.LoadInt32(&errs) != 0 {
		t.Fatal(" errs=", atomic.LoadInt32(&errs))
	}
}
