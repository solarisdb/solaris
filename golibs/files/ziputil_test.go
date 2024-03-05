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
package files

import (
	"errors"
	errors2 "github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestZipWriter(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	zw, err := NewZipWriter(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	createFile(filepath.Join(dir, "aaa", "file2"), "asdf")
	createFile(filepath.Join(dir, "file3"), "asdf")

	zipCopyFile(t, zw, filepath.Join(dir, "aaa", "file2"), filepath.Join("aaa", "file2"))
	zipCopyFile(t, zw, filepath.Join(dir, "file3"), "file3")
	zw.Close()

	zi, err := NewZipIterator(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)
	res := make(map[string]bool)
	for f := zi.Next(); f != nil; f = zi.Next() {
		res[f.Name] = true
	}
	assert.Equal(t, map[string]bool{"aaa/file2": true, "file3": true}, res)
}

func TestZipCopy(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	zw, err := NewZipWriter(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	createFile(filepath.Join(dir, "aaa", "file2"), "asdf3")
	createFile(filepath.Join(dir, "file3"), "asdf2")

	zipCopyFile(t, zw, filepath.Join(dir, "aaa", "file2"), filepath.Join("aaa", "file2"))
	zipCopyFile(t, zw, filepath.Join(dir, "file3"), "file3")
	zw.Close()

	zi, err := NewZipIterator(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)
	zw, err = NewZipWriter(filepath.Join(dir, "file2.zip"))
	assert.Nil(t, err)
	assert.Nil(t, ZipCopy(zw, zi, "hello"))
	zw.Close()
	zi, err = NewZipIterator(filepath.Join(dir, "file2.zip"))
	assert.Nil(t, err)

	res := make(map[string]bool)
	for f := zi.Next(); f != nil; f = zi.Next() {
		res[f.Name] = true
	}
	assert.Equal(t, map[string]bool{"hello/aaa/file2": true, "hello/file3": true}, res)
}

func TestZipWriterRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	zw, err := NewZipWriter(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)

	w, err := zw.Create("file1234")
	assert.Nil(t, err)
	w.Write([]byte("abcde"))

	w, err = zw.Create("file1234")
	assert.True(t, errors.Is(errors2.ErrExist, err))
	zw.Close()

	zr, err := NewZipIterator(filepath.Join(dir, "file1.zip"))
	assert.Nil(t, err)
	r, err := zr.Next().Open()
	assert.Nil(t, err)
	var buf [123]byte
	n, err := r.Read(buf[:])
	assert.Equal(t, 5, n)
	assert.Equal(t, "abcde", string(buf[:n]))
	zr.Close()
}

func zipCopyFile(t *testing.T, zw *ZipWriter, filename, dstFileName string) {
	in, err := os.Open(filename)
	assert.Nil(t, err)
	defer in.Close()
	out, err := zw.Create(dstFileName)
	io.Copy(out, in)
}
