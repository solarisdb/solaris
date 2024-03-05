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
	"crypto/sha256"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestHashDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)

	createFile(filepath.Join(dir, "f1"), "file1")
	h1, err = HashDir(dir, nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)
	h2, err = HashDir(dir, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestHashDir_doesntExit(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	h1, err := HashDir(filepath.Join(dir, "haha"), nil, true)
	assert.Nil(t, err)
	assert.Nil(t, h1)
}

func TestHashDir_DifferentCtx(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	createFile(filepath.Join(dir, "f1"), "file1")
	createFile(filepath.Join(dir2, "f1"), "diffCtx")
	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)
}

func TestHashDir_DifferentNames(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	createFile(filepath.Join(dir, "f1"), "file1")
	createFile(filepath.Join(dir2, "f2"), "file1")
	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)
}

func TestHashDir_SameCtx(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	createFile(filepath.Join(dir, "f1"), "file1")
	createFile(filepath.Join(dir2, "f1"), "file1")
	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestHashDir_SubfoldersSame(t *testing.T) {
	h := sha256.New()
	fmt.Println(h.Sum(nil))

	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir2, "aaa"))
	createFile(filepath.Join(dir, "aaa", "f1"), "file1")
	createFile(filepath.Join(dir2, "aaa", "f1"), "file1")
	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestHashDir_SubfoldersDiff(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir2, "aaa"))
	createFile(filepath.Join(dir, "aaa", "f1"), "file1")
	createFile(filepath.Join(dir2, "f1"), "file1")
	h1, err := HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)
}

func TestHashDir_NonRecursive(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir, "ccc"))
	EnsureDirExists(filepath.Join(dir2, "aaa"))
	EnsureDirExists(filepath.Join(dir2, "bbb"))
	createFile(filepath.Join(dir, "aaa", "f1"), "file1")
	createFile(filepath.Join(dir2, "aaa", "f1"), "dddd")

	createFile(filepath.Join(dir, "f1"), "correct")
	createFile(filepath.Join(dir2, "f1"), "correct")
	createFile(filepath.Join(dir, "f2"), "correct2")
	createFile(filepath.Join(dir2, "f2"), "correct2")

	h1, err := HashDir(dir, nil, false)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, false)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)

	h1, err = HashDir(dir, nil, true)
	assert.Nil(t, err)
	h2, err = HashDir(dir2, nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)

}

func TestHashDir_NonRecursiveTestFunc(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dir2, err := ioutil.TempDir("", "test2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir2)

	createFile(filepath.Join(dir, "f1"), "correct")
	createFile(filepath.Join(dir2, "f1"), "correct")
	createFile(filepath.Join(dir, "f2"), "correct2")
	createFile(filepath.Join(dir2, "f22"), "correct22")

	h1, err := HashDir(dir, nil, false)
	assert.Nil(t, err)
	h2, err := HashDir(dir2, nil, false)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)

	f1Only := func(fi os.FileInfo) bool {
		return fi.Name() == "f1"
	}
	hh1, err := HashDir(dir, f1Only, false)
	assert.Nil(t, err)
	hh2, err := HashDir(dir2, f1Only, false)
	assert.Nil(t, err)
	assert.Equal(t, hh1, hh2)
	assert.NotEqual(t, h1, hh2)
	assert.NotEqual(t, h1, hh1)
}

func createFile(name, data string) {
	f, _ := os.Create(name)
	f.WriteString(data)
	f.Close()
}
