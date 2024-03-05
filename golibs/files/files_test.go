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
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetRoot(t *testing.T) {
	// "" => ""
	// "/" => ""
	// "/abc" => ""
	// "/abc/" => "abc"
	// "/abc/def.js" => "abc"
	// "/abc/ddd/def.js" => "abc"
	// "abc/ddd/def.js" => "abc"
	br, file := GetRoot("")
	assert.Equal(t, "", br)
	assert.Equal(t, "", file)
	br, file = GetRoot("/")
	assert.Equal(t, "", br)
	assert.Equal(t, "", file)
	br, file = GetRoot("///")
	assert.Equal(t, "", br)
	assert.Equal(t, "", file)
	br, file = GetRoot("/abc")
	assert.Equal(t, "", br)
	assert.Equal(t, "abc", file)
	br, file = GetRoot("/abc/")
	assert.Equal(t, "abc", br)
	assert.Equal(t, "", file)
	br, file = GetRoot("abc/")
	assert.Equal(t, "abc", br)
	assert.Equal(t, "", file)
	br, file = GetRoot("/abc/def")
	assert.Equal(t, "abc", br)
	assert.Equal(t, "def", file)
	br, file = GetRoot("/abc/asdf/")
	assert.Equal(t, "abc", br)
	assert.Equal(t, "asdf", file)
	br, file = GetRoot("/abc/asdf/ddd.js")
	assert.Equal(t, "abc", br)
	assert.Equal(t, "asdf/ddd.js", file)
}

func TestListDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	// empty dir
	fis := ListDir(dir)
	assert.Equal(t, 0, len(fis))

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir, "aaa", "bbb")) // this must be ignored as subdir
	fis = ListDir(dir)
	assert.Equal(t, 1, len(fis))
	assert.Equal(t, "aaa", fis[0].Name())

	createFile(filepath.Join(dir, "aaa", "f2"), "file1")
	createFile(filepath.Join(dir, "f1"), "file1")

	fis = ListDir(dir)
	assert.Equal(t, 2, len(fis))
	assert.Equal(t, "aaa", fis[0].Name())
	assert.Equal(t, "f1", fis[1].Name())

	// last one - no dir
	os.RemoveAll(dir)
	fis = ListDir(dir)
	assert.Equal(t, 0, len(fis))
}

func TestZipUnzip(t *testing.T) {
	dir, err := ioutil.TempDir("", "testZip")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir, "aaa", "bbb")) // this must be ignored as subdir
	createFile(filepath.Join(dir, "aaa", "f2"), "file1")
	createFile(filepath.Join(dir, "aaa", "f1"), "file2")
	createFile(filepath.Join(dir, "aaa/bbb", "f2"), "file2")

	assert.Nil(t, ZipFolder(filepath.Join(dir, "aaa"), filepath.Join(dir, "aaa.ziputil"), nil, true))
	assert.Nil(t, UnzipToFolder(filepath.Join(dir, "aaa.ziputil"), filepath.Join(dir, "unzipped")))

	h1, err := HashDir(filepath.Join(dir, "aaa"), nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(filepath.Join(dir, "unzipped"), nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)

	os.Remove(filepath.Join(dir, "aaa.ziputil"))
	os.RemoveAll(filepath.Join(dir, "unzipped"))

	assert.Nil(t, ZipFolder(filepath.Join(dir, "aaa"), filepath.Join(dir, "aaa.ziputil"), nil, false))
	assert.Nil(t, UnzipToFolder(filepath.Join(dir, "aaa.ziputil"), filepath.Join(dir, "unzipped")))

	h1, err = HashDir(filepath.Join(dir, "aaa"), nil, false)
	assert.Nil(t, err)
	h2, err = HashDir(filepath.Join(dir, "unzipped"), nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestZipFilterUnzip(t *testing.T) {
	dir, err := ioutil.TempDir("", "testZip")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	EnsureDirExists(filepath.Join(dir, "aaa"))
	EnsureDirExists(filepath.Join(dir, "aaa", "bbb")) // this must be ignored as subdir
	createFile(filepath.Join(dir, "aaa", "f2"), "file1")
	createFile(filepath.Join(dir, "aaa", "f1"), "file2")
	createFile(filepath.Join(dir, "aaa/bbb", "f2"), "file2")
	createFile(filepath.Join(dir, "aaa/bbb", "f1"), "file2")

	assert.Nil(t, ZipFolder(filepath.Join(dir, "aaa"), filepath.Join(dir, "aaa.ziputil"), func(path string) bool {
		return strings.HasSuffix(path, "f2")
	}, true))
	assert.Nil(t, UnzipToFolder(filepath.Join(dir, "aaa.ziputil"), filepath.Join(dir, "unzipped")))

	h1, err := HashDir(filepath.Join(dir, "aaa"), nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(filepath.Join(dir, "unzipped"), nil, true)
	assert.Nil(t, err)
	assert.NotEqual(t, h1, h2)

	os.Remove(filepath.Join(dir, "aaa", "f1"))
	os.Remove(filepath.Join(dir, "aaa/bbb", "f1"))
	h1, err = HashDir(filepath.Join(dir, "aaa"), nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestZipUnzipEmptyFolder(t *testing.T) {
	dir, err := ioutil.TempDir("", "testZip")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	EnsureDirExists(filepath.Join(dir, "aaa"))

	assert.Nil(t, ZipFolder(filepath.Join(dir, "aaa"), filepath.Join(dir, "aaa.ziputil"), nil, true))
	assert.Nil(t, UnzipToFolder(filepath.Join(dir, "aaa.ziputil"), filepath.Join(dir, "unzipped")))

	h1, err := HashDir(filepath.Join(dir, "aaa"), nil, true)
	assert.Nil(t, err)
	h2, err := HashDir(filepath.Join(dir, "unzipped"), nil, true)
	assert.Nil(t, err)
	assert.Equal(t, h1, h2)
}

func TestCreateRandomDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dd, err := CreateRandomDir(dir, "__TEST__")
	assert.Nil(t, err)
	_, file := filepath.Split(dd)
	assert.True(t, strings.HasPrefix(file, "__TEST__"))
	_, err = os.Stat(dd)
	assert.Nil(t, err)
}

func TestCreateRandomFileName(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dd, err := CreateRandomFileName(dir, "__TEST__")
	assert.Nil(t, err)
	_, file := filepath.Split(dd)
	assert.True(t, strings.HasPrefix(file, "__TEST__"))
	_, err = os.Stat(dd)
	assert.True(t, os.IsNotExist(err))
}

func TestCopyFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "copyTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	fromDir := filepath.Join(dir, "from")
	toDir := filepath.Join(dir, "to")
	_ = EnsureDirExists(fromDir)
	_ = EnsureDirExists(filepath.Join(fromDir, "bbb"))
	createFile(filepath.Join(fromDir, "file1"), "la la")
	createFile(filepath.Join(fromDir, "file2"), "la la")
	createFile(filepath.Join(fromDir, "bbb", "file3"), "la la")
	assert.Nil(t, CopyDir(fromDir, toDir))
	_, err = os.Stat(filepath.Join(toDir, "file3"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(toDir, "bbb"))
	assert.Nil(t, err)
	_, err = os.Stat(filepath.Join(toDir, "file1"))
	assert.Nil(t, err)
	_, err = os.Stat(filepath.Join(toDir, "file2"))
	assert.Nil(t, err)

	h1, _ := HashDir(fromDir, nil, true)
	h2, _ := HashDir(toDir, nil, true)
	assert.Equal(t, h1, h2)
}

func TestRemoveFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "copyTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	fromDir := filepath.Join(dir, "from")
	toDir := filepath.Join(dir, "to")
	_ = EnsureDirExists(fromDir)
	_ = EnsureDirExists(filepath.Join(fromDir, "bbb"))
	_ = EnsureDirExists(filepath.Join(fromDir, "emptyfolder"))
	createFile(filepath.Join(fromDir, "file1"), "la la11")
	createFile(filepath.Join(fromDir, "file2"), "la la333")
	createFile(filepath.Join(fromDir, "bbb", "file1"), "la la222")

	_ = EnsureDirExists(toDir)
	createFile(filepath.Join(toDir, "file2"), "la la333")

	h1, _ := HashDir(fromDir, nil, true)
	h2, _ := HashDir(toDir, nil, true)
	assert.NotEqual(t, h1, h2)

	assert.Nil(t, RemoveFiles(fromDir, func(pth string, fi os.FileInfo) bool { return fi.IsDir() || fi.Name() == "file1" }))
	h1, _ = HashDir(fromDir, nil, true)
	h2, _ = HashDir(toDir, nil, true)
	assert.Equal(t, h1, h2)
}

func TestIsDirEmpty(t *testing.T) {
	dir, err := ioutil.TempDir("", "copyTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	e, err := IsDirEmpty(filepath.Join(dir, "doesntexist"))
	assert.False(t, e)
	assert.NotNil(t, err)

	EnsureDirExists(filepath.Join(dir, "exists"))
	e, err = IsDirEmpty(filepath.Join(dir, "exists"))
	assert.True(t, e)
	assert.Nil(t, err)

	createFile(filepath.Join(dir, "exists", "file"), "ddd")
	e, err = IsDirEmpty(filepath.Join(dir, "exists"))
	assert.False(t, e)
	assert.Nil(t, err)
}
