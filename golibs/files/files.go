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
	"archive/zip"
	"fmt"
	"github.com/solarisdb/solaris/golibs/strutil"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// GetRoot receives absolute or relative file name and returns first folder.
// examples:
// "" => "", ""
// "/" => "", ""
// "/abc" => "", "abc"
// "/abc/" => "abc", ""
// "/abc/def.js" => "abc", "def.js"
// "/abc/ddd/def.js" => "abc", "ddd/def.js"
// "abc/ddd/def.js" => "abc", "ddd/def.js"
func GetRoot(path string) (string, string) {
	if len(path) == 0 {
		return "", ""
	}

	lastSlash := path[len(path)-1] == '/'

	path = filepath.Clean(path)
	if path[0] == '/' {
		path = path[1:]
	}

	idx := strings.IndexRune(path, '/')
	if idx < 0 {
		if lastSlash {
			return path, ""
		}
		return "", path
	}

	return path[:idx], path[idx+1:]
}

// EnsureDirExists checks whether the dir exists and create the new one if it doesn't
func EnsureDirExists(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0740)
		}
	} else {
		d.Close()
	}

	if err != nil {
		return fmt.Errorf("ensure dir %s returns error: %w", dir, err)
	}
	return nil
}

func ensureDirName(path string) string {
	if path == "" {
		return ""
	}
	if path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	return path
}

// ListDir returns files and directories non-recursive (in the dir provided only)
func ListDir(dir string) []os.FileInfo {
	dir = ensureDirName(dir)
	res := make([]os.FileInfo, 0, 10)
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		fpath, _ := filepath.Split(path)
		if ensureDirName(fpath) != dir {
			return nil
		}

		res = append(res, info)
		return nil
	})

	return res
}

// ZipFolder archives srcDir content into destFile.
// The testFunc allows to filter files. If it is provided it will be called for every found file to test
// whether it should be zipped(true) or not(false).
// The recursive param indicates whether sub-folders should be added recursively or not
func ZipFolder(srcDir, destFile string, testFunc func(string) bool, recursive bool) error {
	srcDir = ensureDirName(srcDir)
	f, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("ZipFolder: could not create %s for write, err=%w", destFile, err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if testFunc != nil && !testFunc(path) {
			return nil
		}

		if !recursive {
			dir, _ := filepath.Split(path)
			dir = ensureDirName(dir)
			if dir != srcDir {
				// skipping subfolders
				return nil
			}
		}

		dstFileName := path[len(srcDir):]
		out, err := w.Create(dstFileName)
		if err != nil {
			return fmt.Errorf("ZipFolder: could not write %s into %s, err=%w", path, dstFileName, err)
		}

		in, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("ZipFolder: could not write %s into %s, err=%w", path, dstFileName, err)
		}
		io.Copy(out, in)
		in.Close()
		return nil
	})
	w.Close()
	if err != nil {
		os.Remove(destFile)
	}
	return err
}

// UnzipToFolder unzips zipFile content into the destDir
func UnzipToFolder(zipFile, destDir string) error {
	zi, err := NewZipIterator(zipFile)
	if err != nil {
		return err
	}
	defer zi.Close()

	err = EnsureDirExists(destDir)
	if err != nil {
		return fmt.Errorf("UnzipToFolder: could not create the dest dir %s err=%w", destDir, err)
	}
	pathChecked := make(map[string]bool)
	for z := zi.Next(); z != nil; z = zi.Next() {
		if z.FileInfo().IsDir() {
			continue
		}

		partPath, _ := filepath.Split(z.Name)
		destPath := filepath.Join(destDir, partPath)
		if !pathChecked[destPath] {
			err := EnsureDirExists(destPath)
			if err != nil {
				return fmt.Errorf("UnzipToFolder: could not create folder %s err=%w", destPath, err)
			}
			pathChecked[destPath] = true
		}

		in, err := z.Open()
		if err != nil {
			return fmt.Errorf("UnzipToFolder: cannot open file \"%s\" in the ziputil archive: %w", z.Name, err)
		}

		destFile := filepath.Join(destDir, z.Name)
		out, err := os.Create(destFile)
		if err != nil {
			in.Close()
			return fmt.Errorf("UnzipToFolder: could not open file %s for write, err=%w", destFile, err)
		}

		_, err = io.Copy(out, in)
		in.Close()
		out.Close()
		if err != nil {
			return fmt.Errorf("UnzipToFolder: could not write into %s from %s, err=%w", destDir, z.Name, err)
		}
	}

	return nil
}

// CreateRandomDir creates a randomly name directory in the path with prefix
func CreateRandomDir(path, prefix string) (string, error) {
	return ensureUndique(path, prefix, true)
}

// CreateRandomFileName in the path with prefix, but without creating new file there
func CreateRandomFileName(path, prefix string) (string, error) {
	return ensureUndique(path, prefix, false)
}

// RemoveFiles by path if testFunc() returns true for the FileInfo. The function
// walks into the folders recursively and a folder could be removed if all files from
// the folder are removed as well. testFunc allows to control whether to check a folder
// or not...
func RemoveFiles(path string, testFunc func(path string, fi os.FileInfo) bool) error {
	finfs := ListDir(path)
	for _, fi := range finfs {
		if !testFunc(path, fi) {
			continue
		}

		fileName := filepath.Join(path, fi.Name())
		if fi.IsDir() {
			err := RemoveFiles(filepath.Join(path, fi.Name()), testFunc)
			if err != nil {
				return err
			}
			// ignore the error if not empty
			os.Remove(fileName)
			continue
		}

		if err := os.Remove(fileName); err != nil {
			return err
		}
	}
	return nil
}

// IsDirEmpty returns weather the dir provided by the name is empty or not
func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// CopyDir copies dir by path "from" to the dir by path "to"
func CopyDir(from, to string) error {
	err := EnsureDirExists(to)
	if err != nil {
		return err
	}

	finfos := ListDir(from)
	for _, fi := range finfos {
		if fi.IsDir() {
			err := CopyDir(filepath.Join(from, fi.Name()), filepath.Join(to, fi.Name()))
			if err != nil {
				return err
			}
			continue
		}
		err := copyFile(filepath.Join(from, fi.Name()), filepath.Join(to, fi.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteTo writes the in stream to the toPath
func WriteTo(toPath string, in io.Reader) error {
	out, err := os.Create(toPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

// copyFile copies one file by path "from" to the file by path "to"
func copyFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	return WriteTo(to, in)
}

func ensureUndique(path, prefix string, createDir bool) (string, error) {
	for {
		name := prefix + strutil.RandomString(64)
		filename := filepath.Join(path, name)
		_, err := os.Stat(filename)
		if os.IsNotExist(err) {
			err = nil
			if createDir {
				err = EnsureDirExists(filename)
			}
			return filename, err
		}
	}
}
