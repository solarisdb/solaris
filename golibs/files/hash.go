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
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/strutil"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

// HashDir calculates SHA256 for the whole dir context. Context is the file names and their data
// if recursive is true then the hash will be calculated for all sub-folders as well, otherwise
// only the files in path will be included, but all directories in path will be ignored.
func HashDir(path string, testFunc func(fi os.FileInfo) bool, recursive bool) (strutil.Hash, error) {
	path = ensureDirName(path)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil
	}

	files := make(map[string]bool, 10)
	files[path] = false

	filepath.Walk(path, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			// probably not found?
			return nil
		}
		if !recursive {
			dir, _ := filepath.Split(pth)
			dir = ensureDirName(dir)
			if dir != path || info.IsDir() {
				return nil
			}
		}

		if testFunc != nil && !testFunc(info) {
			return nil
		}

		files[pth] = !info.IsDir()
		return nil
	})

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		h.Write(cast.StringToByteArray(name[len(path):]))
		if files[name] {
			data, err := ioutil.ReadFile(name)
			if err != nil {
				return nil, err
			}
			h.Write(data)
		}
	}

	return strutil.CreateHash(h.Sum(nil))
}
