package inmem

import (
	"bytes"
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/sss"
	"io"
	"io/ioutil"
	"strings"
)

// Storage provides kvs.Storage functionality in local process memory. This instance can be
// used in a single-node configuration or in a test-purposes.
type Storage struct {
	storage map[string][]byte
}

var _ sss.Storage = (*Storage)(nil)

// NewStorage creates new instance of Storage
func NewStorage() *Storage {
	kim := &Storage{}
	kim.storage = make(map[string][]byte)
	return kim
}

// Get allows to read a value by its key. If key is not found the
// ErrNotFound should be returned
func (st *Storage) Get(key string) (io.ReadCloser, error) {
	if !sss.IsKeyValid(key) {
		return nil, fmt.Errorf("Storage.Get(): invalid key=%s", key)
	}

	if buf, ok := st.storage[key]; ok {
		return ioutil.NopCloser(bytes.NewReader(buf)), nil
	}

	return nil, errors.ErrNotExist
}

// Put allows to store value represented by reader r by the key
func (st *Storage) Put(key string, r io.Reader) error {
	if !sss.IsKeyValid(key) {
		return fmt.Errorf("Storage.Put(): invalid key=%s", key)
	}

	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b1 := make([]byte, len(buf))
	copy(b1, buf)
	st.storage[key] = b1
	return nil
}

// List returns a list of keys and sub-paths (part of an existing path which
// is a path itself), which have the prefix of the path argument
//
// Example:
// for the keys list: "/abc", "/def/abc", "/def/aa1"
// List("/") -> "/abc", "/def/"
// List("/def/") -> "/def/abc", "/def/aa1"
func (st *Storage) List(path string) ([]string, error) {
	if !sss.IsPathValid(path) {
		return nil, fmt.Errorf("Storage.List(): invalid path=%s", path)
	}

	res := make([]string, 0, 10)
	added := make(map[string]bool)
	for k := range st.storage {
		if strings.HasPrefix(k, path) {
			idx := strings.Index(k[len(path):], "/")
			if idx == -1 {
				idx = len(k)
			} else {
				idx += len(path) + 1
			}
			val := k[:idx]
			if _, ok := added[val]; !ok {
				res = append(res, val)
				added[val] = true
			}
		}
	}
	return res, nil
}

// Delete allows to delete a value by key. If the key doesn't exist, the operation
// will return no error
func (st *Storage) Delete(key string) error {
	if !sss.IsKeyValid(key) {
		return fmt.Errorf("Storage.Delete(): invalid key=%s", key)
	}

	delete(st.storage, key)
	return nil
}
