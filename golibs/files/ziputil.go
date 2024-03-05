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
	"io"
	"os"
	"path/filepath"
)

// ZipIterator provides an access to zip.Files
type ZipIterator interface {
	io.Closer
	// Next returns the next *zip.File or nil, if there is no ones anymore
	Next() *zip.File
}

// zipIterator struct allows to walk over files in the zip arhcive
type zipIterator struct {
	zr  *zip.Reader
	idx int
	f   *os.File
}

var _ ZipIterator = (*zipIterator)(nil)

// ZipWriter struct allows to write files into a new zip archive
type ZipWriter struct {
	zw      *zip.Writer
	f       *os.File
	created map[string]bool
}

// NewZipIterator returns the zipIterator for the zipFile provided
func NewZipIterator(zipFile string) (ZipIterator, error) {
	fi, err := os.Stat(zipFile)
	if err != nil {
		return nil, fmt.Errorf("Could not obtain file status %s: %w", zipFile, err)
	}

	f, err := os.Open(zipFile)
	if err != nil {
		return nil, fmt.Errorf("Could not open file %s for reading: %w", zipFile, err)
	}

	zr, err := zip.NewReader(f, fi.Size())
	if err != nil {
		return nil, err
	}
	return &zipIterator{zr: zr, idx: 0, f: f}, nil
}

// Next returns the next *zip.File or nil, if there is no one anymore
func (zi *zipIterator) Next() *zip.File {
	if zi.idx >= len(zi.zr.File) {
		return nil
	}
	idx := zi.idx
	zi.idx++
	return zi.zr.File[idx]
}

// Close implements io.Closer
func (zi *zipIterator) Close() error {
	if zi.f != nil {
		err := zi.f.Close()
		zi.f = nil
		return err
	}
	return nil
}

// NewZipWriter returns new ZipWriter
func NewZipWriter(zipFilename string) (*ZipWriter, error) {
	f, err := os.Create(zipFilename)
	if err != nil {
		return nil, fmt.Errorf("Could not create %s for write: %w", zipFilename, err)
	}

	zw := zip.NewWriter(f)
	return &ZipWriter{zw: zw, f: f, created: make(map[string]bool)}, nil
}

// Create creates the new file writer
func (zw *ZipWriter) Create(fileName string) (io.Writer, error) {
	if _, ok := zw.created[fileName]; ok {
		return nil, os.ErrExist
	}
	res, err := zw.zw.Create(fileName)
	if err == nil {
		zw.created[fileName] = true
	}
	return res, err
}

// Close implements io.Closer
func (zw *ZipWriter) Close() error {
	if zw == nil || zw.zw == nil {
		return nil
	}
	err := zw.zw.Close()
	zw.zw = nil
	if err == nil && zw.f != nil {
		err = zw.f.Close()
		zw.f = nil
	}
	return err
}

// ZipCopy copy files from zi to zw adding the prefix name to each file from zi
func ZipCopy(zw *ZipWriter, zi ZipIterator, prefix string) error {
	for f := zi.Next(); f != nil; f = zi.Next() {
		fn := filepath.Join(prefix, f.Name)
		w, err := zw.Create(fn)
		if err != nil {
			return fmt.Errorf("could not create new file with name %s: %w", fn, err)
		}
		in, err := f.Open()
		if err != nil {
			return fmt.Errorf("could not open file %s: %w", f.Name, err)
		}
		io.Copy(w, in)
		in.Close()
	}
	return nil
}
