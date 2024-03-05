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
package strutil

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

// Hash type represents a hash value
type Hash interface {
	Bytes() []byte
	String() string

	unimplemented()
}

type hash struct {
	data    []byte
	encoded string
}

// String returns the hash string for h
func (h hash) String() string {
	return h.encoded
}

// Bytes returns the hash value as []byte
func (h hash) Bytes() []byte {
	return h.data
}

func (h hash) unimplemented() {
	panic("fake method")
}

// NewSha256ForData returns the sha256 Hash for the data provided
func NewSha256ForData(data []byte) (Hash, error) {
	h := sha256.New()
	h.Write(data)
	return CreateHash(h.Sum(nil))
}

// CreateHash returns the Hash value by buf
func CreateHash(buf []byte) (Hash, error) {
	return createHash(buf, true)
}

func isBufOkForHash(buf []byte) bool {
	return len(buf) == 32
}

func createHash(buf []byte, create bool) (hash, error) {
	if !isBufOkForHash(buf) {
		return hash{}, fmt.Errorf("the hash size should be 32 bytes long")
	}
	res := buf
	if create {
		res := make([]byte, len(buf))
		copy(res, buf)
	}
	return hash{res, base64.URLEncoding.EncodeToString(res)}, nil
}

// ParseHash returns the Hash value for the provided string
func ParseHash(h string) (Hash, error) {
	buf, err := base64.URLEncoding.DecodeString(h)
	if err != nil {
		return hash{}, err
	}
	return createHash(buf, false)
}

// SumHashes returns the hash value for the list of hashes
func SumHashes(hashes ...Hash) Hash {
	h := sha256.New()
	for _, hsh := range hashes {
		h.Write((hsh.(hash).data))
	}
	res, _ := CreateHash(h.Sum(nil))
	return res
}

// RandomHash generates a pseudo-random Hash value
func RandomHash() Hash {
	buf := make([]byte, 32)
	rand.Read(buf)
	h, _ := CreateHash(buf)
	return h
}
