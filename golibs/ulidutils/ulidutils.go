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
package ulidutils

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// New returns new ulid.ULID.
func New() ulid.ULID {
	return ulid.Make()
}

// NewUUID returns new ulid.ULID converted to uuid.UUID.
func NewUUID() uuid.UUID {
	return uuid.UUID(New())
}

// NewID returns new ulid.ULID in string format. The returned ID can be compared
// to any other result returned by the function. An ID returned earlier is less lexicographically
// to the ID returned after the first one.
func NewID() string {
	return New().String()
}

// NextID returns theoretical next ulid, which may follow by the ulidID. The returned
// value may be used for search records that with IDs followed by ulidID
//
// The value must never be used for generating new ID. Use NewID() instead
func NextID(ulidID string) string {
	uID, err := ulid.Parse(ulidID)
	if err != nil {
		panic(fmt.Sprintf("could not parse ULID=%q: %v", ulidID, err))
	}
	for i := 15; i >= 0; i-- {
		uID[i] += 1
		if uID[i] != 0 {
			break
		}
	}
	return uID.String()
}

func PrevID(ulidID string) string {
	uID, err := ulid.Parse(ulidID)
	if err != nil {
		panic(fmt.Sprintf("could not parse ULID=%q: %v", ulidID, err))
	}
	for i := 15; i >= 0; i-- {
		uID[i]--
		if uID[i] != 255 {
			break
		}
	}
	return uID.String()
}
