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
package errors

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func TestIs(t *testing.T) {
	assert.True(t, Is(fmt.Errorf("fddd %w", ErrNotExist), ErrNotExist))
	assert.True(t, Is(status.Errorf(codes.NotFound, "ha ha"), ErrNotExist))
	assert.False(t, Is(status.Errorf(codes.Unknown, "ha ha"), ErrNotExist))
	assert.False(t, Is(fmt.Errorf("fddd %s", ErrNotExist), ErrNotExist))
}

func TestEmbedObject(t *testing.T) {
	assert.Panics(t, func() {
		EmbedObject(123, nil)
	})
	assert.Panics(t, func() {
		EmbedObject(nil, ErrInvalid)
	})
	err := EmbedObject(1234, ErrInvalid)
	assert.True(t, Is(err, ErrInvalid))
	assert.Panics(t, func() {
		EmbedObject(123434, err)
	})
	var i int
	assert.True(t, ExtractObject(err, &i))
	assert.Equal(t, 1234, i)

	assert.False(t, ExtractObject(nil, &i))
	assert.False(t, ExtractObject(ErrInvalid, &i))
	assert.False(t, ExtractObject(fmt.Errorf("%sla la la", jsonErrorMarker), &i))
	assert.False(t, ExtractObject(fmt.Errorf("%sla la la%s", jsonErrorMarker, jsonErrorMarker), &i))
	assert.True(t, ExtractObject(fmt.Errorf("%s5%s", jsonErrorMarker, jsonErrorMarker), &i))
	assert.Equal(t, 5, i)
}

func TestGRPCWrapEmbeddings(t *testing.T) {
	err := GRPCWrap(EmbedObject(1234, ErrInvalid))
	var i int
	assert.True(t, ExtractObject(err, &i))
	assert.Equal(t, 1234, i)
}
