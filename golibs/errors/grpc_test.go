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

func TestFromGRPCError(t *testing.T) {
	assert.Nil(t, FromGRPCError(nil))
	assert.Nil(t, FromGRPCError(status.Errorf(codes.OK, "ha ha")))
	assert.Equal(t, ErrCanceled, FromGRPCError(status.Errorf(codes.Canceled, "ha ha")))
	assert.Equal(t, ErrCommunication, FromGRPCError(status.Errorf(codes.Unknown, "ha ha")))
	assert.Equal(t, ErrCommunication, FromGRPCError(status.Errorf(codes.DeadlineExceeded, "ha ha")))
	assert.Equal(t, ErrExhausted, FromGRPCError(status.Errorf(codes.ResourceExhausted, "ha ha")))
	assert.Equal(t, ErrInvalid, FromGRPCError(status.Errorf(codes.InvalidArgument, "ha ha")))
	assert.Equal(t, ErrNotExist, FromGRPCError(status.Errorf(codes.NotFound, "ha ha")))
	assert.Equal(t, ErrDataLoss, FromGRPCError(status.Errorf(codes.DataLoss, "ha ha")))
	assert.Equal(t, ErrExist, FromGRPCError(status.Errorf(codes.AlreadyExists, "ha ha")))
	assert.Equal(t, ErrNotAuthorized, FromGRPCError(status.Errorf(codes.PermissionDenied, "ha ha")))
	assert.Equal(t, ErrNotAuthorized, FromGRPCError(status.Errorf(codes.Unauthenticated, "ha ha")))
	assert.Equal(t, ErrInternal, FromGRPCError(status.Errorf(codes.Aborted, "ha ha")))
	assert.Equal(t, ErrConflict, FromGRPCError(status.Errorf(codes.FailedPrecondition, "ha ha")))
	assert.Equal(t, ErrInternal, FromGRPCError(status.Errorf(codes.OutOfRange, "ha ha")))
	assert.Equal(t, ErrUnimplemented, FromGRPCError(status.Errorf(codes.Unimplemented, "ha ha")))
	assert.Equal(t, ErrInternal, FromGRPCError(status.Errorf(codes.Internal, "ha ha")))
	assert.Equal(t, ErrInternal, FromGRPCError(status.Errorf(codes.Unavailable, "ha ha")))
}

func TestGRPCStatusCode(t *testing.T) {
	assert.Equal(t, codes.OK, GRPCStatusCode(nil))
	assert.Equal(t, codes.OK, GRPCStatusCode(status.Errorf(codes.OK, "ddd")))
	assert.Equal(t, codes.InvalidArgument, GRPCStatusCode(status.Errorf(codes.InvalidArgument, "ddd")))
	assert.Equal(t, codes.InvalidArgument, GRPCStatusCode(ErrInvalid))
	assert.Equal(t, codes.InvalidArgument, GRPCStatusCode(fmt.Errorf("ddd:%w", ErrInvalid)))
	assert.Equal(t, codes.Internal, GRPCStatusCode(fmt.Errorf("ddd:%w", ErrClosed)))
	assert.Equal(t, codes.FailedPrecondition, GRPCStatusCode(fmt.Errorf("ddd:%w", ErrConflict)))
	assert.Equal(t, codes.PermissionDenied, GRPCStatusCode(fmt.Errorf("ddd:%w", ErrNotAuthorized)))
}

func TestGRPCWrap(t *testing.T) {
	var err error
	assert.Equal(t, err, GRPCWrap(err))
	err = status.Errorf(codes.InvalidArgument, "ddd")
	assert.Equal(t, err, GRPCWrap(err))
	err = GRPCWrap(ErrInvalid)
	assert.NotEqual(t, ErrInvalid, err)
	assert.Equal(t, codes.InvalidArgument, GRPCStatusCode(err))
}

func TestFromGRPCErrorMsg(t *testing.T) {
	assert.Equal(t, "", FromGRPCErrorMsg(nil))
	assert.Equal(t, "", FromGRPCErrorMsg(status.Errorf(codes.OK, "ha ha")))
	assert.Equal(t, "ha ha", FromGRPCErrorMsg(status.Errorf(codes.InvalidArgument, "ha ha")))
}
