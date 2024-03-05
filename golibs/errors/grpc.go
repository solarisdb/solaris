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
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var grpcToErrors = map[codes.Code]error{
	codes.OK:                 nil,
	codes.Canceled:           ErrCanceled,
	codes.Unknown:            ErrCommunication,
	codes.DeadlineExceeded:   ErrCommunication,
	codes.ResourceExhausted:  ErrExhausted,
	codes.InvalidArgument:    ErrInvalid,
	codes.NotFound:           ErrNotExist,
	codes.AlreadyExists:      ErrExist,
	codes.Unauthenticated:    ErrNotAuthorized,
	codes.PermissionDenied:   ErrNotAuthorized,
	codes.DataLoss:           ErrDataLoss,
	codes.Unimplemented:      ErrUnimplemented,
	codes.FailedPrecondition: ErrConflict,
}

var errorsToCode = map[error]codes.Code{
	ErrExist:         codes.AlreadyExists,
	ErrNotExist:      codes.NotFound,
	ErrInvalid:       codes.InvalidArgument,
	ErrNotAuthorized: codes.PermissionDenied,
	ErrInternal:      codes.Internal,
	ErrDataLoss:      codes.DataLoss,
	ErrExhausted:     codes.ResourceExhausted,
	ErrUnimplemented: codes.Unimplemented,
	ErrConflict:      codes.FailedPrecondition,
	ErrCanceled:      codes.Canceled,
}

// FromGRPCError receives a gRPC error (code-based) and returns the  one of the
// general errors (ErrNotFound, ErrClosed...)
func FromGRPCError(err error) error {
	if err, ok := grpcToErrors[status.Code(err)]; ok {
		return err
	}
	return ErrInternal
}

// FromGRPCErrorMsg receives a gRPC status error message
func FromGRPCErrorMsg(err error) string {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

// GRPCStatusCode returns the gRPC error status code by the error provided
func GRPCStatusCode(err error) codes.Code {
	code := status.Code(err)
	if code != codes.Unknown {
		return code
	}
	if code, ok := errorsToCode[err]; ok {
		return code
	}
	for e, c := range errorsToCode {
		if errors.Is(err, e) {
			return c
		}
	}
	return codes.Internal
}

// GRPCWrap allows to get an error and wrap it to the grpc response error.
// you may use the function to report gRPC error from your server side like:
// ```
//
//	func RemoteCall(pbRequest *pb.Request) (*pb.Response, error) {
//	   ...
//	   return response, errors.GRPCWrap(err)
//	}
//
// If you need to extend the set of errors responses that are not covered
// by either gRPC codes, or by the package, please encode your error in the
// ErrInternal or codes.Internal error.
func GRPCWrap(err error) error {
	if code := status.Code(err); code != codes.Unknown {
		return err // return err as is, it is already a gRPC formed error
	}
	return status.Error(GRPCStatusCode(err), err.Error())
}
