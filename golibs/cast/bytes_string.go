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
package cast

import (
	"reflect"
	"unsafe"
)

// StringToByteArray gets a string and turns it to []byte without extra memory allocations
//
// NOTE! Using this function is extremely dangerous, so it can be used with
// extra care with clear understanding how it works
func StringToByteArray(v string) []byte {
	var slcHdr reflect.SliceHeader
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&v))
	slcHdr.Data = sh.Data
	slcHdr.Cap = sh.Len
	slcHdr.Len = sh.Len
	return *(*[]byte)(unsafe.Pointer(&slcHdr))
}

// ByteArrayToString turns a slice of bytes to string, without extra memory allocations
//
// NOTE! Using this function is extremely dangerous, so it can be used with
// extra care with clear understanding how it works
func ByteArrayToString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}
