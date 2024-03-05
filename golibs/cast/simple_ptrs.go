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

// String turns the string pointer to the string type
func String(s *string, def string) string {
	if s != nil {
		return *s
	}
	return def
}

// StringPtr returns address of s
func StringPtr(s string) *string {
	return &s
}

// Bool returns *b or def if b == nil
func Bool(b *bool, def bool) bool {
	if b != nil {
		return *b
	}
	return def
}

// BoolPtr returns pointer to bool value which is equal to b
func BoolPtr(b bool) *bool {
	return &b
}

// Int returns *i if i != nil or def
func Int(i *int, def int) int {
	if i != nil {
		return *i
	}
	return def
}

// IntPtr returns pointer to the int value which is equal to i
func IntPtr(i int) *int {
	return &i
}

// Int64 returns *i if i != nil or def
func Int64(i *int64, def int64) int64 {
	if i != nil {
		return *i
	}
	return def
}

// Int64Ptr returns pointer to the int64 value which is equal to i
func Int64Ptr(i int64) *int64 {
	return &i
}

// Value is a generic function which allows to turn a pointer to the value of the ptr, or to the
// def, if the pointer is nil
func Value[T any](v *T, def T) T {
	if v != nil {
		return *v
	}
	return def
}

// Ptr is a generic function, which returns pointer to the type provided (v)
func Ptr[T any](v T) *T {
	return &v
}
