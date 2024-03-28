// Copyright 2024 The Solaris Authors
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

package intervals

import (
	"cmp"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

type (
	// Basis represents a basis for a set of vectors,
	// it defines what [min,max] range the vectors have and
	// what operations are available for the vectors, like:
	// create, intersect, union, etc.
	Basis[T any] struct {
		Min  T                  // min value of the type T
		Max  T                  // max value of the type T
		CmpF func(v1, v2 T) int // comparison function of T type values
	}

	// Interval represents an interval of values of type T,
	// interval can be open, half-open, closed.
	Interval[T any] struct {
		L   T
		R   T
		LIn bool
		RIn bool
	}
)

var (
	// BasisInt is a default basis for type int
	BasisInt = NewBasis(math.MinInt, math.MaxInt, cmp.Compare[int])

	// BasisString is a default basis for type string
	BasisString = NewBasis("", string(utf8.MaxRune), cmp.Compare[string])

	// BasisTime is a default basis for type time.Time
	BasisTime = NewBasis(
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2070, 1, 1, 0, 0, 0, 0, time.UTC),
		func(t1, t2 time.Time) int {
			if t1.Before(t2) {
				return -1
			}
			if t1.Equal(t2) {
				return 0
			}
			return 1
		},
	)
)

// ===================== basis =====================

// NewBasis creates new basis
func NewBasis[T any](min, max T, cmpF func(v1, v2 T) int) Basis[T] {
	return Basis[T]{Min: min, Max: max, CmpF: cmpF}
}

// Open create an open interval `(1, 5)`
func (b Basis[T]) Open(L, R T) Interval[T] {
	if b.CmpF(L, R) > 0 {
		panic(fmt.Sprintf("invalid open interval L > R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R}
}

// OpenL create a left-open interval `(1, 5]`
func (b Basis[T]) OpenL(L, R T) Interval[T] {
	if b.CmpF(L, R) >= 0 {
		panic(fmt.Sprintf("invalid open L interval L >= R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, RIn: true}
}

// OpenR creates a right-open interval `[1, 5)`
func (b Basis[T]) OpenR(L, R T) Interval[T] {
	if b.CmpF(L, R) >= 0 {
		panic(fmt.Sprintf("invalid open R interval L >= R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, LIn: true}
}

// Closed creates a closed interval `[1, 5]`
func (b Basis[T]) Closed(L, R T) Interval[T] {
	if b.CmpF(L, R) > 0 {
		panic(fmt.Sprintf("invalid closed interval L > R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, LIn: true, RIn: true}
}

// Negate negates the current interval, meaning that it returns
// the complement of the current interval.
func (b Basis[T]) Negate(i Interval[T]) []Interval[T] {
	// open
	if i.IsOpen() {
		if b.CmpF(i.L, i.R) == 0 { // !(L, L) = [min, max]
			return []Interval[T]{b.Closed(b.Min, b.Max)}
		}
		// !(L, R) = [min, L] & [R, max]
		return []Interval[T]{b.Closed(b.Min, i.L), b.Closed(i.R, b.Max)}
	}
	//half-open
	if i.IsOpenL() {
		if b.CmpF(i.R, b.Max) == 0 { // !(L, max] = [min, L]
			return []Interval[T]{b.Closed(b.Min, i.L)}
		}
		// !(L, R] = [min, L] & (R, max]
		return []Interval[T]{b.Closed(b.Min, i.L), b.OpenL(i.R, b.Max)}
	}
	if i.IsOpenR() {
		if b.CmpF(i.L, b.Min) == 0 { // ![min, R) = [R, max]
			return []Interval[T]{b.Closed(i.R, b.Max)}
		}
		// ![L, R) = [min, L) & [R, max]
		return []Interval[T]{b.OpenR(b.Min, i.L), b.Closed(i.R, b.Max)}
	}
	// closed
	if i.IsClosed() {
		if b.CmpF(i.L, b.Min) == 0 && b.CmpF(i.R, b.Max) == 0 { // ![min, max] = (L, L)
			return []Interval[T]{b.Open(i.L, i.L)}
		}
		if b.CmpF(i.L, b.Min) == 0 { // ![min, R] = (R, max]
			return []Interval[T]{b.OpenL(i.R, b.Max)}
		}
		if b.CmpF(i.R, b.Max) == 0 { // ![L, max] = [min, L)
			return []Interval[T]{b.OpenR(b.Min, i.L)}
		}
		// ![L, R] = [min, L) && (R, max]
		return []Interval[T]{b.OpenR(b.Min, i.L), b.OpenL(i.R, b.Max)}
	}
	panic("unknown interval type")
}

// Intersect returns the intersection of the i1 interval
// with the i2 interval. If no intersection is found the
// function returns false in the second return value.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are considered
// to have an intersection and the `(L=1, R=3).Intersect((L=2, R=4))`
// call returns `(L=2, R=3)`.
func (b Basis[T]) Intersect(i1, i2 Interval[T]) (Interval[T], bool) {
	if b.Before(i1, i2) || b.After(i1, i2) {
		return Interval[T]{}, false
	}
	res := Interval[T]{}
	if b.StartsBefore(i1, i2) {
		res.L = i2.L
		res.LIn = i2.LIn
	} else {
		res.L = i1.L
		res.LIn = i1.LIn
	}
	if b.EndsAfter(i1, i2) {
		res.R = i2.R
		res.RIn = i2.RIn
	} else {
		res.R = i1.R
		res.RIn = i1.RIn
	}
	return res, true
}

// Union returns the union of the i1 interval with the i2 interval
// only if there is an intersection. If no intersection is found the function
// returns false in the second return value.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// the `(L=1, R=3).Union((L=2, R=4))` call returns `(L=1, R=4)`.
func (b Basis[T]) Union(i1, i2 Interval[T]) (Interval[T], bool) {
	if b.Before(i1, i2) || b.After(i1, i2) {
		return Interval[T]{}, false
	}
	res := Interval[T]{}
	if b.StartsBefore(i1, i2) {
		res.L = i1.L
		res.LIn = i1.LIn
	} else {
		res.L = i2.L
		res.LIn = i2.LIn
	}
	if b.EndsAfter(i1, i2) {
		res.R = i1.R
		res.RIn = i1.RIn
	} else {
		res.R = i2.R
		res.RIn = i2.RIn
	}
	return res, true
}

// After returns true if the L border of the i1 interval
// is lesser than the R border of the i2 interval.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// and the `(L=2, R=4).After.((L=1, R=3))` call returns false.
func (b Basis[T]) After(i1, i2 Interval[T]) bool {
	// i2.R] [i1.L
	if (i1.IsClosed() || i1.IsOpenR()) && (i2.IsClosed() || i2.IsOpenL()) {
		return b.CmpF(i1.L, i2.R) > 0
	}
	// i2.R) [i1.L | i2.R] (i1.L | i2.R) (i1.L
	return b.CmpF(i1.L, i2.R) >= 0
}

// Before returns true if the R border of i1 interval
// is lesser than the L border of the i2 interval.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// and the `(L=1, R=3).Before.((L=2, R=4))` call returns false.
func (b Basis[T]) Before(i1, i2 Interval[T]) bool {
	// i1.R] [i2.L
	if (i1.IsClosed() || i1.IsOpenL()) && (i2.IsClosed() || i2.IsOpenR()) {
		return b.CmpF(i1.R, i2.L) < 0
	}
	// i1.R) [i2.L | i1.R] (i2.L | i1.R) (i2.L
	return b.CmpF(i1.R, i2.L) <= 0
}

// StartsBefore returns true if the L border of the i1 interval
// is lesser than the L border of the i2 interval.
// NOTE: The `(L=2, ...` and `[L=3, ...` starts are equivalent, but
// the function returns true for `(L=2, R=5).StartsBefore([L=3, R=4])`.
func (b Basis[T]) StartsBefore(i1, i2 Interval[T]) bool {
	// [i1.L (i2.L
	if (i1.IsClosed() || i1.IsOpenR()) && (i2.IsOpen() || i2.IsOpenL()) {
		return b.CmpF(i1.L, i2.L) <= 0
	}
	// [i1.L [i2.L | (i1.L (i2.L | (i1.L [i2.L
	return b.CmpF(i1.L, i2.L) < 0
}

// EndsAfter returns true if the R border of the i1 interval
// is greater than the R border of the i2 interval.
// NOTE: The `..., R=5)` and `..., R=4]` ends are equivalent, but
// the function returns true for `(L=2, R=5).EndsAfter([L=3, R=4])`.
func (b Basis[T]) EndsAfter(i1, i2 Interval[T]) bool {
	// i2.R) i1.R]
	if (i1.IsClosed() || i1.IsOpenL()) && (i2.IsOpen() || i2.IsOpenR()) {
		return b.CmpF(i1.R, i2.R) >= 0
	}
	// i2.R] i1.R] | i2.R) i1.R) | i2.R] i1.R)
	return b.CmpF(i1.R, i2.R) > 0
}

// ===================== interval =====================

// IsOpen returns true if interval is open
func (i Interval[T]) IsOpen() bool {
	return !i.LIn && !i.RIn
}

// IsOpenL returns true if interval is left-open
func (i Interval[T]) IsOpenL() bool {
	return !i.LIn && i.RIn
}

// IsOpenR returns true if interval is right-open
func (i Interval[T]) IsOpenR() bool {
	return i.LIn && !i.RIn
}

// IsClosed returns true if interval is closed
func (i Interval[T]) IsClosed() bool {
	return i.LIn && i.RIn
}

// String returns the string representation of the interval
func (i Interval[T]) String() string {
	var sb strings.Builder
	if i.LIn {
		sb.WriteByte('[')
	} else {
		sb.WriteByte('(')
	}
	sb.WriteString(fmt.Sprintf("%v, %v", i.L, i.R))
	if i.RIn {
		sb.WriteByte(']')
	} else {
		sb.WriteByte(')')
	}
	return sb.String()
}
