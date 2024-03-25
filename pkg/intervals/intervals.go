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
	// Basis contains constants and functions for work with values of type T
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
		B   Basis[T]
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

// NewBasis creates new basis
func NewBasis[T any](min, max T, cmpF func(v1, v2 T) int) Basis[T] {
	return Basis[T]{Min: min, Max: max, CmpF: cmpF}
}

// ===================== interval =====================

// Open create an open interval `(1, 5)`
func Open[T any](L, R T, B Basis[T]) Interval[T] {
	if B.CmpF(L, R) > 0 {
		panic(fmt.Sprintf("invalid open interval L > R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, B: B}
}

// OpenL create a left-open interval `(1, 5]`
func OpenL[T any](L, R T, B Basis[T]) Interval[T] {
	if B.CmpF(L, R) >= 0 {
		panic(fmt.Sprintf("invalid open L interval L >= R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, B: B, RIn: true}
}

// OpenR creates a right-open interval `[1, 5)`
func OpenR[T any](L, R T, B Basis[T]) Interval[T] {
	if B.CmpF(L, R) >= 0 {
		panic(fmt.Sprintf("invalid open R interval L >= R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, B: B, LIn: true}
}

// Closed creates a closed interval `[1, 5]`
func Closed[T any](L, R T, B Basis[T]) Interval[T] {
	if B.CmpF(L, R) > 0 {
		panic(fmt.Sprintf("invalid closed interval L > R: %v >= %v", L, R))
	}
	return Interval[T]{L: L, R: R, B: B, LIn: true, RIn: true}
}

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

// Negate negates the current interval, meaning that it returns
// the complement of the current interval.
func (i Interval[T]) Negate() []Interval[T] {
	// open
	if i.IsOpen() {
		if i.B.CmpF(i.L, i.R) == 0 { // !(L, L) = [min, max]
			return []Interval[T]{Closed(i.B.Min, i.B.Max, i.B)}
		}
		// !(L, R) = [min, L] & [R, max]
		return []Interval[T]{Closed(i.B.Min, i.L, i.B), Closed(i.R, i.B.Max, i.B)}
	}
	//half-open
	if i.IsOpenL() {
		if i.B.CmpF(i.R, i.B.Max) == 0 { // !(L, max] = [min, L]
			return []Interval[T]{Closed(i.B.Min, i.L, i.B)}
		}
		// !(L, R] = [min, L] & (R, max]
		return []Interval[T]{Closed(i.B.Min, i.L, i.B), OpenL(i.R, i.B.Max, i.B)}
	}
	if i.IsOpenR() {
		if i.B.CmpF(i.L, i.B.Min) == 0 { // ![min, R) = [R, max]
			return []Interval[T]{Closed(i.R, i.B.Max, i.B)}
		}
		// ![L, R) = [min, L) & [R, max]
		return []Interval[T]{OpenR(i.B.Min, i.L, i.B), Closed(i.R, i.B.Max, i.B)}
	}
	// closed
	if i.IsClosed() {
		if i.B.CmpF(i.L, i.B.Min) == 0 && i.B.CmpF(i.R, i.B.Max) == 0 { // ![min, max] = (L, L)
			return []Interval[T]{Open(i.L, i.L, i.B)}
		}
		if i.B.CmpF(i.L, i.B.Min) == 0 { // ![min, R] = (R, max]
			return []Interval[T]{OpenL(i.R, i.B.Max, i.B)}
		}
		if i.B.CmpF(i.R, i.B.Max) == 0 { // ![L, max] = [min, L)
			return []Interval[T]{OpenR(i.B.Min, i.L, i.B)}
		}
		// ![L, R] = [min, L) && (R, max]
		return []Interval[T]{OpenR(i.B.Min, i.L, i.B), OpenL(i.R, i.B.Max, i.B)}
	}
	panic("unknown interval type")
}

// Intersect returns the intersection of the given interval
// with the current interval. If no intersection is found the
// function returns false in the second return value.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are considered
// to have an intersection and the `(L=1, R=3).Intersect((L=2, R=4))`
// call returns `(L=2, R=3)`.
func (i Interval[T]) Intersect(o Interval[T]) (Interval[T], bool) {
	if i.Before(o) || i.After(o) {
		return Interval[T]{}, false
	}
	res := Interval[T]{B: i.B}
	if i.StartsBefore(o) {
		res.L = o.L
		res.LIn = o.LIn
	} else {
		res.L = i.L
		res.LIn = i.LIn
	}
	if i.EndsAfter(o) {
		res.R = o.R
		res.RIn = o.RIn
	} else {
		res.R = i.R
		res.RIn = i.RIn
	}
	return res, true
}

// Union returns the union of the given interval with the current interval
// only if there is an intersection. If no intersection is found the function
// returns false in the second return value.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// the `(L=1, R=3).Union((L=2, R=4))` call returns `(L=1, R=4)`.
func (i Interval[T]) Union(o Interval[T]) (Interval[T], bool) {
	if i.Before(o) || i.After(o) {
		return Interval[T]{}, false
	}
	res := Interval[T]{B: i.B}
	if i.StartsBefore(o) {
		res.L = i.L
		res.LIn = i.LIn
	} else {
		res.L = o.L
		res.LIn = o.LIn
	}
	if i.EndsAfter(o) {
		res.R = i.R
		res.RIn = i.RIn
	} else {
		res.R = o.R
		res.RIn = o.RIn
	}
	return res, true
}

// After returns true if the L border of the current interval
// is lesser than the R border of the given interval.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// and the `(L=2, R=4).After.((L=1, R=3))` call returns false.
func (i Interval[T]) After(o Interval[T]) bool {
	// o.R] [i.L
	if (i.IsClosed() || i.IsOpenR()) && (o.IsClosed() || o.IsOpenL()) {
		return i.B.CmpF(i.L, o.R) > 0
	}
	// o.R) [i.L | o.R] (i.L | o.R) (i.L
	return i.B.CmpF(i.L, o.R) >= 0
}

// Before returns true if the R border of current interval
// is lesser than the L border of the given interval.
// NOTE: The `(L=1, R=3)` and `(L=2, R=4)` intervals are
// considered to have an intersection (see Intersection description)
// and the `(L=1, R=3).Before.((L=2, R=4))` call returns false.
func (i Interval[T]) Before(o Interval[T]) bool {
	// i.R] [o.L
	if (i.IsClosed() || i.IsOpenL()) && (o.IsClosed() || o.IsOpenR()) {
		return i.B.CmpF(i.R, o.L) < 0
	}
	// i.R) [o.L | i.R] (o.L | i.R) (o.L
	return i.B.CmpF(i.R, o.L) <= 0
}

// StartsBefore returns true if the L border of the current interval
// is lesser than the L border of the given interval.
// NOTE: The `(L=2, ...` and `[L=3, ...` starts are equivalent, but
// the function returns true for `(L=2, R=5).StartsBefore([L=3, R=4])`.
func (i Interval[T]) StartsBefore(o Interval[T]) bool {
	// [i.L (o.L
	if (i.IsClosed() || i.IsOpenR()) && (o.IsOpen() || o.IsOpenL()) {
		return i.B.CmpF(i.L, o.L) <= 0
	}
	// [i.L [o.L | (i.L (o.L | (i.L [o.L
	return i.B.CmpF(i.L, o.L) < 0
}

// EndsAfter returns true if the R border of the current interval
// is greater than the R border of the given interval.
// NOTE: The `..., R=5)` and `..., R=4]` ends are equivalent, but
// the function returns true for `(L=2, R=5).EndsAfter([L=3, R=4])`.
func (i Interval[T]) EndsAfter(o Interval[T]) bool {
	// o.R) i.R]
	if (i.IsClosed() || i.IsOpenL()) && (o.IsOpen() || o.IsOpenR()) {
		return i.B.CmpF(i.R, o.R) >= 0
	}
	// o.R] i.R] | o.R) i.R) | o.R] i.R)
	return i.B.CmpF(i.R, o.R) > 0
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
