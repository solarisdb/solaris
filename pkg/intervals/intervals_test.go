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
	"github.com/stretchr/testify/assert"
	"testing"
)

var b = BasisInt

func TestInterval_NegateOpen(t *testing.T) {
	// (min, max)
	i := b.Open(b.Min, b.Max)
	g := b.Negate(i)
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Min, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, b.Max, n2.R)
	assert.Equal(t, b.Max, n2.R)

	// (min, 1)
	i = b.Open(b.Min, 1)
	g = b.Negate(i)
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Min, n1.R)
	assert.True(t, n2.IsClosed()) // [1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// (1, max)
	i = b.Open(1, b.Max)
	g = b.Negate(i)
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, b.Max, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// (1, 2)
	i = b.Open(1, 2)
	g = b.Negate(i)
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// (1, 1)
	i = b.Open(1, 1)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsClosed()) // [min, max]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Max, n1.R)
}

func TestInterval_NegateOpenL(t *testing.T) {
	// (min, max]
	i := b.OpenL(b.Min, b.Max)
	n1 := b.Negate(i)[0]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Min, n1.R)

	// (1, max]
	i = b.OpenL(1, b.Max)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)

	// (min, 1]
	i = b.OpenL(b.Min, 1)
	g := b.Negate(i)
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Min, n1.R)
	assert.True(t, n2.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// (1, 2]
	i = b.OpenL(1, 2)
	g = b.Negate(i)
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsOpenL()) // (2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)
}

func TestInterval_NegateOpenR(t *testing.T) {
	// [min, max)
	i := b.OpenR(b.Min, b.Max)
	n1 := b.Negate(i)[0]
	assert.True(t, n1.IsClosed()) // [max, max]
	assert.Equal(t, b.Max, n1.L)
	assert.Equal(t, b.Max, n1.R)

	// [1, max)
	i = b.OpenR(1, b.Max)
	g := b.Negate(i)
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, b.Max, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// [min, 1)
	i = b.OpenR(b.Min, 1)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsClosed()) // [1, max]
	assert.Equal(t, i.R, n1.L)
	assert.Equal(t, b.Max, n1.R)

	// [1, 2)
	i = b.OpenR(1, b.Max)
	g = b.Negate(i)
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)
}

func TestInterval_NegateClosed(t *testing.T) {
	// [min, max]
	i := b.Closed(b.Min, b.Max)
	n1 := b.Negate(i)[0]
	assert.True(t, n1.IsOpen()) // (min, min)
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Min, n1.R)

	// [min, 1]
	i = b.Closed(b.Min, 1)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n1.L)
	assert.Equal(t, b.Max, n1.R)

	// [1, max]
	i = b.Closed(1, b.Max)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)

	// [1, 1]
	i = b.Closed(1, 2)
	g := b.Negate(i)
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, b.Max, n2.R)

	// [min, min]
	i = b.Closed(b.Min, b.Min)
	n1 = b.Negate(i)[0]
	assert.True(t, n1.IsOpenL()) // (min, max]
	assert.Equal(t, b.Min, n1.L)
	assert.Equal(t, b.Max, n1.R)
}

func TestInterval_IntersectOpen(t *testing.T) {
	i1 := b.Closed(1, 2)          // (1, 2)
	i2 := b.Closed(3, 4)          // (3, 4)
	i3, ok := b.Intersect(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 2)            // (1, 2)
	i2 = b.Open(2, 3)            // (2, 3)
	i3, ok = b.Intersect(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 3)            // (1, 3)
	i2 = b.Open(2, 4)            // (2, 3)
	i3, ok = b.Intersect(i1, i2) // (2, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_IntersectClosed(t *testing.T) {
	i1 := b.Closed(1, 2)          // [1, 2]
	i2 := b.Closed(3, 4)          // [3, 4]
	i3, ok := b.Intersect(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Closed(1, 2)          // [1, 2]
	i2 = b.Closed(2, 3)          // [2, 3]
	i3, ok = b.Intersect(i1, i2) // [2, 2]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)

	i1 = b.Closed(1, 3)          // [1, 3]
	i2 = b.Closed(2, 4)          // [2, 4]
	i3, ok = b.Intersect(i1, i2) // [2, 3]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_IntersectOpenClosed(t *testing.T) {
	i1 := b.Open(1, 2)            // (1, 2)
	i2 := b.Closed(2, 3)          // [2, 3]
	i3, ok := b.Intersect(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 2)            // (1, 2)
	i2 = b.Closed(3, 4)          // [3, 4]
	i3, ok = b.Intersect(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 3)            // (1, 3)
	i2 = b.Closed(2, 4)          // [2, 4]
	i3, ok = b.Intersect(i1, i2) // [2, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpenR())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)

	i1 = b.Open(1, 2)            // (1, 2)
	i2 = b.Closed(1, 2)          // [1, 2]
	i3, ok = b.Intersect(i1, i2) // (1, 2)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_UnionOpen(t *testing.T) {
	i1 := b.Open(1, 2)        // (1, 2)
	i2 := b.Open(3, 4)        // (3, 4)
	i3, ok := b.Union(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 2)        // (1, 2)
	i2 = b.Open(2, 3)        // (2, 3)
	i3, ok = b.Union(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Open(1, 3)        // (1, 3)
	i2 = b.Open(2, 4)        // (2, 3)
	i3, ok = b.Union(i1, i2) // (1, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)
}

func TestInterval_UnionClosed(t *testing.T) {
	i1 := b.Closed(1, 2)      // [1, 2]
	i2 := b.Closed(3, 4)      // [3, 4]
	i3, ok := b.Union(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Closed(1, 2)      // [1, 2)
	i2 = b.Closed(2, 3)      // [2, 3]
	i3, ok = b.Union(i1, i2) // [1, 3]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)

	i1 = b.Closed(1, 3)      // [1, 3]
	i2 = b.Closed(2, 4)      // [2, 4]
	i3, ok = b.Union(i1, i2) // [1, 4]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)
}

func TestInterval_UnionClosedOpen(t *testing.T) {
	i1 := b.Closed(1, 2)      // [1, 2]
	i2 := b.Open(3, 4)        // (3, 4)
	i3, ok := b.Union(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Closed(1, 2)      // [1, 2]
	i2 = b.Open(2, 3)        // (2, 3)
	i3, ok = b.Union(i1, i2) // none
	assert.False(t, ok)

	i1 = b.Closed(1, 3)      // [1, 3]
	i2 = b.Open(2, 4)        // (2, 4)
	i3, ok = b.Union(i1, i2) // [1, 4)
	assert.True(t, ok)
	assert.True(t, i3.IsOpenR())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)

	i1 = b.Closed(1, 2)      // [1, 2]
	i2 = b.Open(1, 2)        // (1, 2)
	i3, ok = b.Union(i1, i2) // [1, 2]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_String(t *testing.T) {
	assert.Equal(t, "(1, 2)", b.Open(1, 2).String())
	assert.Equal(t, "(1, 2]", b.OpenL(1, 2).String())
	assert.Equal(t, "[1, 2)", b.OpenR(1, 2).String())
	assert.Equal(t, "[1, 2]", b.Closed(1, 2).String())
}
