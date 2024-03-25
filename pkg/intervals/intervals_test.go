package intervals

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInterval_NegateOpen(t *testing.T) {
	// (min, max)
	i := Open(BasisInt.Min, BasisInt.Max, BasisInt)
	g := i.Negate()
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Min, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, BasisInt.Max, n2.R)
	assert.Equal(t, BasisInt.Max, n2.R)

	// (min, 1)
	i = Open(BasisInt.Min, 1, BasisInt)
	g = i.Negate()
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Min, n1.R)
	assert.True(t, n2.IsClosed()) // [1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// (1, max)
	i = Open(1, BasisInt.Max, BasisInt)
	g = i.Negate()
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, BasisInt.Max, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// (1, 2)
	i = Open(1, 2, BasisInt)
	g = i.Negate()
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// (1, 1)
	i = Open(1, 1, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsClosed()) // [min, max]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Max, n1.R)
}

func TestInterval_NegateOpenL(t *testing.T) {
	// (min, max]
	i := OpenL(BasisInt.Min, BasisInt.Max, BasisInt)
	n1 := i.Negate()[0]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Min, n1.R)

	// (1, max]
	i = OpenL(1, BasisInt.Max, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)

	// (min, 1]
	i = OpenL(BasisInt.Min, 1, BasisInt)
	g := i.Negate()
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, min]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Min, n1.R)
	assert.True(t, n2.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// (1, 2]
	i = OpenL(1, 2, BasisInt)
	g = i.Negate()
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsClosed()) // [min, 1]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsOpenL()) // (2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)
}

func TestInterval_NegateOpenR(t *testing.T) {
	// [min, max)
	i := OpenR(BasisInt.Min, BasisInt.Max, BasisInt)
	n1 := i.Negate()[0]
	assert.True(t, n1.IsClosed()) // [max, max]
	assert.Equal(t, BasisInt.Max, n1.L)
	assert.Equal(t, BasisInt.Max, n1.R)

	// [1, max)
	i = OpenR(1, BasisInt.Max, BasisInt)
	g := i.Negate()
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [max, max]
	assert.Equal(t, BasisInt.Max, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// [min, 1)
	i = OpenR(BasisInt.Min, 1, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsClosed()) // [1, max]
	assert.Equal(t, i.R, n1.L)
	assert.Equal(t, BasisInt.Max, n1.R)

	// [1, 2)
	i = OpenR(1, BasisInt.Max, BasisInt)
	g = i.Negate()
	n1, n2 = g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsClosed()) // [2, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)
}

func TestInterval_NegateClosed(t *testing.T) {
	// [min, max]
	i := Closed(BasisInt.Min, BasisInt.Max, BasisInt)
	n1 := i.Negate()[0]
	assert.True(t, n1.IsOpen()) // (min, min)
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Min, n1.R)

	// [min, 1]
	i = Closed(BasisInt.Min, 1, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n1.L)
	assert.Equal(t, BasisInt.Max, n1.R)

	// [1, max]
	i = Closed(1, BasisInt.Max, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)

	// [1, 1]
	i = Closed(1, 2, BasisInt)
	g := i.Negate()
	n1, n2 := g[0], g[1]
	assert.True(t, n1.IsOpenR()) // [min, 1)
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, i.L, n1.R)
	assert.True(t, n2.IsOpenL()) // (1, max]
	assert.Equal(t, i.R, n2.L)
	assert.Equal(t, BasisInt.Max, n2.R)

	// [min, min]
	i = Closed(BasisInt.Min, BasisInt.Min, BasisInt)
	n1 = i.Negate()[0]
	assert.True(t, n1.IsOpenL()) // (min, max]
	assert.Equal(t, BasisInt.Min, n1.L)
	assert.Equal(t, BasisInt.Max, n1.R)
}

func TestInterval_IntersectOpen(t *testing.T) {
	i1 := Closed(1, 2, BasisInt) // (1, 2)
	i2 := Closed(3, 4, BasisInt) // (3, 4)
	i3, ok := i1.Intersect(i2)   // none
	assert.False(t, ok)

	i1 = Open(1, 2, BasisInt) // (1, 2)
	i2 = Open(2, 3, BasisInt) // (2, 3)
	i3, ok = i1.Intersect(i2) // none
	assert.False(t, ok)

	i1 = Open(1, 3, BasisInt) // (1, 3)
	i2 = Open(2, 4, BasisInt) // (2, 3)
	i3, ok = i1.Intersect(i2) // (2, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_IntersectClosed(t *testing.T) {
	i1 := Closed(1, 2, BasisInt) // [1, 2]
	i2 := Closed(3, 4, BasisInt) // [3, 4]
	i3, ok := i1.Intersect(i2)   // none
	assert.False(t, ok)

	i1 = Closed(1, 2, BasisInt) // [1, 2]
	i2 = Closed(2, 3, BasisInt) // [2, 3]
	i3, ok = i1.Intersect(i2)   // [2, 2]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)

	i1 = Closed(1, 3, BasisInt) // [1, 3]
	i2 = Closed(2, 4, BasisInt) // [2, 4]
	i3, ok = i1.Intersect(i2)   // [2, 3]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_IntersectOpenClosed(t *testing.T) {
	i1 := Open(1, 2, BasisInt)   // (1, 2)
	i2 := Closed(2, 3, BasisInt) // [2, 3]
	i3, ok := i1.Intersect(i2)   // none
	assert.False(t, ok)

	i1 = Open(1, 2, BasisInt)   // (1, 2)
	i2 = Closed(3, 4, BasisInt) // [3, 4]
	i3, ok = i1.Intersect(i2)   // none
	assert.False(t, ok)

	i1 = Open(1, 3, BasisInt)   // (1, 3)
	i2 = Closed(2, 4, BasisInt) // [2, 4]
	i3, ok = i1.Intersect(i2)   // [2, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpenR())
	assert.Equal(t, i2.L, i3.L)
	assert.Equal(t, i1.R, i3.R)

	i1 = Open(1, 2, BasisInt)   // (1, 2)
	i2 = Closed(1, 2, BasisInt) // [1, 2]
	i3, ok = i1.Intersect(i2)   // (1, 2)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_UnionOpen(t *testing.T) {
	i1 := Open(1, 2, BasisInt) // (1, 2)
	i2 := Open(3, 4, BasisInt) // (3, 4)
	i3, ok := i1.Union(i2)     // none
	assert.False(t, ok)

	i1 = Open(1, 2, BasisInt) // (1, 2)
	i2 = Open(2, 3, BasisInt) // (2, 3)
	i3, ok = i1.Union(i2)     // none
	assert.False(t, ok)

	i1 = Open(1, 3, BasisInt) // (1, 3)
	i2 = Open(2, 4, BasisInt) // (2, 3)
	i3, ok = i1.Union(i2)     // (1, 3)
	assert.True(t, ok)
	assert.True(t, i3.IsOpen())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)
}

func TestInterval_UnionClosed(t *testing.T) {
	i1 := Closed(1, 2, BasisInt) // [1, 2]
	i2 := Closed(3, 4, BasisInt) // [3, 4]
	i3, ok := i1.Union(i2)       // none
	assert.False(t, ok)

	i1 = Closed(1, 2, BasisInt) // [1, 2)
	i2 = Closed(2, 3, BasisInt) // [2, 3]
	i3, ok = i1.Union(i2)       // [1, 3]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)

	i1 = Closed(1, 3, BasisInt) // [1, 3]
	i2 = Closed(2, 4, BasisInt) // [2, 4]
	i3, ok = i1.Union(i2)       // [1, 4]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)
}

func TestInterval_UnionClosedOpen(t *testing.T) {
	i1 := Closed(1, 2, BasisInt) // [1, 2]
	i2 := Open(3, 4, BasisInt)   // (3, 4)
	i3, ok := i1.Union(i2)       // none
	assert.False(t, ok)

	i1 = Closed(1, 2, BasisInt) // [1, 2]
	i2 = Open(2, 3, BasisInt)   // (2, 3)
	i3, ok = i1.Union(i2)       // none
	assert.False(t, ok)

	i1 = Closed(1, 3, BasisInt) // [1, 3]
	i2 = Open(2, 4, BasisInt)   // (2, 4)
	i3, ok = i1.Union(i2)       // [1, 4)
	assert.True(t, ok)
	assert.True(t, i3.IsOpenR())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i2.R, i3.R)

	i1 = Closed(1, 2, BasisInt) // [1, 2]
	i2 = Open(1, 2, BasisInt)   // (1, 2)
	i3, ok = i1.Union(i2)       // [1, 2]
	assert.True(t, ok)
	assert.True(t, i3.IsClosed())
	assert.Equal(t, i1.L, i3.L)
	assert.Equal(t, i1.R, i3.R)
}

func TestInterval_String(t *testing.T) {
	assert.Equal(t, "(1, 2)", Open(1, 2, BasisInt).String())
	assert.Equal(t, "(1, 2]", OpenL(1, 2, BasisInt).String())
	assert.Equal(t, "[1, 2)", OpenR(1, 2, BasisInt).String())
	assert.Equal(t, "[1, 2]", Closed(1, 2, BasisInt).String())
}
