package float32s

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNew(t *testing.T) {
	require.Equal(t, &compare{xs: make([][]float32, 2),
		ns: make([]*nulls.Nulls, 2),
		vs: make([]*vector.Vector, 2)}, New())
}

func TestCompare_Vector(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T(types.T_float32)})
	require.Equal(t, vector.New(types.Type{Oid: types.T(types.T_float32)}), c.Vector())
}

func TestCompare_Set(t *testing.T) {
	c := New()
	vector := vector.New(types.Type{Oid: types.T(types.T_float32)})
	c.Set(1, vector)
	require.Equal(t, vector, c.vs[1])
}

func TestCompare_Compare(t *testing.T) {
	c := New()
	c.xs[0] = []float32{5, 6}
	c.xs[1] = []float32{7, 8}
	result := c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)
	c.xs[1] = []float32{5, 6}
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)
	c.xs[1] = []float32{3, 4}
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)
}
