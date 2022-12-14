package binary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInstr(t *testing.T) {
	proc := testutil.NewProc()
	kases := []struct {
		strs    []string
		substrs []string
		wants   []int64
	}{
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"", "", "a", "b", "c"},
			wants:   []int64{1, 1, 1, 2, 3},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc"},
			wants:   []int64{2, 2, 2, 2, 2},
		},
		{
			strs:    []string{"abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
	}
	for _, k := range kases {
		inVec := testutil.MakeVarcharVector(k.strs, nil)
		subVec := testutil.MakeVarcharVector(k.substrs, nil)
		if len(k.strs) == 1 {
			inVec.MakeScalar(1)
		}
		if len(k.substrs) == 1 {
			subVec.MakeScalar(1)
		}
		v, err := Instr([]*vector.Vector{inVec, subVec}, proc)
		require.NoError(t, err)
		vSlice := vector.MustTCols[int64](v)
		require.Equal(t, k.wants, vSlice)
		if inVec.IsScalar() {
			inVec.Nsp.Set(0)
			_, err = Instr([]*vector.Vector{inVec, subVec}, proc)
			require.NoError(t, err)
		}
	}

}
