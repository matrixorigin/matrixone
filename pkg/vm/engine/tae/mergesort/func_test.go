package mergesort

import (
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
)

func TestReshape(t *testing.T) {
	pool := mocks.GetTestVectorPool()
	fromLayout := []uint32{2, 4}
	toLayout := []uint32{3, 3}

	vec1 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := uint32(0); i < fromLayout[0]; i++ {
		vec1.Append(int32(i), false)
	}

	vec2 := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	for i := fromLayout[0]; i < fromLayout[0]+fromLayout[1]; i++ {
		vec2.Append(int32(i), false)
	}

	retvec := make([]*vector.Vector, len(toLayout))
	ret := make([]containers.Vector, len(toLayout))
	for i := 0; i < len(toLayout); i++ {
		ret[i] = pool.GetVector(vec1.GetType())
		retvec[i] = ret[i].GetDownstreamVector()
	}

	vecs := []*vector.Vector{vec1.GetDownstreamVector(), vec2.GetDownstreamVector()}
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	Reshape(vecs, retvec, fromLayout, toLayout, pool.MPool())
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	require.Equal(t, []int32{0, 1, 2}, vector.MustFixedCol[int32](retvec[0]))
	require.Equal(t, []int32{3, 4, 5}, vector.MustFixedCol[int32](retvec[1]))
	for _, v := range ret {
		v.Close()
	}
}
