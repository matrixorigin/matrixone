package index

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	idx, err := newTestIndex(types.T_varchar.ToType())
	require.NoError(t, err)

	v0 := vector.New(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("My"),
		[]byte("name"),
		[]byte("is"),
		[]byte("Tom"),
	}, nil))

	err = idx.InsertBatch(v0)
	require.NoError(t, err)

	v1 := vector.New(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v1, [][]byte{
		[]byte("Jack"),
		[]byte("is"),
		[]byte("My"),
		[]byte("friend"),
		[]byte("name"),
		[]byte("Tom"),
	}, nil))

	enc := vector.New(types.T_uint16.ToType())
	err = idx.Encode(enc, v1)
	require.NoError(t, err)
	col := enc.Col.([]uint16)
	require.Equal(t, uint16(0), col[0])
	require.Equal(t, uint16(4), col[1])
	require.Equal(t, uint16(2), col[2])
	require.Equal(t, uint16(0), col[3])
	require.Equal(t, uint16(3), col[4])
	require.Equal(t, uint16(5), col[5])
}

func newTestIndex(typ types.Type) (*LowCardinalityIndex, error) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	return NewLowCardinalityIndex(typ, m)
}
