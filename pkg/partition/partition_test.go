package partition

import (
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

func TestPartition(t *testing.T) {
	v0 := vector.New(types.Type{Oid: types.T(types.T_int8)})
	v0.Data = encoding.EncodeInt8Slice([]int8{3, 4, 5, 6, 7, 8})
	v0.Col = encoding.DecodeInt8Slice(v0.Data)
	v0.Nsp = &nulls.Nulls{Np: roaring.New()}
	partitions := make([]int64, 2)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v0)
	require.Equal(t, []int64{0, 1}, partitions)
	v0.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v0)
	require.Equal(t, []int64{0, 1}, partitions)

	v1 := vector.New(types.Type{Oid: types.T(types.T_int16)})
	v1.Data = encoding.EncodeInt16Slice([]int16{3, 4, 5, 6, 7, 8})
	v1.Col = encoding.DecodeInt16Slice(v1.Data)
	v1.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v1)
	require.Equal(t, []int64{0, 1}, partitions)
	v1.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v1)
	require.Equal(t, []int64{0, 1}, partitions)

	v2 := vector.New(types.Type{Oid: types.T(types.T_int32)})
	v2.Data = encoding.EncodeInt32Slice([]int32{3, 4, 5, 6, 7, 8})
	v2.Col = encoding.DecodeInt32Slice(v2.Data)
	v2.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v2)
	require.Equal(t, []int64{0, 1}, partitions)
	v2.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v2)
	require.Equal(t, []int64{0, 1}, partitions)

	v3 := vector.New(types.Type{Oid: types.T(types.T_int64)})
	v3.Data = encoding.EncodeInt64Slice([]int64{3, 4, 5, 6, 7, 8})
	v3.Col = encoding.DecodeInt64Slice(v3.Data)
	v3.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v3)
	require.Equal(t, []int64{0, 1}, partitions)
	v3.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v3)
	require.Equal(t, []int64{0, 1}, partitions)

	v4 := vector.New(types.Type{Oid: types.T(types.T_uint8)})
	v4.Data = encoding.EncodeUint8Slice([]uint8{3, 4, 5, 6, 7, 8})
	v4.Col = encoding.DecodeUint8Slice(v4.Data)
	v4.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v4)
	require.Equal(t, []int64{0, 1}, partitions)
	v4.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v4)
	require.Equal(t, []int64{0, 1}, partitions)

	v5 := vector.New(types.Type{Oid: types.T(types.T_uint16)})
	v5.Data = encoding.EncodeUint16Slice([]uint16{3, 4, 5, 6, 7, 8})
	v5.Col = encoding.DecodeUint16Slice(v5.Data)
	v5.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v5)
	require.Equal(t, []int64{0, 1}, partitions)
	v5.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v5)
	require.Equal(t, []int64{0, 1}, partitions)

	v6 := vector.New(types.Type{Oid: types.T(types.T_uint32)})
	v6.Data = encoding.EncodeUint32Slice([]uint32{3, 4, 5, 6, 7, 8})
	v6.Col = encoding.DecodeUint32Slice(v6.Data)
	v6.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v6)
	require.Equal(t, []int64{0, 1}, partitions)
	v6.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v6)
	require.Equal(t, []int64{0, 1}, partitions)

	v7 := vector.New(types.Type{Oid: types.T(types.T_uint64)})
	v7.Data = encoding.EncodeUint64Slice([]uint64{3, 4, 5, 6, 7, 8})
	v7.Col = encoding.DecodeUint64Slice(v7.Data)
	v7.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v7)
	require.Equal(t, []int64{0, 1}, partitions)
	v7.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v7)
	require.Equal(t, []int64{0, 1}, partitions)

	v8 := vector.New(types.Type{Oid: types.T(types.T_date)})
	v8.Data = encoding.EncodeDateSlice([]types.Date{3, 4, 5, 6, 7, 8})
	v8.Col = encoding.DecodeDateSlice(v8.Data)
	v8.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v8)
	require.Equal(t, []int64{0, 1}, partitions)
	v8.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v8)
	require.Equal(t, []int64{0, 1}, partitions)

	v9 := vector.New(types.Type{Oid: types.T(types.T_float32)})
	v9.Data = encoding.EncodeFloat32Slice([]float32{3, 4, 5, 6, 7, 8})
	v9.Col = encoding.DecodeFloat32Slice(v9.Data)
	v9.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v9)
	require.Equal(t, []int64{0, 1}, partitions)
	v9.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v9)
	require.Equal(t, []int64{0, 1}, partitions)

	v10 := vector.New(types.Type{Oid: types.T(types.T_float64)})
	v10.Data = encoding.EncodeFloat64Slice([]float64{3, 4, 5, 6, 7, 8})
	v10.Col = encoding.DecodeFloat64Slice(v10.Data)
	v10.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)
	v10.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)

	v11 := vector.New(types.Type{Oid: types.T(types.T_char)})
	v11.Col = &types.Bytes{
		Data:    []byte("helloGutkonichiwanihaonihaoniahonihao"),
		Offsets: []uint32{0, 5, 8, 17, 22, 27, 32},
		Lengths: []uint32{5, 3, 9, 5, 5, 5, 5},
	}
	v11.Nsp = &nulls.Nulls{Np: roaring.New()}
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v10)
	require.Equal(t, []int64{0, 1}, partitions)
	v11.Nsp.Np.Add(1)
	Partition([]int64{1, 3, 5}, []bool{false, false, false}, partitions, v11)
	require.Equal(t, []int64{0, 1}, partitions)
}
