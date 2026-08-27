// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func mockUtilVector(t *testing.T, proc *process.Process) (*batch.Batch, []string) {

	i := 0
	nvec := 19

	bat := batch.NewWithSize(nvec)
	res := make([]string, nvec)

	{
		// json
		bat.Vecs[i] = vector.NewVec(types.New(types.T_json, 65536, 0))
		bj, err := bytejson.ParseFromString("[1,2,3]")
		require.Nil(t, err)
		bytes, err := bj.Marshal()
		require.Nil(t, err)

		vector.AppendBytes(bat.Vecs[i], bytes, false, proc.Mp())
		res[i] = "'[1, 2, 3]'"
		i += 1
	}

	{
		// int64
		bat.Vecs[i] = vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed[int64](bat.Vecs[i], int64(100), false, proc.Mp())
		res[i] = "100"
		i += 1
	}

	{
		// []float32
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_float32, 3, 0)) // float32 array [3]float32
		v32 := []float32{0, 1, 2}
		vector.AppendArray[float32](bat.Vecs[i], v32, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECF32(3))"
		i += 1
	}

	{
		// []float64
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_float64, 3, 0)) // float32 array [3]float64
		v64 := []float64{0, 1, 2}
		vector.AppendArray[float64](bat.Vecs[i], v64, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECF64(3))"
		i += 1
	}

	{
		// []float16 (narrow base column)
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_float16, 3, 0))
		vf16 := types.Float32ToFloat16Slice([]float32{0, 1, 2})
		vector.AppendArray[types.Float16](bat.Vecs[i], vf16, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECF16(3))"
		i += 1
	}

	{
		// []bf16 (narrow base column)
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_bf16, 3, 0))
		vbf16 := types.Float32ToBF16Slice([]float32{0, 1, 2})
		vector.AppendArray[types.BF16](bat.Vecs[i], vbf16, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECBF16(3))"
		i += 1
	}

	{
		// []int8 (narrow base column)
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_int8, 3, 0))
		vector.AppendArray[int8](bat.Vecs[i], []int8{0, 1, 2}, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECINT8(3))"
		i += 1
	}

	{
		// []uint8 (narrow base column)
		bat.Vecs[i] = vector.NewVec(types.New(types.T_array_uint8, 3, 0))
		vector.AppendArray[uint8](bat.Vecs[i], []uint8{0, 1, 2}, false, proc.Mp())
		res[i] = "CAST('[0, 1, 2]' as VECUINT8(3))"
		i += 1
	}

	{
		// date
		bat.Vecs[i] = vector.NewVec(types.New(types.T_date, 4, 0))
		v := 0
		vector.AppendFixed[types.Date](bat.Vecs[i], types.Date(v), false, proc.Mp())
		res[i] = "'0001-01-01'"
		i += 1
	}

	{
		// datetime
		bat.Vecs[i] = vector.NewVec(types.New(types.T_datetime, 8, 0))
		v := 0
		vector.AppendFixed[types.Datetime](bat.Vecs[i], types.Datetime(v), false, proc.Mp())
		res[i] = "'0001-01-01 00:00:00'"
		i += 1
	}

	{
		// time
		bat.Vecs[i] = vector.NewVec(types.New(types.T_time, 8, 0))
		v := 0
		vector.AppendFixed[types.Time](bat.Vecs[i], types.Time(v), false, proc.Mp())
		res[i] = "'00:00:00'"
		i += 1
	}

	{
		// timestamp
		bat.Vecs[i] = vector.NewVec(types.New(types.T_timestamp, 8, 0))
		v := 0
		vector.AppendFixed[types.Timestamp](bat.Vecs[i], types.Timestamp(v), false, proc.Mp())
		res[i] = "'0001-01-01 00:00:00'"
		i += 1
	}

	{
		// decimal64
		bat.Vecs[i] = vector.NewVec(types.New(types.T_decimal64, 8, 0))
		v := 1000
		vector.AppendFixed[types.Decimal64](bat.Vecs[i], types.Decimal64(v), false, proc.Mp())
		res[i] = "'1000'"
		i += 1
	}

	{
		// decimal128
		bat.Vecs[i] = vector.NewVec(types.New(types.T_decimal128, 16, 0))
		v := types.Decimal128{B0_63: 1000, B64_127: 0}
		vector.AppendFixed[types.Decimal128](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'1000'"
		i += 1
	}

	{
		// uuid
		bat.Vecs[i] = vector.NewVec(types.New(types.T_uuid, 16, 0))
		v := types.Uuid([16]byte{0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8})
		vector.AppendFixed[types.Uuid](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'00010002-0003-0004-0005-000600070008'"
		i += 1
	}

	{
		// enum
		bat.Vecs[i] = vector.NewVec(types.New(types.T_enum, 2, 0))
		v := types.Enum(uint16(1))
		vector.AppendFixed[types.Enum](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'1'"
		i += 1
	}

	{
		// rowid
		bat.Vecs[i] = vector.NewVec(types.New(types.T_Rowid, types.RowidSize, 0))
		v := types.Rowid([types.RowidSize]byte{})
		vector.AppendFixed[types.Rowid](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'00000000-0000-0000-0000-000000000000-0-0-0'"
		i += 1
	}

	{
		// blockid
		bat.Vecs[i] = vector.NewVec(types.New(types.T_Blockid, types.BlockidSize, 0))
		v := types.Blockid([types.BlockidSize]byte{})
		vector.AppendFixed[types.Blockid](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'00000000-0000-0000-0000-000000000000-0-0'"
		i += 1
	}

	{
		// TS
		bat.Vecs[i] = vector.NewVec(types.New(types.T_TS, types.TxnTsSize, 0))
		v := types.TS([types.TxnTsSize]byte{})
		vector.AppendFixed[types.TS](bat.Vecs[i], v, false, proc.Mp())
		res[i] = "'0-0'"
		i += 1
	}

	require.Equal(t, nvec, i)

	bat.SetRowCount(1)
	return bat, res
}

func TestRowFromVector(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	ctx := context.Background()

	bat, results := mockUtilVector(t, proc)

	res := make([]any, 1)
	sql := make([]byte, 0, 1024)

	for i, vec := range bat.Vecs {
		err := extractRowFromVector(ctx, vec, 0, res, 0, ReprSQLString)
		require.Nil(t, err)

		sql, err := convertColIntoSql(ctx, res[0], vec.GetType(), sql)
		require.Nil(t, err)

		require.Equal(t, string(sql), results[i])
		sql = sql[:0]
	}

}

// TestExtractRowNativeRepr covers the ReprNative path added for binary CDC consumers
// (the WAND retrieval index): the temporal / decimal / uuid types must come out as
// their exact native Go value under ReprNative, while ReprSQLString keeps yielding the
// SQL-display string; a non-differing control type (int64) must be identical in both.
func TestExtractRowNativeRepr(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	ctx := context.Background()

	mk := func(typ types.Type, appendFn func(v *vector.Vector)) *vector.Vector {
		v := vector.NewVec(typ)
		appendFn(v)
		return v
	}

	cases := []struct {
		name    string
		vec     *vector.Vector
		native  any  // expected value under ReprNative
		differs bool // native form differs from the SQL-display string (so ReprSQLString is a string)
	}{
		{"datetime", mk(types.New(types.T_datetime, 8, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Datetime(0x0123456789ABCDEF), false, proc.Mp())
		}), types.Datetime(0x0123456789ABCDEF), true},
		{"time", mk(types.New(types.T_time, 8, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Time(0x0011223344556677), false, proc.Mp())
		}), types.Time(0x0011223344556677), true},
		{"timestamp", mk(types.New(types.T_timestamp, 8, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Timestamp(0x7FFFFFFFFFFFFFFF), false, proc.Mp())
		}), types.Timestamp(0x7FFFFFFFFFFFFFFF), true},
		{"decimal64", mk(types.New(types.T_decimal64, 8, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Decimal64(1000), false, proc.Mp())
		}), types.Decimal64(1000), true},
		{"decimal128", mk(types.New(types.T_decimal128, 16, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Decimal128{B0_63: 1000, B64_127: 7}, false, proc.Mp())
		}), types.Decimal128{B0_63: 1000, B64_127: 7}, true},
		{"uuid", mk(types.New(types.T_uuid, 16, 0), func(v *vector.Vector) {
			vector.AppendFixed(v, types.Uuid([16]byte{0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8}), false, proc.Mp())
		}), types.Uuid([16]byte{0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8}), true},
		// control: int64 is native in both modes, so the two reprs must agree.
		{"int64", mk(types.New(types.T_int64, 8, 0), func(v *vector.Vector) {
			vector.AppendFixed[int64](v, int64(100), false, proc.Mp())
		}), int64(100), false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			nat := make([]any, 1)
			require.NoError(t, extractRowFromVector(ctx, c.vec, 0, nat, 0, ReprNative))
			require.Equal(t, c.native, nat[0], "ReprNative must yield the exact native value")

			str := make([]any, 1)
			require.NoError(t, extractRowFromVector(ctx, c.vec, 0, str, 0, ReprSQLString))
			if c.differs {
				_, ok := str[0].(string)
				require.Truef(t, ok, "ReprSQLString must yield a string for %s, got %T", c.name, str[0])
			} else {
				require.Equal(t, nat[0], str[0], "a non-differing type must be identical across reprs")
			}
		})
	}
}
