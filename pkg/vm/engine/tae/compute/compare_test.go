// Copyright 2021 Matrix Origin
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

package compute

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCompareGeneric(t *testing.T) {
	defer testutils.AfterTest(t)()
	x := types.Decimal256{
		B0_63: 0, B64_127: 0,
		B128_191: 0, B192_255: 0,
	}
	y := types.Decimal256{
		B0_63: ^x.B0_63, B64_127: ^x.B64_127,
		B128_191: ^x.B128_191, B192_255: ^x.B192_255,
	}
	assert.True(t, CompareGeneric(x, y, types.T_decimal256) == 1)

	t1 := types.TimestampToTS(timestamp.Timestamp{
		PhysicalTime: 100,
		LogicalTime:  10,
	})
	t2 := t1.Next()
	assert.True(t, CompareGeneric(t1, t2, types.T_TS) == -1)

	{
		// Array Float32
		a1 := types.ArrayToBytes[float32]([]float32{1, 2, 3})
		b1 := types.ArrayToBytes[float32]([]float32{1, 2, 3})
		assert.True(t, CompareGeneric(a1, b1, types.T_array_float32) == 0)

		// Array Float64
		a1 = types.ArrayToBytes[float64]([]float64{1, 2, 3})
		b1 = types.ArrayToBytes[float64]([]float64{1, 2, 3})
		assert.True(t, CompareGeneric(a1, b1, types.T_array_float64) == 0)
	}

	obj1 := types.NewObjectid()
	blockId_1_1291 := types.NewBlockidWithObjectID(&obj1, 1291)
	blockId_1_1036 := types.NewBlockidWithObjectID(&obj1, 1036)
	rowid_1_1291_1036 := types.NewRowid(&blockId_1_1291, 1036)
	rowid_1_1291_1291 := types.NewRowid(&blockId_1_1291, 1291)
	rowid_1_1036_1291 := types.NewRowid(&blockId_1_1036, 1291)

	// CompareGeneric Blockid
	assert.Equal(t, 0, CompareGeneric(blockId_1_1291, blockId_1_1291, types.T_Blockid))
	assert.Equal(t, 1, CompareGeneric(blockId_1_1291, blockId_1_1036, types.T_Blockid))
	assert.Equal(t, -1, CompareGeneric(blockId_1_1036, blockId_1_1291, types.T_Blockid))

	// CompareGeneric Rowid
	assert.Equal(t, 0, CompareGeneric(rowid_1_1291_1036, rowid_1_1291_1036, types.T_Rowid))
	assert.Equal(t, -1, CompareGeneric(rowid_1_1291_1036, rowid_1_1291_1291, types.T_Rowid))
	assert.Equal(t, 1, CompareGeneric(rowid_1_1291_1291, rowid_1_1291_1036, types.T_Rowid))
	assert.Equal(t, 1, CompareGeneric(rowid_1_1291_1036, rowid_1_1036_1291, types.T_Rowid))
	assert.Equal(t, -1, CompareGeneric(rowid_1_1036_1291, rowid_1_1291_1036, types.T_Rowid))

	ts1 := types.BuildTS(int64(1036), uint32(1036))
	ts2 := types.BuildTS(int64(1291), uint32(1291))

	// CompareGeneric TS
	assert.Equal(t, 0, CompareGeneric(ts1, ts1, types.T_TS))
	assert.Equal(t, 1, CompareGeneric(ts2, ts1, types.T_TS))
	assert.Equal(t, -1, CompareGeneric(ts1, ts2, types.T_TS))

	// Compare Blockid
	assert.Equal(t, 0, Compare(blockId_1_1036[:], blockId_1_1036[:], types.T_Blockid, 0, 0))
	assert.Equal(t, -1, Compare(blockId_1_1036[:], blockId_1_1291[:], types.T_Blockid, 0, 0))
	assert.Equal(t, 1, Compare(blockId_1_1291[:], blockId_1_1036[:], types.T_Blockid, 0, 0))

	// Compare Rowid
	assert.Equal(t, 0, Compare(rowid_1_1291_1036[:], rowid_1_1291_1036[:], types.T_Rowid, 0, 0))
	assert.Equal(t, -1, Compare(rowid_1_1291_1036[:], rowid_1_1291_1291[:], types.T_Rowid, 0, 0))
	assert.Equal(t, 1, Compare(rowid_1_1291_1291[:], blockId_1_1036[:], types.T_Rowid, 0, 0))
	assert.Equal(t, 1, Compare(rowid_1_1291_1036[:], blockId_1_1036[:], types.T_Rowid, 0, 0))
	assert.Equal(t, 0, Compare(rowid_1_1291_1036[:], blockId_1_1291[:], types.T_Rowid, 0, 0))
	assert.Equal(t, -1, Compare(rowid_1_1036_1291[:], blockId_1_1291[:], types.T_Rowid, 0, 0))
	assert.Equal(t, 1, Compare(blockId_1_1291[:], rowid_1_1036_1291[:], types.T_Rowid, 0, 0))
	assert.Equal(t, -1, Compare(blockId_1_1036[:], rowid_1_1291_1036[:], types.T_Rowid, 0, 0))
	assert.Equal(t, 0, Compare(blockId_1_1291[:], rowid_1_1291_1036[:], types.T_Rowid, 0, 0))
}

// Compare/CompareGeneric enumerated only vecf32/vecf64 and PANIC on the default,
// so a bf16/f16/int8/uint8 vector value crashed the process where the same value
// as vecf32 merely compared. Assert ordering, not just non-panic: a comparator
// that returned a constant would also survive a smoke test.
func TestCompareNarrowVectorTypes(t *testing.T) {
	lt := func(name string, small, big, dup []byte, oid types.T) {
		t.Helper()
		assert.Equal(t, 0, Compare(small, dup, oid, 0, 0), name+" equal")
		assert.True(t, Compare(small, big, oid, 0, 0) < 0, name+" less")
		assert.True(t, Compare(big, small, oid, 0, 0) > 0, name+" greater")
		// CompareGeneric takes the same payload boxed in any.
		assert.Equal(t, 0, CompareGeneric(any(small), any(dup), oid), name+" generic equal")
		assert.True(t, CompareGeneric(any(small), any(big), oid) < 0, name+" generic less")
	}

	bf := func(v ...float32) []byte {
		return types.ArrayToBytes[types.BF16](types.Float32ToBF16Slice(v))
	}
	f16 := func(v ...float32) []byte {
		return types.ArrayToBytes[types.Float16](types.Float32ToFloat16Slice(v))
	}
	lt("bf16", bf(1, 2), bf(1, 3), bf(1, 2), types.T_array_bf16)
	lt("f16", f16(1, 2), f16(1, 3), f16(1, 2), types.T_array_float16)

	lt("int8",
		types.ArrayToBytes[int8]([]int8{-5, 2}),
		types.ArrayToBytes[int8]([]int8{-5, 3}),
		types.ArrayToBytes[int8]([]int8{-5, 2}),
		types.T_array_int8)

	// uint8 specifically: a signed comparator would order 200 before 100.
	lt("uint8",
		types.ArrayToBytes[uint8]([]uint8{1, 100}),
		types.ArrayToBytes[uint8]([]uint8{1, 200}),
		types.ArrayToBytes[uint8]([]uint8{1, 100}),
		types.T_array_uint8)
}
