// Copyright 2025 Matrix Origin
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

package external

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// #31523: a Parquet UINT64 column is a valid BOOL source. Nonzero means true
// across the whole unsigned range, exactly like direct SQL inserts, so the
// mapper must not narrow the value through signed BIGINT first.

// uintBoolFixture writes an unsigned integer annotated column and proves the
// fixture really carries the unsigned INTEGER annotation and the requested
// page encoding.
func uintBoolFixture(t *testing.T, bitWidth int, dictionary bool, rows []parquet.Row) (*parquet.File, parquet.Page) {
	t.Helper()
	node := parquet.Uint(bitWidth)
	if dictionary {
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	f, page := writeColumnAndGetPage(t, node, rows)
	lt := f.Root().Column("c").Type().LogicalType()
	require.NotNil(t, lt, "fixture does not carry an INTEGER logical annotation")
	require.NotNil(t, lt.Integer, "fixture does not carry an INTEGER logical annotation")
	require.False(t, lt.Integer.IsSigned, "fixture is not unsigned")
	require.Equal(t, int8(bitWidth), lt.Integer.BitWidth)
	if dictionary {
		require.NotNil(t, page.Dictionary(), "fixture lost dictionary encoding")
	} else {
		require.Nil(t, page.Dictionary(), "fixture encoding changed")
	}
	return f, page
}

func TestParquetUint64ToBoolBoundary(t *testing.T) {
	proc := testutil.NewProc(t)

	maxInt64Plus1 := uint64(math.MaxInt64) + 1
	for _, encoding := range []struct {
		name string
		dict bool
	}{
		{name: "plain", dict: false},
		{name: "dictionary", dict: true},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			f, page := uintBoolFixture(t, 64, encoding.dict, []parquet.Row{
				{parquet.ValueOf(uint64(0)).Level(0, 0, 0)},
				{parquet.ValueOf(uint64(1)).Level(0, 0, 0)},
				{parquet.ValueOf(uint64(math.MaxInt64)).Level(0, 0, 0)},
				{parquet.ValueOf(maxInt64Plus1).Level(0, 0, 0)},
				{parquet.ValueOf(uint64(math.MaxUint64)).Level(0, 0, 0)},
			})

			vec := vector.NewVec(types.T_bool.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			// Every value is a legal BOOL source: 0 maps to false, and any
			// nonzero value including UINT64_MAX maps to true, matching the
			// direct SQL INSERT oracle from #31523.
			require.Equal(t, []bool{false, true, true, true, true},
				vector.MustFixedColWithTypeCheck[bool](vec))
		})
	}
}

func TestParquetUint64ToBoolPreservesNull(t *testing.T) {
	proc := testutil.NewProc(t)

	for _, encoding := range []struct {
		name string
		dict bool
	}{
		{name: "plain", dict: false},
		{name: "dictionary", dict: true},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			node := parquet.Optional(parquet.Uint(64))
			if encoding.dict {
				node = parquet.Encoded(node, &parquet.RLEDictionary)
			}
			f, page := writeColumnAndGetPage(t, node, []parquet.Row{
				{parquet.ValueOf(uint64(1)).Level(0, 1, 0)},
				{parquet.NullValue().Level(0, 0, 0)},
				{parquet.ValueOf(uint64(math.MaxUint64)).Level(0, 1, 0)},
			})

			vec := vector.NewVec(types.T_bool.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: false})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, 3, vec.Length())
			got := vector.MustFixedColWithTypeCheck[bool](vec)
			require.True(t, got[0])
			require.True(t, got[2])
			require.True(t, vec.GetNulls().Contains(1), "UINT64 NULL must stay NULL for BOOL targets")
			require.False(t, vec.GetNulls().Contains(0))
			require.False(t, vec.GetNulls().Contains(2))
		})
	}
}

// TestParquetUint32ToBoolUnsigned locks the existing unsigned-32 behavior on
// the branch shared with the UINT64 fix: the whole UINT32 range converts
// directly, never through a signed narrowing that could reject values.
func TestParquetUint32ToBoolUnsigned(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := uintBoolFixture(t, 32, false, []parquet.Row{
		{parquet.ValueOf(uint32(0)).Level(0, 0, 0)},
		{parquet.ValueOf(uint32(1)).Level(0, 0, 0)},
		{parquet.ValueOf(uint32(math.MaxUint32)).Level(0, 0, 0)},
	})

	vec := vector.NewVec(types.T_bool.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, []bool{false, true, true}, vector.MustFixedColWithTypeCheck[bool](vec))
}

// TestParquetSignedInt64ToBoolNonzero locks the signed-64 branch: zero maps
// to false and any nonzero signed value maps to true without error.
func TestParquetSignedInt64ToBoolNonzero(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.Int(64), []parquet.Row{
		{parquet.Int64Value(0).Level(0, 0, 0)},
		{parquet.Int64Value(-1).Level(0, 0, 0)},
		{parquet.Int64Value(math.MaxInt64).Level(0, 0, 0)},
	})

	vec := vector.NewVec(types.T_bool.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, []bool{false, true, true}, vector.MustFixedColWithTypeCheck[bool](vec))
}

// TestParquetBoolFromTextRejectsInvalidRow keeps the error path of the BOOL
// family honest: malformed text is rejected with an ordinary error, the whole
// page rolls back, and no partial rows survive, which is what keeps a failed
// LOAD file-atomic.
func TestParquetBoolFromTextRejectsInvalidRow(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := writeColumnAndGetPage(t, parquet.String(), []parquet.Row{
		{parquet.ByteArrayValue([]byte("1")).Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("1")).Level(0, 0, 0)},
		{parquet.ByteArrayValue([]byte("surely-not-a-bool")).Level(0, 0, 0)},
	})

	vec := vector.NewVec(types.T_bool.ToType())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_bool), NotNullable: true})
	require.NotNil(t, mp)
	require.Error(t, mp.mapping(page, proc, vec))
	require.Zero(t, vec.Length(), "rejected mapping appended output")
	require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
}
