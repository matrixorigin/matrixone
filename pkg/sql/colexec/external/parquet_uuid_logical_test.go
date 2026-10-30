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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// The Parquet UUID logical annotation marks fixed_len_byte_array(16) columns
// whose values are the raw 16 RFC 4122 UUID bytes, not UUID text
// (LogicalTypes.md#uuid). #31521: those bytes must import into MatrixOne UUID
// with the UUID value preserved, keep their raw byte form for BINARY targets,
// and render their canonical spelling for text targets.

var uuidLogicalFixtureBytes = [2][16]byte{
	{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
	{0xc8, 0x47, 0x73, 0x87, 0xeb, 0x5b, 0x4d, 0x97, 0xaf, 0x1b, 0x48, 0xd9, 0xdb, 0x74, 0x85, 0x6d},
}

// uuidLogicalFixture writes a UUID-annotated column and proves the fixture
// really carries the UUID logical annotation and the requested page encoding.
func uuidLogicalFixture(t *testing.T, node parquet.Node, dictionary bool, rows []parquet.Row) (*parquet.File, parquet.Page) {
	t.Helper()
	if dictionary {
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	f, page := writeColumnAndGetPage(t, node, rows)
	lt := f.Root().Column("c").Type().LogicalType()
	require.NotNil(t, lt, "fixture does not carry the UUID logical annotation")
	require.NotNil(t, lt.UUID, "fixture does not carry the UUID logical annotation")
	if dictionary {
		require.NotNil(t, page.Dictionary(), "fixture lost dictionary encoding")
	} else {
		require.Nil(t, page.Dictionary(), "fixture encoding changed")
	}
	return f, page
}

func TestParquetUUIDLogicalToUUIDColumn(t *testing.T) {
	proc := testutil.NewProc(t)

	for _, encoding := range []struct {
		name string
		dict bool
	}{
		{name: "plain", dict: false},
		{name: "dictionary", dict: true},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			f, page := uuidLogicalFixture(t, parquet.UUID(), encoding.dict, []parquet.Row{
				{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 0, 0)},
				{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[1][:]).Level(0, 0, 0)},
			})

			vec := vector.NewVec(types.T_uuid.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uuid), NotNullable: true})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, []types.Uuid{uuidLogicalFixtureBytes[0], uuidLogicalFixtureBytes[1]},
				vector.MustFixedColWithTypeCheck[types.Uuid](vec))
		})
	}
}

func TestParquetUUIDLogicalToUUIDColumnPreservesNull(t *testing.T) {
	proc := testutil.NewProc(t)

	for _, encoding := range []struct {
		name string
		dict bool
	}{
		{name: "plain", dict: false},
		{name: "dictionary", dict: true},
	} {
		t.Run(encoding.name, func(t *testing.T) {
			node := parquet.Optional(parquet.UUID())
			f, page := uuidLogicalFixture(t, node, encoding.dict, []parquet.Row{
				{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 1, 0)},
				{parquet.NullValue().Level(0, 0, 0)},
			})

			vec := vector.NewVec(types.T_uuid.ToType())
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uuid), NotNullable: false})
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, 2, vec.Length())
			require.Equal(t, types.Uuid(uuidLogicalFixtureBytes[0]), vector.MustFixedColWithTypeCheck[types.Uuid](vec)[0])
			require.True(t, vec.GetNulls().Contains(1), "UUID logical-type NULL must stay NULL")
		})
	}
}

func TestParquetUUIDLogicalToTextColumns(t *testing.T) {
	proc := testutil.NewProc(t)

	targets := []struct {
		name string
		plan plan.Type
		typ  types.Type
	}{
		{name: "varchar", plan: plan.Type{Id: int32(types.T_varchar), Width: 36, NotNullable: true}, typ: types.New(types.T_varchar, 36, 0)},
		{name: "char", plan: plan.Type{Id: int32(types.T_char), Width: 36, NotNullable: true}, typ: types.New(types.T_char, 36, 0)},
		{name: "text", plan: plan.Type{Id: int32(types.T_text), NotNullable: true}, typ: types.T_text.ToType()},
	}
	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			f, page := uuidLogicalFixture(t, parquet.UUID(), false, []parquet.Row{
				{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 0, 0)},
			})

			vec := vector.NewVec(target.typ)
			t.Cleanup(func() { vec.Free(proc.Mp()) })
			var h ParquetHandler
			mp := h.getMapper(f.Root().Column("c"), target.plan)
			require.NotNil(t, mp)
			require.NoError(t, mp.mapping(page, proc, vec))
			require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff", vec.GetStringAt(0))
		})
	}
}

// TestParquetUUIDLogicalToBinaryControl locks the value-preserving control
// from #31521: the same native UUID column loads unchanged raw bytes into
// BINARY(16), so the fixture bytes and reader are proven independent of the
// UUID target mapping.
func TestParquetUUIDLogicalToBinaryControl(t *testing.T) {
	proc := testutil.NewProc(t)
	f, page := uuidLogicalFixture(t, parquet.UUID(), false, []parquet.Row{
		{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 0, 0)},
	})

	vec := vector.NewVec(types.New(types.T_binary, 16, 0))
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	var h ParquetHandler
	mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_binary), Width: 16, NotNullable: true})
	require.NotNil(t, mp)
	require.NoError(t, mp.mapping(page, proc, vec))
	require.Equal(t, uuidLogicalFixtureBytes[0][:], vec.GetBytesAt(0))
}
