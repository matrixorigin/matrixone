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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// #31521 follow-up boundaries around the native Parquet UUID import: the
// text-target mapping must preserve NULL and respect declared widths, and
// unsupported physical types must stay rejected at prepare time with an
// ordinary error.

// Text-target boundaries: NULL stays NULL when the canonical spelling lands
// in a text column, and a declared capacity below the 36-rune spelling
// rejects the whole page without partial output. Binary-family targets are
// unaffected because they keep the raw 16 bytes.
func TestParquetUUIDLogicalToTextBoundaries(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("null preserved for varchar", func(t *testing.T) {
		f, page := uuidLogicalFixture(t, parquet.Optional(parquet.UUID()), false, []parquet.Row{
			{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 1, 0)},
			{parquet.NullValue().Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_varchar, 36, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), Width: 36, NotNullable: false})
		require.NotNil(t, mp)
		require.NoError(t, mp.mapping(page, proc, vec))
		require.Equal(t, 2, vec.Length())
		require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff", vec.GetStringAt(0))
		require.True(t, vec.GetNulls().Contains(1), "UUID NULL must stay NULL for text targets")
	})

	t.Run("varchar(35) overrun rolls back", func(t *testing.T) {
		f, page := uuidLogicalFixture(t, parquet.UUID(), false, []parquet.Row{
			{parquet.FixedLenByteArrayValue(uuidLogicalFixtureBytes[0][:]).Level(0, 0, 0)},
		})

		vec := vector.NewVec(types.New(types.T_varchar, 35, 0))
		t.Cleanup(func() { vec.Free(proc.Mp()) })
		var h ParquetHandler
		mp := h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_varchar), Width: 35, NotNullable: true})
		require.NotNil(t, mp)
		require.Error(t, mp.mapping(page, proc, vec))
		require.Zero(t, vec.Length(), "rejected mapping appended output")
		require.True(t, vec.GetNulls().IsEmpty(), "rejected mapping changed NULL state")
	})
}

// TestParquetUUIDUnsupportedPhysicalToUUID locks the prepare-time rejection
// of non-text physical types for UUID targets: no mapper exists, so the
// caller reports an ordinary not-yet-supported schema error instead of
// mapping.
func TestParquetUUIDUnsupportedPhysicalToUUID(t *testing.T) {
	var h ParquetHandler

	f, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.Int32Type), []parquet.Row{
		{parquet.Int32Value(7).Level(0, 0, 0)},
	})
	require.Nil(t,
		h.getMapper(f.Root().Column("c"), plan.Type{Id: int32(types.T_uuid), NotNullable: true}),
		"INT32 physical type must be rejected for UUID targets")

	fb, _ := writeColumnAndGetPage(t, parquet.Leaf(parquet.BooleanType), []parquet.Row{
		{parquet.BooleanValue(true).Level(0, 0, 0)},
	})
	require.Nil(t,
		h.getMapper(fb.Root().Column("c"), plan.Type{Id: int32(types.T_uuid), NotNullable: true}),
		"BOOLEAN physical type must be rejected for UUID targets")
}

// TestParquetUuidBytesToUuidGuard pins the per-value byte-length boundary of
// the native UUID import path: exactly 16 bytes pass through unchanged, any
// other length is an ordinary invalid-input error. Compliant writers cannot
// emit the offending schemas, so the guard is proven at its boundary instead.
func TestParquetUuidBytesToUuidGuard(t *testing.T) {
	ctx := context.Background()

	u, err := parquetUuidBytesToUuid(ctx, uuidLogicalFixtureBytes[0][:])
	require.NoError(t, err)
	require.Equal(t, types.Uuid(uuidLogicalFixtureBytes[0]), u)

	for _, length := range []int{15, 17} {
		data := make([]byte, length)
		copy(data, uuidLogicalFixtureBytes[0][:])
		_, err := parquetUuidBytesToUuid(ctx, data)
		require.Error(t, err, "%d-byte value must be rejected", length)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput),
			"non-16-byte UUID data must fail as ordinary invalid input, got %v", err)
		require.ErrorContains(t, err, "parquet UUID value must be 16 bytes")
	}
}
