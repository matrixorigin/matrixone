// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

// writeNullableKeyObject persists one immutable block whose leading key may
// carry NULL rows, the way the object writer does: the block zone map is
// built from non-NULL extrema only, while the per-column physical null
// count records every NULL row.
func writeNullableKeyObject(t *testing.T, fs fileservice.FileService, key int64, null bool) objectio.Location {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer bat.Clean(mp)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, null, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, mp))
	bat.SetRowCount(2)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	block, err := writer.Write(bat)
	require.NoError(t, err)
	zm := index.NewZM(types.T_int64, 0)
	require.NoError(t, index.BatchUpdateZM(zm, bat.Vecs[0]))
	block.ColumnMeta(0).SetZoneMap(zm)
	blocks, err := writer.WriteEnd(context.Background())
	require.NoError(t, err)
	return objectio.BuildLocation(name, blocks[0].GetExtent(), 2, 0)
}

// TestNullBearingBlockKeptUnderLeadingPlacement pins defect D1 of issue
// #32469 at the local reader boundary: block zone maps describe non-NULL
// extrema only, so whenever NULLs may lead the requested head, a NULL
// carrying block must be kept even when its interval lies entirely beyond
// the published bound; under a trailing placement the interval rule alone
// stays sound. The {NULL,12}-style fixtures distinguish the two placements.
func TestNullBearingBlockKeptUnderLeadingPlacement(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("null-keep-test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	point := func(value int64) objectio.ZoneMap {
		zm := objectio.NewZM(types.T_int64, 0)
		index.UpdateZM(zm, types.EncodeInt64(&value))
		return zm
	}
	ascNullBlock := writeNullableKeyObject(t, fs, 12, true)
	ascWorseBlock := writeNullableKeyObject(t, fs, 50, false)
	descNullBlock := writeNullableKeyObject(t, fs, 3, true)

	t.Run("asc default placement leads the head", func(t *testing.T) {
		source := orderedScanSource(fs, ascNullBlock, ascWorseBlock)
		source.SetOrderBy(source.OrderBy)
		source.SetFilterZM(point(10))
		require.NoError(t, source.handleOrderBy(context.Background()))
		require.True(t, source.sorted)
		// Sorted ascending by minimum: {NULL,12} first, strictly better gone.
		require.True(t, source.needReadBlkByZM(0),
			"a NULL-bearing block must never be skipped while NULLs lead the head")
		require.False(t, source.needReadBlkByZM(1),
			"a strictly worse zero-null block stays skipped")
	})

	t.Run("asc reversed input permutes null counts with blocks", func(t *testing.T) {
		// Supply the blocks in the opposite of their zone-map order so the
		// ascending permutation must itself move every null count: with stale
		// positions the {NULL,12} block would inherit the zero-null count and
		// be skipped while the key-50 block would inherit the NULL and be
		// kept - both errors are visible in the decisions below.
		source := orderedScanSource(fs, ascWorseBlock, ascNullBlock)
		source.SetOrderBy(source.OrderBy)
		source.SetFilterZM(point(10))
		require.NoError(t, source.handleOrderBy(context.Background()))
		require.True(t, source.sorted)
		oid := ascNullBlock.ObjectId()
		require.Equal(t, types.NewBlockidWithObjectID(&oid, ascNullBlock.ID()),
			source.rangeSlice.Get(0).BlockID,
			"the ascending permutation must lead with the {NULL,12} block")
		require.True(t, source.needReadBlkByZM(0),
			"the null count must travel with its block, so the permuted NULL-bearing block stays read")
		require.False(t, source.needReadBlkByZM(1),
			"the permuted strictly worse zero-null block keeps its skip decision")
	})

	t.Run("desc default placement trails the head", func(t *testing.T) {
		source := orderedScanSource(fs, ascWorseBlock, descNullBlock)
		source.OrderBy[0].Flag = plan.OrderBySpec_DESC
		source.SetOrderBy(source.OrderBy)
		source.SetFilterZM(point(10))
		require.NoError(t, source.handleOrderBy(context.Background()))
		// Sorted descending by maximum: 50 first, {NULL,3} second.
		require.True(t, source.needReadBlkByZM(0))
		require.False(t, source.needReadBlkByZM(1),
			"NULLs rank after every head candidate, so the interval rule alone keeps sound")
	})

	t.Run("desc explicit nulls first leads the head", func(t *testing.T) {
		source := orderedScanSource(fs, ascWorseBlock, descNullBlock)
		source.OrderBy[0].Flag = plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST
		source.SetOrderBy(source.OrderBy)
		source.SetFilterZM(point(10))
		require.NoError(t, source.handleOrderBy(context.Background()))
		require.True(t, source.needReadBlkByZM(0))
		require.True(t, source.needReadBlkByZM(1),
			"NULLS FIRST leads the head even under DESC, so NULL-bearing blocks stay read")
	})
}
