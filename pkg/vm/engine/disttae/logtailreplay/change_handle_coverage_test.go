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

package logtailreplay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestChangeBatchLayoutValidationMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	oneInt := func() *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		return vec
	}
	oneRowID := func() *vector.Vector {
		vec := vector.NewVec(types.T_Rowid.ToType())
		var blockID types.Blockid
		require.NoError(t, vector.AppendFixed(vec, types.NewRowid(&blockID, 0), false, mp))
		return vec
	}
	oneTS := func() *vector.Vector {
		vec := vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed(vec, types.BuildTS(10, 0), false, mp))
		return vec
	}
	oneBatch := func(vecs ...*vector.Vector) *batch.Batch {
		bat := batch.NewWithSize(len(vecs))
		copy(bat.Vecs, vecs)
		bat.SetRowCount(1)
		return bat
	}

	t.Run("prepend rowid", func(t *testing.T) {
		require.Error(t, prependRowIDVectorIfNeeded(nil, nil, mp))
		missingBlock := oneBatch(oneInt())
		require.ErrorContains(t, prependRowIDVectorIfNeeded(missingBlock, nil, mp), "without block id")
		missingBlock.Clean(mp)

		duplicate := oneBatch(oneRowID(), oneInt(), oneRowID())
		duplicate.SetAttributes([]string{catalog.Row_ID, "value", objectio.PhysicalAddr_Attr})
		require.ErrorContains(t, prependRowIDVectorIfNeeded(duplicate, nil, mp), "2 rowid columns")
		duplicate.Clean(mp)

		reordered := oneBatch(oneInt(), oneRowID())
		reordered.SetAttributes([]string{"value", objectio.PhysicalAddr_Attr})
		require.NoError(t, prependRowIDVectorIfNeeded(reordered, nil, mp))
		require.Equal(t, catalog.Row_ID, reordered.Attrs[0])
		reordered.Clean(mp)

		synthesized := oneBatch(oneInt())
		var blockID types.Blockid
		require.NoError(t, prependRowIDVectorIfNeeded(synthesized, &blockID, mp))
		require.Equal(t, types.T_Rowid, synthesized.Vecs[0].GetType().Oid)
		synthesized.Clean(mp)
	})

	t.Run("CN data special columns", func(t *testing.T) {
		empty := batch.NewWithSize(0)
		require.ErrorContains(t, updateCNDataBatch(empty, types.TS{}, nil, noSpecialColumnLayout(), false, mp), "no user vectors")

		wrongPhysical := oneBatch(oneInt())
		layout := noSpecialColumnLayout()
		layout.PhysicalAddr = 0
		require.ErrorContains(t, updateCNDataBatch(wrongPhysical, types.TS{}, nil, layout, false, mp), "invalid physical rowid")
		wrongPhysical.Clean(mp)

		nilCommit := oneBatch(oneInt(), nil)
		layout = noSpecialColumnLayout()
		layout.CommitTS = 1
		require.ErrorContains(t, updateCNDataBatch(nilCommit, types.TS{}, nil, layout, false, mp), "invalid persisted commit-ts")
		nilCommit.Clean(mp)

		nilAbort := oneBatch(oneInt(), nil)
		layout = noSpecialColumnLayout()
		layout.Abort = 1
		require.ErrorContains(t, updateCNDataBatch(nilAbort, types.TS{}, nil, layout, false, mp), "invalid persisted abort")
		nilAbort.Clean(mp)

		onlySpecial := oneBatch(oneTS())
		layout = noSpecialColumnLayout()
		layout.CommitTS = 0
		require.ErrorContains(t, updateCNDataBatch(onlySpecial, types.TS{}, nil, layout, false, mp), "no user vectors")
		onlySpecial.Clean(mp)

		badCommit := oneBatch(oneInt(), oneInt())
		layout = noSpecialColumnLayout()
		layout.CommitTS = 1
		require.Error(t, updateCNDataBatch(badCommit, types.TS{}, nil, layout, false, mp))
		badCommit.Clean(mp)

		badAbort := oneBatch(oneInt(), oneTS())
		layout = noSpecialColumnLayout()
		layout.Abort = 1
		require.Error(t, updateCNDataBatch(badAbort, types.TS{}, nil, layout, false, mp))
		badAbort.Clean(mp)

		noBlock := oneBatch(oneInt())
		require.ErrorContains(t, updateCNDataBatch(noBlock, types.TS{}, nil, noSpecialColumnLayout(), true, mp), "without block id")
		noBlock.Clean(mp)
	})

	t.Run("persisted data MVCC", func(t *testing.T) {
		require.Error(t, updatePersistedDataBatch(nil, types.TS{}, types.MaxTs(), nil, noSpecialColumnLayout(), false, mp))

		missingCommit := oneBatch(oneInt())
		require.ErrorContains(t, updatePersistedDataBatch(missingCommit, types.TS{}, types.MaxTs(), nil, noSpecialColumnLayout(), false, mp), "commit-ts")
		missingCommit.Clean(mp)

		wrongCommit := oneBatch(oneInt(), oneInt())
		layout := noSpecialColumnLayout()
		layout.CommitTS = 1
		require.ErrorContains(t, updatePersistedDataBatch(wrongCommit, types.TS{}, types.MaxTs(), nil, layout, false, mp), "commit-ts")
		wrongCommit.Clean(mp)

		wrongAbort := oneBatch(oneInt(), oneTS(), oneInt())
		layout = noSpecialColumnLayout()
		layout.CommitTS, layout.Abort = 1, 2
		require.ErrorContains(t, updatePersistedDataBatch(wrongAbort, types.TS{}, types.MaxTs(), nil, layout, false, mp), "abort column")
		wrongAbort.Clean(mp)

		overlap := oneBatch(oneInt(), oneTS())
		layout = noSpecialColumnLayout()
		layout.CommitTS, layout.Abort = 1, 1
		require.ErrorContains(t, updatePersistedDataBatch(overlap, types.TS{}, types.MaxTs(), nil, layout, false, mp), "invalid abort column")
		overlap.Clean(mp)

		nullCommit := oneBatch(oneInt(), vector.NewConstNull(types.T_TS.ToType(), 1, mp))
		layout = noSpecialColumnLayout()
		layout.CommitTS = 1
		require.ErrorContains(t, updatePersistedDataBatch(nullCommit, types.TS{}, types.MaxTs(), nil, layout, false, mp), "commit-ts column is null")
		nullCommit.Clean(mp)

		noBlock := oneBatch(oneInt(), oneTS())
		layout = noSpecialColumnLayout()
		layout.CommitTS = 1
		require.ErrorContains(t, updatePersistedDataBatch(noBlock, types.TS{}, types.MaxTs(), nil, layout, true, mp), "without block id")
		noBlock.Clean(mp)
	})
}

func TestChangeBatchSchemaValidationMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	schema := &engine.CollectChangesSchema{
		Attrs: []string{"value"}, Types: []types.Type{types.T_int64.ToType()}, Seqnums: []uint16{0},
	}
	newSource := func() *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "value"})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		var blockID types.Blockid
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&blockID, 0), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.BuildTS(10, 0), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(1), false, mp))
		bat.SetRowCount(1)
		return bat
	}
	entry := func(bat *batch.Batch) *RowEntry {
		return &RowEntry{Batch: bat, Offset: 0, Time: types.BuildTS(10, 0)}
	}

	t.Run("insert source validation", func(t *testing.T) {
		var out *batch.Batch
		require.Error(t, fillInInsertBatchUsingSchema(nil, nil, schema, false, mp))
		require.Error(t, fillInInsertBatchUsingSchema(&out, nil, schema, false, mp))

		src := newSource()
		badOffset := entry(src)
		badOffset.Offset = 2
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&out, badOffset, schema, false, mp), "has no row")
		src.Clean(mp)

		src = newSource()
		src.Attrs = src.Attrs[:1]
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&out, entry(src), schema, false, mp), "partial attributes")
		src.Clean(mp)

		src = newSource()
		src.Vecs[2].Free(mp)
		src.Vecs[2] = nil
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&out, entry(src), schema, false, mp), "column 2 is nil")
		src.Clean(mp)

		src = newSource()
		src.Attrs[2] = ""
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&out, entry(src), schema, false, mp), "placeholder")
		src.Clean(mp)

		src = newSource()
		src.Vecs[2].SetLength(0)
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&out, entry(src), schema, false, mp), "expected 1")
		src.Clean(mp)
	})

	t.Run("insert destination validation", func(t *testing.T) {
		src := newSource()
		defer src.Clean(mp)
		badShape := batch.NewWithSize(1)
		badShape.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		badShape.SetAttributes([]string{"value"})
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&badShape, entry(src), schema, false, mp), "destination schema")
		badShape.Clean(mp)

		badVec := batch.NewWithSize(2)
		badVec.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
		constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 0, mp)
		require.NoError(t, err)
		badVec.Vecs[0] = constVec
		badVec.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&badVec, entry(src), schema, false, mp), "not appendable")
		badVec.Clean(mp)

		badType := batch.NewWithSize(2)
		badType.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
		badType.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		badType.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&badType, entry(src), schema, false, mp), "column 0 is incompatible")
		badType.Clean(mp)

		badCommit := batch.NewWithSize(2)
		badCommit.SetAttributes([]string{"value", "wrong"})
		badCommit.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		badCommit.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.ErrorContains(t, fillInInsertBatchUsingSchema(&badCommit, entry(src), schema, false, mp), "commit-ts is incompatible")
		badCommit.Clean(mp)

		var retained *batch.Batch
		require.NoError(t, fillInInsertBatchUsingSchema(&retained, entry(src), schema, true, mp))
		require.Equal(t, []string{catalog.Row_ID, "value", objectio.DefaultCommitTS_Attr}, retained.Attrs)
		retained.Clean(mp)
	})

	t.Run("tombstone semantic layout", func(t *testing.T) {
		require.Error(t, updateTombstoneBatch(nil, types.TS{}, types.MaxTs(), nil, false, nil, nil, false, mp))

		duplicateCommit := newSource()
		duplicateCommit.Attrs[2] = objectio.DefaultCommitTS_Attr
		require.ErrorContains(t, updateTombstoneBatch(duplicateCommit, types.TS{}, types.MaxTs(), nil, false, nil, nil, false, mp), "duplicate commit-ts")
		duplicateCommit.Clean(mp)

		duplicateRowID := newSource()
		duplicateRowID.Attrs[2] = objectio.TombstoneAttr_Rowid_Attr
		duplicateRowID.Vecs[2].Free(mp)
		duplicateRowID.Vecs[2] = vector.NewVec(types.T_Rowid.ToType())
		var blockID types.Blockid
		require.NoError(t, vector.AppendFixed(duplicateRowID.Vecs[2], types.NewRowid(&blockID, 0), false, mp))
		require.ErrorContains(t, updateTombstoneBatch(duplicateRowID, types.TS{}, types.MaxTs(), nil, false, nil, nil, false, mp), "duplicate rowid")
		duplicateRowID.Clean(mp)

		duplicatePK := newSource()
		duplicatePK.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr, objectio.TombstoneAttr_PK_Attr}
		require.ErrorContains(t, updateTombstoneBatch(duplicatePK, types.TS{}, types.MaxTs(), nil, false, nil, nil, false, mp), "duplicate primary-key")
		duplicatePK.Clean(mp)

		missingPK := newSource()
		missingPK.Attrs = []string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "value"}
		require.ErrorContains(t, updateTombstoneBatch(missingPK, types.TS{}, types.MaxTs(), nil, false, nil, nil, false, mp), "primary-key")
		missingPK.Clean(mp)
	})

	t.Run("CN tombstone layout", func(t *testing.T) {
		bad := newSource()
		bad.Attrs = []string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, "value"}
		require.ErrorContains(t, updateCNTombstoneBatch(bad, types.BuildTS(20, 0), nil, noSpecialColumnLayout(), false, mp), "identify the tombstone pk")
		bad.Clean(mp)

		bad = newSource()
		bad.Attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
		layout := noSpecialColumnLayout()
		layout.CommitTS = 1
		require.ErrorContains(t, updateCNTombstoneBatch(bad, types.BuildTS(20, 0), nil, layout, false, mp), "unexpectedly has commit-ts metadata")
		bad.Clean(mp)

		bad = newSource()
		bad.Attrs = []string{catalog.Row_ID, objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
		layout = noSpecialColumnLayout()
		layout.Abort = 1
		require.ErrorContains(t, updateCNTombstoneBatch(bad, types.BuildTS(20, 0), nil, layout, false, mp), "unexpectedly has abort metadata")
		bad.Clean(mp)
	})
}

func TestChangeBatchAppendDeleteAndProjectionMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	appendInt := func(vec *vector.Vector, value int64) {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	appendTS := func(vec *vector.Vector, value types.TS) {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	newAppendBatch := func(values []int64, constTS bool) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for _, value := range values {
			appendInt(bat.Vecs[0], value)
		}
		if constTS {
			var err error
			bat.Vecs[1], err = vector.NewConstFixed(
				types.T_TS.ToType(), types.BuildTS(10, 0), len(values), mp,
			)
			require.NoError(t, err)
		} else {
			bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
			for range values {
				appendTS(bat.Vecs[1], types.BuildTS(10, 0))
			}
		}
		bat.SetRowCount(len(values))
		return bat
	}

	t.Run("atomic append materializes const destination", func(t *testing.T) {
		dst := newAppendBatch([]int64{1}, true)
		defer dst.Clean(mp)
		src := newAppendBatch([]int64{2, 3}, false)
		defer src.Clean(mp)
		require.NoError(t, appendChangeBatchWindow(dst, src, 0, 2, mp))
		require.Equal(t, 3, dst.RowCount())
		require.False(t, dst.Vecs[1].IsConst())
		require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColWithTypeCheck[int64](dst.Vecs[0]))

		require.Error(t, appendChangeBatchWindow(nil, src, 0, 1, mp))
		require.Error(t, appendChangeBatchWindow(dst, dst, 0, 1, mp))
		require.Error(t, appendChangeBatchWindow(dst, src, -1, 1, mp))
		require.Error(t, appendChangeBatchWindow(dst, src, 1, 3, mp))
	})

	t.Run("atomic append rejects malformed columns", func(t *testing.T) {
		dst := newAppendBatch([]int64{1}, false)
		defer dst.Clean(mp)
		src := newAppendBatch([]int64{2}, false)
		defer src.Clean(mp)

		saved := src.Vecs[0]
		savedDst := dst.Vecs[0]
		src.Vecs[0] = nil
		dst.Vecs[0] = nil
		require.ErrorContains(t, appendChangeBatchWindow(dst, src, 0, 1, mp), "nil leading")
		src.Vecs[0] = saved
		dst.Vecs[0] = savedDst

		dst.SetRowCount(2)
		require.ErrorContains(t, appendChangeBatchWindow(dst, src, 0, 1, mp), "row count")
		dst.SetRowCount(1)

		saved = src.Vecs[1]
		src.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		appendInt(src.Vecs[1], 2)
		require.ErrorContains(t, appendChangeBatchWindow(dst, src, 0, 1, mp), "incompatible")
		src.Vecs[1].Free(mp)
		src.Vecs[1] = saved

		src.Vecs[1].SetLength(0)
		require.ErrorContains(t, appendChangeBatchWindow(dst, src, 0, 1, mp), "has 0 rows")
		src.Vecs[1].SetLength(1)
		require.NoError(t, appendChangeBatchWindow(dst, src, 0, 0, mp))
	})

	newDeleteSource := func() *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{catalog.Row_ID, objectio.DefaultCommitTS_Attr, objectio.TombstoneAttr_PK_Attr})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		var blockID types.Blockid
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.NewRowid(&blockID, 1), false, mp))
		appendTS(bat.Vecs[1], types.BuildTS(20, 0))
		appendInt(bat.Vecs[2], 42)
		bat.SetRowCount(1)
		return bat
	}

	t.Run("delete replay output shapes", func(t *testing.T) {
		source := newDeleteSource()
		defer source.Clean(mp)
		entry := &RowEntry{Batch: source, Offset: 0, Time: types.BuildTS(20, 0)}

		var compact *batch.Batch
		require.NoError(t, fillInDeleteBatch(&compact, entry, false, mp))
		require.Equal(t, 1, compact.RowCount())
		require.Equal(t, []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}, compact.Attrs)
		compact.Clean(mp)

		var retained *batch.Batch
		require.NoError(t, fillInDeleteBatch(&retained, entry, true, mp))
		require.Equal(t, 3, len(retained.Vecs))
		require.Equal(t, types.T_Rowid, retained.Vecs[0].GetType().Oid)
		retained.Clean(mp)

		require.Error(t, fillInDeleteBatch(nil, entry, false, mp))
		badOffset := *entry
		badOffset.Offset = 2
		var out *batch.Batch
		require.ErrorContains(t, fillInDeleteBatch(&out, &badOffset, false, mp), "unavailable")
	})

	t.Run("delete replay validates destination", func(t *testing.T) {
		source := newDeleteSource()
		defer source.Clean(mp)
		entry := &RowEntry{Batch: source, Offset: 0, Time: types.BuildTS(20, 0)}

		bad := batch.NewWithSize(1)
		bad.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bad.SetAttributes([]string{objectio.TombstoneAttr_PK_Attr})
		require.ErrorContains(t, fillInDeleteBatch(&bad, entry, false, mp), "schema is inconsistent")
		bad.Clean(mp)

		bad = batch.NewWithSize(2)
		bad.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bad.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		bad.SetAttributes([]string{"wrong", objectio.DefaultCommitTS_Attr})
		require.ErrorContains(t, fillInDeleteBatch(&bad, entry, false, mp), "attribute 0")
		bad.Clean(mp)

		bad = batch.NewWithSize(2)
		bad.SetAttributes([]string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr})
		var err error
		bad.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), int64(1), 0, mp)
		require.NoError(t, err)
		bad.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.ErrorContains(t, fillInDeleteBatch(&bad, entry, false, mp), "not appendable")
		bad.Clean(mp)
	})

	t.Run("loaded schema projection", func(t *testing.T) {
		newDerived := func() (*vector.Vector, *vector.Vector) {
			rowID := vector.NewVec(types.T_Rowid.ToType())
			var blockID types.Blockid
			require.NoError(t, vector.AppendFixed(rowID, types.NewRowid(&blockID, 0), false, mp))
			commit := vector.NewVec(types.T_TS.ToType())
			appendTS(commit, types.BuildTS(30, 0))
			return rowID, commit
		}
		schema := &engine.CollectChangesSchema{
			Attrs:   []string{"value", "added"},
			Types:   []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
			Seqnums: []uint16{7, 8},
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		appendInt(bat.Vecs[0], 7)
		bat.SetRowCount(1)
		rowID, commit := newDerived()
		require.NoError(t, projectLoadedDataBatch(bat, []uint16{7}, schema, nil, rowID, commit, true, mp))
		require.Equal(t, []string{catalog.Row_ID, "value", "added", objectio.DefaultCommitTS_Attr}, bat.Attrs)
		require.True(t, bat.Vecs[2].IsConstNull())
		bat.Clean(mp)

		bat = batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		appendInt(bat.Vecs[0], 1)
		appendInt(bat.Vecs[1], 2)
		bat.SetRowCount(1)
		rowID, commit = newDerived()
		require.ErrorContains(t, projectLoadedDataBatch(bat, []uint16{7, 7}, schema, nil, rowID, commit, false, mp), "duplicate user sequence")
		rowID.Free(mp)
		commit.Free(mp)
		bat.Clean(mp)

		bat = batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
		bat.SetRowCount(1)
		rowID, commit = newDerived()
		require.ErrorContains(t, projectLoadedDataBatch(bat, []uint16{7}, schema, nil, rowID, commit, false, mp), "incompatible")
		rowID.Free(mp)
		commit.Free(mp)
		bat.Clean(mp)
	})
}

func TestChangeHandleStateContracts(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)

	t.Run("collection input validation", func(t *testing.T) {
		start, end := types.BuildTS(1, 0), types.BuildTS(2, 0)
		require.Error(t, validateChangeCollectionInputs(nil, start, end, 1, 0, mp, fs))
		require.Error(t, validateChangeCollectionInputs(context.Background(), end, start, 1, 0, mp, fs))
		require.Error(t, validateChangeCollectionInputs(context.Background(), start, end, 0, 0, mp, fs))
		require.Error(t, validateChangeCollectionInputs(context.Background(), start, end, 1, -2, mp, fs))
		require.NoError(t, validateChangeCollectionInputs(context.Background(), start, end, 1, -1, mp, fs))
		_, err := validateChangeObjectBlockCount(nil)
		require.Error(t, err)
		stats := objectio.ObjectStats{}
		_, err = validateChangeObjectBlockCount(&stats)
		require.Error(t, err)
		require.Empty(t, changeObjectIdentity(nil))
		require.Equal(t, float64(0), calcPruneRate(1, 0))
		require.Equal(t, float64(0.5), calcPruneRate(1, 2))
	})

	newRows := func() *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr})
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, mp))
		bat.Vecs[1], err = vector.NewConstFixed(
			types.T_TS.ToType(), types.BuildTS(10, 0), 2, mp,
		)
		require.NoError(t, err)
		bat.SetRowCount(2)
		return bat
	}

	t.Run("row handle state and output", func(t *testing.T) {
		parent := &baseHandle{changesHandle: &ChangeHandler{}}
		handle := NewRowHandle(newRows(), mp, parent, context.Background(), false)
		require.False(t, handle.IsEmpty())
		require.Equal(t, 2, handle.Rows())
		require.False(t, handle.isEnd())
		require.Equal(t, types.BuildTS(10, 0), handle.NextTS())
		require.NoError(t, handle.init(true, mp))

		var out *batch.Batch
		require.NoError(t, handle.Next(&out, mp))
		require.Equal(t, 1, out.RowCount())
		require.NoError(t, handle.Next(&out, mp))
		require.Equal(t, 2, out.RowCount())
		require.Error(t, handle.Next(&out, mp))
		out.Clean(mp)
		handle.Close()
		require.True(t, handle.IsEmpty())
		require.Zero(t, handle.NextTS())

		handle = NewRowHandle(newRows(), mp, parent, context.Background(), false)
		var quick *batch.Batch
		require.NoError(t, handle.QuickNext(&quick, mp))
		require.Equal(t, 2, quick.RowCount())
		require.Error(t, handle.QuickNext(&quick, mp))
		quick.Clean(mp)
		handle.Close()

		var nilHandle *BatchHandle
		require.True(t, nilHandle.IsEmpty())
		require.Zero(t, nilHandle.Rows())
	})

	t.Run("empty object handles", func(t *testing.T) {
		parent := &baseHandle{changesHandle: &ChangeHandler{}}
		cn := NewCNObjectHandle(false, nil, fs, parent, mp)
		require.True(t, cn.IsEmpty())
		require.True(t, cn.isEnd())
		var out *batch.Batch
		require.Error(t, cn.Next(context.Background(), &out, mp))
		require.Error(t, cn.QuickNext(context.Background(), &out, mp))
		require.Zero(t, cn.NextTS())
		cn.Close()
		var nilCN *CNObjectHandle
		require.True(t, nilCN.IsEmpty())
		nilCN.Close()

		aobj := NewAObjectHandle(
			context.Background(), parent, false,
			types.BuildTS(1, 0), types.BuildTS(2, 0), nil, fs, mp,
		)
		require.True(t, aobj.IsEmpty())
		require.Zero(t, aobj.RowCount())
		require.True(t, aobj.isEnd())
		require.Error(t, aobj.Next(context.Background(), &out, mp))
		require.Error(t, aobj.QuickNext(context.Background(), &out, mp))
		aobj.Close()
		var nilAObj *AObjectHandle
		require.True(t, nilAObj.IsEmpty())
		require.Zero(t, nilAObj.RowCount())
	})

	t.Run("base handle ordering", func(t *testing.T) {
		rows := NewRowHandle(newRows(), mp, &baseHandle{changesHandle: &ChangeHandler{}}, context.Background(), false)
		defer rows.Close()
		base := &baseHandle{
			changesHandle:  &ChangeHandler{},
			inMemoryHandle: rows,
			aobjHandle:     NewAObjectHandle(context.Background(), nil, false, types.TS{}, types.MaxTs(), nil, fs, mp),
			cnObjectHandle: NewCNObjectHandle(false, nil, fs, nil, mp),
		}
		require.False(t, base.IsEmpty())
		require.True(t, base.IsSmall())
		require.Equal(t, types.BuildTS(10, 0), base.NextTS())
		require.False(t, base.less(types.TS{}, types.BuildTS(1, 0)))
		require.True(t, base.less(types.BuildTS(1, 0), types.TS{}))
		base.Close()
	})
}

func TestLoadedDataSchemaSuccessMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	const userSeqnum uint16 = 7
	schema := &engine.CollectChangesSchema{
		Attrs: []string{"value"}, Types: []types.Type{types.T_int64.ToType()}, Seqnums: []uint16{userSeqnum},
	}
	newPersisted := func() (*batch.Batch, objectio.SpecialColumnLayout, []uint16) {
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{"value", objectio.DefaultCommitTS_Attr, objectio.TombstoneAttr_Abort_Attr})
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, mp))
		var err error
		bat.Vecs[1], err = vector.NewConstFixed(types.T_TS.ToType(), types.BuildTS(10, 0), 2, mp)
		require.NoError(t, err)
		bat.Vecs[2], err = vector.NewConstFixed(types.T_bool.ToType(), false, 2, mp)
		require.NoError(t, err)
		bat.SetRowCount(2)
		layout := noSpecialColumnLayout()
		layout.CommitTS, layout.Abort = 1, 2
		return bat, layout, []uint16{userSeqnum, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}
	}
	newCN := func() *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{"value"})
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, mp))
		bat.SetRowCount(2)
		return bat
	}
	var blockID types.Blockid

	t.Run("persisted projection synthesizes rowids", func(t *testing.T) {
		bat, layout, seqnums := newPersisted()
		defer bat.Clean(mp)
		require.NoError(t, updatePersistedDataBatchWithSchema(
			bat, types.BuildTS(1, 0), types.BuildTS(20, 0), &blockID,
			layout, seqnums, schema, true, mp,
		))
		require.Equal(t, []string{catalog.Row_ID, "value", objectio.DefaultCommitTS_Attr}, bat.Attrs)
		require.Equal(t, 2, bat.RowCount())
	})

	t.Run("persisted positional output synthesizes rowids", func(t *testing.T) {
		bat, layout, _ := newPersisted()
		defer bat.Clean(mp)
		require.NoError(t, updatePersistedDataBatch(
			bat, types.BuildTS(1, 0), types.BuildTS(20, 0), &blockID, layout, true, mp,
		))
		require.Equal(t, catalog.Row_ID, bat.Attrs[0])
		require.Equal(t, 3, len(bat.Vecs))
	})

	t.Run("CN projection synthesizes rowids and commit timestamp", func(t *testing.T) {
		bat := newCN()
		defer bat.Clean(mp)
		require.NoError(t, updateCNDataBatchWithSchema(
			bat, types.BuildTS(10, 0), &blockID, noSpecialColumnLayout(),
			[]uint16{userSeqnum}, schema, true, mp,
		))
		require.Equal(t, []string{catalog.Row_ID, "value", objectio.DefaultCommitTS_Attr}, bat.Attrs)
		require.Equal(t, 2, bat.RowCount())
	})

	t.Run("CN positional output synthesizes rowids and commit timestamp", func(t *testing.T) {
		bat := newCN()
		defer bat.Clean(mp)
		require.NoError(t, updateCNDataBatch(
			bat, types.BuildTS(10, 0), &blockID, noSpecialColumnLayout(), true, mp,
		))
		require.Equal(t, catalog.Row_ID, bat.Attrs[0])
		require.Equal(t, objectio.DefaultCommitTS_Attr, bat.Attrs[len(bat.Attrs)-1])
		require.Equal(t, 2, bat.RowCount())
	})

	t.Run("schema projection failure retains source ownership", func(t *testing.T) {
		bat, layout, seqnums := newPersisted()
		defer bat.Clean(mp)
		badSchema := &engine.CollectChangesSchema{
			Attrs: []string{"value"}, Types: []types.Type{types.T_int32.ToType()}, Seqnums: []uint16{userSeqnum},
		}
		require.Error(t, updatePersistedDataBatchWithSchema(
			bat, types.BuildTS(1, 0), types.BuildTS(20, 0), &blockID,
			layout, seqnums, badSchema, true, mp,
		))
	})
}

func TestChangeHandlerNextAndFilterGuards(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)

	newEmptyBase := func(parent *ChangeHandler) *baseHandle {
		base := &baseHandle{changesHandle: parent, skipTS: make(map[types.TS]struct{})}
		base.inMemoryHandle = NewRowHandle(nil, mp, base, context.Background(), false)
		base.aobjHandle = NewAObjectHandle(
			context.Background(), base, false, types.TS{}, types.MaxTs(), nil, fs, mp,
		)
		base.cnObjectHandle = NewCNObjectHandle(false, nil, fs, base, mp)
		return base
	}

	t.Run("quick handler reaches EOF", func(t *testing.T) {
		parent := &ChangeHandler{
			quick: true, coarseMaxRow: 1, LogThreshold: LogThreshold,
			lastPrint: time.Now(), fs: fs,
		}
		parent.dataHandle = newEmptyBase(parent)
		parent.tombstoneHandle = newEmptyBase(parent)
		data, tombstone, hint, err := parent.Next(context.Background(), mp)
		require.NoError(t, err)
		require.Nil(t, data)
		require.Nil(t, tombstone)
		require.Equal(t, engine.ChangesHandle_Tail_done, hint)
		require.NoError(t, parent.Close())
	})

	t.Run("handler input and owner guards", func(t *testing.T) {
		var parent *ChangeHandler
		_, _, _, err := parent.Next(context.Background(), mp)
		require.Error(t, err)

		parent = &ChangeHandler{outputMP: mp, lastPrint: time.Now(), LogThreshold: LogThreshold}
		other := mpool.MustNewZero()
		defer mpool.DeleteMPool(other)
		_, _, _, err = parent.Next(context.Background(), other)
		require.ErrorContains(t, err, "cannot switch output mpool")

		terminal := fmt.Errorf("terminal")
		parent = &ChangeHandler{terminalErr: terminal}
		_, _, _, err = parent.Next(context.Background(), mp)
		require.ErrorIs(t, err, terminal)
		require.NoError(t, parent.Close())
		var nilParent *ChangeHandler
		require.NoError(t, nilParent.Close())
	})

	t.Run("filter validation and cancellation", func(t *testing.T) {
		require.NoError(t, filterBatchWithContext(context.Background(), nil, nil, 0, false, false))
		emptyData := batch.NewWithSize(0)
		emptyTombstone := batch.NewWithSize(0)
		require.Error(t, filterBatchWithContext(nil, emptyData, emptyTombstone, 0, false, false))
		require.Error(t, filterBatchWithContext(context.Background(), emptyData, emptyTombstone, 0, false, false))

		newData := func() *batch.Batch {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.BuildTS(1, 0), false, mp))
			bat.SetRowCount(1)
			return bat
		}
		newTombstone := func() *batch.Batch {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.BuildTS(2, 0), false, mp))
			bat.SetRowCount(1)
			return bat
		}
		data, tombstone := newData(), newTombstone()
		defer data.Clean(mp)
		defer tombstone.Clean(mp)
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := fmt.Errorf("cancel filter")
		cancel(cause)
		require.ErrorIs(t, filterBatchWithContext(ctx, data, tombstone, 0, false, false), cause)
	})
}

func TestChangeHandlerSchemaAndFailureGuards(t *testing.T) {
	validSchema := func() *engine.CollectChangesSchema {
		return &engine.CollectChangesSchema{
			Attrs: []string{"value"}, Types: []types.Type{types.T_int64.ToType()}, Seqnums: []uint16{7},
		}
	}
	withSchema := func(schema *engine.CollectChangesSchema) context.Context {
		return engine.WithCollectChangesSchema(context.Background(), schema)
	}

	t.Run("request schema validation", func(t *testing.T) {
		handle := &ChangeHandler{primarySeqnum: -1}
		require.NoError(t, handle.applyRequestSchema(context.Background()))

		invalidContext := withSchema(validSchema())
		engine.CollectChangesSchemaFromContext(invalidContext).Types = nil
		require.Error(t, handle.applyRequestSchema(invalidContext))

		invalid := validSchema()
		invalid.Types[0] = types.T_any.ToType()
		require.ErrorContains(t, handle.applyRequestSchema(withSchema(invalid)), "column 0 is invalid")

		duplicate := &engine.CollectChangesSchema{
			Attrs:   []string{"a", "b"},
			Types:   []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
			Seqnums: []uint16{7, 7},
		}
		require.ErrorContains(t, handle.applyRequestSchema(withSchema(duplicate)), "duplicate sequence")

		handle.primarySeqnum = 8
		require.ErrorContains(t, handle.applyRequestSchema(withSchema(validSchema())), "absent")
		handle.primarySeqnum = 7
		require.NoError(t, handle.applyRequestSchema(withSchema(validSchema())))
		require.Zero(t, handle.primaryPosition)

		handle = &ChangeHandler{
			primarySeqnum: -1,
			pkFilter: &engine.PKFilter{
				ReplaySpec: &engine.PKReplaySpec{Op: function.EQUAL, Keys: [][]byte{nil}},
			},
		}
		require.NoError(t, handle.applyRequestSchema(context.Background()))
		require.Nil(t, handle.pkFilter)
	})

	t.Run("sticky terminal failure", func(t *testing.T) {
		handle := &ChangeHandler{}
		require.NoError(t, handle.fail(nil))
		first := fmt.Errorf("first")
		require.ErrorIs(t, handle.fail(first), first)
		require.ErrorIs(t, handle.terminalError(), first)
		require.ErrorIs(t, handle.fail(fmt.Errorf("second")), first)
		var nilHandle *ChangeHandler
		require.NoError(t, nilHandle.terminalError())
	})
}

func TestFilterBatchMalformedMatrix(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	newBatch := func(nullPK, wrongTS bool) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), nullPK, mp))
		if wrongTS {
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, mp))
		} else {
			bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.BuildTS(1, 0), false, mp))
		}
		bat.SetRowCount(1)
		return bat
	}
	run := func(t *testing.T, data, tombstone *batch.Batch, primary int) error {
		t.Helper()
		defer data.Clean(mp)
		defer tombstone.Clean(mp)
		return filterBatchWithContext(context.Background(), data, tombstone, primary, false, false)
	}

	t.Run("data primary position", func(t *testing.T) {
		require.Error(t, run(t, newBatch(false, false), newBatch(false, false), 3))
	})
	t.Run("data shape", func(t *testing.T) {
		data := newBatch(false, false)
		data.SetRowCount(2)
		require.Error(t, run(t, data, newBatch(false, false), 0))
	})
	t.Run("data commit type", func(t *testing.T) {
		require.Error(t, run(t, newBatch(false, true), newBatch(false, false), 0))
	})
	t.Run("data null primary key", func(t *testing.T) {
		require.Error(t, run(t, newBatch(true, false), newBatch(false, false), 0))
	})
	t.Run("tombstone schema", func(t *testing.T) {
		tombstone := batch.NewWithSize(1)
		tombstone.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], int64(1), false, mp))
		tombstone.SetRowCount(1)
		require.Error(t, run(t, newBatch(false, false), tombstone, 0))
	})
	t.Run("tombstone shape", func(t *testing.T) {
		tombstone := newBatch(false, false)
		tombstone.SetRowCount(2)
		require.Error(t, run(t, newBatch(false, false), tombstone, 0))
	})
	t.Run("tombstone commit type", func(t *testing.T) {
		require.Error(t, run(t, newBatch(false, false), newBatch(false, true), 0))
	})
	t.Run("tombstone null primary key", func(t *testing.T) {
		require.Error(t, run(t, newBatch(false, false), newBatch(true, false), 0))
	})
}
