// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAffectRowsTestOp builds a bare MultiUpdate wired with the real
// affected-rows accumulator so the counting helpers can be exercised directly.
func newAffectRowsTestOp(action actionType, countDelete bool) *MultiUpdate {
	op := &MultiUpdate{}
	op.ctr.action = action
	op.CountDeleteAffectRows = countDelete
	op.addAffectedRowsFunc = op.doAddAffectedRows
	return op
}

func TestInsertAffectedRowsUsesChangedRowsMarker(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	changed := vector.NewVec(types.T_bool.ToType())
	defer changed.Free(proc.Mp())
	for _, value := range []bool{false, true, false, true} {
		require.NoError(t, vector.AppendFixed(changed, value, false, proc.Mp()))
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = changed
	input.SetRowCount(4)

	markerCol := 0
	require.EqualValues(t, 2, insertAffectedRows(&MultiUpdateCtx{ChangedRowsCol: &markerCol}, input))
	require.EqualValues(t, 4, insertAffectedRows(&MultiUpdateCtx{}, input))
}

func TestFilterODKUPhysicalRowsSeparatesLogicalCount(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	weights := vector.NewVec(types.T_uint64.ToType())
	physical := vector.NewVec(types.T_bool.ToType())
	defer weights.Free(proc.Mp())
	defer physical.Free(proc.Mp())
	for _, value := range []uint64{6, 4, 0} {
		require.NoError(t, vector.AppendFixed(weights, value, false, proc.Mp()))
	}
	for _, value := range []bool{true, false, false} {
		require.NoError(t, vector.AppendFixed(physical, value, false, proc.Mp()))
	}
	input := batch.NewWithSize(2)
	input.Vecs[0], input.Vecs[1] = weights, physical
	input.SetRowCount(3)
	weightCol, physicalCol := 0, 1

	filtered, owned, affected, err := filterODKUPhysicalRows(proc, &MultiUpdateCtx{
		AffectedRowsWeightCol:  &weightCol,
		PhysicalChangedRowsCol: &physicalCol,
	}, input)
	require.NoError(t, err)
	require.True(t, owned)
	defer filtered.Clean(proc.Mp())
	require.EqualValues(t, 10, affected)
	require.Equal(t, 1, filtered.RowCount(), "restored/no-op rows must count logically but not be written")
}

func TestFilterODKUPhysicalRowsFastPathAndMalformedMetadata(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	weights := vector.NewVec(types.T_uint64.ToType())
	physical := vector.NewVec(types.T_bool.ToType())
	defer weights.Free(proc.Mp())
	defer physical.Free(proc.Mp())
	for _, value := range []uint64{1, 2} {
		require.NoError(t, vector.AppendFixed(weights, value, false, proc.Mp()))
	}
	for range 2 {
		require.NoError(t, vector.AppendFixed(physical, true, false, proc.Mp()))
	}
	input := batch.NewWithSize(2)
	input.Vecs[0], input.Vecs[1] = weights, physical
	input.SetRowCount(2)
	weightCol, physicalCol := 0, 1
	ctx := &MultiUpdateCtx{
		AffectedRowsWeightCol:  &weightCol,
		PhysicalChangedRowsCol: &physicalCol,
	}

	filtered, owned, affected, err := filterODKUPhysicalRows(proc, ctx, input)
	require.NoError(t, err)
	require.False(t, owned)
	require.Same(t, input, filtered, "all changed rows must not pay for a clone/selection")
	require.EqualValues(t, 3, affected)

	badWeightCol := 1
	_, _, _, err = filterODKUPhysicalRows(proc, &MultiUpdateCtx{
		AffectedRowsWeightCol: &badWeightCol,
	}, input)
	require.ErrorContains(t, err, "invalid ODKU affected-row weight column")
}

func TestS3ODKUAffectedRowsTransferredToFlush(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	analyzer := process.NewAnalyzer(0, false, false, "s3-affected-rows-test")

	pending := uint64(9)
	writer := &s3WriterDelegate{
		takeAffectedRows: func() uint64 {
			rows := pending
			pending = 0
			return rows
		},
	}
	require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
	require.Zero(t, pending)
	// A merge may append control records from independent remote writer
	// operators. Their counts must be additive at the single flush owner.
	require.NoError(t, writer.addAffectedRowsToOutput(proc.Mp(), 4))
	require.Equal(t, 2, writer.outputBat.RowCount(),
		"a logical count must survive even when a no-op produces no storage batch")
	require.Equal(t, []uint8{uint8(actionAffectedRows), uint8(actionAffectedRows)},
		vector.MustFixedColWithTypeCheck[uint8](writer.outputBat.Vecs[0]))
	require.Equal(t, []uint64{9, 4},
		vector.MustFixedColWithTypeCheck[uint64](writer.outputBat.Vecs[2]))

	// The control record is consumed without resolving a table or unmarshalling
	// a storage batch. This models the coordinator after any local/remote merge.
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{writer.outputBat})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	writer.outputBat = nil // transfer ownership to the mock pipeline source
	flush := &MultiUpdate{}
	flush.addAffectedRowsFunc = flush.doAddAffectedRows
	flush.AppendChild(child)
	_, err := flush.updateFlushS3Info(proc, analyzer)
	require.NoError(t, err)
	require.EqualValues(t, 13, flush.GetAffectedRows())
}

func TestS3ODKUAffectedRowsDrainedExactlyOnceAcrossWriters(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	analyzer := process.NewAnalyzer(0, false, false, "s3-affected-rows-test")

	pending := uint64(7)
	take := func() uint64 {
		rows := pending
		pending = 0
		return rows
	}
	writers := []*s3WriterDelegate{{takeAffectedRows: take}, {takeAffectedRows: take}}
	for _, writer := range writers {
		require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
		defer writer.outputBat.Clean(proc.Mp())
	}
	require.Equal(t, 1, writers[0].outputBat.RowCount())
	require.Zero(t, writers[1].outputBat.RowCount(),
		"only one parallel writer may transfer the shared logical count")
}

func TestPrepareSelectsTopologyStableS3AffectedRowsOwner(t *testing.T) {
	_, _, proc := prepareTestCtx(t, true)
	defer proc.Free()
	objRef, tableDef := getTestMainTable()
	weightCol := 0

	newODKUWriter := func() *MultiUpdate {
		return &MultiUpdate{
			Action: UpdateWriteS3,
			MultiUpdateCtx: []*MultiUpdateCtx{{
				ObjRef:                objRef,
				TableDef:              tableDef,
				TargetUpdateCtxIdx:    0,
				AffectedRowsWeightCol: &weightCol,
			}},
		}
	}

	t.Run("ordinary writer keeps legacy physical owner", func(t *testing.T) {
		op := &MultiUpdate{
			Action: UpdateWriteS3,
			MultiUpdateCtx: []*MultiUpdateCtx{{
				ObjRef:             objRef,
				TableDef:           tableDef,
				TargetUpdateCtxIdx: 0,
			}},
		}
		require.NoError(t, op.Prepare(proc))
		op.addAffectedRowsFunc(3)
		require.EqualValues(t, 3, op.GetAffectedRows())
		require.Nil(t, op.takeS3AffectedRowsFunc)
		op.Free(proc, false, nil)
	})

	t.Run("direct ODKU writer transfers instead of publishing", func(t *testing.T) {
		op := newODKUWriter()
		require.NoError(t, op.Prepare(proc))
		op.addAffectedRowsFunc(5)
		require.Zero(t, op.GetAffectedRows())
		require.EqualValues(t, 5, op.takeS3AffectedRowsFunc())
		require.Zero(t, op.takeS3AffectedRowsFunc())
		op.Free(proc, false, nil)
	})

	t.Run("failed attempt cannot leak its count into operator reuse", func(t *testing.T) {
		op := newODKUWriter()
		require.NoError(t, op.Prepare(proc))
		op.addAffectedRowsFunc(11)
		op.Reset(proc, true, assert.AnError)
		require.Zero(t, op.ctr.s3AffectedRows)
		require.Zero(t, op.takeS3AffectedRowsFunc())
		op.Free(proc, true, assert.AnError)
	})

	t.Run("partition ODKU writer transfers once at wrapper boundary", func(t *testing.T) {
		raw := newODKUWriter()
		op := NewPartitionMultiUpdate(raw).(*PartitionMultiUpdate)
		require.NoError(t, op.Prepare(proc))
		op.raw.addAffectedRowsFunc(7)
		require.Zero(t, op.GetAffectedRows())
		controlWriter := op.getFlushableS3Writer()
		require.Same(t, op.raw.ctr.s3Writer, controlWriter,
			"an all-no-op partition statement must still have a control output owner")
		require.NoError(t, controlWriter.flushTailAndWriteToOutput(
			proc, process.NewAnalyzer(0, false, false, "partition-s3-affected-rows-test")))
		require.Equal(t, 1, controlWriter.outputBat.RowCount())
		require.Equal(t, uint8(actionAffectedRows),
			vector.MustFixedColWithTypeCheck[uint8](controlWriter.outputBat.Vecs[0])[0])
		require.EqualValues(t, 7,
			vector.MustFixedColWithTypeCheck[uint64](controlWriter.outputBat.Vecs[2])[0])
		require.Nil(t, op.getFlushableS3Writer())
		op.Free(proc, false, nil)
	})

	t.Run("failed partition attempt discards pending control state", func(t *testing.T) {
		raw := newODKUWriter()
		op := NewPartitionMultiUpdate(raw).(*PartitionMultiUpdate)
		require.NoError(t, op.Prepare(proc))
		op.raw.addAffectedRowsFunc(13)
		op.Reset(proc, true, assert.AnError)
		require.Zero(t, op.s3AffectedRows)
		require.Nil(t, op.getFlushableS3Writer())
		op.Free(proc, true, assert.AnError)
	})
}

// TestUpsertAffectRowsAccounting pins the MySQL-compatible affected-rows
// accounting for the main table: a plain UPDATE counts the matched row once
// (INSERT side only), while an upsert (REPLACE / INSERT ... ON DUPLICATE KEY
// UPDATE) counts both the conflicting-row DELETE and the INSERT, yielding 2 for
// a replaced/updated row and 1 for a newly inserted row.
func TestUpsertAffectRowsAccounting(t *testing.T) {
	t.Run("plain update counts matched row once", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, false)
		op.addDeleteAffectRows(UpdateMainTable, 1) // not counted for plain UPDATE
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 1, op.GetAffectedRows())
	})

	t.Run("upsert updates an existing row -> 2", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addDeleteAffectRows(UpdateMainTable, 1)
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 2, op.GetAffectedRows())
	})

	t.Run("upsert inserts a brand new row -> 1", func(t *testing.T) {
		// A new row has no conflicting DELETE, only the INSERT is counted.
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 1, op.GetAffectedRows())
	})

	t.Run("pure delete counts deleted rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionDelete, false)
		op.addDeleteAffectRows(UpdateMainTable, 3)
		require.EqualValues(t, 3, op.GetAffectedRows())
	})

	t.Run("pure insert counts inserted rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionInsert, false)
		op.addInsertAffectRows(UpdateMainTable, 5)
		require.EqualValues(t, 5, op.GetAffectedRows())
	})

	t.Run("index tables never affect rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateUniqueIndexTable, 4)
		op.addDeleteAffectRows(UpdateUniqueIndexTable, 4)
		op.addInsertAffectRows(UpdateSecondaryIndexTable, 7)
		op.addDeleteAffectRows(UpdateSecondaryIndexTable, 7)
		require.EqualValues(t, 0, op.GetAffectedRows())
	})

	t.Run("foreign key side effects never affect rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, true)
		op.MultiUpdateCtx = []*MultiUpdateCtx{{IgnoreAffectedRows: true}}
		op.addInsertAffectRows(UpdateMainTable, 4)
		op.addDeleteAffectRows(UpdateMainTable, 3)
		require.EqualValues(t, 0, op.GetAffectedRows())
	})

	t.Run("batch upsert: new + updated rows", func(t *testing.T) {
		// 2 brand new rows (INSERT only) + 3 conflicting rows (DELETE + INSERT):
		// inserts cover all 5 rows, deletes cover the 3 conflicts => 5 + 3 = 8.
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateMainTable, 5)
		op.addDeleteAffectRows(UpdateMainTable, 3)
		require.EqualValues(t, 8, op.GetAffectedRows())
	})
}
