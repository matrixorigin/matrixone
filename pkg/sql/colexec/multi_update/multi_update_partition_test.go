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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestClonePartitionPhaseContextsSeparatesDeleteAndInsert(t *testing.T) {
	contexts := []*MultiUpdateCtx{{
		ObjRef:             &plan.ObjectRef{},
		TableDef:           &plan.TableDef{},
		InsertCols:         []int{1, 2},
		DeleteCols:         []int{3, 4},
		DedupByTargetRowID: true,
	}}
	deleteContexts := clonePartitionPhaseContexts(contexts, true)
	insertContexts := clonePartitionPhaseContexts(contexts, false)

	require.Empty(t, deleteContexts[0].InsertCols)
	require.Equal(t, []int{3, 4}, deleteContexts[0].DeleteCols)
	require.Empty(t, insertContexts[0].DeleteCols)
	require.Equal(t, []int{1, 2}, insertContexts[0].InsertCols)
	require.False(t, deleteContexts[0].DedupByTargetRowID)
	require.False(t, insertContexts[0].DedupByTargetRowID)
	require.Equal(t, []int{1, 2}, contexts[0].InsertCols)
	require.Equal(t, []int{3, 4}, contexts[0].DeleteCols)
}

func TestResetMultiUpdateCtxsClassifiesTemporaryIndexTables(t *testing.T) {
	uniqueName := "__mo_tmp_018f1f767b9d7f35b2d99b8d7774bde8_db_" +
		catalog.UniqueIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	secondaryName := "__mo_tmp_018f1f767b9d7f35b2d99b8d7774bde8_db_" +
		catalog.SecondaryIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	op := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{
		{TableDef: &plan.TableDef{Name: "main_table"}},
		{TableDef: &plan.TableDef{Name: uniqueName}},
		{TableDef: &plan.TableDef{Name: secondaryName}},
	}}

	op.resetMultiUpdateCtxs()

	require.Equal(t, UpdateMainTable, lookupUpdateCtxInfo(op.ctr.updateCtxInfos, op.MultiUpdateCtx[0]).tableType)
	require.Equal(t, UpdateUniqueIndexTable, lookupUpdateCtxInfo(op.ctr.updateCtxInfos, op.MultiUpdateCtx[1]).tableType)
	require.Equal(t, UpdateSecondaryIndexTable, lookupUpdateCtxInfo(op.ctr.updateCtxInfos, op.MultiUpdateCtx[2]).tableType)
}

func TestPartitionMultiUpdateString(t *testing.T) {
	op := &PartitionMultiUpdate{}
	buf := new(bytes.Buffer)
	op.String(buf)
	require.Equal(t, "MultiUpdate: partition_multi_update", buf.String())
}

func TestBuildPartitionUpdateTargetsKeepsPhysicalTargetsIndependent(t *testing.T) {
	contexts := []*MultiUpdateCtx{
		{
			ObjRef:             &plan.ObjectRef{},
			TableDef:           &plan.TableDef{TblId: 10, Name: "plain"},
			TargetUpdateCtxIdx: 0,
		},
		{
			ObjRef: &plan.ObjectRef{},
			TableDef: &plan.TableDef{
				TblId:       20,
				Name:        "partitioned",
				FeatureFlag: features.Partitioned,
			},
			TargetUpdateCtxIdx: 1,
		},
		{
			ObjRef: &plan.ObjectRef{},
			TableDef: &plan.TableDef{
				TblId:       21,
				Name:        "partition_index",
				FeatureFlag: features.IndexTable,
			},
			TargetUpdateCtxIdx: 1,
		},
	}

	targets := buildPartitionUpdateTargets(contexts)

	require.Len(t, targets, 2)
	require.Equal(t, uint64(10), targets[0].tableID)
	require.Len(t, targets[0].contexts, 1)
	require.Equal(t, uint64(20), targets[1].tableID)
	require.Len(t, targets[1].contexts, 2)
	require.Equal(t, 0, targets[1].contexts[1].TargetUpdateCtxIdx)
	require.NotSame(t, contexts[1].TableDef, targets[1].contexts[0].TableDef)
	require.NotSame(t, contexts[1], targets[1].contexts[0])
}

func TestPartitionWriterIDsSeparateAliasesOfSamePhysicalTable(t *testing.T) {
	first := &partitionUpdateTarget{writerIDs: make(map[uint64]uint64)}
	second := &partitionUpdateTarget{writerIDs: make(map[uint64]uint64)}
	op := &PartitionMultiUpdate{}

	firstID := op.writerID(first, 100)
	require.Equal(t, firstID, op.writerID(first, 100))
	require.NotEqual(t, firstID, op.writerID(second, 100))
}

func TestNewPartitionMultiUpdateFrom(t *testing.T) {
	ps := &PartitionMultiUpdate{
		raw: &MultiUpdate{RejectZeroTemporal: true},
	}
	op := NewPartitionMultiUpdateFrom(ps)
	require.Equal(t, ps.raw.MultiUpdateCtx, op.(*PartitionMultiUpdate).raw.MultiUpdateCtx)
	require.Equal(t, ps.raw.Action, op.(*PartitionMultiUpdate).raw.Action)
	require.Equal(t, ps.raw.IsOnduplicateKeyUpdate, op.(*PartitionMultiUpdate).raw.IsOnduplicateKeyUpdate)
	require.Equal(t, ps.raw.Engine, op.(*PartitionMultiUpdate).raw.Engine)
	require.Equal(t, ps.raw.RejectZeroTemporal, op.(*PartitionMultiUpdate).raw.RejectZeroTemporal)
}

func TestPartitionMultiUpdateSetRejectZeroTemporalUpdatesWriters(t *testing.T) {
	active := &s3WriterDelegate{}
	free := &s3WriterDelegate{}
	op := &PartitionMultiUpdate{
		raw:         &MultiUpdate{},
		writers:     map[uint64]*s3WriterDelegate{1: active},
		freeWriters: []*s3WriterDelegate{free},
	}

	op.SetRejectZeroTemporal(true)
	require.True(t, op.raw.RejectZeroTemporal)
	require.True(t, active.rejectZeroTemporal)
	require.True(t, free.rejectZeroTemporal)
}

func TestPartitionMultiUpdateResetReleasesWriters(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := &partitionUpdateTarget{writerIDs: map[uint64]uint64{10: 1}}
	op := &PartitionMultiUpdate{
		raw:          &MultiUpdate{},
		targets:      []*partitionUpdateTarget{target},
		writers:      map[uint64]*s3WriterDelegate{1: {}},
		freeWriters:  []*s3WriterDelegate{{}},
		nextWriterID: 1,
	}

	op.Reset(proc, false, nil)

	require.Empty(t, op.writers)
	require.Nil(t, op.freeWriters)
	require.Empty(t, target.writerIDs)
	require.Zero(t, op.nextWriterID)
}

func TestAddInsertAffectRows(t *testing.T) {
	tests := []struct {
		name       string
		action     actionType
		tableType  UpdateTableType
		rowCount   uint64
		expectRows uint64
	}{
		{
			name:       "actionInsert with main table",
			action:     actionInsert,
			tableType:  UpdateMainTable,
			rowCount:   5,
			expectRows: 5,
		},
		{
			name:       "actionUpdate with main table (REPLACE INTO)",
			action:     actionUpdate,
			tableType:  UpdateMainTable,
			rowCount:   3,
			expectRows: 3,
		},
		{
			name:       "actionInsert with unique index table (should not count)",
			action:     actionInsert,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   5,
			expectRows: 0,
		},
		{
			name:       "actionUpdate with unique index table (should not count)",
			action:     actionUpdate,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionDelete with main table (should not count)",
			action:     actionDelete,
			tableType:  UpdateMainTable,
			rowCount:   2,
			expectRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := &MultiUpdate{
				ctr: container{
					action:       tt.action,
					affectedRows: 0,
				},
			}
			update.addAffectedRowsFunc = update.doAddAffectedRows

			update.addInsertAffectRows(tt.tableType, tt.rowCount)

			require.Equal(t, tt.expectRows, update.ctr.affectedRows, "affected rows should match expected value")
		})
	}
}

func TestAddDeleteAffectRows(t *testing.T) {
	tests := []struct {
		name       string
		action     actionType
		tableType  UpdateTableType
		rowCount   uint64
		expectRows uint64
	}{
		{
			name:       "actionDelete with main table",
			action:     actionDelete,
			tableType:  UpdateMainTable,
			rowCount:   5,
			expectRows: 5,
		},
		{
			name:       "actionUpdate with main table (should not count for REPLACE INTO)",
			action:     actionUpdate,
			tableType:  UpdateMainTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionDelete with unique index table (should not count)",
			action:     actionDelete,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   5,
			expectRows: 0,
		},
		{
			name:       "actionUpdate with unique index table (should not count)",
			action:     actionUpdate,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionInsert with main table (should not count)",
			action:     actionInsert,
			tableType:  UpdateMainTable,
			rowCount:   2,
			expectRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := &MultiUpdate{
				ctr: container{
					action:       tt.action,
					affectedRows: 0,
				},
			}
			update.addAffectedRowsFunc = update.doAddAffectedRows

			update.addDeleteAffectRows(tt.tableType, tt.rowCount)

			require.Equal(t, tt.expectRows, update.ctr.affectedRows, "affected rows should match expected value")
		})
	}
}

func TestUpdateAffectedRows(t *testing.T) {
	// Test UPDATE scenario: should only count INSERT rows (updated rows), not DELETE rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionUpdate, // UPDATE uses actionUpdate
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	// Simulate UPDATE: DELETE 3 rows, INSERT 3 rows
	// Should only count INSERT rows (3), not DELETE rows
	update.addDeleteAffectRows(UpdateMainTable, 3) // Should not count
	require.Equal(t, uint64(0), update.ctr.affectedRows, "DELETE rows should not be counted for UPDATE")

	update.addInsertAffectRows(UpdateMainTable, 3) // Should count
	require.Equal(t, uint64(3), update.ctr.affectedRows, "INSERT rows should be counted for UPDATE")
}

func TestInsertAffectedRows(t *testing.T) {
	// Test INSERT scenario: should count INSERT rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionInsert,
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	update.addInsertAffectRows(UpdateMainTable, 5)
	require.Equal(t, uint64(5), update.ctr.affectedRows, "INSERT rows should be counted")
}

func TestDeleteAffectedRows(t *testing.T) {
	// Test DELETE scenario: should count DELETE rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionDelete,
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	update.addDeleteAffectRows(UpdateMainTable, 4)
	require.Equal(t, uint64(4), update.ctr.affectedRows, "DELETE rows should be counted")
}

// TestMultiUpdateCtxClonePartitionCols verifies that clone() correctly copies
// PartitionCols and keeps nested objects independent.
func TestMultiUpdateCtxClonePartitionCols(t *testing.T) {
	original := &MultiUpdateCtx{
		InsertCols:         []int{1, 2, 3},
		DeleteCols:         []int{4, 5},
		PartitionCols:      []int{6, 7, 8, 9},
		DedupByTargetRowID: true,
		TargetUpdateCtxIdx: 10,
		ObjRef:             &plan.ObjectRef{SchemaName: "test", ObjName: "t1"},
		TableDef:           &plan.TableDef{Name: "t1"},
	}

	cloned := original.clone()

	require.Equal(t, original.PartitionCols, cloned.PartitionCols,
		"PartitionCols should match original")
	require.NotEqual(t, original.DeleteCols, cloned.PartitionCols,
		"PartitionCols should not be DeleteCols")
	require.Equal(t, original.InsertCols, cloned.InsertCols)
	require.Equal(t, original.DeleteCols, cloned.DeleteCols)
	require.True(t, cloned.DedupByTargetRowID)
	require.Equal(t, original.TargetUpdateCtxIdx, cloned.TargetUpdateCtxIdx)
	require.Equal(t, original.ObjRef.SchemaName, cloned.ObjRef.SchemaName)
	require.Equal(t, original.TableDef.Name, cloned.TableDef.Name)
	require.NotSame(t, original.ObjRef, cloned.ObjRef)
	require.NotSame(t, original.TableDef, cloned.TableDef)
	cloned.ObjRef.ObjName = "modified"
	require.Equal(t, "t1", original.ObjRef.ObjName, "original ObjRef should be unchanged")
}
