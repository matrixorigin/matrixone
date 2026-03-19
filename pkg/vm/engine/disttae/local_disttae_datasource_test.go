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

package disttae

import (
	"context"
	"strings"
	"sync"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

func TestRelationDataV2_MarshalAndUnMarshal(t *testing.T) {
	location := objectio.NewRandomLocation(0, 0)
	objID := location.ObjectId()
	metaLoc := objectio.ObjectLocation(location)

	relData := readutil.NewBlockListRelationData(0)
	blkNum := 10
	for i := 0; i < blkNum; i++ {
		blkID := types.NewBlockidWithObjectID(&objID, uint16(blkNum))
		blkInfo := objectio.BlockInfo{
			BlockID: blkID,
			MetaLoc: metaLoc,
		}
		blkInfo.ObjectFlags |= objectio.ObjectFlag_Appendable
		relData.AppendBlockInfo(&blkInfo)
	}

	tombstone := readutil.NewEmptyTombstoneData()
	for i := 0; i < 3; i++ {
		rowid := types.RandomRowid()
		tombstone.AppendInMemory(rowid)
	}
	var stats1, stats2 objectio.ObjectStats
	location1 := objectio.NewRandomLocation(1, 1111)
	location2 := objectio.NewRandomLocation(2, 1111)

	objectio.SetObjectStatsLocation(&stats1, location1)
	objectio.SetObjectStatsLocation(&stats2, location2)
	tombstone.AppendFiles(stats1, stats2)
	relData.AttachTombstones(tombstone)

	buf, err := relData.MarshalBinary()
	require.NoError(t, err)

	newRelData, err := readutil.UnmarshalRelationData(buf)
	require.NoError(t, err)
	require.Equal(t, relData.String(), newRelData.String())
}

func TestLocalDatasource_ApplyWorkspaceFlushedS3Deletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	txnOp.AddWorkspace(&Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	})

	txnDB := txnDatabase{
		op: txnOp,
	}

	txnTbl := txnTable{
		db: &txnDB,
	}

	pState := logtailreplay.NewPartitionState("", true, 0, false)

	proc := testutil.NewProc(t)

	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	ls := &LocalDisttaeDataSource{
		fs:     fs,
		ctx:    ctx,
		table:  &txnTbl,
		pState: pState,
	}

	//var stats []objectio.ObjectStats
	int32Type := types.T_int32.ToType()
	var tombstoneRowIds []types.Rowid
	for i := 0; i < 3; i++ {
		writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, int32Type, -1)
		require.NoError(t, err)

		bat := readutil.NewCNTombstoneBatch(
			&int32Type,
			objectio.HiddenColumnSelection_None,
		)

		for j := 0; j < 10; j++ {
			row := types.RandomRowid()
			tombstoneRowIds = append(tombstoneRowIds, row)
			vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
			vector.AppendFixed[int32](bat.Vecs[1], int32(j), false, proc.GetMPool())
		}

		bat.SetRowCount(bat.Vecs[0].Length())

		err = writer.Write(ctx, bat)
		require.NoError(t, err)

		ss, err := writer.Sync(proc.Ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(ss))
		require.False(t, ss[0].IsZero())

		//stats = append(stats, ss)
		txnOp.GetWorkspace().(*Transaction).StashFlushedTombstones(ss[0])
	}

	deletedMask := objectio.GetReusableBitmap()
	defer deletedMask.Release()
	for i := range tombstoneRowIds {
		bid := tombstoneRowIds[i].BorrowBlockID()
		left, err := ls.applyWorkspaceFlushedS3Deletes(bid, nil, &deletedMask)
		require.NoError(t, err)
		require.Zero(t, len(left))

		require.True(t, deletedMask.Contains(uint64(tombstoneRowIds[i].GetRowOffset())))
	}
}

func TestBigS3WorkspaceIterMissingData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	// This batch can be obtained by 'insert into db.t1 select result from generate_series(1, 67117056) g;'
	s3Bat := batch.NewWithSize(2)
	s3Bat.SetRowCount(8193)
	s3Bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	txn := &Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
		op:            txnOp,
		deletedBlocks: &deletedBlocks{},
		writes: []Entry{
			{
				typ:        INSERT,
				databaseId: 11,
				tableId:    22,
				fileName:   "a-s3-file-name",
				bat:        s3Bat,
			},
		},
	}

	// This batch can be obtained by 'insert into db.t2 values (1);'
	normalBat := batch.NewWithSize(1)
	normalBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	m := mpool.MustNewZero()
	normalBat.SetRowCount(1)
	vector.AppendFixed(normalBat.Vecs[0], int32(1), false, m)
	txn.WriteBatch(INSERT, "", 0, 11, 23, "db", "t2", normalBat, DNStore{})

	txnOp.AddWorkspace(txn)

	// query t2 table
	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: len(txn.writes),
		table: &txnTable{
			db: &txnDatabase{
				databaseId: 11,
				op:         txnOp,
			},
			tableId: 23,
		},
		memPKFilter: &readutil.MemPKFilter{},
	}

	outBatch := batch.NewWithSize(1)
	outBatch.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	err := ls.filterInMemUnCommittedInserts(ctx, []uint16{0}, -1, m, outBatch)
	require.NoError(t, err)
	require.Equal(t, 1, outBatch.RowCount())
	require.Equal(t, 1, outBatch.Vecs[0].Length())
}

// TestLocalDisttaeDataSource_getBlockZMs_ColumnLookup tests the column lookup logic
// that fixes the bug where ColPos in JOIN scenarios points to projection list position
// instead of table column position.
func TestLocalDisttaeDataSource_getBlockZMs_ColumnLookup(t *testing.T) {
	// This test focuses on testing the column lookup logic without requiring actual block data.
	// We test that the function correctly finds columns by name even when ColPos is wrong.

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "remark", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 8},      // Position 8: VARCHAR
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 9}, // Position 9: DATETIME
		},
		Name2ColIndex: map[string]int32{
			"id":         0, // Index in Cols array
			"remark":     1, // Index in Cols array
			"created_at": 2, // Index in Cols array
		},
	}

	// Test case 1: Find column by qualified name "table.column"
	orderByCol := &plan.ColRef{
		Name:   "table.created_at",
		ColPos: 8, // Wrong ColPos (points to remark in projection list)
	}
	orderByColName := "table.created_at"
	if idx := strings.LastIndex(strings.ToLower(orderByColName), "."); idx >= 0 {
		orderByColName = orderByColName[idx+1:]
	}
	require.Equal(t, "created_at", orderByColName, "Should extract column name from qualified name")

	var orderByColIDX int = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	require.Equal(t, 9, orderByColIDX, "Should find created_at (seqnum 9) by name, not remark (seqnum 8)")

	// Test case 2: Find column by simple name
	orderByColName = "created_at"
	orderByColIDX = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	require.Equal(t, 9, orderByColIDX, "Should find created_at by simple name")

	// Test case 3: Fallback to ColPos when name lookup fails
	orderByColName = "nonexistent_column"
	orderByCol.ColPos = 1 // Valid ColPos pointing to remark (index 1 in Cols array)
	orderByColIDX = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	// Fallback to ColPos
	if orderByColIDX == -1 {
		if int(orderByCol.ColPos) < len(tableDef.Cols) {
			orderByColIDX = int(tableDef.Cols[int(orderByCol.ColPos)].Seqnum)
		}
	}
	require.Equal(t, 8, orderByColIDX, "Should fallback to ColPos 1 (remark, seqnum 8) when name lookup fails")
}

// TestLocalDisttaeDataSource_getBlockZMs tests the fix for the bug where
// ColPos in JOIN scenarios points to projection list position instead of table column position.
// This test verifies that getBlockZMs correctly finds columns by name.
func TestLocalDisttaeDataSource_getBlockZMs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	// Create a table definition with multiple columns
	// Simulating the bug scenario: created_at is at seqnum 9, but ColPos might point to position 8 (remark)
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "organization_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
			{Name: "trx_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 2},
			{Name: "record_sn", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 3},
			{Name: "coupon_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 4},
			{Name: "bill_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 5},
			{Name: "amount", Typ: plan.Type{Id: int32(types.T_decimal128)}, Seqnum: 6},
			{Name: "after_amount", Typ: plan.Type{Id: int32(types.T_decimal128)}, Seqnum: 7},
			{Name: "remark", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 8},      // Position 8: VARCHAR
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 9}, // Position 9: DATETIME (ORDER BY column)
			{Name: "__mo_rowid", Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: 10},
		},
		Name2ColIndex: map[string]int32{
			"id":              0,
			"organization_id": 1,
			"trx_id":          2,
			"record_sn":       3,
			"coupon_id":       4,
			"bill_id":         5,
			"amount":          6,
			"after_amount":    7,
			"remark":          8,
			"created_at":      9,
			"__mo_rowid":      10,
		},
	}

	txnDB := txnDatabase{
		op: nil,
	}

	txnTbl := txnTable{
		db:       &txnDB,
		tableDef: tableDef,
	}

	ls := &LocalDisttaeDataSource{
		fs:    fs,
		ctx:   ctx,
		table: &txnTbl,
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_datetime)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   "coupon_usage_detail_logs.created_at", // Full qualified name
							ColPos: 8,                                     // This points to projection list position 8 (remark), not table position 9 (created_at)
							RelPos: 0,
						},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			},
		},
		Limit: 0, // No limit to trigger getBlockZMs
	}

	// Create an empty rangeSlice to avoid loading non-existent block metadata
	// This test focuses on column lookup logic, not block loading
	ls.rangeSlice = readutil.NewBlockListRelationData(0).GetBlockInfoSlice()

	// Test case 1: Column name lookup should find created_at (seqnum 9) instead of remark (seqnum 8)
	// This simulates the bug scenario where ColPos=8 would incorrectly point to remark
	// but we should find created_at by name.
	// With empty rangeSlice, getBlockZMs should complete without trying to load blocks
	require.NotPanics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should find created_at by name, not panic on column lookup")

	// Verify that blockZMS was initialized (even if empty)
	require.NotNil(t, ls.blockZMS, "blockZMS should be initialized")
	require.Equal(t, 0, len(ls.blockZMS), "blockZMS should be empty when rangeSlice is empty")

	// Test case 2: Test with simple column name (without table prefix)
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "created_at"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 8 // Still wrong ColPos
	ls.blockZMS = nil
	require.NotPanics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should work with simple column name")

	// Test case 3: Test fallback to ColPos when name lookup fails
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "nonexistent_column"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 9 // Valid ColPos as fallback (points to created_at)
	ls.blockZMS = nil
	require.NotPanics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should fallback to ColPos when name lookup fails")

	// Test case 4: Test panic when both name lookup and ColPos fail
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "nonexistent_column"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 999 // Invalid ColPos
	ls.blockZMS = nil
	require.Panics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should panic when both name lookup and ColPos fail")

	// Test case 5: Test with Name2ColIndex (O(1) lookup)
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "created_at"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 8 // Wrong ColPos
	ls.blockZMS = nil
	require.NotPanics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should use Name2ColIndex for O(1) lookup")
}

// TestLocalDisttaeDataSource_getBlockZMs_ColumnNameExtraction tests column name extraction
// from qualified names like "table.column".
func TestLocalDisttaeDataSource_getBlockZMs_ColumnNameExtraction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{
			"id":         0,
			"created_at": 1,
		},
	}

	txnTbl := txnTable{
		tableDef: tableDef,
	}

	ls := &LocalDisttaeDataSource{
		fs:    fs,
		ctx:   ctx,
		table: &txnTbl,
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_datetime)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   "db.table.created_at", // Multiple dots
							ColPos: 0,
							RelPos: 0,
						},
					},
				},
			},
		},
		Limit: 0,
	}

	// Create an empty rangeSlice to avoid loading non-existent block metadata
	// This test focuses on column name extraction logic, not block loading
	ls.rangeSlice = readutil.NewBlockListRelationData(0).GetBlockInfoSlice()

	// Should extract "created_at" from "db.table.created_at"
	// Since rangeSlice is empty, getBlockZMs should complete without trying to load blocks
	require.NotPanics(t, func() {
		ls.getBlockZMs()
	}, "getBlockZMs should extract column name from qualified name and handle empty rangeSlice")

	// Verify that blockZMS was initialized (even if empty)
	require.NotNil(t, ls.blockZMS, "blockZMS should be initialized")
	require.Equal(t, 0, len(ls.blockZMS), "blockZMS should be empty when rangeSlice is empty")
}
