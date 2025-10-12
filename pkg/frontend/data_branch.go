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

package frontend

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func handleDataBranch(
	execCtx *ExecCtx,
	ses *Session,
	stmt tree.Statement,
) error {

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		//return dataBranchCreateTable(execCtx, ses, st)
	case *tree.DataBranchCreateDatabase:
	case *tree.DataBranchDeleteTable:
	case *tree.DataBranchDeleteDatabase:
	case *tree.SnapshotDiff:
		return handleSnapshotDiff(execCtx, ses, st)
	case *tree.SnapshotMerge:
		return handleSnapshotMerge(execCtx, ses, st)
	default:
		return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
	}

	return nil
}

func handleSnapshotDiff(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.SnapshotDiff,
) (err error) {

	var (
		tarDbName   string
		tarTblName  string
		baseDbName  string
		baseTblName string

		tarSnapshot  *plan.Snapshot
		baseSnapshot *plan.Snapshot
	)

	baseDbName = stmt.BaseTable.SchemaName.String()
	baseTblName = stmt.BaseTable.ObjectName.String()
	if len(baseDbName) == 0 {
		baseDbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	tarDbName = stmt.TargetTable.SchemaName.String()
	tarTblName = stmt.TargetTable.ObjectName.String()
	if len(tarDbName) == 0 {
		tarDbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	if len(baseDbName) == 0 || len(tarDbName) == 0 {
		err = moerr.NewInternalErrorNoCtxf("the base or target database cannot be empty.")
		return
	}

	var (
		tarTxnOp  TxnOperator
		baseTxnOp TxnOperator

		tarDB   engine.Database
		baseDB  engine.Database
		tarRel  engine.Relation
		baseRel engine.Relation

		eng         engine.Engine
		txnSnapshot timestamp.Timestamp
	)

	if tarSnapshot, err = resolveSnapshot(ses, stmt.TargetTable.AtTsExpr); err != nil {
		return
	}

	if baseSnapshot, err = resolveSnapshot(ses, stmt.BaseTable.AtTsExpr); err != nil {
		return
	}

	tarTxnOp = ses.GetTxnHandler().GetTxn()
	baseTxnOp = ses.GetTxnHandler().GetTxn()

	if tarSnapshot != nil && tarSnapshot.TS != nil {
		tarTxnOp = tarTxnOp.CloneSnapshotOp(*tarSnapshot.TS)
	}

	if baseSnapshot != nil && baseSnapshot.TS != nil {
		baseTxnOp = baseTxnOp.CloneSnapshotOp(*baseSnapshot.TS)
	}

	eng = ses.proc.GetSessionInfo().StorageEngine
	if tarDB, err = eng.Database(
		execCtx.reqCtx, tarDbName, tarTxnOp,
	); err != nil {
		return err
	}

	if tarRel, err = tarDB.Relation(execCtx.reqCtx, tarTblName, nil); err != nil {
		return err
	}

	if baseDB, err = eng.Database(
		execCtx.reqCtx, baseDbName, baseTxnOp,
	); err != nil {
		return err
	}

	if baseRel, err = baseDB.Relation(execCtx.reqCtx, baseTblName, nil); err != nil {
		return err
	}

	if !isSchemaEquivalent(tarRel.GetTableDef(execCtx.reqCtx), baseRel.GetTableDef(execCtx.reqCtx)) {
		err = moerr.NewInternalErrorNoCtx("the target table schema is not equivalent to the base table.")
		return
	}

	var (
		dag    *databranchutils.DataBranchDAG
		hasLca bool

		lcaTableID uint64
		tarHandle  engine.ChangesHandle
		baseHandle engine.ChangesHandle

		tarBranchTS  int64
		baseBranchTS int64

		tarIsClonedTable  bool
		baseIsClonedTable bool

		tarToTS  timestamp.Timestamp
		baseToTS timestamp.Timestamp

		tarFromTS  timestamp.Timestamp
		baseFromTS timestamp.Timestamp
	)

	defer func() {
		if tarHandle != nil {
			_ = tarHandle.Close()
		}
		if baseHandle != nil {
			_ = baseHandle.Close()
		}
	}()

	if dag, err = constructBranchDAG(execCtx.reqCtx, ses); err != nil {
		return
	}

	txnSnapshot = ses.GetTxnHandler().GetTxn().SnapshotTS()
	tarToTS = txnSnapshot
	baseToTS = txnSnapshot

	// 1. has no lca
	//		[0, now] join [0, now]
	// 2. t1 and t2 has lca
	//		1. t0 is the lca
	//			t1's [branch_t1_ts + 1, now] join t2's [branch_t2_ts + 1, now]
	// 		2. t1 is the lca
	//			t1's [branch_t2_ts + 1, now] join t2's [branch_t2_ts + 1, now]
	//      3. t2 is the lca
	//			t1's [branch_t1_ts + 1, now] join t2's [branch_t1_ts + 1, now]
	//
	// if a table is cloned table, the commit ts of the cloned data
	// should be the creation time of the table.
	if lcaTableID, tarBranchTS, baseBranchTS, hasLca = dag.FindLCA(
		tarRel.GetTableID(execCtx.reqCtx), baseRel.GetTableID(execCtx.reqCtx),
	); hasLca {
		if lcaTableID == baseRel.GetTableID(execCtx.reqCtx) {
			ts := timestamp.Timestamp{PhysicalTime: tarBranchTS}.Next()
			tarFromTS = ts
			baseFromTS = ts
			tarIsClonedTable = true
			baseIsClonedTable = dag.HasParent(baseRel.GetTableID(execCtx.reqCtx))
		} else if lcaTableID == tarRel.GetTableID(execCtx.reqCtx) {
			ts := timestamp.Timestamp{PhysicalTime: baseBranchTS}.Next()
			tarFromTS = ts
			baseFromTS = ts
			baseIsClonedTable = true
			tarIsClonedTable = dag.HasParent(tarRel.GetTableID(execCtx.reqCtx))
		} else {
			tarIsClonedTable = true
			baseIsClonedTable = true
			tarFromTS = timestamp.Timestamp{PhysicalTime: tarBranchTS}.Next()
			baseFromTS = timestamp.Timestamp{PhysicalTime: baseBranchTS}.Next()
		}

		fmt.Printf("hasLca, tar(%d-%d), base(%d-%d)\n",
			tarFromTS.PhysicalTime, tarToTS.PhysicalTime,
			baseFromTS.PhysicalTime, baseToTS.PhysicalTime)
	}

	if tarHandle, err = databranchutils.CollectChanges(
		execCtx.reqCtx,
		eng,
		ses.GetAccountId(),
		txnSnapshot,
		tarRel,
		types.TimestampToTS(tarFromTS),
		types.TimestampToTS(tarToTS),
		ses.proc.Mp(),
		tarIsClonedTable,
	); err != nil {
		return
	}

	if baseHandle, err = databranchutils.CollectChanges(
		execCtx.reqCtx,
		eng,
		ses.GetAccountId(),
		txnSnapshot,
		baseRel,
		types.TimestampToTS(baseFromTS),
		types.TimestampToTS(baseToTS),
		ses.proc.Mp(),
		baseIsClonedTable,
	); err != nil {
		return
	}

	if err = diff(
		execCtx.reqCtx,
		ses,
		ses.proc.Mp(),
		tarRel.GetTableDef(execCtx.reqCtx),
		baseRel.GetTableDef(execCtx.reqCtx),
		tarHandle,
		baseHandle,
	); err != nil {
		return
	}

	return
}

func handleSnapshotMerge(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.SnapshotMerge,
) (err error) {

	return nil
}

func diff(
	ctx context.Context,
	ses *Session,
	mp *mpool.MPool,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
	tarHandle engine.ChangesHandle,
	baseHandle engine.ChangesHandle,
) (err error) {

	var (
		//hint         engine.ChangesHandle_Hint
		dataBat      *batch.Batch
		tombstoneBat *batch.Batch

		pkIdxes []int

		baseTableHashmap databranchutils.BranchHashmap
	)

	defer func() {
		if dataBat != nil {
			dataBat.Clean(mp)
		}
		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}
		if baseTableHashmap != nil {
			baseTableHashmap.Close()
		}
	}()

	if baseTableHashmap, err = databranchutils.NewBranchHashmap(); err != nil {
		return
	}

	if baseTblDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		pkIdxes = append(pkIdxes, int(baseTblDef.Name2ColIndex[baseTblDef.Pkey.PkeyColName]))
	} else {
		for i, col := range baseTblDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName && col.Name != catalog.Row_ID {
				pkIdxes = append(pkIdxes, i)
			}
		}
	}

	d := time.Duration(0)
	// build hash for base table
	for {
		if dataBat, tombstoneBat, _, err = baseHandle.Next(
			ctx, mp,
		); err != nil {
			return
		} else if dataBat == nil && tombstoneBat == nil {
			// out of data
			break
		}

		if dataBat != nil {
			s := time.Now()
			if err = baseTableHashmap.PutByVectors(dataBat.Vecs, pkIdxes); err != nil {
				return
			}
			d += time.Since(s)
			//fmt.Println("putByVectors", time.Since(s), dataBat.RowCount())
			dataBat.Clean(mp)
		}
	}

	fmt.Println("build hashmap takes", d)
	d = time.Duration(0)

	var (
		rows          [][]interface{}
		showCols      []*MysqlColumn
		pkVecs        []*vector.Vector
		checkRet      []databranchutils.GetResult
		tableColIdxes []int
	)

	//  -----------------------------------------
	// |  tar_table_name  | flag |  columns data |
	//  -----------------------------------------
	showCols = append(showCols, new(MysqlColumn), new(MysqlColumn))
	showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	showCols[0].SetName("target_table_name")
	showCols[1].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	showCols[1].SetName("flag")

	for i, col := range tarTblDef.Cols {
		if col.Name == catalog.Row_ID ||
			col.Name == catalog.FakePrimaryKeyColName ||
			col.Name == catalog.CPrimaryKeyColName {
			continue
		}

		t := types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale)

		nCol := new(MysqlColumn)

		switch t.Oid {
		case types.T_bool:
			nCol.SetColumnType(defines.MYSQL_TYPE_BOOL)
		case types.T_char, types.T_varchar:
			nCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		case types.T_datetime, types.T_date:
			nCol.SetColumnType(defines.MYSQL_TYPE_DATE)
		case types.T_int8, types.T_uint8:
			nCol.SetColumnType(defines.MYSQL_TYPE_TINY)
		case types.T_int16, types.T_uint16:
			nCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
		case types.T_int32, types.T_uint32:
			nCol.SetColumnType(defines.MYSQL_TYPE_LONG)
		case types.T_int64, types.T_uint64:
			nCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		case types.T_json:
			nCol.SetColumnType(defines.MYSQL_TYPE_JSON)
		case types.T_blob:
			nCol.SetColumnType(defines.MYSQL_TYPE_BLOB)
		}

		nCol.SetName(col.Name)
		showCols = append(showCols, nCol)
		tableColIdxes = append(tableColIdxes, i)
	}

	mrs := ses.GetMysqlResultSet()
	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	// check existence
	for {
		if dataBat, tombstoneBat, _, err = tarHandle.Next(
			ctx, mp,
		); err != nil {
			return
		} else if dataBat == nil && tombstoneBat == nil {
			// out of data
			break
		}

		if dataBat != nil {
			pkVecs = pkVecs[:0]
			for _, idx := range pkIdxes {
				pkVecs = append(pkVecs, dataBat.Vecs[idx])
			}
			s := time.Now()
			if checkRet, err = baseTableHashmap.PopByVectors(pkVecs); err != nil {
				return
			}
			d += time.Since(s)

			for i := range checkRet {
				// not exists in the base table
				if !checkRet[i].Exists {
					row := append([]interface{}{}, tarTblDef.Name, "+")
					for _, idx := range tableColIdxes {
						row = append(row, types.DecodeValue(dataBat.Vecs[idx].GetRawBytesAt(i), dataBat.Vecs[idx].GetType().Oid))
					}
					rows = append(rows, row)
				} else {
					// exists in the base table, do nothing
				}
			}
		}
	}

	fmt.Println("pop hashmap takes", d)

	if err = baseTableHashmap.ForEach(func(key []byte, data [][]byte) error {
		row := append([]interface{}{}, tarTblDef.Name, "-")
		for _, r := range data {
			if t, err := baseTableHashmap.DecodeRow(r); err != nil {
				return err
			} else {
				for i := range t {
					row = append(row, t[i])
				}
			}
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		return
	}

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return trySaveQueryResult(ctx, ses, mrs)
}

func isSchemaEquivalent(leftDef, rightDef *plan.TableDef) bool {
	if len(leftDef.Cols) != len(rightDef.Cols) {
		return false
	}

	for i := range leftDef.Cols {
		if leftDef.Cols[i].ColId != rightDef.Cols[i].ColId {
			return false
		}

		if leftDef.Cols[i].Typ.Id != rightDef.Cols[i].Typ.Id {
			return false
		}

		if leftDef.Cols[i].ClusterBy != rightDef.Cols[i].ClusterBy {
			return false
		}

		if leftDef.Cols[i].Primary != rightDef.Cols[i].Primary {
			return false
		}

		if leftDef.Cols[i].Seqnum != rightDef.Cols[i].Seqnum {
			return false
		}

		if leftDef.Cols[i].NotNull != rightDef.Cols[i].NotNull {
			return false
		}
	}

	return true
}

func constructBranchDAG(
	ctx context.Context,
	ses *Session,
) (dag *databranchutils.DataBranchDAG, err error) {

	var (
		data    databranchutils.DataBranchMetadata
		rowData []databranchutils.DataBranchMetadata
		sqlRet  []ExecResult

		bh = ses.GetBackgroundExec(ctx)
	)

	bh.ClearExecResultSet()
	defer func() {
		bh.Close()
	}()

	sysCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	if err = bh.Exec(
		sysCtx,
		fmt.Sprintf(scanBranchMetadataSql, catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA),
	); err != nil {
		return
	}

	if sqlRet, err = getResultSet(sysCtx, bh); err != nil {
		return
	}

	if execResultArrayHasData(sqlRet) {
		rowData = make([]databranchutils.DataBranchMetadata, 0, sqlRet[0].GetRowCount())
		for i := uint64(0); i < sqlRet[0].GetRowCount(); i++ {
			if data.TableID, err = sqlRet[0].GetUint64(sysCtx, i, 0); err != nil {
				return
			}
			if data.CloneTS, err = sqlRet[0].GetInt64(sysCtx, i, 1); err != nil {
				return
			}
			if data.PTableID, err = sqlRet[0].GetUint64(sysCtx, i, 2); err != nil {
				return
			}

			v := int64(0)
			if v, err = sqlRet[0].GetInt64(sysCtx, i, 5); err != nil {
				return
			}

			if v == 1 {
				data.TableDeleted = true
			}

			rowData = append(rowData, data)
		}
	}

	return databranchutils.NewDAG(rowData), nil
}
