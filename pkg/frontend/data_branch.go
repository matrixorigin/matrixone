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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

var CollectChanges = func(
	ctx context.Context,
	rel engine.Relation,
	fromTs, toTs types.TS,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	return rel.CollectChanges(ctx, fromTs, toTs, true, mp)
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
		tarDB   engine.Database
		baseDB  engine.Database
		tarRel  engine.Relation
		baseRel engine.Relation

		tarTxnOp  TxnOperator
		baseTxnOp TxnOperator
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

	if tarDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
		execCtx.reqCtx, tarDbName, tarTxnOp,
	); err != nil {
		return err
	}

	if tarRel, err = tarDB.Relation(execCtx.reqCtx, tarTblName, nil); err != nil {
		return err
	}

	if baseDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
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
		dag    *branchDAG
		hasLca bool

		tarHandle  engine.ChangesHandle
		baseHandle engine.ChangesHandle

		tarBranchTS  int64
		baseBranchTS int64

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

	if dag, err = constructBranchDAG(execCtx, ses); err != nil {
		return
	}

	tarToTS = ses.GetTxnHandler().GetTxn().SnapshotTS()
	baseToTS = ses.GetTxnHandler().GetTxn().SnapshotTS()

	if _, tarBranchTS, baseBranchTS, hasLca = dag.FindLCA(
		tarRel.GetTableID(execCtx.reqCtx), baseRel.GetTableID(execCtx.reqCtx),
	); hasLca {
		tarFromTS = timestamp.Timestamp{PhysicalTime: tarBranchTS}
		baseFromTS = timestamp.Timestamp{PhysicalTime: baseBranchTS}
	}

	if tarHandle, err = CollectChanges(
		execCtx.reqCtx, tarRel, types.TimestampToTS(tarFromTS), types.TimestampToTS(tarToTS), ses.proc.Mp(),
	); err != nil {
		return
	}

	if baseHandle, err = CollectChanges(
		execCtx.reqCtx, tarRel, types.TimestampToTS(baseFromTS), types.TimestampToTS(baseToTS), ses.proc.Mp(),
	); err != nil {
		return
	}

	err = diff(execCtx.reqCtx, ses.proc.Mp(), tarHandle, baseHandle)
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
	mp *mpool.MPool,
	tarHandle engine.ChangesHandle,
	baseHandle engine.ChangesHandle,
) (err error) {

	var (
		hint         engine.ChangesHandle_Hint
		dataBat      *batch.Batch
		tombstoneBat *batch.Batch
	)

	// data:
	// 	cols, ts, row_id
	// tombstone:
	// 	pk, ts, row_id

	for {
		if dataBat, tombstoneBat, hint, err = tarHandle.Next(
			ctx, mp,
		); err != nil {
			return
		} else if dataBat == nil && tombstoneBat == nil {
			// out of data
			break
		}

		switch hint {
		case engine.ChangesHandle_Snapshot:
			fmt.Println("data", dataBat.Attrs)
		case engine.ChangesHandle_Tail_done:
			fmt.Println("tombstone", tombstoneBat.Attrs)
		default:
			panic(fmt.Sprintf("unexpected hint: %v", hint))
		}
	}

	return
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
	execCtx *ExecCtx,
	ses *Session,
) (dag *branchDAG, err error) {

	var (
		data    branchMetadata
		rowData []branchMetadata
		sqlRet  []ExecResult

		bh = ses.GetBackgroundExec(execCtx.reqCtx)
	)

	bh.ClearExecResultSet()
	defer func() {
		bh.Close()
	}()

	sysCtx := defines.AttachAccountId(execCtx.reqCtx, sysAccountID)
	if err = bh.Exec(
		sysCtx,
		fmt.Sprintf(scanBranchMetadataSql, moCatalog, catalog.MO_BRANCH_METADATA),
	); err != nil {
		return
	}

	if sqlRet, err = getResultSet(sysCtx, bh); err != nil {
		return
	}

	if execResultArrayHasData(sqlRet) {
		rowData = make([]branchMetadata, 0, sqlRet[0].GetRowCount())
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
		}
	}

	return newDAG(rowData), nil
}

type branchMetadata struct {
	TableID      uint64
	CloneTS      int64
	PTableID     uint64
	TableDeleted bool
}

type dagNode struct {
	TableID  uint64
	CloneTS  int64
	ParentID uint64

	Parent *dagNode
	Depth  int
}

type branchDAG struct {
	nodes map[uint64]*dagNode
}

func newDAG(rows []branchMetadata) *branchDAG {
	dag := &branchDAG{
		nodes: make(map[uint64]*dagNode, len(rows)),
	}

	// --- Pass 1: Create all Node objects ---
	// Iterate through each row and create a corresponding Node object.
	// We store them in the map but do not link them yet, as parent nodes may not have been processed.
	for _, row := range rows {
		node := &dagNode{
			TableID: row.TableID,
			Depth:   -1, // Initialize depth as -1 to mark it as "not calculated".
		}

		node.CloneTS = row.CloneTS
		node.ParentID = row.PTableID
		dag.nodes[node.TableID] = node
	}

	// --- Pass 2: Link Parent pointers ---
	// Now that all nodes exist in the map, we can iterate through them again
	// and set the `Parent` pointer based on the `ParentID`.
	for _, node := range dag.nodes {
		if node.ParentID != 0 {
			if parentNode, ok := dag.nodes[node.ParentID]; ok {
				node.Parent = parentNode
			}
		}
	}

	// --- Pass 3: Calculate the depth for every node ---
	// Depth is crucial for an efficient LCA algorithm. We calculate it once during setup.
	for _, node := range dag.nodes {
		dag.calculateDepth(node)
	}

	return dag
}

// calculateDepth computes the depth of a given dagNode.
// It uses memoization (by checking if `dagNode.Depth != -1`) to avoid re-computation.
func (d *branchDAG) calculateDepth(node *dagNode) int {
	// If depth is already calculated, return it immediately.
	if node.Depth != -1 {
		return node.Depth
	}

	// If a node has no parent, it's a root node, so its depth is 0.
	if node.Parent == nil {
		node.Depth = 0
		return 0
	}

	// Otherwise, the depth is the parent's depth + 1.
	// This recursive call will eventually reach a root or a pre-calculated node.
	node.Depth = d.calculateDepth(node.Parent) + 1
	return node.Depth
}

func (d *branchDAG) Exists(tableID uint64) bool {
	_, ok := d.nodes[tableID]
	return ok
}

// FindLCA finds the Lowest Common Ancestor (LCA) for two given table IDs.
// It returns:
// 1. lcaTableID: The table_id of the common ancestor.
// 2. branchTS1: The clone_ts of the direct child of the LCA that is on the path to tableID1.
// 3. branchTS2: The clone_ts of the direct child of the LCA that is on the path to tableID2.
// 4. ok: A boolean indicating if a common ancestor was found.
func (d *branchDAG) FindLCA(tableID1, tableID2 uint64) (lcaTableID uint64, branchTS1 int64, branchTS2 int64, ok bool) {
	node1, exists1 := d.nodes[tableID1]
	node2, exists2 := d.nodes[tableID2]

	// Ensure both nodes exist in our DAG.
	if !exists1 || !exists2 {
		return 0, 0, 0, false
	}

	// If it's the same node, it is its own LCA.
	// Both branch timestamps can be defined as the node's own creation timestamp.
	if tableID1 == tableID2 {
		return tableID1, node1.CloneTS, node1.CloneTS, true
	}

	// Keep references to the original nodes for the final step of finding branch timestamps.
	originalNode1 := node1
	originalNode2 := node2

	// --- Step 1: Bring both nodes to the same depth ---
	// Move the deeper node up the tree until it is at the same depth as the shallower node.
	if node1.Depth < node2.Depth {
		for node2.Depth > node1.Depth {
			node2 = node2.Parent
		}
	} else if node2.Depth < node1.Depth {
		for node1.Depth > node2.Depth {
			node1 = node1.Parent
		}
	}

	// --- Step 2: Move both nodes up in lockstep until they meet ---
	// Now that they are at the same depth, we move them up one parent at a time.
	// The first node they both share is the LCA.
	for node1 != node2 {
		// If either path hits a root before they meet, they are in different trees.
		if node1.Parent == nil || node2.Parent == nil {
			return 0, 0, 0, false
		}
		node1 = node1.Parent
		node2 = node2.Parent
	}

	lcaNode := node1
	if lcaNode == nil {
		// Should not happen if they are in the same tree and not both nil roots.
		return 0, 0, 0, false
	}

	lcaTableID = lcaNode.TableID

	// --- Step 3: Find the specific children of the LCA that lead to the original nodes ---

	// Find the branch timestamp for the path to tableID1
	child1 := originalNode1
	if child1 != lcaNode { // If the original node is not the LCA itself
		for child1 != nil && child1.Parent != lcaNode {
			child1 = child1.Parent
		}
		branchTS1 = child1.CloneTS
	} else { // If the original node IS the LCA (ancestor case)
		branchTS1 = lcaNode.CloneTS
	}

	// Find the branch timestamp for the path to tableID2
	child2 := originalNode2
	if child2 != lcaNode { // If the original node is not the LCA itself
		for child2 != nil && child2.Parent != lcaNode {
			child2 = child2.Parent
		}
		branchTS2 = child2.CloneTS
	} else { // If the original node IS the LCA (ancestor case)
		branchTS2 = lcaNode.CloneTS
	}

	ok = true
	return
}

//func dataBranchCreateTable(
//	execCtx *ExecCtx,
//	ses *Session,
//	stmt *tree.DataBranchCreateTable,
//) (err error) {
//
//	var (
//		reqCtx = execCtx.reqCtx
//		bh     = ses.GetBackgroundExec(reqCtx)
//
//		deferred    func(error) error
//		newSql      string
//		tempExecCtx *ExecCtx
//		cloneTable  *tree.CloneTable
//	)
//
//	// do not open another transaction,
//	// if the clone already executed within a transaction.
//	if bh, deferred, err = getBackExecutor(reqCtx, ses); err != nil {
//		return err
//	}
//
//	defer func() {
//		if deferred != nil {
//			err = deferred(err)
//		}
//	}()
//
//	newSql = rewriteBranchNewTableToClone(execCtx.input.sql, stmt)
//
//	tempExecCtx = &ExecCtx{
//		reqCtx: execCtx.reqCtx,
//		input: &UserInput{
//			sql: newSql,
//		},
//	}
//
//	cloneTable = &tree.CloneTable{
//		CreateTable:  stmt.CreateTable,
//		SrcTable:     stmt.SrcTable,
//		ToAccountOpt: stmt.ToAccountOpt,
//	}
//
//	var (
//		srcTblDef *plan.TableDef
//		dstTblDef *plan.TableDef
//
//		dstDB      engine.Database
//		dstRel     engine.Relation
//		cloneTxnOp TxnOperator
//		receipt    cloneReceipt
//	)
//
//	// 1. clone table
//	if err = handleCloneTable(
//		tempExecCtx, ses, cloneTable, &receipt, bh,
//	); err != nil {
//		return err
//	}
//
//	// 2.1 get src table id
//	if _, srcTblDef, err = ses.GetTxnCompileCtx().Resolve(
//		receipt.srcDb, receipt.srcTbl, receipt.snapshot,
//	); err != nil {
//		return err
//	}
//
//	// 2.2 get dst table id
//	// the back session did the clone operation,
//	// we need it's txnOp to read the uncommit table info.
//	cloneTxnOp = bh.(*backExec).backSes.GetTxnHandler().GetTxn()
//	if dstDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
//		reqCtx, receipt.dstDb, cloneTxnOp,
//	); err != nil {
//		return err
//	}
//
//	if dstRel, err = dstDB.Relation(reqCtx, receipt.dstTbl, nil); err != nil {
//		return err
//	}
//
//	dstTblDef = dstRel.GetTableDef(reqCtx)
//
//	if receipt.snapshot != nil {
//		receipt.snapshotTS = receipt.snapshot.TS.PhysicalTime
//	}
//
//	// 2.3 write branch info into branch_metadata table
//	updateMetadataSql := fmt.Sprintf(
//		insertIntoBranchMetadataSql,
//		catalog.MO_CATALOG,
//		catalog.MO_BRANCH_METADATA,
//		dstTblDef.TblId,
//		receipt.snapshotTS,
//		srcTblDef.TblId,
//		receipt.opAccount,
//		dataBranchLevel_Table,
//	)
//
//	tempCtx := reqCtx
//	if receipt.opAccount != sysAccountID {
//		tempCtx = defines.AttachAccountId(tempCtx, sysAccountID)
//	}
//
//	if err = bh.Exec(tempCtx, updateMetadataSql); err != nil {
//		return err
//	}
//
//	return nil
//}

//func rewriteBranchNewTableToClone(
//	oldSql string,
//	stmt *tree.DataBranchCreateTable,
//) (newSql string) {
//	// create table d2.t2 clone d1.t1 {snapshot} to x
//
//	var (
//		dstTable    string
//		srcTable    string
//		dstDatabase string
//		srcDatabase string
//
//		sp, src, dst string
//	)
//
//	dstTable = stmt.CreateTable.Table.ObjectName.String()
//	dstDatabase = stmt.CreateTable.Table.SchemaName.String()
//
//	srcTable = stmt.SrcTable.ObjectName.String()
//	srcDatabase = stmt.SrcTable.SchemaName.String()
//
//	src = srcTable
//	if srcDatabase != "" {
//		src = fmt.Sprintf("`%s`.`%s`", srcDatabase, srcTable)
//	}
//
//	dst = dstTable
//	if dstDatabase != "" {
//		dst = fmt.Sprintf("`%s`.`%s`", dstDatabase, dstTable)
//	}
//
//	sp = snapConditionRegex.FindString(oldSql)
//	newSql = fmt.Sprintf("create table %s clone %s %s", dst, src, sp)
//
//	if stmt.ToAccountOpt != nil {
//		newSql += fmt.Sprintf(" to account %s", stmt.ToAccountOpt.AccountName.String())
//	}
//
//	return newSql
//}
