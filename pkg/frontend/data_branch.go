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
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	fakeKind = iota
	normalKind
	compositeKind
)

const (
	diffAddedLine   = "+"
	diffRemovedLine = "-"

	diffInsert = iota
	diffDelete
	diffUpdate
)

const (
	lcaEmpty = iota
	lcaOther
	lcaLeft
	lcaRight
)

type diffExtra struct {
	inputArgs struct {
		bh BackgroundExec

		tarRel  engine.Relation
		baseRel engine.Relation

		tarSnapshot  *plan.Snapshot
		baseSnapshot *plan.Snapshot
	}

	outputArgs struct {
		rows       [][]any
		pkKind     int
		pkColIdxes []int
		pkTypes    []types.Type
	}
}

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
	case *tree.DataBranchDiff:
		return handleSnapshotDiff(execCtx, ses, st, nil)
	case *tree.DataBranchMerge:
		return handleSnapshotMerge(execCtx, ses, st)
	default:
		return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
	}

	return nil
}

func handleSnapshotDiff(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchDiff,
	extra *diffExtra,
) (err error) {

	var (
		bh      BackgroundExec
		tarRel  engine.Relation
		baseRel engine.Relation

		tarSnapshot  *plan.Snapshot
		baseSnapshot *plan.Snapshot

		deferred func(error) error
	)

	if extra != nil {
		bh = extra.inputArgs.bh
		tarRel = extra.inputArgs.tarRel
		baseRel = extra.inputArgs.baseRel
		tarSnapshot = extra.inputArgs.tarSnapshot
		baseSnapshot = extra.inputArgs.baseSnapshot
	} else {
		// do not open another transaction,
		// if the clone already executed within a transaction.
		if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
			return
		}

		defer func() {
			if deferred != nil {
				err = deferred(err)
			}
		}()

		if tarRel, baseRel, tarSnapshot, baseSnapshot, err = getRelations(
			execCtx.reqCtx, ses, bh, stmt.TargetTable, stmt.BaseTable,
		); err != nil {
			return
		}
	}

	var (
		lcaTableID   uint64
		tarBranchTS  timestamp.Timestamp
		baseBranchTS timestamp.Timestamp
		tarHandle    engine.ChangesHandle
		baseHandle   engine.ChangesHandle

		tarTblDef  = tarRel.GetTableDef(execCtx.reqCtx)
		baseTblDef = baseRel.GetTableDef(execCtx.reqCtx)
	)

	defer func() {
		if tarHandle != nil {
			_ = tarHandle.Close()
		}
		if baseHandle != nil {
			_ = baseHandle.Close()
		}
	}()

	if lcaTableID, tarBranchTS, baseBranchTS, tarHandle, baseHandle, err =
		constructChangeHandle(
			execCtx.reqCtx, ses, tarRel, baseRel, tarSnapshot, baseSnapshot,
		); err != nil {
		return
	}

	if err = diff(
		execCtx.reqCtx, ses,
		tarTblDef, baseTblDef,
		tarHandle, baseHandle,
		stmt.DiffAsOpts,
		lcaTableID, tarBranchTS, baseBranchTS,
		bh, extra,
	); err != nil {
		return
	}

	return
}

func handleSnapshotMerge(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchMerge,
) (err error) {

	var (
		bh          BackgroundExec
		deferred    func(error) error
		conflictOpt int

		srcRel engine.Relation
		dstRel engine.Relation

		lcaTableID  uint64
		srcSnapshot *plan.Snapshot
		dstSnapshot *plan.Snapshot
		srcBranchTS timestamp.Timestamp
		dstBranchTS timestamp.Timestamp
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return err
	}
	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if srcRel, dstRel, srcSnapshot, dstSnapshot, err = getRelations(
		execCtx.reqCtx, ses, bh, stmt.SrcTable, stmt.DstTable,
	); err != nil {
		return
	}

	if lcaTableID, srcBranchTS, dstBranchTS, err = decideLCABranchTSFromBranchDAG(
		execCtx.reqCtx, ses, srcRel, dstRel,
	); err != nil {
		return
	}

	conflictOpt = tree.CONFLICT_FAIL
	if stmt.ConflictOpt != nil {
		conflictOpt = stmt.ConflictOpt.Opt
	}

	if lcaTableID == 0 {
		// has no lca
		var (
			extra diffExtra
		)
		extra.inputArgs.bh = bh
		extra.inputArgs.tarRel = srcRel
		extra.inputArgs.baseRel = dstRel
		extra.inputArgs.tarSnapshot = srcSnapshot
		extra.inputArgs.baseSnapshot = dstSnapshot

		if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra); err != nil {
			return
		}

		extra.outputArgs.rows = sortDiffResultRows(extra)
		return mergeDiff(
			execCtx, bh, ses, dstRel,
			extra.outputArgs.rows, nil,
			lcaEmpty, conflictOpt,
			extra.outputArgs.pkTypes,
			extra.outputArgs.pkColIdxes,
		)

	}

	// merge left into right
	var (
		lcaType   int
		lcaRel    engine.Relation
		lcaTblDef *plan.TableDef
	)

	if lcaTableID == srcRel.GetTableID(execCtx.reqCtx) {
		// left is the LCA
		lcaType = lcaLeft
		lcaRel = srcRel
	} else if lcaTableID == dstRel.GetTableID(execCtx.reqCtx) {
		// right is the LCA
		lcaType = lcaRight
		lcaRel = dstRel
	} else {
		// LCA is other table
		lcaType = lcaOther
		if _, lcaTblDef, err = ses.GetTxnCompileCtx().ResolveById(lcaTableID, &plan2.Snapshot{}); err != nil {
			return
		}
		if _, lcaRel, err = ses.GetTxnCompileCtx().getRelation(
			lcaTblDef.DbName, lcaTblDef.Name, nil,
			&plan2.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()}, TS: &srcBranchTS},
		); err != nil {
			return
		}
	}

	var (
		extra1 diffExtra
		extra2 diffExtra
	)

	extra1.inputArgs.bh = bh
	extra1.inputArgs.tarRel = srcRel
	extra1.inputArgs.baseRel = lcaRel
	extra1.inputArgs.tarSnapshot = srcSnapshot
	extra1.inputArgs.baseSnapshot = &plan2.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()}, TS: &srcBranchTS}

	if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra1); err != nil {
		return
	}

	extra2.inputArgs.bh = bh
	extra2.inputArgs.tarRel = dstRel
	extra2.inputArgs.baseRel = lcaRel
	extra2.inputArgs.tarSnapshot = dstSnapshot
	extra2.inputArgs.baseSnapshot = &plan2.Snapshot{Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()}, TS: &dstBranchTS}

	if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra2); err != nil {
		return
	}

	extra1.outputArgs.rows = sortDiffResultRows(extra1)
	extra2.outputArgs.rows = sortDiffResultRows(extra2)
	return mergeDiff(
		execCtx, bh, ses, dstRel,
		extra1.outputArgs.rows, extra2.outputArgs.rows,
		lcaType, conflictOpt,
		extra1.outputArgs.pkTypes,
		extra1.outputArgs.pkColIdxes,
	)
}

func mergeDiff(
	execCtx *ExecCtx,
	bh BackgroundExec,
	ses *Session,
	dstRel engine.Relation,
	rows1 [][]any,
	rows2 [][]any,
	lcaType int,
	conflictOpt int,
	pkTypes []types.Type,
	pkColIdxes []int,
) (err error) {

	var (
		cnt int
		buf bytes.Buffer
	)

	initSqlBuf := func() {
		cnt = 0
		buf.Reset()
		buf.WriteString(fmt.Sprintf("replace into %s.%s values ",
			dstRel.GetTableDef(execCtx.reqCtx).DbName,
			dstRel.GetTableDef(execCtx.reqCtx).Name),
		)
	}

	writeOneRow := func(row []any) error {
		if buf.Bytes()[buf.Len()-1] == ')' {
			buf.WriteString(",")
		}
		cnt++
		buf.WriteString("(")
		for j := 2; j < len(row); j++ {
			switch row[j].(type) {
			case string, []byte:
				buf.WriteString(fmt.Sprintf("'%v'", row[j]))
			default:
				buf.WriteString(fmt.Sprintf("%v", row[j]))
			}
			if j != len(row)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString(")")

		if cnt >= objectio.BlockMaxRows {
			bh.ClearExecResultSet()
			if err = bh.Exec(execCtx.reqCtx, buf.String()); err != nil {
				return err
			}

			initSqlBuf()
		}
		return nil
	}

	initSqlBuf()

	switch lcaType {
	case lcaEmpty:
		for _, row := range rows1 {
			if flag := row[1].(int); flag == diffInsert {
				// apply into dstRel
				err = writeOneRow(row)
			} else if flag == diffUpdate {
				// conflict: t1 insert/update, t2 insert/update
				switch conflictOpt {
				case tree.CONFLICT_FAIL:
					return moerr.NewInternalErrorNoCtxf("merge diff conflict happend")
				case tree.CONFLICT_SKIP:
				// do nothing
				case tree.CONFLICT_ACCEPT:
					// apply into dstRel
					err = writeOneRow(row)
				}
			} else {
				// diff delete
				// do nothing
			}

			if err != nil {
				return err
			}
		}
	case lcaOther, lcaLeft, lcaRight:
		i, j := 0, 0
		for i < len(rows1) && j < len(rows2) {
			// row1.(insert, delete, update) v.s. row2.(insert, delete, update)
			if rows1[i][1].(int) == diffDelete {
				i++
				continue
			}

			cmp := compareRows(rows1[i], rows2[j], pkColIdxes, pkTypes, true)
			if cmp == 0 { // conflict
				switch conflictOpt {
				case tree.CONFLICT_FAIL:
					return moerr.NewInternalErrorNoCtxf("merge diff conflict happend")
				case tree.CONFLICT_SKIP:
					i++
				case tree.CONFLICT_ACCEPT:
					err = writeOneRow(rows1[i])
					i++
					j++
				}
			} else {
				err = writeOneRow(rows1[i])
				i++
				if cmp > 0 {
					j++
				}
			}

			if err != nil {
				return err
			}
		}

		for ; i < len(rows1); i++ {
			if rows1[i][1].(int) == diffDelete {
				continue
			}

			if err = writeOneRow(rows1[i]); err != nil {
				return err
			}
		}
	default:
	}

	if buf.Len() > 0 && cnt > 0 {
		bh.ClearExecResultSet()
		if err = bh.Exec(execCtx.reqCtx, buf.String()); err != nil {
			return err
		}
	}

	return nil
}

func compareRows(
	row1 []any,
	row2 []any,
	pkColIdxes []int,
	pkTypes []types.Type,
	onlyByPK bool,
) int {

	for _, idx := range pkColIdxes {
		if cmp := types.CompareValues(
			row1[idx+2], row2[idx+2], pkTypes[idx].Oid,
		); cmp == 0 {
			continue
		} else {
			return cmp
		}
	}

	if onlyByPK {
		return 0
	}

	// "+" < "-"
	// duplicate pks, the "-" will be the first
	return strings.Compare(row2[1].(string), row1[1].(string))
}

func sortDiffResultRows(
	extra diffExtra,
) (newRows [][]any) {

	var (
		rows = extra.outputArgs.rows
	)

	twoRowsCompare := func(a, b []any, onlyByPK bool) int {
		return compareRows(a, b, extra.outputArgs.pkColIdxes, extra.outputArgs.pkTypes, onlyByPK)
	}

	slices.SortFunc(rows, func(a, b []any) int {
		return twoRowsCompare(a, b, false)
	})

	appendNewRow := func(row []any) {
		newRows = append(newRows, row)
		if row[1].(string) == diffAddedLine {
			newRows[len(newRows)-1][1] = diffInsert
		} else {
			newRows[len(newRows)-1][1] = diffDelete
		}
	}

	mergeTwoRows := func(row1, row2 []any) {
		// merge two rows to a single row
		// key the row with "+" flag, should be the second
		newRows = append(newRows, row2)
		newRows[len(newRows)-1][1] = diffUpdate
	}

	for i := 0; i < len(rows); {
		if i+2 > len(rows) {
			appendNewRow(rows[i])
			break
		}

		// check if the row[i], r[i+1] has the same pk
		if twoRowsCompare(rows[i], rows[i+1], true) == 0 {
			mergeTwoRows(rows[i], rows[i+1])
			i += 2
		} else {
			appendNewRow(rows[i])
			i++
		}
	}

	return
}

func diff(
	ctx context.Context,
	ses *Session,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
	tarHandle engine.ChangesHandle,
	baseHandle engine.ChangesHandle,
	diffAsOpt *tree.DiffAsOpt,
	lcaTableID uint64,
	tarBranchTS timestamp.Timestamp,
	baseBranchTS timestamp.Timestamp,
	bh BackgroundExec,
	extra *diffExtra,
) (err error) {

	var (
		rows   [][]any
		mp     = ses.proc.Mp()
		pkKind int

		pkColIdxes []int
		pkTypes    []types.Type

		baseDataHashmap      databranchutils.BranchHashmap
		baseTombstoneHashmap databranchutils.BranchHashmap
	)

	defer func() {
		if baseDataHashmap != nil {
			baseDataHashmap.Close()
		}
		if baseTombstoneHashmap != nil {
			baseTombstoneHashmap.Close()
		}

		if extra != nil && err == nil {
			extra.outputArgs.rows = rows
			extra.outputArgs.pkKind = pkKind
			extra.outputArgs.pkTypes = pkTypes
			extra.outputArgs.pkColIdxes = pkColIdxes
		}
	}()

	// case 1: fake pk, combined all columns as the PK
	if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		pkKind = fakeKind
		for i, col := range baseTblDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName && col.Name != catalog.Row_ID {
				pkColIdxes = append(pkColIdxes, i)
				pkTypes = append(pkTypes, types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale))
			}
		}
	} else if baseTblDef.Pkey.CompPkeyCol != nil {
		// case 2: composite pk, combined all pks columns as the PK
		pkKind = compositeKind
		pkNames := baseTblDef.Pkey.Names
		for _, name := range pkNames {
			idx := int(baseTblDef.Name2ColIndex[name])
			pkColIdxes = append(pkColIdxes, idx)
			pkCol := baseTblDef.Cols[idx]
			pkTypes = append(pkTypes, types.New(types.T(pkCol.Typ.Id), pkCol.Typ.Width, pkCol.Typ.Scale))
		}
	} else {
		// normal pk
		pkKind = normalKind
		pkName := baseTblDef.Pkey.PkeyColName
		idx := int(baseTblDef.Name2ColIndex[pkName])
		pkColIdxes = append(pkColIdxes, idx)
		pkCol := baseTblDef.Cols[idx]
		pkTypes = append(pkTypes, types.New(types.T(pkCol.Typ.Id), pkCol.Typ.Width, pkCol.Typ.Scale))
	}

	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForBaseTable(
		ctx, mp, lcaTableID, pkColIdxes, baseHandle,
	); err != nil {
		return
	}

	var (
		mrs      *MysqlResultSet
		tmpRows  [][]any
		pkVecs   []*vector.Vector
		checkRet []databranchutils.GetResult

		dataBat        *batch.Batch
		tombstoneBat   *batch.Batch
		neededColIdxes []int

		tarDelsOnLCA  *vector.Vector
		baseDelsOnLCA *vector.Vector
	)

	defer func() {
		if dataBat != nil {
			dataBat.Clean(mp)
		}
		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}
		if tarDelsOnLCA != nil {
			tarDelsOnLCA.Free(mp)
		}
		if baseDelsOnLCA != nil {
			baseDelsOnLCA.Free(mp)
		}
	}()

	if mrs, neededColIdxes, err = buildShowDiffSchema(ctx, ses, tarTblDef, baseTblDef); err != nil {
		return
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

		if lcaTableID == 0 && tombstoneBat != nil {
			// if there has no LCA, the tombstones are not expected
			err = moerr.NewInternalErrorNoCtx("tombstone are not expected from target table with no LCA")
			return
		}

		if dataBat != nil {
			pkVecs = pkVecs[:0]
			for _, idx := range pkColIdxes {
				pkVecs = append(pkVecs, dataBat.Vecs[idx])
			}
			if checkRet, err = baseDataHashmap.PopByVectors(pkVecs); err != nil {
				return
			}

			for i := range checkRet {
				// not exists in the base table
				if !checkRet[i].Exists {
					row := make([]any, len(neededColIdxes)+2)
					row[0] = tarTblDef.Name
					row[1] = diffAddedLine

					for _, idx := range neededColIdxes {
						if err = extractRowFromVector(
							ctx, ses, dataBat.Vecs[idx], idx+2, row, i, true,
						); err != nil {
							return
						}
					}
					rows = append(rows, row)
				} else {
					// exists in the base table, we should compare the left columns
					if pkKind == fakeKind {
						// already compared, do nothing here
					} else {
						var (
							allEqual = true
							tuple    types.Tuple
							valTypes []types.Type
						)

						if tuple, valTypes, err = baseDataHashmap.DecodeRow(checkRet[i].Rows[0]); err != nil {
							return
						}

						for _, idx := range neededColIdxes {
							// skip the keys, already compared
							if slices.Index(pkColIdxes, idx) != -1 {
								continue
							}

							left := types.EncodeValue(tuple[idx], dataBat.Vecs[idx].GetType().Oid)
							if !bytes.Equal(left, dataBat.Vecs[idx].GetRawBytesAt(i)) {
								allEqual = false
								break
							}
						}

						if !allEqual { // the diff comes from the update operations
							row1 := make([]any, len(neededColIdxes)+2)
							row2 := make([]any, len(neededColIdxes)+2)
							row1[0], row1[1] = tarTblDef.Name, diffRemovedLine
							row2[0], row2[1] = tarTblDef.Name, diffAddedLine
							for _, idx := range neededColIdxes {
								switch val := tuple[idx].(type) {
								case types.Timestamp:
									row1[idx+2] = val.String2(ses.timeZone, valTypes[idx].Scale)
								default:
									row1[idx+2] = val
								}

								if err = extractRowFromVector(
									ctx, ses, dataBat.Vecs[idx], idx+2, row2, i, true,
								); err != nil {
									return
								}
							}
							rows = append(rows, row1, row2)
						}
					}
				}
			}

			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			pkVecs = pkVecs[:0]
			pkVecs = append(pkVecs, tombstoneBat.Vecs[0])
			if checkRet, err = baseTombstoneHashmap.PopByVectors(pkVecs); err != nil {
				return
			}

			for i := range checkRet {
				// target table delete on the LCA, but base table not
				if !checkRet[i].Exists {
					if tarDelsOnLCA == nil {
						tarDelsOnLCA = vector.NewVec(*tombstoneBat.Vecs[0].GetType())
					}

					if err = tarDelsOnLCA.UnionOne(tombstoneBat.Vecs[0], int64(i), mp); err != nil {
						return
					}

					if tarDelsOnLCA.Length() >= objectio.BlockMaxRows {
						if tmpRows, err = handleDelsOnLCA(
							ctx, ses, bh, tarDelsOnLCA, true, tarTblDef, baseTblDef, tarBranchTS, lcaTableID,
						); err != nil {
							return
						}
						tarDelsOnLCA.CleanOnlyData()
						rows = append(rows, tmpRows...)
					}

				} else {
					// both delete on the LCA
					// do nothing
				}
			}
		}
	}

	// iterate the left base table data
	if err = baseDataHashmap.ForEach(func(_ []byte, data [][]byte) error {
		row := append([]interface{}{}, tarTblDef.Name, diffRemovedLine)
		for _, r := range data {
			if tuple, valTypes, err := baseDataHashmap.DecodeRow(r); err != nil {
				return err
			} else {
				for i := range tuple {
					switch val := tuple[i].(type) {
					case types.Timestamp:
						row = append(row, val.String2(ses.timeZone, valTypes[i].Scale))
					default:
						row = append(row, tuple[i])
					}
				}
			}
		}
		rows = append(rows, row)
		return nil
	}); err != nil {
		return
	}

	// iterate the left base table tombstones on the LCA
	if err = baseTombstoneHashmap.ForEach(func(key []byte, _ [][]byte) error {
		if tuple, valTypes, err := baseTombstoneHashmap.DecodeRow(key); err != nil {
			return err
		} else {
			if baseDelsOnLCA == nil {
				baseDelsOnLCA = vector.NewVec(valTypes[0])
			}

			if err = vector.AppendAny(baseDelsOnLCA, tuple[0], false, mp); err != nil {
				return err
			}

			if baseDelsOnLCA.Length() >= objectio.BlockMaxRows {
				if tmpRows, err = handleDelsOnLCA(
					ctx, ses, bh, baseDelsOnLCA, false, tarTblDef, baseTblDef, baseBranchTS, lcaTableID,
				); err != nil {
					return err
				}
				baseDelsOnLCA.CleanOnlyData()
				rows = append(rows, tmpRows...)
			}
		}
		return nil
	}); err != nil {
		return
	}

	if tarDelsOnLCA != nil && tarDelsOnLCA.Length() > 0 {
		if tmpRows, err = handleDelsOnLCA(
			ctx, ses, bh, tarDelsOnLCA, true, tarTblDef, baseTblDef, tarBranchTS, lcaTableID,
		); err != nil {
			return
		}
		rows = append(rows, tmpRows...)
	}

	if baseDelsOnLCA != nil && baseDelsOnLCA.Length() > 0 {
		if tmpRows, err = handleDelsOnLCA(
			ctx, ses, bh, baseDelsOnLCA, false, tarTblDef, baseTblDef, baseBranchTS, lcaTableID,
		); err != nil {
			return
		}
		rows = append(rows, tmpRows...)
	}

	for _, row := range rows {
		mrs.AddRow(row)
	}

	fmt.Println("diff", rows)

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

// should read the LCA table to get all column values.
func handleDelsOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dels *vector.Vector,
	isTarDels bool,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
	snapshot timestamp.Timestamp,
	lcaTableId uint64,
) (rows [][]any, err error) {

	var (
		sql       string
		buf       bytes.Buffer
		flag      = diffRemovedLine
		lcaTblDef *plan.TableDef
		mots      = fmt.Sprintf(" {MO_TS=%d} ", snapshot.PhysicalTime)
	)

	if lcaTableId == 0 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"%s.%s and %s.%s should have LCA", tarTblDef.DbName, tarTblDef.Name, baseTblDef.DbName, baseTblDef.Name,
		)
	} else if lcaTableId == baseTblDef.TblId {
		// base is the LCA
		lcaTblDef = baseTblDef
	} else if lcaTableId == tarTblDef.TblId {
		// tar is the LCA
		lcaTblDef = tarTblDef
	} else {
		if _, lcaTblDef, err = ses.GetTxnCompileCtx().ResolveById(lcaTableId, &plan2.Snapshot{
			Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()},
			TS:     &snapshot,
		}); err != nil {
			return nil, err
		}
	}

	if !isTarDels {
		flag = diffAddedLine
	}

	// composite pk
	if baseTblDef.Pkey.CompPkeyCol != nil {
		var (
			tuple   types.Tuple
			pkNames = lcaTblDef.Pkey.Names
		)

		cols, area := vector.MustVarlenaRawData(dels)
		for i := range cols {
			b := cols[i].GetByteSlice(area)
			if tuple, err = types.Unpack(b); err != nil {
				return nil, err
			}

			buf.WriteString("(")

			for j, _ := range tuple {
				buf.WriteString(pkNames[j])
				buf.WriteString(" = ")

				switch pk := tuple[j].(type) {
				case string:
					buf.WriteString("'")
					buf.WriteString(pk)
					buf.WriteString("'")
				case float32:
					buf.WriteString(strconv.FormatFloat(float64(pk), 'f', -1, 32))
				case float64:
					buf.WriteString(strconv.FormatFloat(pk, 'f', -1, 64))
				case bool:
					buf.WriteString(strconv.FormatBool(pk))
				case uint8:
					buf.WriteString(strconv.FormatUint(uint64(pk), 10))
				case int8:
					buf.WriteString(strconv.FormatInt(int64(pk), 10))
				case uint16:
					buf.WriteString(strconv.FormatUint(uint64(pk), 10))
				case int16:
					buf.WriteString(strconv.FormatInt(int64(pk), 10))
				case uint32:
					buf.WriteString(strconv.FormatUint(uint64(pk), 10))
				case int32:
					buf.WriteString(strconv.FormatInt(int64(pk), 10))
				case uint64:
					buf.WriteString(strconv.FormatUint(pk, 10))
				case int64:
					buf.WriteString(strconv.FormatInt(pk, 10))
				case []uint8:
					buf.WriteString("'")
					buf.WriteString(string(pk))
					buf.WriteString("'")
				case types.Timestamp:
					buf.WriteString("'")
					buf.WriteString(pk.String2(time.Local, dels.GetType().Scale))
					buf.WriteString("'")
				case types.Datetime:
					buf.WriteString("'")
					buf.WriteString(pk.String2(dels.GetType().Scale))
					buf.WriteString("'")
				case types.Date:
					buf.WriteString("'")
					buf.WriteString(pk.String())
					buf.WriteString("'")
				case types.Decimal64:
					buf.WriteString(pk.Format(dels.GetType().Scale))
				case types.Decimal128:
					buf.WriteString(pk.Format(dels.GetType().Scale))
				case types.Decimal256:
					buf.WriteString(pk.Format(dels.GetType().Scale))
				default:
					return nil, fmt.Errorf("unknown pk type: %T", pk)
				}

				if j != len(tuple)-1 {
					buf.WriteString(" AND ")
				}
			}

			buf.WriteString(")")

			if i != len(cols)-1 {
				buf.WriteString(" OR ")
			}
		}

		sql = fmt.Sprintf(
			"select * from %s.%s %s where %s ",
			lcaTblDef.DbName, lcaTblDef.Name, mots, buf.String(),
		)

		// fake pk
	} else if baseTblDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		pks := vector.MustFixedColNoTypeCheck[uint64](dels)
		for i, pk := range pks {
			buf.WriteString(strconv.FormatUint(pk, 10))
			if i != len(pks)-1 {
				buf.WriteString(",")
			}
		}

		sql = fmt.Sprintf(
			"select * from %s.%s %s where `__mo_fake_pk_col` in (%s) ",
			lcaTblDef.DbName, lcaTblDef.Name, mots, buf.String(),
		)

		// real pk
	} else {
		for i := range dels.Length() {
			b := dels.GetRawBytesAt(i)
			val := types.DecodeValue(b, dels.GetType().Oid)
			switch val.(type) {
			case []byte:
				buf.WriteString("'")
				buf.WriteString(string(val.([]byte)))
				buf.WriteString("'")
			default:
				buf.WriteString(fmt.Sprintf("%v", val))
			}

			if i != dels.Length()-1 {
				buf.WriteString(",")
			}
		}

		sql = fmt.Sprintf(
			"select * from %s.%s %s where %s in (%s) ",
			lcaTblDef.DbName, lcaTblDef.Name, mots, lcaTblDef.Pkey.PkeyColName, buf.String(),
		)
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	var (
		val    interface{}
		tmpV   []ExecResult
		sqlRet *MysqlResultSet
	)

	if tmpV, err = getResultSet(ctx, bh); err != nil {
		return
	}

	if execResultArrayHasData(tmpV) {
		sqlRet = tmpV[0].(*MysqlResultSet)
		for i := range sqlRet.GetRowCount() {
			row := append([]any{}, tarTblDef.Name, flag)
			for j := range sqlRet.GetColumnCount() {
				if val, err = sqlRet.GetValue(ctx, i, j); err != nil {
					return
				}
				row = append(row, val)
			}
			rows = append(rows, row)
		}
	}

	return
}

func buildShowDiffSchema(
	ctx context.Context,
	ses *Session,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
) (mrs *MysqlResultSet, neededColIdxes []int, err error) {

	var (
		showCols []*MysqlColumn
	)

	//  -----------------------------------------
	// |  tar_table_name  | flag |  columns data |
	//  -----------------------------------------
	showCols = append(showCols, new(MysqlColumn), new(MysqlColumn))
	showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	showCols[0].SetName(fmt.Sprintf("diff %s against %s", tarTblDef.Name, baseTblDef.Name))
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

		if err = convertEngineTypeToMysqlType(ctx, t.Oid, nCol); err != nil {
			return
		}

		nCol.SetName(col.Name)
		showCols = append(showCols, nCol)
		neededColIdxes = append(neededColIdxes, i)
	}

	mrs = ses.GetMysqlResultSet()
	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	return mrs, neededColIdxes, nil
}

func buildHashmapForBaseTable(
	ctx context.Context,
	mp *mpool.MPool,
	lcaTableID uint64,
	pkIdxes []int,
	baseHandle engine.ChangesHandle,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
	err error,
) {

	var (
		dataBat      *batch.Batch
		tombstoneBat *batch.Batch
	)

	defer func() {
		if dataBat != nil {
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}
	}()

	if dataHashmap, err = databranchutils.NewBranchHashmap(); err != nil {
		return
	}

	if tombstoneHashmap, err = databranchutils.NewBranchHashmap(); err != nil {
		return
	}

	for {
		if dataBat, tombstoneBat, _, err = baseHandle.Next(
			ctx, mp,
		); err != nil {
			return
		} else if dataBat == nil && tombstoneBat == nil {
			// out of data
			break
		}

		if lcaTableID == 0 && tombstoneBat != nil {
			// if there has no LCA, the tombstones are not expected
			err = moerr.NewInternalErrorNoCtx("tombstone are not expected from base table with no LCA")
			return
		}

		if dataBat != nil {
			//fmt.Println("base", "\n", common.MoBatchToString(dataBat, dataBat.RowCount()))
			//fmt.Println()
			if err = dataHashmap.PutByVectors(dataBat.Vecs, pkIdxes); err != nil {
				return
			}
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			if err = tombstoneHashmap.PutByVectors(tombstoneBat.Vecs, []int{0}); err != nil {
				return
			}
			tombstoneBat.Clean(mp)
		}
	}

	return
}

func getRelations(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tableA tree.TableName,
	tableB tree.TableName,
) (
	relA engine.Relation,
	relB engine.Relation,
	snapshotA *plan.Snapshot,
	snapshotB *plan.Snapshot,
	err error,
) {

	var (
		dbA engine.Database
		dbB engine.Database

		dbNameA  string
		dbNameB  string
		tblNameA string
		tblNameB string
	)

	if snapshotA, err = resolveSnapshot(ses, tableA.AtTsExpr); err != nil {
		return
	}

	if snapshotB, err = resolveSnapshot(ses, tableB.AtTsExpr); err != nil {
		return
	}

	txnOpA := bh.(*backExec).backSes.GetTxnHandler().txnOp
	txnOpB := bh.(*backExec).backSes.GetTxnHandler().txnOp

	if snapshotA != nil && snapshotA.TS != nil {
		txnOpA = txnOpA.CloneSnapshotOp(*snapshotA.TS)
	}

	if snapshotB != nil && snapshotB.TS != nil {
		txnOpB = txnOpB.CloneSnapshotOp(*snapshotB.TS)
	}

	dbNameA = tableA.SchemaName.String()
	tblNameA = tableA.ObjectName.String()
	if len(dbNameA) == 0 {
		dbNameA = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	dbNameB = tableB.SchemaName.String()
	tblNameB = tableB.ObjectName.String()
	if len(dbNameB) == 0 {
		dbNameB = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	if len(dbNameA) == 0 || len(dbNameB) == 0 {
		err = moerr.NewInternalErrorNoCtxf("the base or target database cannot be empty.")
		return
	}

	eng := ses.proc.GetSessionInfo().StorageEngine
	if dbA, err = eng.Database(ctx, dbNameA, txnOpA); err != nil {
		return
	}

	if relA, err = dbA.Relation(ctx, tblNameA, nil); err != nil {
		return
	}

	if dbB, err = eng.Database(ctx, dbNameB, txnOpB); err != nil {
		return
	}

	if relB, err = dbB.Relation(ctx, tblNameB, nil); err != nil {
		return
	}

	if !isSchemaEquivalent(relA.GetTableDef(ctx), relB.GetTableDef(ctx)) {
		err = moerr.NewInternalErrorNoCtx("the target table schema is not equivalent to the base table.")
		return
	}

	return
}

func constructChangeHandle(
	ctx context.Context,
	ses *Session,
	tarRel engine.Relation,
	baseRel engine.Relation,
	tarSnapshot *plan.Snapshot,
	baseSnapshot *plan.Snapshot,
) (
	lcaTableID uint64,
	tarBranchTS timestamp.Timestamp,
	baseBranchTS timestamp.Timestamp,
	tarHandle engine.ChangesHandle,
	baseHandle engine.ChangesHandle,
	err error,
) {

	var (
		tarEnd   types.TS
		baseEnd  types.TS
		tarFrom  types.TS
		baseFrom types.TS
	)

	if lcaTableID, tarBranchTS, baseBranchTS, err = decideLCABranchTSFromBranchDAG(
		ctx, ses, tarRel, baseRel,
	); err != nil {
		return
	}

	if tarEnd, baseEnd, tarFrom, baseFrom, err = decideCollectRange(
		ctx, ses, tarRel, baseRel, lcaTableID,
		types.TimestampToTS(tarBranchTS),
		types.TimestampToTS(baseBranchTS),
		tarSnapshot, baseSnapshot,
	); err != nil {
		return
	}

	if tarHandle, err = databranchutils.CollectChanges(
		ctx,
		tarRel,
		tarFrom,
		tarEnd,
		ses.proc.Mp(),
	); err != nil {
		return
	}

	if baseHandle, err = databranchutils.CollectChanges(
		ctx,
		baseRel,
		baseFrom,
		baseEnd,
		ses.proc.Mp(),
	); err != nil {
		return
	}

	return
}

func decideCollectRange(
	ctx context.Context,
	ses *Session,
	tarRel engine.Relation,
	baseRel engine.Relation,
	lcaTableID uint64,
	tarBranchTS types.TS,
	baseBranchTS types.TS,
	tarSnapshot *plan.Snapshot,
	baseSnapshot *plan.Snapshot,
) (
	tarEndTS types.TS,
	baseEndTS types.TS,
	tarFromTS types.TS,
	baseFromTS types.TS,
	err error,
) {

	var (
		tarSp  types.TS
		baseSp types.TS

		tarCTS  types.TS
		baseCTS types.TS

		tarTableID  = tarRel.GetTableID(ctx)
		baseTableID = baseRel.GetTableID(ctx)

		mp    = ses.proc.Mp()
		eng   = ses.proc.GetSessionInfo().StorageEngine
		txnOp = ses.GetTxnHandler().GetTxn()

		txnSnapshot = types.TimestampToTS(txnOp.SnapshotTS())
	)

	tarSp = txnSnapshot
	if tarSnapshot != nil && tarSnapshot.TS != nil {
		tarSp = types.TimestampToTS(*tarSnapshot.TS)
	}

	baseSp = txnSnapshot
	if baseSnapshot != nil && baseSnapshot.TS != nil {
		baseSp = types.TimestampToTS(*baseSnapshot.TS)
	}

	// now we got the t1.snapshot, t1.branchTS, t2.snapshot, t2.branchTS and txnSnapshot,
	// and then we need to decide the range that t1 and t2 should collect.
	//
	// case 1: t1 and t2 have no LCA
	//	==> t1 collect [0, sp], t2 collect [0, sp]
	//
	// case 2: t1 and t2 have the LCA t0 (not t1 nor t2)
	// 	i. t1 and t2 branched from to at the same ts
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	//	ii. t1 and t2 have different branchTS
	//		==> t1 collect [0, sp], t2 collect [0, sp]
	//
	// case 3: t1 is the LCA of t1 and t2
	//	i. t1.sp < t2.branchTS
	//		==> t1 collect [0, sp], t2 collect [0, sp]
	//  ii. t1.sp == t2.branchTS
	//		==> t1 collect nothing, t2 collect [branchTS+1, sp]
	// iii. t1.sp > t2.branchTS
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	//
	// case 4: t2 is the LCA of t1 and t2
	//	...

	// Note That:
	// 1. the branchTS+1 cannot skip the cloned data, we need get the clone commitTS (the table creation commitTS)

	getTarCommitTS := func() (types.TS, error) {
		return getTableCreationCommitTS(ctx, eng, mp, tarBranchTS, tarSp, txnOp, tarRel)
	}

	getBaseCommitTS := func() (types.TS, error) {
		return getTableCreationCommitTS(ctx, eng, mp, baseBranchTS, baseSp, txnOp, baseRel)
	}

	worstRange := func() {
		tarEndTS = tarSp
		baseEndTS = baseSp
		tarFromTS = types.MinTs()
		baseFromTS = types.MinTs()
	}

	tarCollectNothing := func() {
		tarFromTS = tarSp
		tarEndTS = tarSp.Prev()
	}

	baseCollectNothing := func() {
		baseFromTS = baseSp
		baseEndTS = baseSp.Prev()
	}

	givenCommonBaseIsLCA := func() error {
		tarCTS, err = getTarCommitTS()

		tarFromTS = tarCTS.Next()
		tarEndTS = tarSp
		baseFromTS = tarCTS.Next()
		baseEndTS = baseSp
		return err
	}

	givenCommonTarIsLCA := func() error {
		baseCTS, err = getBaseCommitTS()
		tarFromTS = baseCTS.Next()
		tarEndTS = tarSp
		baseFromTS = baseCTS.Next()
		baseEndTS = baseSp
		return err
	}

	if lcaTableID == 0 {
		// no LCA
		worstRange()
	} else if lcaTableID == baseTableID { // base is the LCA
		if baseSp.LT(&tarBranchTS) {
			worstRange()
		} else if baseSp.EQ(&tarBranchTS) {
			// base collect nothing, tar collect [branchTS+1, sp]
			tarCTS, err = getTarCommitTS()
			tarFromTS = tarCTS.Next()
			tarEndTS = tarSp
			baseCollectNothing()
		} else {
			// baseSp.GT(&tarBranchTS)
			err = givenCommonBaseIsLCA()
		}

	} else if lcaTableID == tarTableID { // tar is the LCA
		if tarSp.LT(&baseBranchTS) {
			worstRange()
		} else if tarSp.EQ(&baseBranchTS) {
			// tar collect nothing, base collect [branchTS+1, sp]
			tarCollectNothing()
			baseCTS, err = getBaseCommitTS()
			baseFromTS = baseCTS.Next()
			baseEndTS = baseSp
		} else {
			// tarSp.GT(&baseBranchTS)
			err = givenCommonTarIsLCA()
		}

	} else {
		// LCA not tar or base
		if tarBranchTS.EQ(&baseBranchTS) {
			tarCTS, err = getTarCommitTS()
			tarFromTS = tarCTS.Next()
			tarEndTS = tarSp

			if err != nil {
				return
			}

			baseCTS, err = getBaseCommitTS()
			baseEndTS = baseSp
			baseFromTS = baseCTS.Next()
		} else {
			worstRange()
		}
	}

	return
}

func getTableCreationCommitTS(
	ctx context.Context,
	eng engine.Engine,
	mp *mpool.MPool,
	branchTS types.TS,
	snapshot types.TS,
	txnOp client.TxnOperator,
	rel engine.Relation,
) (commitTS types.TS, err error) {

	var (
		data          *batch.Batch
		tombstone     *batch.Batch
		moTableRel    engine.Relation
		moTableHandle engine.ChangesHandle
	)

	defer func() {
		if data != nil {
			data.Clean(mp)
		}
		if moTableHandle != nil {
			moTableHandle.Close()
		}
	}()

	if _, _, moTableRel, err = eng.GetRelationById(
		ctx, txnOp, catalog.MO_TABLES_ID,
	); err != nil {
		return
	}

	zeroTS := types.MinTs()
	if branchTS.EQ(&zeroTS) {
		layout := "2006-01-02 15:04:05"
		golangZero, _ := time.Parse(layout, layout)
		branchTS = types.BuildTS(golangZero.UnixNano(), 0)
	}

	if moTableHandle, err = moTableRel.CollectChanges(
		ctx, branchTS, snapshot, true, mp,
	); err != nil {
		return
	}

	for commitTS.IsEmpty() {
		if data, tombstone, _, err = moTableHandle.Next(ctx, mp); err != nil {
			return
		} else if data == nil && tombstone == nil {
			break
		}

		if tombstone != nil {
			tombstone.Clean(mp)
		}

		if data != nil {
			relIdCol := vector.MustFixedColNoTypeCheck[uint64](data.Vecs[0])
			commitTSCol := vector.MustFixedColNoTypeCheck[types.TS](data.Vecs[len(data.Vecs)-1])

			if idx := slices.Index(relIdCol, rel.GetTableID(ctx)); idx != -1 {
				commitTS = commitTSCol[idx]
			}

			data.Clean(mp)
		}
	}

	if commitTS.IsEmpty() {
		err = moerr.NewInternalErrorNoCtx("cannot find the commit ts of the cloned table")
	}

	return commitTS, err
}

func decideLCABranchTSFromBranchDAG(
	ctx context.Context,
	ses *Session,
	tarRel engine.Relation,
	baseRel engine.Relation,
) (
	lcaTableID uint64,
	tarBranchTS timestamp.Timestamp,
	baseBranchTS timestamp.Timestamp,
	err error,
) {

	var (
		dag *databranchutils.DataBranchDAG

		tarTS  int64
		baseTS int64
		hasLca bool
	)

	if dag, err = constructBranchDAG(ctx, ses); err != nil {
		return
	}

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
	if lcaTableID, tarTS, baseTS, hasLca = dag.FindLCA(
		tarRel.GetTableID(ctx), baseRel.GetTableID(ctx),
	); hasLca {
		if lcaTableID == baseRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: tarTS}
			tarBranchTS = ts
			baseBranchTS = ts
		} else if lcaTableID == tarRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: baseTS}
			tarBranchTS = ts
			baseBranchTS = ts
		} else {
			tarBranchTS = timestamp.Timestamp{PhysicalTime: tarTS}
			baseBranchTS = timestamp.Timestamp{PhysicalTime: baseTS}
		}
	}

	return
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
