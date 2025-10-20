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

type collectRange struct {
	from []types.TS
	end  []types.TS
	rel  []engine.Relation
}

type branchMetaInfo struct {
	lcaTableId   uint64
	tarBranchTS  types.TS
	baseBranchTS types.TS
}

type tablePair struct {
	tarRel       engine.Relation
	baseRel      engine.Relation
	tarSnapshot  *plan.Snapshot
	baseSnapshot *plan.Snapshot
}

type diffExtra struct {
	inputArgs struct {
		bh BackgroundExec

		dagInfo branchMetaInfo
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
		t := time.Now()
		defer func() {
			fmt.Println("handle snapshot diff takes", time.Since(t))
		}()
		return handleSnapshotDiff(execCtx, ses, st, nil)
	case *tree.DataBranchMerge:
		t := time.Now()
		defer func() {
			fmt.Println("handle snapshot merge takes", time.Since(t))
		}()
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
		bh       BackgroundExec
		tables   tablePair
		deferred func(error) error
		dagInfo  branchMetaInfo
	)

	if extra != nil {
		bh = extra.inputArgs.bh
		tables.tarRel = extra.inputArgs.tarRel
		tables.baseRel = extra.inputArgs.baseRel
		tables.tarSnapshot = extra.inputArgs.tarSnapshot
		tables.baseSnapshot = extra.inputArgs.baseSnapshot
		dagInfo = extra.inputArgs.dagInfo
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

		if tables, err = getPairedRelations(
			execCtx.reqCtx, ses, bh, stmt.TargetTable, stmt.BaseTable,
		); err != nil {
			return
		}

		if dagInfo, err = decideLCABranchTSFromBranchDAG(
			execCtx.reqCtx, ses, tables,
		); err != nil {
			return
		}
	}

	var (
		tarHandle  []engine.ChangesHandle
		baseHandle []engine.ChangesHandle

		tarTblDef  = tables.tarRel.GetTableDef(execCtx.reqCtx)
		baseTblDef = tables.baseRel.GetTableDef(execCtx.reqCtx)
	)

	defer func() {
		for _, h := range tarHandle {
			_ = h.Close()
		}
		for _, h := range baseHandle {
			_ = h.Close()
		}
	}()

	if tarHandle, baseHandle, err = constructChangeHandle(
		execCtx.reqCtx, ses, bh, tables, dagInfo,
	); err != nil {
		return
	}

	if err = diff(
		execCtx.reqCtx, ses,
		tarTblDef, baseTblDef,
		tarHandle, baseHandle,
		stmt.DiffAsOpts,
		dagInfo,
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

		tables  tablePair
		dagInfo branchMetaInfo
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return err
	}
	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if tables, err = getPairedRelations(
		execCtx.reqCtx, ses, bh, stmt.SrcTable, stmt.DstTable,
	); err != nil {
		return
	}

	if dagInfo, err = decideLCABranchTSFromBranchDAG(
		execCtx.reqCtx, ses, tables,
	); err != nil {
		return
	}

	conflictOpt = tree.CONFLICT_FAIL
	if stmt.ConflictOpt != nil {
		conflictOpt = stmt.ConflictOpt.Opt
	}

	if dagInfo.lcaTableId == 0 {
		// has no lca
		var (
			extra diffExtra
		)
		extra.inputArgs.bh = bh
		extra.inputArgs.dagInfo = dagInfo
		extra.inputArgs.tarRel = tables.tarRel
		extra.inputArgs.baseRel = tables.baseRel
		extra.inputArgs.tarSnapshot = tables.tarSnapshot
		extra.inputArgs.baseSnapshot = tables.baseSnapshot

		if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra); err != nil {
			return
		}

		extra.outputArgs.rows = sortDiffResultRows(extra)
		return mergeDiff(
			execCtx, bh, ses, tables.baseRel,
			extra.outputArgs.rows, nil,
			lcaEmpty, conflictOpt,
			extra.outputArgs.pkTypes,
			extra.outputArgs.pkColIdxes,
		)
	}

	// merge left into right
	var (
		extra1      diffExtra
		extra2      diffExtra
		lcaType     int
		lcaRel      engine.Relation
		lcaSnapshot *plan.Snapshot
	)

	lcaSnapshot = &plan2.Snapshot{
		Tenant: &plan.SnapshotTenant{
			TenantID: ses.GetAccountId(),
		},
	}

	if dagInfo.lcaTableId == tables.tarRel.GetTableID(execCtx.reqCtx) {
		// left is the LCA
		lcaType = lcaLeft
		lcaRel = tables.tarRel
		extra1.inputArgs.dagInfo = dagInfo
		extra2.inputArgs.dagInfo = dagInfo
		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.baseBranchTS.Physical()}
		if tables.tarSnapshot != nil && tables.tarSnapshot.TS.Less(*lcaSnapshot.TS) {
			lcaSnapshot.TS = tables.tarSnapshot.TS
		}
	} else if dagInfo.lcaTableId == tables.baseRel.GetTableID(execCtx.reqCtx) {
		// right is the LCA
		lcaType = lcaRight
		lcaRel = tables.baseRel
		extra1.inputArgs.dagInfo = dagInfo
		extra2.inputArgs.dagInfo = dagInfo
		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
		if tables.baseSnapshot != nil && tables.baseSnapshot.TS.Less(*lcaSnapshot.TS) {
			lcaSnapshot.TS = tables.baseSnapshot.TS
		}
	} else {
		// LCA is other table
		lcaType = lcaOther
		//minBranchTS := dagInfo.tarBranchTS
		//if dagInfo.baseBranchTS.LT(&minBranchTS) {
		//	minBranchTS = dagInfo.baseBranchTS
		//}
		extra1.inputArgs.dagInfo = dagInfo
		extra2.inputArgs.dagInfo = dagInfo

		lcaSnapshot.TS = &timestamp.Timestamp{PhysicalTime: dagInfo.tarBranchTS.Physical()}
		if dagInfo.baseBranchTS.LT(&dagInfo.tarBranchTS) {
			lcaSnapshot.TS.PhysicalTime = dagInfo.baseBranchTS.Physical()
		}

		if lcaRel, err = getRelationById(
			execCtx.reqCtx, ses, bh, dagInfo.lcaTableId, lcaSnapshot); err != nil {
			return
		}
	}

	extra1.inputArgs.bh = bh
	extra1.inputArgs.tarRel = tables.tarRel
	extra1.inputArgs.baseRel = lcaRel
	extra1.inputArgs.tarSnapshot = tables.tarSnapshot
	extra1.inputArgs.baseSnapshot = lcaSnapshot

	if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra1); err != nil {
		return
	}

	extra2.inputArgs.bh = bh
	extra2.inputArgs.tarRel = tables.baseRel
	extra2.inputArgs.baseRel = lcaRel
	extra2.inputArgs.tarSnapshot = tables.baseSnapshot
	extra2.inputArgs.baseSnapshot = lcaSnapshot

	if err = handleSnapshotDiff(execCtx, ses, &tree.DataBranchDiff{}, &extra2); err != nil {
		return
	}

	extra1.outputArgs.rows = sortDiffResultRows(extra1)
	extra2.outputArgs.rows = sortDiffResultRows(extra2)
	return mergeDiff(
		execCtx, bh, ses, tables.baseRel,
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
					j++
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
		// if pk equal and flag not equal, means this is an update,
		// or duplicate insert with no pk table.
		if twoRowsCompare(rows[i], rows[i+1], true) == 0 &&
			rows[i][1] != rows[i+1][1] {
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
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	diffAsOpt *tree.DiffAsOpt,
	dagInfo branchMetaInfo,
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
		ctx, mp, dagInfo.lcaTableId, pkColIdxes, baseHandle,
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
	for _, handle := range tarHandle {
		for {
			if dataBat, tombstoneBat, _, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
			}

			if dagInfo.lcaTableId == 0 && tombstoneBat != nil {
				// if there has no LCA, the tombstones are not expected
				err = moerr.NewInternalErrorNoCtx("tombstone are not expected from target table with no LCA")
				return
			}

			if dataBat != nil {
				pkVecs = pkVecs[:0]
				for _, idx := range pkColIdxes {
					pkVecs = append(pkVecs, dataBat.Vecs[idx])
				}
				if checkRet, err = baseDataHashmap.PopByVectors(pkVecs, false); err != nil {
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
							fmt.Println("find", len(checkRet[i].Rows))
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
				if checkRet, err = baseTombstoneHashmap.PopByVectors(pkVecs, true); err != nil {
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
								ctx, ses, bh, tarDelsOnLCA, true,
								tarTblDef, baseTblDef,
								dagInfo.tarBranchTS.ToTimestamp(), dagInfo.lcaTableId,
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
	}

	// iterate the left base table data
	if err = baseDataHashmap.ForEach(func(_ []byte, data [][]byte) error {
		//row := append([]interface{}{}, tarTblDef.Name, diffRemovedLine)
		for _, r := range data {
			row := append([]interface{}{}, tarTblDef.Name, diffRemovedLine)
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
			rows = append(rows, row)
		}
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
					ctx, ses, bh, baseDelsOnLCA, false,
					tarTblDef, baseTblDef,
					dagInfo.baseBranchTS.ToTimestamp(), dagInfo.lcaTableId,
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
			ctx, ses, bh, tarDelsOnLCA, true,
			tarTblDef, baseTblDef,
			dagInfo.tarBranchTS.ToTimestamp(), dagInfo.lcaTableId,
		); err != nil {
			return
		}
		rows = append(rows, tmpRows...)
	}

	if baseDelsOnLCA != nil && baseDelsOnLCA.Length() > 0 {
		if tmpRows, err = handleDelsOnLCA(
			ctx, ses, bh, baseDelsOnLCA, false,
			tarTblDef, baseTblDef,
			dagInfo.baseBranchTS.ToTimestamp(), dagInfo.lcaTableId,
		); err != nil {
			return
		}
		rows = append(rows, tmpRows...)
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
					return nil, moerr.NewInternalErrorNoCtxf("unknown pk type: %T", pk)
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
	baseHandle []engine.ChangesHandle,
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

	for _, handle := range baseHandle {
		for {
			if dataBat, tombstoneBat, _, err = handle.Next(
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
	}

	return
}

func getRelationById(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tableId uint64,
	snapshot *plan.Snapshot,
) (rel engine.Relation, err error) {

	txnOp := bh.(*backExec).backSes.GetTxnHandler().txnOp

	if snapshot != nil && snapshot.TS != nil {
		txnOp = txnOp.CloneSnapshotOp(*snapshot.TS)
	}

	_, _, rel, err = ses.GetTxnHandler().GetStorage().GetRelationById(ctx, txnOp, tableId)
	return rel, err
}

func getPairedRelations(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tarName tree.TableName,
	baseName tree.TableName,
) (
	tables tablePair,
	err error,
) {

	var (
		tarDB  engine.Database
		baseDB engine.Database

		tarDBName   string
		baseDBName  string
		tarTblName  string
		baseTblName string

		tarRel   engine.Relation
		baseRel  engine.Relation
		tarSnap  *plan.Snapshot
		baseSnap *plan.Snapshot
	)

	defer func() {
		tables.tarRel = tarRel
		tables.baseRel = baseRel
		tables.tarSnapshot = tarSnap
		tables.baseSnapshot = baseSnap
	}()

	if tarSnap, err = resolveSnapshot(ses, tarName.AtTsExpr); err != nil {
		return
	}

	if baseSnap, err = resolveSnapshot(ses, baseName.AtTsExpr); err != nil {
		return
	}

	txnOpA := bh.(*backExec).backSes.GetTxnHandler().txnOp
	txnOpB := bh.(*backExec).backSes.GetTxnHandler().txnOp

	if tarSnap != nil && tarSnap.TS != nil {
		txnOpA = txnOpA.CloneSnapshotOp(*tarSnap.TS)
	}

	if baseSnap != nil && baseSnap.TS != nil {
		txnOpB = txnOpB.CloneSnapshotOp(*baseSnap.TS)
	}

	tarDBName = tarName.SchemaName.String()
	tarTblName = tarName.ObjectName.String()
	if len(tarDBName) == 0 {
		tarDBName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	baseDBName = baseName.SchemaName.String()
	baseTblName = baseName.ObjectName.String()
	if len(baseDBName) == 0 {
		baseDBName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	if len(tarDBName) == 0 || len(baseDBName) == 0 {
		err = moerr.NewInternalErrorNoCtxf("the base or target database cannot be empty.")
		return
	}

	eng := ses.proc.GetSessionInfo().StorageEngine
	if tarDB, err = eng.Database(ctx, tarDBName, txnOpA); err != nil {
		return
	}

	if tarRel, err = tarDB.Relation(ctx, tarTblName, nil); err != nil {
		return
	}

	if baseDB, err = eng.Database(ctx, baseDBName, txnOpB); err != nil {
		return
	}

	if baseRel, err = baseDB.Relation(ctx, baseTblName, nil); err != nil {
		return
	}

	if !isSchemaEquivalent(tarRel.GetTableDef(ctx), baseRel.GetTableDef(ctx)) {
		err = moerr.NewInternalErrorNoCtx("the target table schema is not equivalent to the base table.")
		return
	}

	return
}

func constructChangeHandle(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tablePair,
	branchInfo branchMetaInfo,
) (
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	err error,
) {
	var (
		handle    engine.ChangesHandle
		tarRange  collectRange
		baseRange collectRange
	)

	if tarRange, baseRange, err = decideCollectRange(
		ctx, ses, bh, tables, branchInfo,
	); err != nil {
		return
	}

	for i := range tarRange.rel {
		if handle, err = databranchutils.CollectChanges(
			ctx,
			tarRange.rel[i],
			tarRange.from[i],
			tarRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}

		tarHandle = append(tarHandle, handle)
	}

	for i := range baseRange.rel {
		if handle, err = databranchutils.CollectChanges(
			ctx,
			baseRange.rel[i],
			baseRange.from[i],
			baseRange.end[i],
			ses.proc.Mp(),
		); err != nil {
			return
		}

		baseHandle = append(baseHandle, handle)
	}

	return
}

func decideCollectRange(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tables tablePair,
	dagInfo branchMetaInfo,
) (
	tarCollectRange collectRange,
	baseCollectRange collectRange,
	err error,
) {

	var (
		lcaRel engine.Relation

		tarSp  types.TS
		baseSp types.TS

		tarCTS  types.TS
		baseCTS types.TS

		tarTableID  = tables.tarRel.GetTableID(ctx)
		baseTableID = tables.baseRel.GetTableID(ctx)

		mp    = ses.proc.Mp()
		eng   = ses.proc.GetSessionInfo().StorageEngine
		txnOp = ses.GetTxnHandler().GetTxn()

		txnSnapshot = types.TimestampToTS(txnOp.SnapshotTS())
	)

	tarSp = txnSnapshot
	if tables.tarSnapshot != nil && tables.tarSnapshot.TS != nil {
		tarSp = types.TimestampToTS(*tables.tarSnapshot.TS)
	}

	baseSp = txnSnapshot
	if tables.baseSnapshot != nil && tables.baseSnapshot.TS != nil {
		baseSp = types.TimestampToTS(*tables.baseSnapshot.TS)
	}

	if tarCTS, err = getTableCreationCommitTS(
		ctx, eng, mp, types.MinTs(), tarSp, txnOp, tables.tarRel,
	); err != nil {
		return
	}

	if baseCTS, err = getTableCreationCommitTS(
		ctx, eng, mp, types.MinTs(), baseSp, txnOp, tables.baseRel,
	); err != nil {
		return
	}

	// Note That:
	// 1. the branchTS+1 cannot skip the cloned data, we need get the clone commitTS (the table creation commitTS)
	//

	// now we got the t1.snapshot, t1.branchTS, t2.snapshot, t2.branchTS and txnSnapshot,
	// and then we need to decide the range that t1 and t2 should collect.
	//
	// case 0: special cases:
	//	i. tar = base
	//	 same table ==> same branch TS ==> same from ts
	//	 data branch diff t1{sp1} against t1{sp2}
	//	 t1: -----from ts-------sp1-----------sp2----->
	//
	//	diff t1.[fromTS, sp1] against t1.[fromTS, sp2]
	//
	//	 minEnd = mix(sp1,sp2)
	//	 ==> t1.[minEnd, sp1] against t1.[minEnd, sp2]
	if tarTableID == baseTableID {
		minSp := tarSp
		if minSp.GT(&baseSp) {
			minSp = baseSp
		}
		tarCollectRange = collectRange{
			from: []types.TS{minSp.Next()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
		baseCollectRange = collectRange{
			from: []types.TS{minSp.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		return
	}

	//
	// case 1: t1 and t2 have no LCA
	//	==> t1 collect [0, sp], t2 collect [0, sp]
	if dagInfo.lcaTableId == 0 {
		tarCollectRange = collectRange{
			from: []types.TS{types.MinTs()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
		baseCollectRange = collectRange{
			from: []types.TS{types.MinTs()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		return
	}

	// case 2: t1 and t2 have the LCA t0 (not t1 nor t2)
	// 	i. t1 and t2 branched from to at the same ts
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	//	ii. t1 and t2 have different branchTS
	//                             seg2  sp2
	//		  common    seg1  t2 --------|--->
	//   t0 |-------|---------|-------------->
	//             t1 ----------------|----->
	//					seg3		 sp1
	// the diff between	(t0.seg1 ∩ t2.seg2)	 and t1.seg3
	if dagInfo.lcaTableId != tarTableID && dagInfo.lcaTableId != baseTableID {
		tarCollectRange = collectRange{
			from: []types.TS{tarCTS.Next()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		if dagInfo.tarBranchTS.EQ(&dagInfo.baseBranchTS) {
			// do nothing
		} else {
			if lcaRel, err = getRelationById(
				ctx, ses, bh, dagInfo.lcaTableId, &plan2.Snapshot{
					Tenant: &plan.SnapshotTenant{TenantID: ses.GetAccountId()},
					TS:     &timestamp.Timestamp{PhysicalTime: tarSp.Physical()},
				}); err != nil {
				return
			}

			if dagInfo.tarBranchTS.GT(&dagInfo.baseBranchTS) {
				tarCollectRange.rel = append(tarCollectRange.rel, lcaRel)
				tarCollectRange.from = append(tarCollectRange.from, dagInfo.baseBranchTS.Next())
				tarCollectRange.end = append(tarCollectRange.end, dagInfo.tarBranchTS)
			} else {
				baseCollectRange.rel = append(baseCollectRange.rel, lcaRel)
				baseCollectRange.from = append(baseCollectRange.from, dagInfo.tarBranchTS.Next())
				baseCollectRange.end = append(baseCollectRange.end, dagInfo.baseBranchTS)
			}
		}
		return
	}

	// case 3: t1 is the LCA of t1 and t2
	//	i. t1.sp < t2.branchTS
	//		t1 -----sp1--------|-------------->
	//				    seg1  t2-------sp2---->
	//							  seg2
	//		==> the diff between null and (t1.seg1 ∩ t2.seg2)
	//  ii. t1.sp == t2.branchTS
	//		==> t1 collect nothing, t2 collect [branchTS+1, sp]
	// iii. t1.sp > t2.branchTS
	//		==> t1 collect [branchTS+1, sp], t2 collect [branchTS+1, sp]
	if dagInfo.lcaTableId == baseTableID {
		// base is the lca
		if baseSp.LT(&dagInfo.tarBranchTS) {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next(), baseSp.Next()},
				end:  []types.TS{tarSp, dagInfo.tarBranchTS},
				rel:  []engine.Relation{tables.tarRel, tables.baseRel},
			}
			// base collect nothing
		} else if baseSp.EQ(&dagInfo.tarBranchTS) {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}
			// base collect nothing
		} else {
			tarCollectRange = collectRange{
				from: []types.TS{tarCTS.Next()},
				end:  []types.TS{tarSp},
				rel:  []engine.Relation{tables.tarRel},
			}
			baseCollectRange = collectRange{
				from: []types.TS{dagInfo.tarBranchTS.Next()},
				end:  []types.TS{baseSp},
				rel:  []engine.Relation{tables.baseRel},
			}
		}
		return
	}

	// case 4: t2 is the LCA of t1 and t2
	// tar is the lca
	if tarSp.LT(&dagInfo.baseBranchTS) {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next(), tarSp.Next()},
			end:  []types.TS{baseSp, dagInfo.baseBranchTS},
			rel:  []engine.Relation{tables.baseRel, tables.tarRel},
		}
		// tar collect nothing
	} else if tarSp.EQ(&dagInfo.baseBranchTS) {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		// tar collect nothing
	} else {
		baseCollectRange = collectRange{
			from: []types.TS{baseCTS.Next()},
			end:  []types.TS{baseSp},
			rel:  []engine.Relation{tables.baseRel},
		}
		tarCollectRange = collectRange{
			from: []types.TS{dagInfo.baseBranchTS.Next()},
			end:  []types.TS{tarSp},
			rel:  []engine.Relation{tables.tarRel},
		}
	}

	//worstRange := func() {
	//	tarEndTS = tarSp
	//	baseEndTS = baseSp
	//	tarFromTS = types.MinTs()
	//	baseFromTS = types.MinTs()
	//}
	//
	//tarCollectNothing := func() {
	//	tarFromTS = tarSp
	//	tarEndTS = tarSp.Prev()
	//}
	//
	//baseCollectNothing := func() {
	//	baseFromTS = baseSp
	//	baseEndTS = baseSp.Prev()
	//}
	//
	//givenUpCommonBaseIsLCA := func() error {
	//	tarCTS, err = getTarCommitTS()
	//
	//	tarFromTS = tarCTS.Next()
	//	tarEndTS = tarSp
	//	baseFromTS = tarCTS.Next()
	//	baseEndTS = baseSp
	//	return err
	//}
	//
	//givenUpCommonTarIsLCA := func() error {
	//	baseCTS, err = getBaseCommitTS()
	//	tarFromTS = baseCTS.Next()
	//	tarEndTS = tarSp
	//	baseFromTS = baseCTS.Next()
	//	baseEndTS = baseSp
	//	return err
	//}
	//
	//if lcaTableID == 0 {
	//	// no LCA
	//	worstRange()
	//} else if lcaTableID == baseTableID { // base is the LCA
	//	if baseSp.LT(&tarBranchTS) {
	//		worstRange()
	//	} else if baseSp.EQ(&tarBranchTS) {
	//		// base collect nothing, tar collect [branchTS+1, sp]
	//		tarCTS, err = getTarCommitTS()
	//		tarFromTS = tarCTS.Next()
	//		tarEndTS = tarSp
	//		baseCollectNothing()
	//	} else {
	//		// baseSp.GT(&tarBranchTS)
	//		err = givenUpCommonBaseIsLCA()
	//	}
	//
	//} else if lcaTableID == tarTableID { // tar is the LCA
	//	if tarSp.LT(&baseBranchTS) {
	//		worstRange()
	//	} else if tarSp.EQ(&baseBranchTS) {
	//		// tar collect nothing, base collect [branchTS+1, sp]
	//		tarCollectNothing()
	//		baseCTS, err = getBaseCommitTS()
	//		baseFromTS = baseCTS.Next()
	//		baseEndTS = baseSp
	//	} else {
	//		// tarSp.GT(&baseBranchTS)
	//		err = givenUpCommonTarIsLCA()
	//	}
	//
	//} else {
	//	// LCA not tar or base
	//	if tarBranchTS.EQ(&baseBranchTS) {
	//		tarCTS, err = getTarCommitTS()
	//		tarFromTS = tarCTS.Next()
	//		tarEndTS = tarSp
	//
	//		if err != nil {
	//			return
	//		}
	//
	//		baseCTS, err = getBaseCommitTS()
	//		baseEndTS = baseSp
	//		baseFromTS = baseCTS.Next()
	//	} else {
	//		worstRange()
	//	}
	//}
	//
	//if tarTableID == baseTableID {
	//	// same table ==> same branch TS ==> same from ts
	//	// data branch diff t1{sp1} against t1{sp2}
	//	// t1: -----from ts-------sp1-----------sp2----->
	//	//
	//	//diff t1.[fromTS, sp1] against t1.[fromTS, sp2]
	//	//
	//	// minEnd = mix(sp1,sp2)
	//	// ==> t1.[minEnd, sp1] against t1.[minEnd, sp2]
	//	minEnd := tarEndTS
	//	if minEnd.LT(&baseEndTS) {
	//		minEnd = baseEndTS
	//	}
	//	minEnd = minEnd.Next()
	//
	//	tarFromTS = minEnd
	//	baseFromTS = minEnd
	//}

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
	tables tablePair,
) (
	branchInfo branchMetaInfo,
	err error,
) {

	var (
		dag *databranchutils.DataBranchDAG

		tarTS  int64
		baseTS int64
		hasLca bool

		lcaTableID   uint64
		tarBranchTS  timestamp.Timestamp
		baseBranchTS timestamp.Timestamp
	)

	defer func() {
		branchInfo = branchMetaInfo{
			lcaTableId:   lcaTableID,
			tarBranchTS:  types.TimestampToTS(tarBranchTS),
			baseBranchTS: types.TimestampToTS(baseBranchTS),
		}
	}()

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
		tables.tarRel.GetTableID(ctx), tables.baseRel.GetTableID(ctx),
	); hasLca {
		if lcaTableID == tables.baseRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: tarTS}
			tarBranchTS = ts
			baseBranchTS = ts
		} else if lcaTableID == tables.tarRel.GetTableID(ctx) {
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
