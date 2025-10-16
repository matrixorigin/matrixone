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
	addedLine   = "+"
	removedLine = "-"
)

type diffReceipt struct {
	rows       [][]any
	pkKind     int
	pkColIdxes []int
	pkTypes    []types.Type
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
	case *tree.SnapshotDiff:
		return handleSnapshotDiff(execCtx, ses, st, nil)
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
	receipt *diffReceipt,
) (err error) {

	var (
		tarRel  engine.Relation
		baseRel engine.Relation

		tarSnapshot  *plan.Snapshot
		baseSnapshot *plan.Snapshot

		bh       BackgroundExec
		deferred func(error) error
	)

	// do not open another transaction,
	// if the clone already executed within a transaction.
	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}

	defer func() {
		if deferred != nil {
			//if r := recover(); r != nil {
			//	err = moerr.ConvertPanicError(reqCtx, r)
			//}
			err = deferred(err)
		}
	}()

	if tarSnapshot, err = resolveSnapshot(ses, stmt.TargetTable.AtTsExpr); err != nil {
		return
	}

	if baseSnapshot, err = resolveSnapshot(ses, stmt.BaseTable.AtTsExpr); err != nil {
		return
	}

	if tarRel, baseRel, err = getRelations(
		execCtx.reqCtx, ses, stmt.TargetTable, stmt.BaseTable, tarSnapshot, baseSnapshot,
	); err != nil {
		return
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
		execCtx.reqCtx,
		ses,
		tarTblDef,
		baseTblDef,
		tarHandle,
		baseHandle,
		stmt.DiffAsOpts,
		lcaTableID,
		tarBranchTS,
		baseBranchTS,
		bh,
		receipt,
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

	var (
		conflictOpt int
		diffStmt    = &tree.SnapshotDiff{
			TargetTable: stmt.SrcTable,
			BaseTable:   stmt.DstTable,
		}

		receipt diffReceipt
	)

	if stmt.ConflictOpt == nil {
		conflictOpt = tree.CONFLICT_FAIL
	} else {
		conflictOpt = stmt.ConflictOpt.Opt
	}

	if err = handleSnapshotDiff(execCtx, ses, diffStmt, &receipt); err != nil {
		return
	}

	if len(receipt.rows) != 0 {
		slices.SortFunc(receipt.rows, func(a, b []any) int {
			//cmp := 0
			for i := 2; i < len(a); i++ {
				// this should be much faster
				//switch receipt.pkTypes[i].Oid {
				//case types.T_uint8:
				//	cmp = int(a[i].(uint8) - b[i].(uint8))
				//case types.T_int8:
				//}
				left := types.EncodeValue(a[i], receipt.pkTypes[i].Oid)
				right := types.EncodeValue(b[i], receipt.pkTypes[i].Oid)
				if cmp := bytes.Compare(left, right); cmp == 0 {
					continue
				} else {
					return cmp
				}
			}

			left := a[1].(string)
			right := b[1].(string)
			return strings.Compare(left, right)
		})
	}

	for _, _ = range receipt.rows {
		//flag := row[1].(string)
		//switch conflictOpt {
		//case tree.CONFLICT_FAIL:
		//case tree.CONFLICT_SKIP:
		//case tree.CONFLICT_ACCEPT:
		//default:
		//	return moerr.NewInternalErrorNoCtxf("got wrong conflict opt: %v", conflictOpt)
		//}
		switch conflictOpt {
		}
	}

	// diff t2 against t1
	// t2 -:
	//	i. 	t2 not change, t1 insert new
	//	ii. t2 delete one, t1 not change
	//
	// t2 +:
	//	i.	t2 not change, t1 delete one
	//	ii.	t2 insert one, t1 not change

	return nil
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
	receipt *diffReceipt,
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

		if receipt != nil && err == nil {
			receipt.rows = rows
			receipt.pkKind = pkKind
			receipt.pkTypes = pkTypes
			receipt.pkColIdxes = pkColIdxes
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

	mrs, neededColIdxes = buildShowDiffSchema(ses, tarTblDef, baseTblDef)

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
					row := append([]any{}, tarTblDef.Name, addedLine)
					for _, idx := range neededColIdxes {
						row = append(row, types.DecodeValue(dataBat.Vecs[idx].GetRawBytesAt(i), dataBat.Vecs[idx].GetType().Oid))
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
						)

						if tuple, err = baseDataHashmap.DecodeRow(checkRet[i].Rows[0]); err != nil {
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
							row1 := append([]any{}, tarTblDef.Name, removedLine)
							row2 := append([]any{}, tarTblDef.Name, addedLine)
							for _, idx := range neededColIdxes {
								row1 = append(row1, tuple[idx])
								row2 = append(row2, types.DecodeValue(dataBat.Vecs[idx].GetRawBytesAt(i), dataBat.Vecs[idx].GetType().Oid))
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
		row := append([]interface{}{}, tarTblDef.Name, removedLine)
		for _, r := range data {
			if t, err := baseDataHashmap.DecodeRow(r); err != nil {
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

	// iterate the left base table tombstones on the LCA
	if err = baseTombstoneHashmap.ForEach(func(key []byte, _ [][]byte) error {
		if t, err := baseTombstoneHashmap.DecodeRow(key); err != nil {
			return err
		} else {
			if baseDelsOnLCA == nil {
				pkCol := baseTblDef.Cols[baseTblDef.Name2ColIndex[baseTblDef.Pkey.PkeyColName]]
				pkTyp := types.New(types.T(pkCol.Typ.Id), pkCol.Typ.Width, pkCol.Typ.Scale)
				baseDelsOnLCA = vector.NewVec(pkTyp)
			}

			if err = vector.AppendAny(baseDelsOnLCA, t[0], false, mp); err != nil {
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
		flag      = removedLine
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
		flag = addedLine
	}

	// composite pk
	if baseTblDef.Pkey.CompPkeyCol != nil {
		var (
			pks     types.Tuple
			pkNames = lcaTblDef.Pkey.Names
		)

		cols, area := vector.MustVarlenaRawData(dels)
		for i := range cols {
			b := cols[i].GetByteSlice(area)
			if pks, err = types.Unpack(b); err != nil {
				return nil, err
			}

			buf.WriteString("(")

			for j, pk := range pks {
				buf.WriteString(pkNames[j])
				buf.WriteString(" = ")

				switch pk.(type) {
				case string:
					buf.WriteString("'")
					buf.WriteString(pk.(string))
					buf.WriteString("'")
				case float32:
					buf.WriteString(strconv.FormatFloat(pk.(float64), 'f', -1, 32))
				case float64:
					buf.WriteString(strconv.FormatFloat(pk.(float64), 'f', -1, 64))
				case bool:
					buf.WriteString(strconv.FormatBool(pk.(bool)))
				case uint8:
					buf.WriteString(strconv.FormatUint(uint64(pk.(uint8)), 10))
				case int8:
					buf.WriteString(strconv.FormatInt(int64(pk.(int8)), 10))
				case uint16:
					buf.WriteString(strconv.FormatUint(uint64(pk.(uint16)), 10))
				case int16:
					buf.WriteString(strconv.FormatInt(int64(pk.(int16)), 10))
				case uint32:
					buf.WriteString(strconv.FormatUint(uint64(pk.(uint32)), 10))
				case int32:
					buf.WriteString(strconv.FormatInt(int64(pk.(int32)), 10))
				case uint64:
					buf.WriteString(strconv.FormatUint(pk.(uint64), 10))
				case int64:
					buf.WriteString(strconv.FormatInt(pk.(int64), 10))
				default:
					return nil, fmt.Errorf("unknown pk type: %T", pk)
				}

				if j != len(pks)-1 {
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
		//switch dels.GetType().Oid {
		//case types.T_bool:
		//	pks := vector.MustFixedColNoTypeCheck[bool](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatBool(pk))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_uint8:
		//	pks := vector.MustFixedColNoTypeCheck[uint8](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_int8:
		//	pks := vector.MustFixedColNoTypeCheck[int8](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_uint16:
		//	pks := vector.MustFixedColNoTypeCheck[uint16](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_int16:
		//	pks := vector.MustFixedColNoTypeCheck[int16](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_uint32:
		//	pks := vector.MustFixedColNoTypeCheck[uint32](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_int32:
		//	pks := vector.MustFixedColNoTypeCheck[int32](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_uint64:
		//	pks := vector.MustFixedColNoTypeCheck[uint64](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(pk, 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_int64:
		//	pks := vector.MustFixedColNoTypeCheck[int64](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatUint(uint64(pk), 10))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_float32:
		//	pks := vector.MustFixedColNoTypeCheck[float32](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatFloat(float64(pk), 'f', -1, 32))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_float64:
		//	pks := vector.MustFixedColNoTypeCheck[float64](dels)
		//	for i, pk := range pks {
		//		buf.WriteString(strconv.FormatFloat(pk, 'f', -1, 64))
		//		if i != len(pks)-1 {
		//			buf.WriteString(",")
		//		}
		//	}
		//case types.T_char, types.T_varchar, types.T_json, types.T_text:
		//	var (
		//		val string
		//	)
		//
		//	cols, area := vector.MustVarlenaRawData(dels)
		//	for i := range cols {
		//		b := cols[i].GetByteSlice(area)
		//		types.DecodeValue(b, dels.GetType().Oid)
		//	}
		//}

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
	ses *Session,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
) (mrs *MysqlResultSet, neededColIdxes []int) {

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
		neededColIdxes = append(neededColIdxes, i)
	}

	mrs = ses.GetMysqlResultSet()
	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	return mrs, neededColIdxes
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
	tarTable tree.TableName,
	baseTable tree.TableName,
	tarSnapshot *plan.Snapshot,
	baseSnapshot *plan.Snapshot,
) (
	tarRel engine.Relation,
	baseRel engine.Relation,
	err error,
) {

	var (
		tarDB  engine.Database
		baseDB engine.Database

		tarDbName   string
		tarTblName  string
		baseDbName  string
		baseTblName string
	)

	tarTxnOp := ses.GetTxnHandler().GetTxn()
	baseTxnOp := ses.GetTxnHandler().GetTxn()

	if tarSnapshot != nil && tarSnapshot.TS != nil {
		tarTxnOp = tarTxnOp.CloneSnapshotOp(*tarSnapshot.TS)
	}

	if baseSnapshot != nil && baseSnapshot.TS != nil {
		baseTxnOp = baseTxnOp.CloneSnapshotOp(*baseSnapshot.TS)
	}

	baseDbName = baseTable.SchemaName.String()
	baseTblName = baseTable.ObjectName.String()
	if len(baseDbName) == 0 {
		baseDbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	tarDbName = tarTable.SchemaName.String()
	tarTblName = tarTable.ObjectName.String()
	if len(tarDbName) == 0 {
		tarDbName = ses.GetTxnCompileCtx().DefaultDatabase()
	}

	if len(baseDbName) == 0 || len(tarDbName) == 0 {
		err = moerr.NewInternalErrorNoCtxf("the base or target database cannot be empty.")
		return
	}

	eng := ses.proc.GetSessionInfo().StorageEngine
	if tarDB, err = eng.Database(ctx, tarDbName, tarTxnOp); err != nil {
		return
	}

	if tarRel, err = tarDB.Relation(ctx, tarTblName, nil); err != nil {
		return
	}

	if baseDB, err = eng.Database(ctx, baseDbName, baseTxnOp); err != nil {
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

		tarEndTS = tarSp
		tarFromTS = tarCTS.Next()
		baseEndTS = tarCTS.Next()
		baseFromTS = baseSp
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
			baseBranchTS = ts
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
