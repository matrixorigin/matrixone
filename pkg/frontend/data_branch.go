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
	"strconv"

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
		tarRel  engine.Relation
		baseRel engine.Relation

		eng          engine.Engine
		tarSnapshot  *plan.Snapshot
		baseSnapshot *plan.Snapshot

		bh       BackgroundExec
		deferred func(error) error
	)

	// do not open another transaction,
	// if the clone already executed within a transaction.
	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return err
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

	eng = ses.proc.GetSessionInfo().StorageEngine
	if tarRel, baseRel, err = getRelations(
		execCtx.reqCtx, ses, eng, stmt.TargetTable, stmt.BaseTable, tarSnapshot, baseSnapshot,
	); err != nil {
		return
	}

	var (
		hasLca       bool
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

	if hasLca, tarBranchTS, baseBranchTS, tarHandle, baseHandle, err =
		constructChangeHandle(
			execCtx.reqCtx, ses, eng, tarRel, baseRel, tarSnapshot, baseSnapshot,
		); err != nil {
		return err
	}

	if err = diff(
		execCtx.reqCtx,
		ses,
		ses.proc.Mp(),
		tarTblDef,
		baseTblDef,
		tarHandle,
		baseHandle,
		stmt.DiffAsOpts,
		hasLca,
		tarBranchTS,
		baseBranchTS,
		bh,
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
	diffAsOpt *tree.DiffAsOpt,
	hasLca bool,
	tarBranchTS timestamp.Timestamp,
	baseBranchTS timestamp.Timestamp,
	bh BackgroundExec,
) (err error) {

	var (
		pkIdxes []int

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
	}()

	if baseTblDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		pkIdxes = append(pkIdxes, int(baseTblDef.Name2ColIndex[baseTblDef.Pkey.PkeyColName]))
	} else {
		for i, col := range baseTblDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName && col.Name != catalog.Row_ID {
				pkIdxes = append(pkIdxes, i)
			}
		}
	}

	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForBaseTable(
		ctx, mp, hasLca, pkIdxes, baseHandle,
	); err != nil {
		return
	}

	var (
		mrs      *MysqlResultSet
		tmpRows  [][]interface{}
		rows     [][]interface{}
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

		if !hasLca && tombstoneBat != nil {
			// if there has no LCA, the tombstones are not expected
			err = moerr.NewInternalErrorNoCtx("tombstone are not expected from target table with no LCA")
			return
		}

		if dataBat != nil {
			pkVecs = pkVecs[:0]
			for _, idx := range pkIdxes {
				pkVecs = append(pkVecs, dataBat.Vecs[idx])
			}
			if checkRet, err = baseDataHashmap.PopByVectors(pkVecs); err != nil {
				return
			}

			for i := range checkRet {
				// not exists in the base table
				if !checkRet[i].Exists {
					row := append([]interface{}{}, tarTblDef.Name, "+")
					for _, idx := range neededColIdxes {
						row = append(row, types.DecodeValue(dataBat.Vecs[idx].GetRawBytesAt(i), dataBat.Vecs[idx].GetType().Oid))
					}
					rows = append(rows, row)
				} else {
					// exists in the base table, do nothing
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
							ctx, bh, tarDelsOnLCA, true, tarTblDef, baseTblDef, tarBranchTS,
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
		row := append([]interface{}{}, tarTblDef.Name, "-")
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
				pkType := types.New(types.T(pkCol.Typ.Id), pkCol.Typ.Width, pkCol.Typ.Scale)
				baseDelsOnLCA = vector.NewVec(pkType)
			}

			if err = vector.AppendAny(baseDelsOnLCA, t[0], false, mp); err != nil {
				return err
			}

			if baseDelsOnLCA.Length() >= objectio.BlockMaxRows {
				if tmpRows, err = handleDelsOnLCA(
					ctx, bh, baseDelsOnLCA, false, tarTblDef, baseTblDef, baseBranchTS,
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
			ctx, bh, tarDelsOnLCA, true, tarTblDef, baseTblDef, tarBranchTS,
		); err != nil {
			return
		}
		rows = append(rows, tmpRows...)
	}

	if baseDelsOnLCA != nil && baseDelsOnLCA.Length() > 0 {
		if tmpRows, err = handleDelsOnLCA(
			ctx, bh, baseDelsOnLCA, false, tarTblDef, baseTblDef, baseBranchTS,
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

func handleDelsOnLCA(
	ctx context.Context,
	bh BackgroundExec,
	dels *vector.Vector,
	isTarDels bool,
	tarTblDef *plan.TableDef,
	baseTblDef *plan.TableDef,
	snapshot timestamp.Timestamp,
) (rows [][]interface{}, err error) {

	var (
		sql    string
		buf    bytes.Buffer
		tblDef = tarTblDef
		flag   = "-"
		mots   = fmt.Sprintf(" {MO_TS=%d} ", snapshot.PhysicalTime)
	)

	if !isTarDels {
		flag = "+"
		tblDef = baseTblDef
	}

	// composite pk
	if baseTblDef.Pkey.CompPkeyCol != nil {
		var (
			pks     types.Tuple
			pkNames = tblDef.Pkey.Names
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
			tblDef.DbName, tblDef.Name, mots, buf.String(),
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
			tblDef.DbName, tblDef.Name, mots, buf.String(),
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
			tblDef.DbName, tblDef.Name, mots, tblDef.Pkey.PkeyColName, buf.String(),
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
			row := append([]interface{}{}, tarTblDef.Name, flag)
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
	hasLca bool,
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

		if !hasLca && tombstoneBat != nil {
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

	return
}

func getRelations(
	ctx context.Context,
	ses *Session,
	eng engine.Engine,
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

	if tarDB, err = eng.Database(
		ctx, tarDbName, tarTxnOp,
	); err != nil {
		return
	}

	if tarRel, err = tarDB.Relation(ctx, tarTblName, nil); err != nil {
		return
	}

	if baseDB, err = eng.Database(
		ctx, baseDbName, baseTxnOp,
	); err != nil {
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
	eng engine.Engine,
	tarRel engine.Relation,
	baseRel engine.Relation,
	tarSnapshot *plan.Snapshot,
	baseSnapshot *plan.Snapshot,
) (
	hasLca bool,
	tarBranchTS timestamp.Timestamp,
	baseBranchTS timestamp.Timestamp,
	tarHandle engine.ChangesHandle,
	baseHandle engine.ChangesHandle,
	err error,
) {

	var (
		tarIsClonedTable  bool
		baseIsClonedTable bool

		tarToTS  timestamp.Timestamp
		baseToTS timestamp.Timestamp

		tarFromTS   timestamp.Timestamp
		baseFromTS  timestamp.Timestamp
		txnSnapshot timestamp.Timestamp

		tarScanSnapshot  timestamp.Timestamp
		baseScanSnapshot timestamp.Timestamp
	)

	txnOp := ses.GetTxnHandler().GetTxn()
	txnSnapshot = txnOp.SnapshotTS()
	tarToTS = txnSnapshot
	baseToTS = txnSnapshot

	if hasLca, tarFromTS, baseFromTS, tarIsClonedTable, baseIsClonedTable, err = decideFromTS(
		ctx, ses, tarRel, baseRel,
	); err != nil {
		return
	}

	if hasLca {
		tarBranchTS = tarFromTS.Prev()
		baseBranchTS = baseFromTS.Prev()
	}

	tarScanSnapshot = txnSnapshot
	if tarSnapshot != nil && tarSnapshot.TS != nil {
		tarScanSnapshot = *tarSnapshot.TS
	}

	if tarHandle, err = databranchutils.CollectChanges(
		ctx,
		eng,
		ses.GetAccountId(),
		tarScanSnapshot,
		txnOp,
		tarRel,
		types.TimestampToTS(tarFromTS),
		types.TimestampToTS(tarToTS),
		ses.proc.Mp(),
		tarIsClonedTable,
	); err != nil {
		return
	}

	baseScanSnapshot = txnSnapshot
	if baseSnapshot != nil && baseSnapshot.TS != nil {
		baseScanSnapshot = *baseSnapshot.TS
	}

	if baseHandle, err = databranchutils.CollectChanges(
		ctx,
		eng,
		ses.GetAccountId(),
		baseScanSnapshot,
		txnOp,
		baseRel,
		types.TimestampToTS(baseFromTS),
		types.TimestampToTS(baseToTS),
		ses.proc.Mp(),
		baseIsClonedTable,
	); err != nil {
		return
	}

	return
}

func decideFromTS(
	ctx context.Context,
	ses *Session,
	tarRel engine.Relation,
	baseRel engine.Relation,
) (
	hasLca bool,
	tarFromTS timestamp.Timestamp,
	baseFromTS timestamp.Timestamp,
	tarIsClonedTable bool,
	baseIsClonedTable bool,
	err error,
) {

	var (
		dag *databranchutils.DataBranchDAG

		lcaTableID   uint64
		tarBranchTS  int64
		baseBranchTS int64
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
	if lcaTableID, tarBranchTS, baseBranchTS, hasLca = dag.FindLCA(
		tarRel.GetTableID(ctx), baseRel.GetTableID(ctx),
	); hasLca {
		if lcaTableID == baseRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: tarBranchTS}.Next()
			tarFromTS = ts
			baseFromTS = ts
			tarIsClonedTable = true
			baseIsClonedTable = dag.HasParent(baseRel.GetTableID(ctx))
		} else if lcaTableID == tarRel.GetTableID(ctx) {
			ts := timestamp.Timestamp{PhysicalTime: baseBranchTS}.Next()
			tarFromTS = ts
			baseFromTS = ts
			baseIsClonedTable = true
			tarIsClonedTable = dag.HasParent(tarRel.GetTableID(ctx))
		} else {
			tarIsClonedTable = true
			baseIsClonedTable = true
			tarFromTS = timestamp.Timestamp{PhysicalTime: tarBranchTS}.Next()
			baseFromTS = timestamp.Timestamp{PhysicalTime: baseBranchTS}.Next()
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
