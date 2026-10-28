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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

type lcaProbeLayout struct {
	attrs       []string
	types       []types.Type
	targetIdxes []int
	enumValues  []string
}

func sortDataBranchBatchByPrimaryKey(bat *batch.Batch, pkColIdx int, mp *mpool.MPool) error {
	pkVec := bat.Vecs[pkColIdx]
	if !isDataBranchFloatType(*pkVec.GetType()) {
		return mergeutil.SortColumnsByIndex(bat.Vecs, pkColIdx, mp)
	}

	identityVec := vector.NewVec(types.T_uint64.ToType())
	defer identityVec.Free(mp)
	for row := range pkVec.Length() {
		identity, isNull, err := dataBranchFloatPKIdentityAt(pkVec, row)
		if err != nil {
			return err
		}
		if err = vector.AppendFixed(identityVec, identity, isNull, mp); err != nil {
			return err
		}
	}

	cols := make([]*vector.Vector, 0, len(bat.Vecs)+1)
	cols = append(cols, bat.Vecs...)
	cols = append(cols, identityVec)
	return mergeutil.SortColumnsByIndex(cols, len(cols)-1, mp)
}

func (layout lcaProbeLayout) columnNameForTargetIndex(targetIdx int) (string, bool) {
	for i, candidateIdx := range layout.targetIdxes {
		if candidateIdx == targetIdx {
			return layout.attrs[i], true
		}
	}
	return "", false
}

func (layout lcaProbeLayout) columnNamesForTargetIndexes(targetIdxes []int) ([]string, error) {
	names := make([]string, len(targetIdxes))
	for i, targetIdx := range targetIdxes {
		name, ok := layout.columnNameForTargetIndex(targetIdx)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf(
				"data branch: target column index %d is absent from LCA probe layout",
				targetIdx,
			)
		}
		names[i] = name
	}
	return names, nil
}

func lcaProbeResultTargetIndexes(
	layout lcaProbeLayout,
	targetColumnCount int,
	resultColumnCount int,
	fullTargetLayout bool,
) ([]int, error) {
	if fullTargetLayout {
		if resultColumnCount != targetColumnCount {
			return nil, moerr.NewInternalErrorNoCtxf(
				"unexpected LCA probe result width %d for full target layout with %d columns",
				resultColumnCount, targetColumnCount,
			)
		}
		idxes := make([]int, targetColumnCount)
		for i := range idxes {
			idxes[i] = i
		}
		return idxes, nil
	}
	if resultColumnCount != len(layout.targetIdxes) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected LCA probe result width %d for projected layout with %d columns",
			resultColumnCount, len(layout.targetIdxes),
		)
	}
	return append([]int(nil), layout.targetIdxes...), nil
}

// lcaProbeColumnLayout selects compatible common columns from the LCA and
// target layouts. The caller reconstructs target-only columns as NULL because
// they are excluded from endpoint comparison and MERGE apply even when an
// ancestor generation still contains an older representation of the column.
func lcaProbeColumnLayout(
	lcaDef *plan2.TableDef,
	targetDef *plan2.TableDef,
	targetColNames []string,
	targetColTypes []types.Type,
	targetOnlyIdxes []int,
	lcaColNames []string,
) (lcaProbeLayout, error) {
	targetOnly := make(map[int]struct{}, len(targetOnlyIdxes))
	for _, idx := range targetOnlyIdxes {
		targetOnly[idx] = struct{}{}
	}
	if len(lcaDef.Cols) == 0 {
		layout := lcaProbeLayout{}
		for i, name := range targetColNames {
			if _, ok := targetOnly[i]; ok {
				continue
			}
			layout.attrs = append(layout.attrs, name)
			layout.types = append(layout.types, targetColTypes[i])
			layout.targetIdxes = append(layout.targetIdxes, i)
			layout.enumValues = append(layout.enumValues, "")
		}
		return layout, nil
	}
	if targetDef == nil {
		return lcaProbeLayout{}, moerr.NewInternalErrorNoCtx("DATA BRANCH target table definition is unavailable")
	}

	layout := lcaProbeLayout{}
	for targetIdx, name := range targetColNames {
		if _, ok := targetOnly[targetIdx]; ok {
			continue
		}
		targetCol := dataBranchColumnDefByName(targetDef, name)
		if targetCol == nil {
			return lcaProbeLayout{}, moerr.NewInternalErrorNoCtxf(
				"DATA BRANCH target column %q is unavailable", name,
			)
		}
		var (
			lcaCol      *plan2.ColDef
			lcaAttrName string
		)
		if targetIdx < len(lcaColNames) && lcaColNames[targetIdx] != "" {
			lcaAttrName = lcaColNames[targetIdx]
			lcaCol = dataBranchColumnDefByName(lcaDef, lcaAttrName)
			if lcaCol == nil {
				// The LCA relation may also be a current endpoint whose visible
				// name changed after the LCA snapshot. Keep the validated
				// historical name for time-travel SQL while taking compatible
				// type metadata from the same stable endpoint identity.
				lcaCol = dataBranchEndpointColumnDef(lcaDef, targetCol)
			}
			if lcaCol == nil {
				return lcaProbeLayout{}, moerr.NewInternalErrorNoCtxf(
					"LCA data branch column %q cannot be resolved at the current endpoint",
					lcaAttrName,
				)
			}
		}
		if lcaCol == nil {
			lcaCol = dataBranchColumnDefByLogicalName(lcaDef, targetCol)
		}
		candidate := dataBranchColumnDefByName(lcaDef, targetCol.Name)
		if candidate != nil &&
			isDataBranchDerivedCompositePKColumn(lcaDef, targetDef, candidate, targetCol) {
			// A rebuilt composite-key column can reuse the former RowID's
			// numeric ColId/Seqnum pair. Prefer its canonical derived identity
			// over that accidental positional collision.
			lcaCol = candidate
		}
		if lcaCol == nil {
			continue
		}
		if lcaAttrName == "" {
			lcaAttrName = lcaCol.Name
		}
		derivedCompositePK := isDataBranchDerivedCompositePKColumn(
			lcaDef, targetDef, lcaCol, targetCol,
		)
		if lcaCol.Name == catalog.Row_ID ||
			(!isDataBranchLogicalTypeEquivalent(lcaCol.Typ, targetCol.Typ) &&
				!(derivedCompositePK && lcaCol.Typ.Id == targetCol.Typ.Id)) {
			return lcaProbeLayout{}, moerr.NewNotSupportedNoCtxf(
				"LCA data branch column %s has a different type from the endpoint schema",
				targetCol.Name,
			)
		}
		layout.attrs = append(layout.attrs, lcaAttrName)
		layout.types = append(layout.types, types.MustTypeFromPlan(lcaCol.Typ))
		layout.targetIdxes = append(layout.targetIdxes, targetIdx)
		layout.enumValues = append(layout.enumValues, lcaCol.Typ.Enumvalues)
	}
	return layout, nil
}

// should read the LCA table to get all column values.
func handleDelsOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshot timestamp.Timestamp,
) (dBat *batch.Batch, err error) {

	if snapshot.PhysicalTime == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("invalid branch ts: %s", snapshot.DebugString())
	}

	var (
		sqlRet           executor.Result
		fullTargetLayout bool

		lcaTblDef    = tblStuff.lcaRel.GetTableDef(ctx)
		targetTblDef = tblStuff.tarRel.GetTableDef(ctx)

		colTypes        = tblStuff.def.colTypes
		probePKColIdxes = tblStuff.def.pkColIdxes
		snapshotTS      = types.TimestampToTS(snapshot)
	)
	if tblStuff.def.pkKind == fakeKind {
		// pkColIdxes contains the visible columns that define a fake PK for
		// hash-diff matching. The SQL LCA probe joins on the generated hidden
		// PK instead, so its cast and equality type must come from that hidden
		// column rather than from the first visible column.
		probePKColIdxes = []int{tblStuff.def.pkColIdx}
	}
	lcaLayout, err := lcaProbeColumnLayout(
		lcaTblDef, targetTblDef, tblStuff.def.colNames, tblStuff.def.colTypes,
		tblStuff.def.tarOnlyIdxes, tblStuff.def.lcaColNames,
	)
	if err != nil {
		return nil, err
	}

	// The SQL probe resolves db.table by name under the statement account.
	// A lineage node owned by another account (design §6.4, e.g. reopened
	// under a subscription source owner) would resolve that name to a
	// same-named local decoy or fail outright, so it is always scanned by
	// id under its own owner identity via the reader fallback.
	lcaOwnerMismatch := tblStuff.resolvedLCAIdentity(ctx, ses).account != ses.GetAccountId()
	forceReaderProbe := lcaOwnerMismatch ||
		(tblStuff.lcaReaderProbeMode != nil && tblStuff.lcaReaderProbeMode.Load())
	if forceReaderProbe {
		sqlRet, err = runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
		if err != nil {
			return nil, err
		}
		fullTargetLayout = true
	} else {
		sqlBuf := acquireBuffer(tblStuff.bufPool)
		valsBuf := acquireBuffer(tblStuff.bufPool)
		defer func() {
			releaseBuffer(tblStuff.bufPool, sqlBuf)
			releaseBuffer(tblStuff.bufPool, valsBuf)
		}()

		// Keep SQL as the fast path because it is much cheaper than reader-based
		// probing. However, this path still relies on name-based time travelling
		// like db.table{MO_TS=...}. After GC, if no snapshot or PITR history was
		// created for the corresponding account/db/table, catalog lookup at that
		// timestamp may fail with unknown db/table even though the caller already
		// knows the stable table ID.
		mots := fmt.Sprintf("{MO_TS=%d} ", snapshot.PhysicalTime)
		var pkNames []string
		if tblStuff.def.pkKind == fakeKind {
			// The synthetic hash column is internal and cannot be renamed.
			pkNames = []string{catalog.FakePrimaryKeyColName}
		} else {
			pkNames, err = lcaLayout.columnNamesForTargetIndexes(probePKColIdxes)
			if err != nil {
				return nil, err
			}
		}
		quotedPKNames := make([]string, len(pkNames))
		quotedPKValueAliases := make([]string, len(pkNames))
		for i, name := range pkNames {
			quotedPKNames[i] = quoteIdentifierForSQL(name)
			quotedPKValueAliases[i] = quoteIdentifierForSQL(fmt.Sprintf("__mo_data_branch_pk_%d", i))
		}
		quotedOrdinalAlias := quoteIdentifierForSQL("__mo_data_branch_ordinal")

		// composite pk
		if tblStuff.def.pkKind == compositeKind {
			var tuple types.Tuple
			cols, area := vector.MustVarlenaRawData(tBat.Vecs[0])
			for i := range cols {
				b := cols[i].GetByteSlice(area)
				if tuple, err = types.Unpack(b); err != nil {
					return nil, err
				}

				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				for j := range tuple {
					if err = formatValIntoStringWithFloatCast(
						ses, tuple[j], colTypes[probePKColIdxes[j]], valsBuf, true,
					); err != nil {
						return nil, err
					}
					if j != len(tuple)-1 {
						valsBuf.WriteString(", ")
					}
				}
				valsBuf.WriteString(")")
				if i != len(cols)-1 {
					valsBuf.WriteString(", ")
				}
			}
		} else if tblStuff.def.pkKind == fakeKind {
			// fake pk
			pks := vector.MustFixedColNoTypeCheck[uint64](tBat.Vecs[0])
			for i := range pks {
				valsBuf.WriteString(fmt.Sprintf("row(%d,%d)", i, pks[i]))
				if i != len(pks)-1 {
					valsBuf.WriteString(", ")
				}
			}
		} else {
			// real pk
			valsBuf.Reset()
			pkType := colTypes[probePKColIdxes[0]]
			for i := range tBat.Vecs[0].Length() {
				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				b := tBat.Vecs[0].GetRawBytesAt(i)
				val := types.DecodeValue(b, tBat.Vecs[0].GetType().Oid)
				if err = formatValIntoStringWithFloatCast(ses, val, pkType, valsBuf, true); err != nil {
					return nil, err
				}
				valsBuf.WriteString(")")
				if i != tBat.Vecs[0].Length()-1 {
					valsBuf.WriteString(",")
				}
			}
		}

		selectCols := make([]string, len(lcaLayout.attrs)+1)
		selectCols[0] = fmt.Sprintf("pks.%s", quotedOrdinalAlias)
		for i, colName := range lcaLayout.attrs {
			colExpr := fmt.Sprintf("lca.%s", quoteIdentifierForSQL(colName))
			if lcaLayout.types[i].Oid == types.T_uint64 && lcaLayout.enumValues[i] != "" {
				// SET is stored as uint64 but ordinary SQL projection exposes its
				// display label. Request the physical bitmap explicitly so the
				// probe remains lossless even when the SET has an empty member.
				colExpr = fmt.Sprintf("cast(%s as unsigned)", colExpr)
			}
			selectCols[i+1] = colExpr
		}

		sqlBuf.Reset()
		sqlBuf.WriteString(fmt.Sprintf(
			"select %s from %s.%s%s as lca ",
			strings.Join(selectCols, ", "),
			quoteIdentifierForSQL(lcaTblDef.DbName),
			quoteIdentifierForSQL(lcaTblDef.Name),
			mots),
		)
		sqlBuf.WriteString(fmt.Sprintf(
			"right join (values %s) as pks(%s,%s) on ",
			valsBuf.String(), quotedOrdinalAlias, strings.Join(quotedPKValueAliases, ",")),
		)

		for i := range quotedPKNames {
			left := fmt.Sprintf("lca.%s", quotedPKNames[i])
			right := fmt.Sprintf("pks.%s", quotedPKValueAliases[i])
			if castType, ok := lcaProbeJoinCastType(colTypes[probePKColIdxes[i]]); ok {
				right = fmt.Sprintf("cast(%s as %s)", right, castType)
			}
			sqlBuf.WriteString(dataBranchSQLKeyEqual(
				left, right, colTypes[probePKColIdxes[i]],
			))
			if i != len(quotedPKNames)-1 {
				sqlBuf.WriteString(" AND ")
			}
		}
		sqlBuf.WriteString(fmt.Sprintf(" order by pks.%s", quotedOrdinalAlias))

		sqlPreview := sqlBuf.String()
		if len(sqlPreview) > 512 {
			sqlPreview = sqlPreview[:512]
		}
		logutil.Debug(
			"DataBranch-LCA-SQL-Start",
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("db-name", lcaTblDef.DbName),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", tBat.RowCount()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("sql-len", sqlBuf.Len()),
			zap.String("sql-preview", sqlPreview),
		)

		if sqlRet, err = runSql(ctx, ses, bh, sqlBuf.String(), nil, nil); err != nil {
			logutil.Error(
				"DataBranch-LCA-SQL-Error",
				zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
				zap.String("db-name", lcaTblDef.DbName),
				zap.String("table-name", lcaTblDef.Name),
				zap.Int("input-pk-rows", tBat.RowCount()),
				zap.String("snapshot-ts", snapshotTS.ToString()),
				zap.Error(err),
			)
			if shouldUseLCAReaderFallback(err) {
				if tblStuff.lcaReaderProbeMode != nil {
					tblStuff.lcaReaderProbeMode.Store(true)
				}
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Start",
					zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", tBat.RowCount()),
					zap.String("snapshot-ts", snapshotTS.ToString()),
					zap.Error(err),
				)
				sqlRet, err = runLCAProbeWithReaderFallback(ctx, ses, tBat, tblStuff, snapshotTS)
				if err != nil {
					logutil.Error(
						"DataBranch-LCA-SQL-Fallback-Error",
						zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
						zap.String("table-name", lcaTblDef.Name),
						zap.Int("input-pk-rows", tBat.RowCount()),
						zap.String("snapshot-ts", snapshotTS.ToString()),
						zap.Error(err),
					)
					return nil, err
				}
				fullTargetLayout = true
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Done",
					zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", tBat.RowCount()),
					zap.Int("fallback-batches", len(sqlRet.Batches)),
				)
			} else {
				return nil, err
			}
		}
	}
	defer sqlRet.Close()

	if forceReaderProbe {
		logutil.Debug(
			"DataBranch-LCA-ReaderProbe-Direct",
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", tBat.RowCount()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
		)
	}

	notExist := func(cols []*vector.Vector, r int) bool {
		exist := false
		for _, col := range cols {
			if !col.GetNulls().Contains(uint64(r)) {
				exist = true
				break
			}
		}
		return !exist
	}

	dBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
	defer func() {
		if err != nil && dBat != nil {
			tblStuff.retPool.releaseRetBatch(dBat, false)
			dBat = nil
		}
	}()

	sels := make([]int64, 0, 100)
	joinedRows := 0
	lcaHitRows := 0
	lcaMissRows := 0
	sqlRet.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
		if len(cols) == 0 {
			err = moerr.NewInternalErrorNoCtx("LCA probe returned a batch without index column")
			return false
		}
		var resultTargetIdxes []int
		if resultTargetIdxes, err = lcaProbeResultTargetIndexes(
			lcaLayout, len(tblStuff.def.colNames), len(cols)-1, fullTargetLayout,
		); err != nil {
			return false
		}
		joinedRows += rowCnt
		for i := range rowCnt {
			if notExist(cols[1:], i) {
				idx := vector.GetFixedAtNoTypeCheck[int64](cols[0], i)
				sels = append(sels, int64(idx))
				lcaMissRows++
				continue
			}

			for j := 1; j < len(cols); j++ {
				targetIdx := resultTargetIdxes[j-1]
				enumValues := ""
				if layoutIdx := slices.Index(lcaLayout.targetIdxes, targetIdx); layoutIdx >= 0 {
					enumValues = lcaLayout.enumValues[layoutIdx]
				}
				if err = appendLCAProbeValue(
					dBat.Vecs[targetIdx], cols[j], i,
					enumValues, ses.proc.Mp(),
				); err != nil {
					return false
				}
			}
			lcaHitRows++

		}

		return true
	})
	if err != nil {
		return nil, err
	}
	selectedTargetIdxes := make(map[int]struct{}, len(tblStuff.def.colNames))
	if fullTargetLayout {
		for targetIdx := range tblStuff.def.colNames {
			selectedTargetIdxes[targetIdx] = struct{}{}
		}
	} else {
		for _, targetIdx := range lcaLayout.targetIdxes {
			selectedTargetIdxes[targetIdx] = struct{}{}
		}
	}
	for targetIdx, typ := range tblStuff.def.colTypes {
		if _, selected := selectedTargetIdxes[targetIdx]; selected {
			continue
		}
		nullVec := vector.NewConstNull(typ, lcaHitRows, ses.proc.Mp())
		if err = dBat.Vecs[targetIdx].UnionBatch(nullVec, 0, lcaHitRows, nil, ses.proc.Mp()); err != nil {
			nullVec.Free(ses.proc.Mp())
			return nil, err
		}
		nullVec.Free(ses.proc.Mp())
	}
	dBat.SetRowCount(lcaHitRows)
	logutil.Debug(
		"DataBranch-LCA-Join-Result",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("joined-rows", joinedRows),
		zap.Int("lca-hit-rows", lcaHitRows),
		zap.Int("lca-miss-rows", lcaMissRows),
		zap.Int("delete-candidate-rows", len(sels)),
		zap.Int("output-dbat-rows", dBat.RowCount()),
	)

	if len(sels) == 0 {
		for i := range tBat.Vecs {
			tBat.Vecs[i].CleanOnlyData()
		}
		tBat.SetRowCount(0)
	} else {
		for i := range tBat.Vecs {
			tBat.Vecs[i].Shrink(sels, false)
		}
		tBat.SetRowCount(tBat.Vecs[0].Length())
	}
	logutil.Debug(
		"DataBranch-LCA-PostProcess-Done",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("remaining-tbat-rows", tBat.RowCount()),
		zap.Int("remaining-dbat-rows", dBat.RowCount()),
	)

	return
}

// appendLCAProbeValue appends a column returned by the LCA SQL probe to the
// corresponding physical table vector. A RIGHT JOIN exposes ENUM columns as
// VARCHAR labels, so they need to be restored to their physical enum codes
// before the batch enters the diff pipeline. SET columns are projected as
// unsigned bitmaps by the SQL probe and therefore arrive with their physical
// type. Other columns must retain their physical type; copying mismatched vector
// storage would silently corrupt data.
func appendLCAProbeValue(
	dst, src *vector.Vector,
	row int,
	enumValues string,
	mp *mpool.MPool,
) error {
	if dst.GetType().Oid == src.GetType().Oid {
		return dst.UnionOne(src, int64(row), mp)
	}

	if dst.GetType().Oid != types.T_enum ||
		(src.GetType().Oid != types.T_varchar && src.GetType().Oid != types.T_char && src.GetType().Oid != types.T_text) {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected LCA probe type conversion: source=%s target=%s",
			src.GetType().String(), dst.GetType().String(),
		)
	}

	if src.IsConstNull() || src.GetNulls().Contains(uint64(row)) {
		return vector.AppendNull(dst, mp)
	}
	if src.IsConst() {
		row = 0
	}

	val, err := types.ParseEnum(enumValues, src.UnsafeGetStringAt(row))
	if err != nil {
		return err
	}
	return vector.AppendFixed(dst, val, false, mp)
}

func lcaProbeJoinCastType(typ types.Type) (string, bool) {
	switch typ.Oid {
	case types.T_bit:
		return typ.DescString(), true
	case types.T_int8, types.T_int16, types.T_int32:
		return "INT", true
	case types.T_int64:
		return "BIGINT", true
	case types.T_uint8, types.T_uint16, types.T_uint32:
		return "INT UNSIGNED", true
	case types.T_uint64:
		return "BIGINT UNSIGNED", true
	case types.T_float32:
		return "FLOAT", true
	case types.T_float64:
		return "DOUBLE", true
	case types.T_char, types.T_varchar, types.T_text:
		return "VARCHAR", true
	case types.T_binary, types.T_varbinary:
		return "VARBINARY", true
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return typ.DescString(), true
	case types.T_date, types.T_year:
		return typ.String(), true
	case types.T_datetime, types.T_time, types.T_timestamp:
		return fmt.Sprintf("%s(%d)", typ.String(), typ.Scale), true
	default:
		return "", false
	}
}

// runLCAProbeWithReaderFallback reconstructs the same row shape as
// "select pks.__idx_, lca.* ... right join(values ...)" using snapshot readers.
// It returns an executor.Result so handleDelsOnLCA can reuse the same post-SQL
// processing logic regardless of whether SQL or reader fallback produced rows.
func runLCAProbeWithReaderFallback(
	ctx context.Context,
	ses *Session,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshotTS types.TS,
) (sqlRet executor.Result, err error) {
	start := time.Now()
	sqlRet.Mp = ses.proc.Mp()
	if tBat == nil {
		return sqlRet, nil
	}
	rowCount := tBat.RowCount()
	if rowCount == 0 {
		bat := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for i, typ := range tblStuff.def.colTypes {
			bat.Vecs[i+1] = vector.NewVec(typ)
		}
		bat.SetRowCount(0)
		sqlRet.Batches = []*batch.Batch{bat}
		return sqlRet, nil
	}

	mp := ses.proc.Mp()
	pkVec := tBat.Vecs[0]
	var (
		prepareCost        time.Duration
		scanCost           time.Duration
		outputCost         time.Duration
		callbackCost       time.Duration
		callbackFilterCost time.Duration
		callbackUnionCost  time.Duration
	)
	inputKeys := make(map[string]struct{}, rowCount)
	for i := 0; i < rowCount; i++ {
		inputKeys[string(pkVec.GetRawBytesAt(i))] = struct{}{}
	}
	lcaTblDef := tblStuff.lcaRel.GetTableDef(ctx)
	targetTblDef := tblStuff.tarRel.GetTableDef(ctx)
	lcaLayout, err := lcaProbeColumnLayout(
		lcaTblDef, targetTblDef, tblStuff.def.colNames, tblStuff.def.colTypes,
		tblStuff.def.tarOnlyIdxes, tblStuff.def.lcaColNames,
	)
	if err != nil {
		return executor.Result{}, err
	}
	lcaPKName, ok := lcaLayout.columnNameForTargetIndex(tblStuff.def.pkColIdx)
	if !ok {
		return executor.Result{}, moerr.NewInternalErrorNoCtxf(
			"data branch: target primary key column index %d is absent from LCA probe layout",
			tblStuff.def.pkColIdx,
		)
	}
	lcaPKIdx := slices.Index(lcaLayout.attrs, lcaPKName)
	// Build a sorted IN vector for reader-side PK filtering.
	// The sorted-search path uses binary search over the IN value array and
	// assumes the array is ordered; an unsorted IN vector can drop valid hits.
	filterVec := vector.NewVec(*pkVec.GetType())
	defer filterVec.Free(mp)
	if err = filterVec.UnionBatch(pkVec, 0, rowCount, nil, mp); err != nil {
		return executor.Result{}, err
	}
	filterVec.InplaceSort()
	pkFilterExpr, err := readutil.ConstructInExpr(ctx, lcaPKName, filterVec)
	if err != nil {
		return executor.Result{}, err
	}
	prepareCost = time.Since(start)

	tmp := batch.NewWithSize(len(lcaLayout.attrs))
	for i, typ := range lcaLayout.types {
		tmp.Vecs[i] = vector.NewVec(typ)
	}
	defer tmp.Clean(mp)
	tmpRowByKey := make(map[string]int, rowCount)
	var callbackMu sync.Mutex
	var (
		scannedBatchCnt int
		scannedRowCnt   int
		acceptedRowCnt  int
		duplicateRows   int
	)
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-ScanStart",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("input-pk-rows", rowCount),
		zap.Int("input-unique-keys", len(inputKeys)),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	defer func() {
		fields := []zap.Field{
			zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", rowCount),
			zap.Int("input-unique-keys", len(inputKeys)),
			zap.Int("scan-batch-cnt", scannedBatchCnt),
			zap.Int("scan-row-cnt", scannedRowCnt),
			zap.Int("accepted-row-cnt", acceptedRowCnt),
			zap.Int("dedup-row-cnt", duplicateRows),
			zap.Int("accepted-key-cnt", len(tmpRowByKey)),
			zap.Duration("prepare-cost", prepareCost),
			zap.Duration("scan-cost", scanCost),
			zap.Duration("callback-cost", callbackCost),
			zap.Duration("callback-filter-cost", callbackFilterCost),
			zap.Duration("callback-union-cost", callbackUnionCost),
			zap.Duration("output-cost", outputCost),
			zap.Duration("total-cost", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-LCA-ReaderFallback-Error", fields...)
			return
		}
		logutil.Info("DataBranch-LCA-ReaderFallback-Done", fields...)
	}()

	scanStart := time.Now()
	// Scan under the lineage node's owner identity; the statement context
	// alone would route the by-id catalog read to the wrong account when the
	// node lives under another owner (design §6.4). Derive the scan context
	// from the *current* caller context: the descriptor's captured resolution
	// context predates the enclosing probe's child cancellation, so attaching
	// the owner account to it would make cancellation and deadline lossy for
	// every in-flight catalog and storage read here (identityCtx keeps the
	// live context when accounts already match).
	scanCtx := identityCtx(ctx, tblStuff.resolvedLCAIdentity(ctx, ses))
	err = scanSnapshotRelationByIDWithFallback(
		scanCtx,
		"lca-reader-fallback",
		ses,
		tblStuff.lcaRel.GetTableID(ctx),
		snapshotTS,
		tblStuff.lcaRel,
		lcaLayout.attrs,
		lcaLayout.types,
		pkFilterExpr,
		0,
		func(readBatch *batch.Batch) error {
			callbackStart := time.Now()
			callbackMu.Lock()
			defer func() {
				callbackCost += time.Since(callbackStart)
				callbackMu.Unlock()
			}()
			scannedBatchCnt++
			scannedRowCnt += readBatch.RowCount()
			readPK := readBatch.Vecs[lcaPKIdx]
			sels := make([]int64, 0, readBatch.RowCount())
			keys := make([]string, 0, readBatch.RowCount())
			filterStart := time.Now()
			for row := 0; row < readBatch.RowCount(); row++ {
				key := string(readPK.GetRawBytesAt(row))
				if _, ok := inputKeys[key]; !ok {
					continue
				}
				if _, exists := tmpRowByKey[key]; exists {
					duplicateRows++
					continue
				}
				sels = append(sels, int64(row))
				keys = append(keys, key)
			}
			callbackFilterCost += time.Since(filterStart)
			if len(sels) == 0 {
				return nil
			}
			base := tmp.Vecs[0].Length()
			unionStart := time.Now()
			for colIdx := range lcaLayout.attrs {
				if err = tmp.Vecs[colIdx].Union(readBatch.Vecs[colIdx], sels, mp); err != nil {
					return err
				}
			}
			for i, key := range keys {
				tmpRowByKey[key] = base + i
			}
			acceptedRowCnt += len(sels)
			callbackUnionCost += time.Since(unionStart)
			return nil
		},
	)
	scanCost = time.Since(scanStart)
	if err != nil {
		return executor.Result{}, err
	}
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-ScanDone",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("scan-batch-cnt", scannedBatchCnt),
		zap.Int("scan-row-cnt", scannedRowCnt),
		zap.Int("accepted-row-cnt", acceptedRowCnt),
		zap.Int("dedup-row-cnt", duplicateRows),
		zap.Int("accepted-key-cnt", len(tmpRowByKey)),
	)

	out := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for i, typ := range tblStuff.def.colTypes {
		out.Vecs[i+1] = vector.NewVec(typ)
	}
	defer func() {
		if err != nil {
			out.Clean(mp)
		}
	}()

	outputStart := time.Now()
	orderedIdx := make([]int64, rowCount)
	sels := make([]int64, rowCount)
	missedRows := make([]uint64, 0)
	hasAnyHit := false
	for i := 0; i < rowCount; i++ {
		orderedIdx[i] = int64(i)
		if rowIdx, ok := tmpRowByKey[string(pkVec.GetRawBytesAt(i))]; ok {
			sels[i] = int64(rowIdx)
			hasAnyHit = true
		} else {
			missedRows = append(missedRows, uint64(i))
		}
	}

	if err = vector.AppendFixedList(out.Vecs[0], orderedIdx, nil, mp); err != nil {
		return executor.Result{}, err
	}

	sourceIdxByTarget := make([]int, len(tblStuff.def.colNames))
	for i := range sourceIdxByTarget {
		sourceIdxByTarget[i] = -1
	}
	for sourceIdx, targetIdx := range lcaLayout.targetIdxes {
		sourceIdxByTarget[targetIdx] = sourceIdx
	}
	if !hasAnyHit {
		for colIdx := range tblStuff.def.colNames {
			nullConst := vector.NewConstNull(tblStuff.def.colTypes[colIdx], rowCount, mp)
			err = out.Vecs[colIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
			nullConst.Free(mp)
			if err != nil {
				return executor.Result{}, err
			}
		}
	} else if len(missedRows) == 0 {
		for targetIdx, sourceIdx := range sourceIdxByTarget {
			if sourceIdx < 0 {
				nullConst := vector.NewConstNull(tblStuff.def.colTypes[targetIdx], rowCount, mp)
				err = out.Vecs[targetIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
				nullConst.Free(mp)
				if err != nil {
					return executor.Result{}, err
				}
				continue
			}
			if err = out.Vecs[targetIdx+1].UnionBatch(tmp.Vecs[sourceIdx], 0, rowCount, nil, mp); err != nil {
				return executor.Result{}, err
			}
		}
	} else {
		for targetIdx, sourceIdx := range sourceIdxByTarget {
			if sourceIdx < 0 {
				nullConst := vector.NewConstNull(tblStuff.def.colTypes[targetIdx], rowCount, mp)
				err = out.Vecs[targetIdx+1].UnionBatch(nullConst, 0, rowCount, nil, mp)
				nullConst.Free(mp)
				if err != nil {
					return executor.Result{}, err
				}
				continue
			}
			if err = out.Vecs[targetIdx+1].Union(tmp.Vecs[sourceIdx], sels, mp); err != nil {
				return executor.Result{}, err
			}
			for _, rowIdx := range missedRows {
				nulls.Add(out.Vecs[targetIdx+1].GetNulls(), rowIdx)
			}
		}
	}
	out.SetRowCount(rowCount)
	sqlRet.Batches = []*batch.Batch{out}
	outputCost = time.Since(outputStart)
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-Output",
		zap.Uint64("table-id", tblStuff.lcaRel.GetTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("output-rows", rowCount),
		zap.Bool("has-any-hit", hasAnyHit),
		zap.Int("missed-rows", len(missedRows)),
		zap.Int("matched-keys", len(tmpRowByKey)),
	)
	return sqlRet, nil
}

func hashDiff(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	copt compositeOption,
	emit emitFunc,
	tarHandle []engine.ChangesHandle,
	baseHandle []engine.ChangesHandle,
	pickKeyHashmap databranchutils.BranchHashmap,
) (
	err error,
) {
	start := time.Now()
	var (
		buildBaseCost   time.Duration
		buildTargetCost time.Duration
		diffCost        time.Duration
	)

	var (
		baseDataHashmap      databranchutils.BranchHashmap
		baseTombstoneHashmap databranchutils.BranchHashmap
		basePkMoveEvidence   *pkMoveStreamEvidence

		tarDataHashmap      databranchutils.BranchHashmap
		tarTombstoneHashmap databranchutils.BranchHashmap
		tarPkMoveEvidence   *pkMoveStreamEvidence
	)

	defer func() {
		if baseDataHashmap != nil {
			baseDataHashmap.Close()
		}
		if baseTombstoneHashmap != nil {
			baseTombstoneHashmap.Close()
		}
		if tarDataHashmap != nil {
			tarDataHashmap.Close()
		}
		if tarTombstoneHashmap != nil {
			tarTombstoneHashmap.Close()
		}

		fields := []zap.Field{
			zap.Int("base-handle-cnt", len(baseHandle)),
			zap.Int("target-handle-cnt", len(tarHandle)),
			zap.Bool("has-lca", dagInfo.hasLCA()),
			zap.Duration("build-base-cost", buildBaseCost),
			zap.Duration("build-target-cost", buildTargetCost),
			zap.Duration("diff-cost", diffCost),
			zap.Duration("total-cost", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-HashDiff-Error", fields...)
			return
		}
		logutil.Info("DataBranch-HashDiff-Done", fields...)
	}()

	// The evidence capture matches findDeleteAndUpdateBat's own provider
	// gate (apply-capable diff over a real-pk table): chain walks consume
	// it only when those same conditions hold there.
	wantPkMoveEvidence := copt.expandUpdate && tblStuff.def.pkKind != fakeKind

	buildBaseStart := time.Now()
	if baseDataHashmap, baseTombstoneHashmap, basePkMoveEvidence, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, baseHandle, "base", pickKeyHashmap,
		wantPkMoveEvidence,
	); err != nil {
		return
	}
	buildBaseCost = time.Since(buildBaseStart)

	buildTargetStart := time.Now()
	if tarDataHashmap, tarTombstoneHashmap, tarPkMoveEvidence, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, tarHandle, "target", pickKeyHashmap,
		wantPkMoveEvidence,
	); err != nil {
		return
	}
	buildTargetCost = time.Since(buildTargetStart)

	diffStart := time.Now()
	if !dagInfo.hasLCA() || tblStuff.lcaHasZeroHistory {
		if err = hashDiffIfNoLCA(
			ctx, ses, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	} else {
		if err = hashDiffIfHasLCA(
			ctx, ses, bh, dagInfo, tblStuff, copt, emit,
			tarDataHashmap, tarTombstoneHashmap, tarPkMoveEvidence,
			baseDataHashmap, baseTombstoneHashmap, basePkMoveEvidence,
		); err != nil {
			return
		}
	}
	diffCost = time.Since(diffStart)

	return
}

func hashDiffIfHasLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	tarTombstoneHashmap databranchutils.BranchHashmap,
	tarPkMoveEvidence *pkMoveStreamEvidence,
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
	basePkMoveEvidence *pkMoveStreamEvidence,
) (err error) {

	// Resolve per-side LCA snapshots once. Both sides must resolve
	// their collected changes against the shared divergence point of
	// the two lineages: the earlier of the two per-side fork anchors
	// (see branchMetaInfo helper docs). The side that forked off the
	// LCA later inherits the LCA's writes between the two forks, and
	// those writes are exactly the divergence its collect range
	// gathered (buildSideCollectRange clamps the LCA window up to
	// otherFork.Next()). Resolving that side against its own fork
	// absorbs the divergence into the fork baseline — handleDelsOnLCA
	// returns the post-fork rows, the collected inserts compare equal
	// and are dropped — erasing real both-sides conflicts: WHEN
	// CONFLICT FAIL and SKIP would silently behave like ACCEPT and
	// DATA BRANCH DIFF would hide the destination rows. The
	// earlier-forked side keeps its own fork as the anchor, and when
	// both sides anchor identically (e.g. the other endpoint IS the
	// LCA) the mutual anchor equals that moment.
	tarSP, baseSP := tblStuff.resolvedSnapshots(ses)
	var tarLCAProbe, baseLCAProbe types.TS
	if copt.preservePickConflicts {
		// PICK keeps the per-side fork anchors. Its conflict rule counts
		// only writes each side made after its own fork: state the
		// destination inherited from the LCA between the two forks is
		// part of its baseline, and such picks must apply without a
		// conflict (the byte-chain pick in branch_binary_pk.sql).
		tarLCAProbe = dagInfo.tarLCASnapshot(baseSP)
		baseLCAProbe = dagInfo.baseLCASnapshot(tarSP)
	} else {
		mutualProbe := dagInfo.lcaProbeSnapshot(tarSP, baseSP)
		tarLCAProbe, baseLCAProbe = mutualProbe, mutualProbe
	}
	// A parent created earlier in the same explicit transaction has no
	// catalog state at the pre-commit CloneTS recorded by its child.  Both
	// tables become visible together, so the parent's creation commit is the
	// earliest valid (and equivalent) LCA probe for that zero-history edge.
	if !tblStuff.lcaCTS.IsEmpty() {
		if tarLCAProbe.LT(&tblStuff.lcaCTS) {
			tarLCAProbe = tblStuff.lcaCTS
		}
		if baseLCAProbe.LT(&tblStuff.lcaCTS) {
			baseLCAProbe = tblStuff.lcaCTS
		}
	}
	var (
		wg        sync.WaitGroup
		atomicErr atomic.Value

		baseDeleteBatches  []batchWithKind
		baseUpdateBatches  []batchWithKind
		restoreMissingKeys = make(map[string]struct{})
		// parkedRestoreBatches holds the restored-from-delete insert
		// emissions until every surviving key-moving move of this table
		// has been planned and emitted. A restore lands at the lineage's
		// landed key, which a surviving move in this same window may
		// first need to vacate: replayed before that move, the restore
		// insert's authorized victim delete removes the very occupant the
		// move was going to relocate, and the move's UPDATE then rewrites
		// (and cascades onto) the restored row — the destination silently
		// loses both lineages the clause meant to keep (r23 review F1).
		// The walk resolves restore-vs-move dependency order HERE (the
		// drain after phase 2); the topological ranker continues to
		// resolve move-vs-move order upstream.
		parkedRestoreBatches []batchWithKind
		// skippedKeyMovingOldKeys records the OLD keys of key-moving updates
		// whose delete half was suppressed by a SKIP conflict, so the insert
		// half can drop the same move instead of overwriting the destination.
		skippedKeyMovingOldKeys = make(map[string]struct{})
		// A source update reaches this matcher as a delete of its LCA row followed
		// by an insert of its replacement row. Record destination updates that
		// changed an indexed special column so their source replacement can use a
		// full-row native UPDATE even after the destination batch is consumed.
		destinationChangedIndexedSpecialKeys = make(map[string]struct{})
		sourceNativeUpdateKeys               = make(map[string]struct{})
		// destinationMovedNewKeys maps the OLD key of a destination-side
		// key-moving update to the NEW primary-key values the destination
		// moved that row to. When the source moved the same original row
		// (both sides moved it in this window), the accepted source version
		// must be applied to the destination row at the key where it
		// currently lives; applying it at the vacated old key would update
		// zero rows and silently drop the source move.
		destinationMovedNewKeys = make(map[string][]any)
		// keyMovingResolvedNewKeys records the NEW keys of folded source pk
		// moves that the delete/update conflict matcher already resolved
		// against destination batches. The independent-occupant check at
		// apply time must skip those keys: their collision was settled by
		// the resolver, not bypassed.
		keyMovingResolvedNewKeys = make(map[string]struct{})
	)

	// Release any parked restore batch the walk never drained (error paths);
	// successfully drained entries are zeroed one by one as they emit.
	defer func() {
		for i := range parkedRestoreBatches {
			if parkedRestoreBatches[i].batch != nil {
				tblStuff.retPool.releaseRetBatch(parkedRestoreBatches[i].batch, false)
			}
		}
	}()

	rememberDestinationChangedIndexedSpecialKeys := func(wrapped batchWithKind) error {
		if copt.conflictOpt == nil || copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT ||
			!copt.expandUpdate || !wrapped.fromUpdate || !wrapped.requiresNativeUpdate ||
			wrapped.kind != diffInsert {
			return nil
		}
		for rowIdx := range wrapped.batch.RowCount() {
			key, keyErr := extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
			if keyErr != nil {
				return keyErr
			}
			destinationChangedIndexedSpecialKeys[key] = struct{}{}
		}
		return nil
	}

	handleBaseDeleteAndUpdates := func(wrapped batchWithKind) error {
		wrapped.side = diffSideBase
		if wrapped.keyMovingUpdate && wrapped.kind == diffInsert {
			// Key-moving batches arrive in topological apply order; the
			// merge-joins below need primary-key order, and every sidecar
			// must be permuted identically or rows detach from their keys.
			if err2 := reorderKeyMovingBatch(
				wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(), nil,
				wrapped.keyMovingOldKeys, wrapped.keyMovingWhereKeys,
				wrapped.keyMovingVictimAuth, wrapped.keyMovingUnchangedOcc,
			); err2 != nil {
				return err2
			}
		} else if err2 := sortDataBranchBatchByPrimaryKey(
			wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}
		if wrapped.kind == diffInsert && wrapped.keyMovingUpdate {
			for rowIdx := range wrapped.batch.RowCount() {
				if rowIdx >= len(wrapped.keyMovingOldKeys) || wrapped.keyMovingOldKeys[rowIdx] == nil {
					return moerr.NewInternalErrorNoCtxf(
						"data branch key-moving update row %d misses its old key", rowIdx)
				}
				key, keyErr := formatPKValuesAsString(ses, tblStuff, wrapped.keyMovingOldKeys[rowIdx])
				if keyErr != nil {
					return keyErr
				}
				// The destination moved this row to a new key inside the
				// window; remember where it lives now.
				newKeys := make([]any, len(tblStuff.def.colNames))
				for _, colIdx := range tblStuff.def.pkColIdxes {
					if err2 := extractDataBranchSQLRowValue(
						ctx, ses, wrapped.batch.Vecs[colIdx], colIdx, newKeys, rowIdx,
					); err2 != nil {
						return err2
					}
				}
				destinationMovedNewKeys[key] = newKeys
			}
		}
		if wrapped.kind == diffDelete {
			baseDeleteBatches = append(baseDeleteBatches, wrapped)
		} else {
			if err := rememberDestinationChangedIndexedSpecialKeys(wrapped); err != nil {
				return err
			}
			baseUpdateBatches = append(baseUpdateBatches, wrapped)
		}

		return nil
	}

	handleTarDeleteAndUpdates := func(wrapped batchWithKind) (err2 error) {
		wrapped.side = diffSideTarget
		var pickConflictBat *batch.Batch

		// resolveKeyMovingNewKeyOccupantRows enforces conflict semantics
		// for every live destination row sitting at a folded move's NEW
		// key, over one insert batch and its victim-authorization flags.
		// Two occupant kinds exist, and neither may be deleted silently:
		//
		//   - DESTINATION-DELTA occupant (present in baseDataHashmap): a
		//     genuine merge conflict that never reaches foo() (only
		//     destination deletes/updates are conflict-matched), so this
		//     pass applies the effective option: SKIP drops the move
		//     whole, ACCEPT authorizes the inline victim delete (PICK
		//     additionally preserves the victim for conflict
		//     materialization), FAIL — and a bare statement, matching
		//     fail-like same-kind overlaps — is a conflict error.
		//   - UNCHANGED occupant (never touched by the destination since
		//     the branch point; the fold plan withheld victim
		//     authorization): no statement on either side wrote that row,
		//     so FAIL/bare is a conflict error, SKIP drops the move
		//     whole, and ACCEPT installs nothing — authorization stays
		//     withheld and the statement meets the raw duplicate-key
		//     guard atomically at apply, exactly as legacy delete+insert
		//     replay would.
		//
		// Keys the matcher already resolved (markKeyMovingResolved) are
		// skipped. Ordinary folded inserts and restored-from-delete
		// inserts share this arbiter: a restored row the destination
		// deleted at its OLD key lands at its NEW key just like any
		// folded move, so an independent destination row already holding
		// the landed key must lose (ACCEPT), keep (SKIP), or abort
		// (FAIL) the restore exactly the same way — before emission,
		// never as a duplicate-key accident at apply. ACCEPT grants the
		// authorization in place (victimAuth); the caller shrinks the
		// batch and every parallel sidecar by the returned rows.
		resolveKeyMovingNewKeyOccupantRows := func(bat *batch.Batch, victimAuth []bool, operation string) (dropSels []int64, err error) {
			if bat.RowCount() == 0 || baseDataHashmap == nil {
				return nil, nil
			}
			// effOpt is the conflict option this resolution obeys. PICK
			// forces the producer option to ACCEPT so its consumer sees
			// every preserved conflict, but the folded move's independent
			// occupant must follow the clause the user actually wrote.
			effOpt := copt.conflictOpt
			if copt.preservePickConflicts {
				effOpt = copt.pickUserConflictOpt
			}
			packer := types.NewPacker()
			defer packer.Close()
			for rowIdx := range bat.RowCount() {
				key, keyErr := extractPKAsString(ses, tblStuff, bat, rowIdx)
				if keyErr != nil {
					return nil, keyErr
				}
				if _, resolved := keyMovingResolvedNewKeys[key]; resolved {
					continue
				}
				authorized := rowIdx < len(victimAuth) && victimAuth[rowIdx]
				packer.Reset()
				if err := vector.EncodeRowToPacker(
					packer, bat.Vecs[tblStuff.def.pkColIdx], rowIdx,
				); err != nil {
					return nil, err
				}
				ret, gerr := baseDataHashmap.GetByEncodedKey(packer.Bytes())
				if gerr != nil {
					return nil, gerr
				}
				if ret.Exists && len(ret.Rows) == 1 {
					// Destination-delta occupant: a tracked conflict.
					if effOpt == nil || effOpt.Opt == tree.CONFLICT_FAIL {
						return nil, moerr.NewInternalErrorNoCtxf(
							"conflict: %s %s and %s %s on pk(%s)",
							wrapped.name, operation,
							tblStuff.baseRel.GetTableName(), diffInsert, key)
					}
					switch effOpt.Opt {
					case tree.CONFLICT_SKIP:
						// The destination keeps its row; the move drops as
						// a whole (its delete half is conflict-only and
						// inert).
						dropSels = append(dropSels, int64(rowIdx))
					default:
						// ACCEPT: the victim is a tracked destination
						// change, so the clause authorizes removing it
						// inline at apply; PICK additionally preserves it
						// for conflict materialization.
						if copt.preservePickConflicts {
							tuple, _, decErr := baseDataHashmap.DecodeRow(ret.Rows[0])
							if decErr != nil {
								return nil, decErr
							}
							if pickConflictBat == nil {
								pickConflictBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
							}
							if err := appendTupleToBat(ses, pickConflictBat, tuple, tblStuff); err != nil {
								return nil, err
							}
						}
						if !authorized && len(victimAuth) != 0 {
							victimAuth[rowIdx] = true
						}
					}
					continue
				}
				if authorized {
					continue
				}
				// Unchanged occupant (or an unauthorized fixture row):
				// never deleted inline; the clause decides.
				if effOpt == nil || effOpt.Opt == tree.CONFLICT_FAIL {
					return nil, moerr.NewInternalErrorNoCtxf(
						"conflict: %s %s and %s %s on pk(%s)",
						wrapped.name, operation,
						tblStuff.baseRel.GetTableName(), diffInsert, key)
				}
				if effOpt.Opt == tree.CONFLICT_SKIP {
					dropSels = append(dropSels, int64(rowIdx))
				}
			}
			return dropSels, nil
		}

		if wrapped.keyMovingUpdate && wrapped.kind == diffInsert && len(skippedKeyMovingOldKeys) > 0 {
			// SKIP suppressed the delete half of these moves on conflict; the
			// move as a whole keeps the destination version, so drop the
			// matching insert rows too — and, through the dependency
			// cascade, every folded move that needed one of the dropped
			// lineages to vacate its landed key (r24 review F1).
			var initialDrops []int64
			for rowIdx := range wrapped.batch.RowCount() {
				if rowIdx >= len(wrapped.keyMovingOldKeys) || wrapped.keyMovingOldKeys[rowIdx] == nil {
					return moerr.NewInternalErrorNoCtxf(
						"data branch key-moving update row %d misses its old key", rowIdx)
				}
				key, keyErr := formatPKValuesAsString(ses, tblStuff, wrapped.keyMovingOldKeys[rowIdx])
				if keyErr != nil {
					return keyErr
				}
				if _, skipped := skippedKeyMovingOldKeys[key]; !skipped {
					continue
				}
				delete(skippedKeyMovingOldKeys, key)
				initialDrops = append(initialDrops, int64(rowIdx))
			}
			var alive bool
			if wrapped, alive, err2 = shrinkKeyMovingFoldCascade(
				ses, tblStuff, wrapped, initialDrops); err2 != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return err2
			}
			if !alive {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return nil
			}
		}
		if wrapped.kind == diffInsert && wrapped.fromUpdate && len(restoreMissingKeys) > 0 {
			var (
				keep                []int64
				keepOldKeys         [][]any
				restoreOldKeys      [][]any
				restoreVictimAuth   []bool
				restoreUnchangedOcc []bool
			)
			restoreBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
			for rowIdx := range wrapped.batch.RowCount() {
				var (
					key    string
					keyErr error
				)
				if wrapped.keyMovingUpdate {
					// The restore marker rides on the OLD key: the destination
					// deleted the very row the source then moved elsewhere.
					if rowIdx >= len(wrapped.keyMovingOldKeys) || wrapped.keyMovingOldKeys[rowIdx] == nil {
						tblStuff.retPool.releaseRetBatch(restoreBat, false)
						return moerr.NewInternalErrorNoCtxf(
							"data branch key-moving update row %d misses its old key", rowIdx)
					}
					key, keyErr = formatPKValuesAsString(ses, tblStuff, wrapped.keyMovingOldKeys[rowIdx])
				} else {
					key, keyErr = extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
				}
				if keyErr != nil {
					tblStuff.retPool.releaseRetBatch(restoreBat, false)
					return keyErr
				}
				if _, restore := restoreMissingKeys[key]; !restore {
					keep = append(keep, int64(rowIdx))
					if wrapped.keyMovingUpdate {
						keepOldKeys = append(keepOldKeys, wrapped.keyMovingOldKeys[rowIdx])
					}
					continue
				}
				if err2 = restoreBat.UnionOne(wrapped.batch, int64(rowIdx), ses.proc.Mp()); err2 != nil {
					tblStuff.retPool.releaseRetBatch(restoreBat, false)
					return err2
				}
				if wrapped.keyMovingUpdate {
					// The restored row is a proven move: carry the selected
					// old-key identity and the fold's authorizations with
					// it so the apply consumers can consume the ACCEPT
					// conflict marker riding on the old key (PICK) and run
					// the authorized victim delete at the landed key
					// before re-inserting what the destination deleted.
					restoreOldKeys = append(restoreOldKeys, wrapped.keyMovingOldKeys[rowIdx])
					restoreVictimAuth = append(restoreVictimAuth,
						keyMovingRowVictimAuthorized(wrapped, rowIdx))
					restoreUnchangedOcc = append(restoreUnchangedOcc,
						rowIdx < len(wrapped.keyMovingUnchangedOcc) && wrapped.keyMovingUnchangedOcc[rowIdx])
				}
				delete(restoreMissingKeys, key)
			}
			// Landed-key occupant resolution for the restored rows: a
			// restored move lands at its NEW key exactly like an ordinary
			// folded move, so an independent destination row already
			// holding that key must follow the same clause — ACCEPT
			// authorizes the inline victim delete (PICK additionally
			// preserves the victim for conflict materialization), SKIP
			// drops the restore whole, FAIL aborts. Without this pass the
			// ACCEPT restore insert met the tracked occupant as a
			// duplicate key at apply (r22 review F4).
			if wrapped.keyMovingUpdate {
				dropSels, rerr := resolveKeyMovingNewKeyOccupantRows(
					restoreBat, restoreVictimAuth, wrapped.logicalOperation())
				if rerr != nil {
					tblStuff.retPool.releaseRetBatch(restoreBat, false)
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return rerr
				}
				if len(dropSels) != 0 {
					restoreBat.Shrink(dropSels, true)
					restoreOldKeys = removeKeyMovingOldKeys(restoreOldKeys, dropSels)
					restoreVictimAuth = removeKeyMovingBoolFlags(restoreVictimAuth, dropSels)
					restoreUnchangedOcc = removeKeyMovingBoolFlags(restoreUnchangedOcc, dropSels)
				}
			}
			if restoreBat.Vecs[0].Length() > 0 {
				restoreBat.SetRowCount(restoreBat.Vecs[0].Length())
				// Park the restore until every surviving move of this table
				// has been planned and emitted; the drain after phase 2 emits
				// the parked restores in encounter order (see the declaration
				// of parkedRestoreBatches for the dependency-order argument).
				parkedRestoreBatches = append(parkedRestoreBatches, batchWithKind{
					batch:                 restoreBat,
					kind:                  diffInsert,
					name:                  wrapped.name,
					side:                  wrapped.side,
					fromUpdate:            true,
					restoreMissing:        true,
					keyMovingUpdate:       wrapped.keyMovingUpdate,
					keyMovingOldKeys:      restoreOldKeys,
					keyMovingVictimAuth:   restoreVictimAuth,
					keyMovingUnchangedOcc: restoreUnchangedOcc,
				})
			} else {
				tblStuff.retPool.releaseRetBatch(restoreBat, false)
			}
			if len(keep) == 0 {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return nil
			}
			// Keep the rows that were not restored; the restored ones already
			// left through restoreBat. negate=false keeps the given rows — the
			// previous negate=true inverted that and would have re-processed
			// restored rows as ordinary inserts once a batch held both kinds.
			wrapped.batch.Shrink(keep, false)
			if wrapped.keyMovingUpdate {
				wrapped.keyMovingOldKeys = keepOldKeys
				wrapped.keyMovingVictimAuth = keepKeyMovingBoolFlags(wrapped.keyMovingVictimAuth, keep)
				wrapped.keyMovingUnchangedOcc = keepKeyMovingBoolFlags(wrapped.keyMovingUnchangedOcc, keep)
			}
		}
		if wrapped.keyMovingUpdate && wrapped.kind == diffInsert && len(destinationMovedNewKeys) > 0 {
			// The destination moved the same original row in this window.
			// Retarget the accepted source replacement at the destination
			// row's current key so the native UPDATE lands instead of
			// silently matching zero rows at the vacated old key.
			for rowIdx := range wrapped.batch.RowCount() {
				if rowIdx >= len(wrapped.keyMovingOldKeys) || wrapped.keyMovingOldKeys[rowIdx] == nil {
					return moerr.NewInternalErrorNoCtxf(
						"data branch key-moving update row %d misses its old key", rowIdx)
				}
				key, keyErr := formatPKValuesAsString(ses, tblStuff, wrapped.keyMovingOldKeys[rowIdx])
				if keyErr != nil {
					return keyErr
				}
				whereRow, moved := destinationMovedNewKeys[key]
				if !moved {
					continue
				}
				if wrapped.keyMovingWhereKeys == nil {
					wrapped.keyMovingWhereKeys = make([][]any, wrapped.batch.RowCount())
				}
				wrapped.keyMovingWhereKeys[rowIdx] = whereRow
			}
		}
		if copt.conflictOpt != nil && copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT &&
			copt.expandUpdate && wrapped.kind == diffInsert && wrapped.fromUpdate &&
			!wrapped.requiresNativeUpdate {
			for rowIdx := range wrapped.batch.RowCount() {
				key, keyErr := extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
				if keyErr != nil {
					return keyErr
				}
				if _, changed := destinationChangedIndexedSpecialKeys[key]; changed {
					sourceNativeUpdateKeys[key] = struct{}{}
				}
			}
		}
		// resolveKeyMovingNewKeyOccupants applies the shared landed-key
		// occupant arbiter (resolveKeyMovingNewKeyOccupantRows) to the
		// handler batch and shrinks every keyMoving sidecar by the dropped
		// rows, extended through the move-dependency cascade so a
		// SKIP-dropped move drags down every folded move whose landed key
		// it would have vacated (r24 review F1). MERGE+ACCEPT no longer
		// skips this pass: only a tracked destination change may grant the
		// inline delete now.
		resolveKeyMovingNewKeyOccupants := func() error {
			if !wrapped.keyMovingUpdate || wrapped.kind != diffInsert ||
				wrapped.batch.RowCount() == 0 || baseDataHashmap == nil {
				return nil
			}
			dropSels, err := resolveKeyMovingNewKeyOccupantRows(
				wrapped.batch, wrapped.keyMovingVictimAuth, wrapped.logicalOperation())
			if err != nil {
				return err
			}
			if len(dropSels) != 0 {
				wrapped, _, err = shrinkKeyMovingFoldCascade(ses, tblStuff, wrapped, dropSels)
				if err != nil {
					return err
				}
			}
			return nil
		}

		// emitPickConflicts flushes the preserved destination victims.
		emitPickConflicts := func() (bool, error) {
			if pickConflictBat == nil {
				return false, nil
			}
			bat := pickConflictBat
			pickConflictBat = nil
			return emitBatch(emit, batchWithKind{
				batch:          bat,
				kind:           diffDelete,
				name:           tblStuff.baseRel.GetTableName(),
				side:           diffSideBase,
				hasReplacement: wrapped.kind == diffInsert || wrapped.fromUpdate,
			}, false, tblStuff.retPool)
		}

		if len(baseUpdateBatches) == 0 && len(baseDeleteBatches) == 0 && len(sourceNativeUpdateKeys) == 0 {
			// No ordinary delete/update conflict to check; independent
			// destination inserts at folded new keys still need resolution.
			if err2 = resolveKeyMovingNewKeyOccupants(); err2 != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return err2
			}
			if stop, e := emitPickConflicts(); e != nil {
				return e
			} else if stop {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return nil
			}
			if wrapped.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return nil
			}
			// no need to check conflict
			if stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
			return nil
		}

		// Key-moving insert batches arrive in topological apply order and
		// carry sidecar slices: sort them by primary key for the conflict
		// merge-joins below, permuting every sidecar identically so no
		// permutation can detach a row from its keys or flags. The
		// topological order is restored before emission.
		if wrapped.keyMovingUpdate && wrapped.kind == diffInsert {
			if err2 = reorderKeyMovingBatch(
				wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(), nil,
				wrapped.keyMovingOldKeys, wrapped.keyMovingWhereKeys,
				wrapped.keyMovingVictimAuth, wrapped.keyMovingUnchangedOcc,
			); err2 != nil {
				return err2
			}
		} else if err2 = sortDataBranchBatchByPrimaryKey(
			wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
		}

		appendPickConflict := func(baseWrapped batchWithKind, rowIdx int) error {
			if !copt.preservePickConflicts || copt.conflictOpt == nil ||
				copt.conflictOpt.Opt != tree.CONFLICT_ACCEPT {
				return nil
			}
			if pickConflictBat == nil {
				pickConflictBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
			}
			return pickConflictBat.UnionOne(baseWrapped.batch, int64(rowIdx), ses.proc.Mp())
		}

		markKeyMovingResolved := func(tarWrapped batchWithKind, i int) error {
			if !tarWrapped.keyMovingUpdate || tarWrapped.kind != diffInsert {
				return nil
			}
			key, keyErr := extractPKAsString(ses, tblStuff, tarWrapped.batch, i)
			if keyErr != nil {
				return keyErr
			}
			keyMovingResolvedNewKeys[key] = struct{}{}
			return nil
		}

		checkConflict := func(tarWrapped, baseWrapped batchWithKind) (sels1, sels2 []int64, err3 error) {
			var (
				cmp     int
				tarVec  = tarWrapped.batch.Vecs[tblStuff.def.pkColIdx]
				baseVec = baseWrapped.batch.Vecs[tblStuff.def.pkColIdx]
			)

			i, j := 0, 0
			for i < tarVec.Length() && j < baseVec.Length() {
				if cmp, err3 = compareDataBranchPrimaryKeyInVectors(
					ctx, ses, i, j, tarVec, baseVec,
				); err3 != nil {
					return
				}

				if cmp == 0 {
					// conflict
					// tar and base both deleted on pk1 => empty
					// tar and base both updated on pk1 => we need compare the left columns, consider
					// the conflict option.
					if copt.conflictOpt == nil {
						if tarWrapped.kind == baseWrapped.kind {
							// compare the left column values
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								if err3 = markKeyMovingResolved(tarWrapped, i); err3 != nil {
									return
								}
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
							}
						}
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT {
						if tarWrapped.kind == diffDelete && tarWrapped.fromUpdate &&
							baseWrapped.kind == diffDelete && !baseWrapped.fromUpdate {
							key, keyErr := extractPKAsString(ses, tblStuff, tarWrapped.batch, i)
							if keyErr != nil {
								err3 = keyErr
								return
							}
							restoreMissingKeys[key] = struct{}{}
						}
						if tarWrapped.kind == diffDelete &&
							baseWrapped.kind == diffDelete &&
							!tarWrapped.fromUpdate && !baseWrapped.fromUpdate {
							if cmp, err3 = compareRowInWrappedBatches(
								ctx, ses, tblStuff, i, j, true,
								tarWrapped, baseWrapped,
							); err3 != nil {
								return
							} else if cmp == 0 {
								sels1 = append(sels1, int64(i))
								sels2 = append(sels2, int64(j))
								i++
								j++
								continue
							}
						}
						if err3 = appendPickConflict(baseWrapped, j); err3 != nil {
							return
						}
						if err3 = markKeyMovingResolved(tarWrapped, i); err3 != nil {
							return
						}
						// only keep the rows from tar
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
						if err3 = markKeyMovingResolved(tarWrapped, i); err3 != nil {
							return
						}
						sels1 = append(sels1, int64(i))
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else {
						// copt.conflictOpt == tree.CONFLICT_FAIL
						key, keyErr := extractPKAsString(ses, tblStuff, tarWrapped.batch, i)
						if keyErr != nil {
							err3 = keyErr
							return
						}

						err3 = moerr.NewDataBranchConflictNoCtxf(
							"%s %s and %s %s on pk(%v)",
							tarWrapped.name, tarWrapped.logicalOperation(),
							baseWrapped.name, baseWrapped.logicalOperation(), key,
						)
						return
					}
				} else if cmp < 0 {
					// tarVal < baseVal
					i++
				} else {
					// tarVal > baseVal
					j++
				}
			}

			return
		}

		foo := func(baseWrappedList []batchWithKind) (err3 error) {
			for _, baseWrapped := range baseWrappedList {
				var (
					sels1 []int64
					sels2 []int64
				)
				if sels1, sels2, err3 = checkConflict(wrapped, baseWrapped); err3 != nil {
					return
				} else {
					if len(sels1) != 0 {
						if wrapped.keyMovingUpdate && wrapped.kind == diffDelete &&
							copt.conflictOpt != nil && copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
							// The delete half of the move lost the conflict under
							// SKIP; remember the old keys so the insert half
							// drops the same moves (the destination wins).
							for _, rowIdx := range sels1 {
								key, keyErr := extractPKAsString(ses, tblStuff, wrapped.batch, int(rowIdx))
								if keyErr != nil {
									return keyErr
								}
								skippedKeyMovingOldKeys[key] = struct{}{}
							}
						}
						if wrapped.keyMovingUpdate && wrapped.kind == diffInsert &&
							copt.conflictOpt != nil && copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
							// SKIP removes the conflicting folded moves as a
							// whole: drag down every surviving move whose
							// landed key one of them would have vacated, or
							// its authorized victim delete would erase the
							// destination row the clause kept (r24 review F1).
							if wrapped, _, err2 = shrinkKeyMovingFoldCascade(
								ses, tblStuff, wrapped, sels1); err2 != nil {
								return err2
							}
						} else {
							wrapped.batch.Shrink(sels1, true)
							if len(wrapped.keyMovingOldKeys) != 0 {
								wrapped.keyMovingOldKeys = removeKeyMovingOldKeys(wrapped.keyMovingOldKeys, sels1)
								if len(wrapped.keyMovingWhereKeys) != 0 {
									wrapped.keyMovingWhereKeys = removeKeyMovingOldKeys(wrapped.keyMovingWhereKeys, sels1)
								}
							}
							if len(wrapped.keyMovingVictimAuth) != 0 {
								wrapped.keyMovingVictimAuth = removeKeyMovingBoolFlags(wrapped.keyMovingVictimAuth, sels1)
								if len(wrapped.keyMovingUnchangedOcc) != 0 {
									wrapped.keyMovingUnchangedOcc = removeKeyMovingBoolFlags(
										wrapped.keyMovingUnchangedOcc, sels1)
								}
							}
						}
					}

					if len(sels2) != 0 {
						if baseWrapped.keyMovingUpdate && len(baseWrapped.keyMovingOldKeys) != 0 {
							// A pruned destination move must stop retargeting
							// accepted source replacements.
							for _, rowIdx := range sels2 {
								if int(rowIdx) >= len(baseWrapped.keyMovingOldKeys) ||
									baseWrapped.keyMovingOldKeys[rowIdx] == nil {
									continue
								}
								key, keyErr := formatPKValuesAsString(
									ses, tblStuff, baseWrapped.keyMovingOldKeys[rowIdx])
								if keyErr != nil {
									return keyErr
								}
								delete(destinationMovedNewKeys, key)
							}
							baseWrapped.keyMovingOldKeys = removeKeyMovingOldKeys(
								baseWrapped.keyMovingOldKeys, sels2)
						}
						baseWrapped.batch.Shrink(sels2, true)
					}
				}

				if wrapped.batch.RowCount() == 0 {
					break
				}
			}

			return
		}

		if err2 = foo(baseDeleteBatches); err2 != nil {
			return
		}

		baseDeleteBatches = plan2.RemoveIf(baseDeleteBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if err2 = foo(baseUpdateBatches); err2 != nil {
			return
		}

		baseUpdateBatches = plan2.RemoveIf(baseUpdateBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		// Resolve folded moves against independent destination inserts at
		// their new keys (rows the delete/update matcher never sees).
		if err2 = resolveKeyMovingNewKeyOccupants(); err2 != nil {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return err2
		}

		if stop, e := emitPickConflicts(); e != nil {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return e
		} else if stop {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return nil
		}
		if wrapped.keyMovingUpdate && wrapped.kind == diffInsert {
			// The conflict stage merged in primary-key order; the apply
			// replays in the producer's topological order, so a move whose
			// source row another move lands on applies after it and never
			// meets that row as a victim at its new key.
			rank, rankErr := keyMovingApplyRanks(ses, tblStuff, wrapped)
			if rankErr != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return rankErr
			}
			if rank != nil {
				if err2 = reorderKeyMovingBatch(
					wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(), rank,
					wrapped.keyMovingOldKeys, wrapped.keyMovingWhereKeys,
					wrapped.keyMovingVictimAuth, wrapped.keyMovingUnchangedOcc,
				); err2 != nil {
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return err2
				}
			}
		}
		if len(sourceNativeUpdateKeys) != 0 {
			var nativeUpdateBat *batch.Batch
			nativeUpdateBat, err2 = splitDataBranchBatchForNativeUpdate(
				ses, tblStuff, wrapped.batch, sourceNativeUpdateKeys,
			)
			if err2 != nil {
				return
			}
			if nativeUpdateBat != nil {
				nativeWrapped := wrapped
				nativeWrapped.batch = nativeUpdateBat
				nativeWrapped.requiresNativeUpdate = true
				if stop, e := emitBatch(emit, nativeWrapped, false, tblStuff.retPool); e != nil {
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return e
				} else if stop {
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return nil
				}
			}
		}
		if wrapped.batch.RowCount() == 0 {
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			return nil
		}

		stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool)
		if e != nil {
			return e
		}
		if stop {
			return nil
		}

		return
	}

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncDelsAndUpdatesHandler := func(forBase bool, tmpCh chan batchWithKind) (err2 error) {
		var (
			branchTS = baseLCAProbe
			hashmap1 = baseDataHashmap
			hashmap2 = baseTombstoneHashmap
			evidence = basePkMoveEvidence
			name     = tblStuff.baseRel.GetTableName()
		)

		if !forBase {
			branchTS = tarLCAProbe
			hashmap1 = tarDataHashmap
			hashmap2 = tarTombstoneHashmap
			evidence = tarPkMoveEvidence
			name = tblStuff.tarRel.GetTableName()
		}

		wg.Add(1)
		if err2 = tblStuff.worker.Submit(func() {
			defer func() {
				close(tmpCh)
				tmpCh = nil
				wg.Done()
			}()

			if err3 := findDeleteAndUpdateBat(
				newCtx, ses, bh, tblStuff, name,
				func() int {
					if forBase {
						return diffSideBase
					}
					return diffSideTarget
				}(),
				func(wrapped batchWithKind) error {
					select {
					case <-newCtx.Done():
						tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
						return newCtx.Err()
					case tmpCh <- wrapped:
						return nil
					}
				}, branchTS, copt.expandUpdate, hashmap1, hashmap2, evidence, -1,
			); err3 != nil {
				atomicErr.Store(err3)
			}
		}); err2 != nil {
			wg.Done()
			return
		}
		return nil
	}

	stepHandler := func(forBase bool) (err2 error) {
		if copt.onlineApply {
			name, side, branchTS := tblStuff.baseRel.GetTableName(), diffSideBase, baseLCAProbe
			hashmap1, hashmap2, evidence, handle := baseDataHashmap, baseTombstoneHashmap, basePkMoveEvidence, handleBaseDeleteAndUpdates
			if !forBase {
				name, side, branchTS = tblStuff.tarRel.GetTableName(), diffSideTarget, tarLCAProbe
				hashmap1, hashmap2, evidence, handle = tarDataHashmap, tarTombstoneHashmap, tarPkMoveEvidence, handleTarDeleteAndUpdates
			}
			// Delivery owns the batch even on failure. Held batches remain in
			// retPool until the agency unwinds; handlers release emitted failures.
			return findDeleteAndUpdateBat(ctx, ses, bh, tblStuff, name, side,
				handle, branchTS, copt.expandUpdate, hashmap1, hashmap2, evidence, 1)
		}

		var (
			tmpCh = make(chan batchWithKind, 1)
			first error
		)

		if err2 = asyncDelsAndUpdatesHandler(forBase, tmpCh); err2 != nil {
			return err2
		}

		for wrapped := range tmpCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			select {
			case <-ctx.Done():
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			default:
				if forBase {
					err2 = handleBaseDeleteAndUpdates(wrapped)
				} else {
					err2 = handleTarDeleteAndUpdates(wrapped)
				}

				if errors.Is(err2, context.Canceled) {
					err2 = nil
					cancel()
				}
			}

			if err2 != nil {
				first = err2
				cancel()
			}
		}

		wg.Wait()
		if first != nil {
			return first
		}

		if atomicErr.Load() != nil {
			return atomicErr.Load().(error)
		}

		return first
	}

	// phase 1: handle base dels and updates on lca
	if err = stepHandler(true); err != nil {
		return
	}

	// phase2: handle tar dels and updates on lca
	if err = stepHandler(false); err != nil {
		return
	}

	// Drain the parked restores only now: a restore lands at its
	// lineage's landed key, and a surviving move in this same window may
	// first need to vacate that key. Emitted ahead of the moves, the
	// restore's authorized victim delete removed the very occupant the
	// move was about to relocate, and the move's UPDATE then rewrote
	// (and cascaded onto) the restored row — the destination silently
	// lost both lineages the clause meant to keep (r23 review F1). Every
	// surviving move of this table is emitted by this point (the
	// topological ranker resolved move-vs-move order upstream), so a
	// restore's victim delete now fires into a vacated key and its
	// insert lands on exactly what the clause accepted.
	for i := range parkedRestoreBatches {
		parked := parkedRestoreBatches[i]
		// Zero before emitting: emitBatch owns every attempted batch
		// (handlers release emitted failures), so the error-path defer
		// must only ever see batches the walk never attempted.
		parkedRestoreBatches[i] = batchWithKind{}
		var stop bool
		if stop, err = emitBatch(emit, parked, false, tblStuff.retPool); err != nil {
			return err
		}
		if stop {
			for j := i + 1; j < len(parkedRestoreBatches); j++ {
				tblStuff.retPool.releaseRetBatch(parkedRestoreBatches[j].batch, false)
				parkedRestoreBatches[j] = batchWithKind{}
			}
			break
		}
	}

	if len(restoreMissingKeys) != 0 {
		return moerr.NewInternalErrorNoCtxf(
			"data branch source update is missing %d replacement row(s)",
			len(restoreMissingKeys),
		)
	}

	// what can I do with these left base updates/inserts ?
	if copt.conflictOpt == nil {
		stopped := false
		for i, w := range baseUpdateBatches {
			var stop bool
			if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
				return err
			}
			if stop {
				stopped = true
				for j := i + 1; j < len(baseUpdateBatches); j++ {
					tblStuff.retPool.releaseRetBatch(baseUpdateBatches[j].batch, false)
				}
				for _, bw := range baseDeleteBatches {
					tblStuff.retPool.releaseRetBatch(bw.batch, false)
				}
				break
			}
		}

		if !stopped {
			for i, w := range baseDeleteBatches {
				var stop bool
				if stop, err = emitBatch(emit, w, false, tblStuff.retPool); err != nil {
					return err
				}
				if stop {
					for j := i + 1; j < len(baseDeleteBatches); j++ {
						tblStuff.retPool.releaseRetBatch(baseDeleteBatches[j].batch, false)
					}
					break
				}
			}
		}
	}

	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func hashDiffIfNoLCA(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	copt compositeOption,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	_ databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
	_ databranchutils.BranchHashmap,
) (err error) {
	// buildHashmapForTable already reconciles data versions with tombstones by
	// commit timestamp. Any data row left for a key is the latest live version.
	// In particular, a bounded UPDATE produces a same-commit data+tombstone
	// pair; removing the data merely because the marker remains would erase the
	// update before the two independent tables are compared.
	return diffDataHelper(ctx, ses, copt, tblStuff, emit, tarDataHashmap, baseDataHashmap)
}

func compareRowInWrappedBatches(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	rowIdx1 int,
	rowIdx2 int,
	skipPKCols bool,
	wrapped1 batchWithKind,
	wrapped2 batchWithKind,
) (int, error) {
	if wrapped1.kind == wrapped2.kind && wrapped1.kind == diffDelete && skipPKCols {
		return 0, nil
	}

	compareIdxes := tblStuff.def.commonIdxes
	if len(compareIdxes) == 0 {
		compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
	}
	for _, colIdx := range compareIdxes {
		if skipPKCols {
			if slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
				continue
			}
		}

		var (
			vec1 = wrapped1.batch.Vecs[colIdx]
			vec2 = wrapped2.batch.Vecs[colIdx]
		)

		if cmp, err := compareSingleValInVector(
			ctx, ses, rowIdx1, rowIdx2, vec1, vec2,
		); err != nil {
			return 0, err
		} else if cmp != 0 {
			return cmp, nil
		}

	}

	return 0, nil
}

func compareTupleWithBatchRow(
	tblStuff tableStuff,
	tuple types.Tuple,
	bat *batch.Batch,
	rowIdx int,
	skipPKCols bool,
) (int, error) {
	compareIdxes := tblStuff.def.commonIdxes
	if len(compareIdxes) == 0 {
		compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
	}
	for _, colIdx := range compareIdxes {
		if skipPKCols && slices.Index(tblStuff.def.pkColIdxes, colIdx) != -1 {
			continue
		}
		if colIdx < 0 || colIdx >= bat.VectorCount() {
			return 0, moerr.NewInternalErrorNoCtxf(
				"column index %d out of range for batch width %d",
				colIdx, bat.VectorCount(),
			)
		}
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return 0, err
		}
		cmp, err := compareTupleValueWithVector(val, bat.Vecs[colIdx], rowIdx)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func dataBranchUpdateRequiresNativeUpdateFromBatch(
	tblStuff tableStuff,
	tuple types.Tuple,
	bat *batch.Batch,
	rowIdx int,
) (bool, error) {
	for _, colIdx := range tblStuff.def.indexedSpecialUpdateIdxes {
		if colIdx < 0 || colIdx >= bat.VectorCount() {
			return false, moerr.NewInternalErrorNoCtxf(
				"column index %d out of range for batch width %d",
				colIdx, bat.VectorCount(),
			)
		}
		value, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return false, err
		}
		cmp, err := compareTupleValueWithVector(value, bat.Vecs[colIdx], rowIdx)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return true, nil
		}
	}
	return false, nil
}

// splitDataBranchBatchForNativeUpdate moves only the accepted conflict rows
// that need a full-row native UPDATE out of a staged source-update batch. The
// remaining rows retain the bounded staged apply path.
func splitDataBranchBatchForNativeUpdate(
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	nativeUpdateKeys map[string]struct{},
) (*batch.Batch, error) {
	if len(nativeUpdateKeys) == 0 {
		return nil, nil
	}

	nativeBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	nativeRowIdxes := make([]int64, 0, len(nativeUpdateKeys))
	for rowIdx := range bat.RowCount() {
		key, err := extractPKAsString(ses, tblStuff, bat, rowIdx)
		if err != nil {
			tblStuff.retPool.releaseRetBatch(nativeBat, false)
			return nil, err
		}
		if _, ok := nativeUpdateKeys[key]; !ok {
			continue
		}
		if err := nativeBat.UnionOne(bat, int64(rowIdx), ses.proc.Mp()); err != nil {
			tblStuff.retPool.releaseRetBatch(nativeBat, false)
			return nil, err
		}
		nativeRowIdxes = append(nativeRowIdxes, int64(rowIdx))
		delete(nativeUpdateKeys, key)
	}
	if len(nativeRowIdxes) == 0 {
		tblStuff.retPool.releaseRetBatch(nativeBat, false)
		return nil, nil
	}

	nativeBat.SetRowCount(nativeBat.Vecs[0].Length())
	bat.Shrink(nativeRowIdxes, true)
	return nativeBat, nil
}

func dataBranchUpdateRequiresNativeUpdateFromTuples(
	tblStuff tableStuff,
	left, right types.Tuple,
) (bool, error) {
	for _, colIdx := range tblStuff.def.indexedSpecialUpdateIdxes {
		leftValue, err := getTupleColumnValue(left, tblStuff, colIdx)
		if err != nil {
			return false, err
		}
		rightValue, err := getTupleColumnValue(right, tblStuff, colIdx)
		if err != nil {
			return false, err
		}
		if types.CompareValue(leftValue, rightValue) != 0 {
			return true, nil
		}
	}
	return false, nil
}

func findDeleteAndUpdateBat(
	ctx context.Context, ses *Session, bh BackgroundExec,
	tblStuff tableStuff, tblName string, side int, deliver func(batchWithKind) error, branchTS types.TS,
	expandUpdate bool,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
	pkMoveEvidence *pkMoveStreamEvidence,
	parallelism int,
) (err error) {

	maxTombstoneBatchCnt := tblStuff.maxTombstoneBatchCnt
	if maxTombstoneBatchCnt <= 0 {
		maxTombstoneBatchCnt = maxSqlBatchCnt
	}

	// Prepare on-demand access to the recorded pk moves for this side so
	// tombstones produced by a pk-moving UPDATE can be folded back into one
	// native destination UPDATE instead of a delete+insert pair. Only apply
	// contexts (expandUpdate) consume them, fake-pk tables never record
	// links, and tenants without the catalog table degrade to legacy
	// replay. The provider probes every physical table whose stream feeds
	// this side (multi-generation branches) and bounds write_ts by each
	// table's collect window; it caps its cache and fails closed past the
	// cap.
	var pkMoveProvider *pkMoveLinkProvider
	if expandUpdate && tblStuff.def.pkKind != fakeKind {
		var windows []pkMoveLinkTableWindow
		switch side {
		case diffSideBase:
			windows = tblStuff.pkMoveBaseWindows
		case diffSideTarget:
			windows = tblStuff.pkMoveTarWindows
		}
		// Fall back to the endpoint table with no time bound when the
		// collect path did not record windows (unit-test fixtures).
		if len(windows) == 0 {
			var rel engine.Relation
			switch side {
			case diffSideBase:
				rel = tblStuff.baseRel
			case diffSideTarget:
				rel = tblStuff.tarRel
			}
			if rel != nil {
				if id := rel.GetTableID(ctx); id != 0 {
					windows = []pkMoveLinkTableWindow{{
						tableID: id,
						endPhys: math.MaxInt64,
					}}
				}
			}
		}
		if len(windows) != 0 && bh != nil {
			var linkTableExists bool
			if linkTableExists, err = pkMoveLinkTableExists(ctx, ses, bh); err != nil {
				return err
			}
			if linkTableExists {
				pkMoveProvider = newPkMoveLinkProvider(ctx, ses, bh, windows)
			}
		}
	}
	// claimed records the final keys proven lineages claimed during
	// this diff; unseen tombstones at those keys are displaced
	// occupants the fold already replaced, so the tombstones must be
	// absorbed (skipped), not replayed as deletes.
	claimed := newPkMoveClaimedKeys()

	// The walk's tombstone evidence normally arrives from
	// buildHashmapForTable's pre-reconciliation capture (see
	// pkMoveStreamEvidence): generations deleted inside this window are
	// already netted out of the maps here, so a capture against the maps
	// would miss exactly the cross-transaction departures it exists to
	// record. The inline capture below is the degraded fallback for
	// direct callers without build-threaded evidence (unit-test fixtures);
	// it still runs before this function's own shard loop starts holding
	// the tombstone map exclusively.
	var pkMoveView *pkMoveHashmapView
	if pkMoveProvider != nil {
		var departed map[pkMoveDepartKey]types.TS
		var tombs map[string][]pkMoveTombRef
		var capped bool
		var tainted map[types.TS]struct{}
		if pkMoveEvidence != nil {
			departed, tombs, capped, tainted = pkMoveEvidence.departed,
				pkMoveEvidence.tombs, pkMoveEvidence.capped, pkMoveEvidence.tainted
		} else if departed, tombs, capped, err = capturePkMoveStreamEvidence(
			ctx, dataHashmap, tombstoneHashmap, parallelism,
		); err != nil {
			return err
		}
		pkMoveView = &pkMoveHashmapView{
			data: dataHashmap, departed: departed, tombs: tombs,
			capped: capped, tainted: tainted,
		}
	}

	// Prove, claim, and order every pk-move fold of this side BEFORE the
	// shard loop matches or emits a single row: chunk boundaries and shard
	// scheduling can then never race provenance against legacy matching or
	// against another fold (a replacement consumed by same-key matching, an
	// occupant deleted by an out-of-order apply). The loop below only
	// consults the finished plan.
	pkMovePlan, err := resolveAndBuildPkMoveFolds(
		ctx, ses, bh, tblStuff, side, pkMoveProvider, pkMoveView,
		tombstoneHashmap, claimed, branchTS.ToTimestamp(),
	)
	if err != nil {
		return err
	}

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
		)

		processBatch := func(tombBat *batch.Batch) error {
			if tombBat.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return nil
			}

			dBat, err2 := handleDelsOnLCA(
				ctx, ses, bh, tombBat, tblStuff, branchTS.ToTimestamp(),
			)
			if err2 != nil {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return err2
			}

			// merge inserts and deletes on the tar
			// this deletes is not on the lca
			if tombBat.RowCount() > 0 {
				removedLiveRows := 0
				tombRowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](tombBat.Vecs[1])
				for i := 0; i < tombBat.RowCount(); i++ {
					keyBytes := tombBat.Vecs[2].GetBytesAt(i)
					var dataRet databranchutils.GetResult
					if dataRet, err2 = dataHashmap.GetByEncodedKey(keyBytes); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					if !dataRet.Exists {
						continue
					}
					var matchedRow []byte
					for _, row := range dataRet.Rows {
						var liveTuple types.Tuple
						if liveTuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							tblStuff.retPool.releaseRetBatch(tombBat, true)
							tblStuff.retPool.releaseRetBatch(dBat, false)
							return err2
						}
						liveRowIDBytes, ok := liveTuple[0].([]uint8)
						if !ok {
							continue
						}
						liveRowID := types.DecodeFixed[types.Rowid](liveRowIDBytes)
						if !liveRowID.EQ(&tombRowIDs[i]) {
							continue
						}
						matchedRow = row
						break
					}
					if matchedRow == nil {
						continue
					}
					var removed int
					if removed, err2 = dataHashmap.PopByEncodedKeyValue(keyBytes, matchedRow, false); err2 != nil {
						tblStuff.retPool.releaseRetBatch(tombBat, true)
						tblStuff.retPool.releaseRetBatch(dBat, false)
						return err2
					}
					removedLiveRows += removed
				}
				tblStuff.retPool.releaseRetBatch(tombBat, true)
			}

			// find update
			if dBat.RowCount() > 0 {
				pkVecLen := dBat.Vecs[tblStuff.def.pkColIdx].Length()
				if pkVecLen != dBat.RowCount() {
					logutil.Error(
						"DataBranch-findDeleteAndUpdateBat-VecLenMismatch",
						zap.String("table-name", tblName),
						zap.Int("dBat-row-count", dBat.RowCount()),
						zap.Int("pkVec-length", pkVecLen),
						zap.Int("pkColIdx", tblStuff.def.pkColIdx),
						zap.Int("vec-count", dBat.VectorCount()),
					)
				}
				tBat2 := tblStuff.retPool.acquireRetBatch(tblStuff, false)
				seen := make([]bool, dBat.RowCount())
				var stagedUpdateBat *batch.Batch
				var nativeUpdateBat *batch.Batch
				var updateDeleteBat *batch.Batch
				streamPkVec := dBat.Vecs[tblStuff.def.pkColIdx]
				var streamIdx []int

				releaseWithStreamPk := func() {
					if streamIdx != nil {
						streamPkVec.Free(ses.proc.Mp())
					}
					tblStuff.retPool.releaseRetBatch(dBat, false)
					tblStuff.retPool.releaseRetBatch(tBat2, false)
					if stagedUpdateBat != nil {
						tblStuff.retPool.releaseRetBatch(stagedUpdateBat, false)
					}
					if nativeUpdateBat != nil {
						tblStuff.retPool.releaseRetBatch(nativeUpdateBat, false)
					}
					if updateDeleteBat != nil {
						tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
					}
				}

				// Provenance classification runs BEFORE any matching or
				// emission: the central fold pass already claimed every
				// replacement row, so its winner anchors and the displaced
				// occupants the folds replaced are marked seen here and
				// legacy matching below only ever touches rows provenance
				// left alone — in any chunk or shard order.
				if pkMovePlan != nil {
					seenPacker := types.NewPacker()
					for i := 0; i < dBat.RowCount(); i++ {
						if dBat.Vecs[tblStuff.def.pkColIdx].IsNull(uint64(i)) {
							continue
						}
						seenPacker.Reset()
						if err2 = vector.EncodeRowToPacker(
							seenPacker, dBat.Vecs[tblStuff.def.pkColIdx], i,
						); err2 != nil {
							seenPacker.Close()
							releaseWithStreamPk()
							return err2
						}
						kb := seenPacker.Bytes()
						if _, anchor := pkMovePlan.anchorKeys[string(kb)]; anchor {
							seen[i] = true
							continue
						}
						if claimed.has(kb) {
							// A lineage fold replaced the row at this
							// key: the unseen tombstone belongs to the
							// displaced previous occupant and is
							// absorbed — replaying it as a delete would
							// kill the row the fold installs.
							seen[i] = true
						}
					}
					seenPacker.Close()
				}

				// Legacy same-key matching probes only the rows provenance
				// left unclaimed.
				if pkMovePlan != nil {
					streamIdx = make([]int, 0, dBat.RowCount())
					streamPkVec = vector.NewVec(*dBat.Vecs[tblStuff.def.pkColIdx].GetType())
					for i := 0; i < dBat.RowCount(); i++ {
						if seen[i] {
							continue
						}
						if err2 = streamPkVec.UnionOne(
							dBat.Vecs[tblStuff.def.pkColIdx], int64(i), ses.proc.Mp(),
						); err2 != nil {
							streamPkVec.Free(ses.proc.Mp())
							streamIdx = nil
							releaseWithStreamPk()
							return err2
						}
						streamIdx = append(streamIdx, i)
					}
				}

				if streamPkVec.Length() > 0 {
					if _, err2 = dataHashmap.PopByVectorsStream(
						[]*vector.Vector{streamPkVec}, false,
						func(idx int, key []byte, row []byte) error {
							origIdx := idx
							if streamIdx != nil {
								origIdx = streamIdx[idx]
							}
							seen[origIdx] = true

							if tuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
								return err2
							}

							var cmp int
							skipPKCols := tblStuff.def.pkKind != fakeKind
							if cmp, err2 = compareTupleWithBatchRow(
								tblStuff, tuple, dBat, origIdx, skipPKCols,
							); err2 != nil {
								return err2
							}
							if cmp == 0 {
								return nil
							}

							// delete on lca and insert into tar ==> update
							requiresNativeUpdate := false
							if expandUpdate {
								var updateErr error
								requiresNativeUpdate, updateErr = dataBranchUpdateRequiresNativeUpdateFromBatch(
									tblStuff, tuple, dBat, origIdx,
								)
								if updateErr != nil {
									return updateErr
								}
							}
							updateBat := stagedUpdateBat
							if requiresNativeUpdate {
								updateBat = nativeUpdateBat
							}
							if updateBat == nil {
								updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
								if requiresNativeUpdate {
									nativeUpdateBat = updateBat
								} else {
									stagedUpdateBat = updateBat
								}
							}
							if expandUpdate && updateDeleteBat == nil {
								updateDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
							}
							if expandUpdate {
								if err2 = updateDeleteBat.UnionOne(dBat, int64(origIdx), ses.proc.Mp()); err2 != nil {
									return err2
								}
							}
							if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
								return err2
							}
							return nil
						},
					); err2 != nil {
						releaseWithStreamPk()
						return err2
					}
				}
				if streamIdx != nil {
					streamPkVec.Free(ses.proc.Mp())
				}

				for i := 0; i < dBat.RowCount(); i++ {
					if seen[i] {
						continue
					}

					// delete on lca
					if err2 = tBat2.UnionOne(dBat, int64(i), ses.proc.Mp()); err2 != nil {
						tblStuff.retPool.releaseRetBatch(dBat, false)
						tblStuff.retPool.releaseRetBatch(tBat2, false)
						if stagedUpdateBat != nil {
							tblStuff.retPool.releaseRetBatch(stagedUpdateBat, false)
						}
						if nativeUpdateBat != nil {
							tblStuff.retPool.releaseRetBatch(nativeUpdateBat, false)
						}
						if updateDeleteBat != nil {
							tblStuff.retPool.releaseRetBatch(updateDeleteBat, false)
						}
						return err2
					}
				}

				tblStuff.retPool.releaseRetBatch(dBat, false)
				tBat2.SetRowCount(tBat2.Vecs[0].Length())

				if stagedUpdateBat != nil || nativeUpdateBat != nil {
					if expandUpdate {
						updateDeleteBat.SetRowCount(updateDeleteBat.Vecs[0].Length())
						if err2 = deliver(batchWithKind{
							name:       tblName,
							side:       side,
							fromUpdate: true,
							batch:      updateDeleteBat,
							kind:       diffDelete,
						}); err2 != nil {
							return err2
						}
						for _, update := range []struct {
							bat                  *batch.Batch
							requiresNativeUpdate bool
						}{
							{bat: stagedUpdateBat},
							{bat: nativeUpdateBat, requiresNativeUpdate: true},
						} {
							if update.bat == nil {
								continue
							}
							update.bat.SetRowCount(update.bat.Vecs[0].Length())
							if err2 = deliver(batchWithKind{
								name:                 tblName,
								side:                 side,
								batch:                update.bat,
								kind:                 diffInsert,
								fromUpdate:           tblStuff.def.pkKind != fakeKind,
								requiresNativeUpdate: update.requiresNativeUpdate,
							}); err2 != nil {
								return err2
							}
						}
					} else {
						if stagedUpdateBat == nil {
							stagedUpdateBat = nativeUpdateBat
						}
						stagedUpdateBat.SetRowCount(stagedUpdateBat.Vecs[0].Length())
						if err2 = deliver(batchWithKind{
							name:  tblName,
							side:  side,
							batch: stagedUpdateBat,
							kind:  diffUpdate,
						}); err2 != nil {
							return err2
						}
					}
				}

				if err2 = deliver(batchWithKind{
					name:  tblName,
					side:  side,
					batch: tBat2,
					kind:  diffDelete,
				}); err2 != nil {
					return err2
				}
				return nil
			}

			tblStuff.retPool.releaseRetBatch(dBat, false)
			return nil
		}

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = tombstoneHashmap.DecodeRow(key); err2 != nil {
				return err2
			}
			// The decoded tombstone key carries the packed heap encoding of
			// the PK column. Fixed-width key payloads such as Decimal256 decode
			// as raw bytes, so normalize to the PK column type before appending;
			// AppendAny would assert the typed representation and panic.
			if err2 = appendTupleValueToVector(tBat1.Vecs[0], tuple[0], ses.proc.Mp()); err2 != nil {
				return err2
			}
			var tombTuple types.Tuple
			if tombTuple, _, err2 = tombstoneHashmap.DecodeRow(row); err2 != nil {
				return err2
			}
			rowIDBytes, ok := tombTuple[0].([]uint8)
			if !ok {
				return moerr.NewInternalErrorNoCtx("tombstone row missing rowid in hashmap payload")
			}
			rowID := types.DecodeFixed[types.Rowid](rowIDBytes)
			if err2 = vector.AppendFixed(tBat1.Vecs[1], rowID, false, ses.proc.Mp()); err2 != nil {
				return err2
			}
			if err2 = vector.AppendBytes(tBat1.Vecs[2], append([]byte(nil), key...), false, ses.proc.Mp()); err2 != nil {
				return err2
			}

			tBat1.SetRowCount(tBat1.Vecs[0].Length())
			if tBat1.RowCount() >= maxTombstoneBatchCnt {
				tombBat := tBat1
				tBat1 = nil
				if err2 = processBatch(tombBat); err2 != nil {
					return err2
				}
				tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true)
			}
			return nil
		}); err2 != nil {
			if tBat1 != nil {
				tblStuff.retPool.releaseRetBatch(tBat1, true)
			}
			return err2
		}

		if tBat1 != nil {
			return processBatch(tBat1)
		}
		return nil

	}, parallelism); err != nil {
		releasePkMoveFoldPlan(tblStuff, pkMovePlan)
		return
	}

	// The proven folds deliver last, in apply order: topological so a move
	// whose source row another move lands on applies first, lexicographic
	// everywhere else. Consumers must preserve this order through their own
	// re-sorts (they reorder for conflict merging only and restore it
	// before emission).
	if pkMovePlan != nil {
		if err = deliver(batchWithKind{
			name:            tblName,
			side:            side,
			fromUpdate:      true,
			keyMovingUpdate: true,
			batch:           pkMovePlan.deleteBat,
			kind:            diffDelete,
		}); err != nil {
			tblStuff.retPool.releaseRetBatch(pkMovePlan.insertBat, false)
			return err
		}
		if err = deliver(batchWithKind{
			name:                  tblName,
			side:                  side,
			fromUpdate:            true,
			requiresNativeUpdate:  true,
			keyMovingUpdate:       true,
			keyMovingOldKeys:      pkMovePlan.oldKeys,
			keyMovingVictimAuth:   pkMovePlan.victimAuth,
			keyMovingUnchangedOcc: pkMovePlan.unchangedOcc,
			batch:                 pkMovePlan.insertBat,
			kind:                  diffInsert,
		}); err != nil {
			return err
		}
	}

	return nil
}

// releasePkMoveFoldPlan returns an undelivered fold plan's batches to their
// pool (error paths before delivery).
func releasePkMoveFoldPlan(tblStuff tableStuff, plan *pkMoveFoldPlan) {
	if plan == nil {
		return
	}
	if plan.deleteBat != nil {
		tblStuff.retPool.releaseRetBatch(plan.deleteBat, false)
		plan.deleteBat = nil
	}
	if plan.insertBat != nil {
		tblStuff.retPool.releaseRetBatch(plan.insertBat, false)
		plan.insertBat = nil
	}
}

// pkMoveFoldPlan carries one side's fully resolved key-moving folds: every
// fold was proven, claimed, and ordered BEFORE any shard-loop chunk matched
// or emitted a row, so legacy same-key matching can never consume a
// replacement that a different chunk's anchor needs. Chunk boundaries and
// shard order no longer influence fold outcomes.
type pkMoveFoldPlan struct {
	// anchorKeys are the OLD-key encodings of emitted folds; the shard loop
	// marks their LCA-hit rows seen so anchor tombstones never re-enter
	// legacy delete/update emission.
	anchorKeys map[string]struct{}
	// deleteBat / insertBat carry the folds in apply order (topologically:
	// a move whose source row another move lands on applies first, so no
	// fold's new-key handling ever meets a row that is itself moving away;
	// lexicographic by final key everywhere else, so replay is
	// deterministic). oldKeys / victimAuth / unchangedOcc align by row.
	deleteBat    *batch.Batch
	insertBat    *batch.Batch
	oldKeys      [][]any
	victimAuth   []bool
	unchangedOcc []bool
}

// pkMoveLCAAnchorProbe resolves one batch of anchor tombstones against
// the LCA snapshot. A var so the fold pass's unit tests can pin LCA
// relevance without a SQL executor; production always runs
// handleDelsOnLCA.
var pkMoveLCAAnchorProbe = handleDelsOnLCA

// pkMoveFoldCandidate is one anchor tombstone considered for a fold: its
// key/row id address the anchor, and a successful chain walk fills the
// landed (proven replacement) coordinates.
type pkMoveFoldCandidate struct {
	key      []byte
	rid      types.Rowid
	finalPK  []byte
	finalRow []byte
	resolved bool
}

// resolveAndBuildPkMoveFolds proves and materializes every pk-move fold of
// one side's diff in a single pre-loop pass:
//
//  0. Capped evidence fails the pass closed BEFORE any legacy meaning is
//     assigned: a cut-off tombstone index means the diff cannot tell a
//     moved row's departure from an independent delete. When the link
//     catalog proves NO move was recorded in window, legacy replay is the
//     proven-absence semantics every tombstone would receive anyway;
//     otherwise the apply aborts (statement-atomic) rather than risk a
//     destructive DELETE+INSERT replay under a declared cascade.
//  1. Relevance BEFORE resolution: every candidate anchor tombstone from
//     the pre-captured index probes the LCA snapshot in ONE
//     handleDelsOnLCA call, and candidates whose old key the destination
//     lineage does not hold (source-only lineages) or that a PICK
//     admitted as dependency keys only are excluded from walks AND from
//     the unresolved-provenance abort of step 3 — their stream rows keep
//     legacy meaning no matter what their provenance would conclude, so
//     an empty net merge over source-only history stays a no-op.
//  2. Every RELEVANT anchor is walked (resolvePkMoveChainVerdict) in
//     deterministic (key, cts, rowid) order; resolved anchors are
//     fold-grade before-images by construction of step 1's probe.
//  3. Each materialized fold pops its proven replacement out of the data
//     map and records it in claimed, so the shard loop's legacy matching —
//     in ANY chunk or shard order — operates only on rows provenance did
//     not take. At most one fold emits per old key (earliest anchor commit
//     wins); later anchors of the same key keep legacy meaning (their
//     replacement rows flow on as ordinary inserts).
//  4. Emitted folds are ordered so that a move landing on the key another
//     fold vacates applies after it. A key-rotation cycle (or a fold
//     depending on one) cannot be replayed as in-place native UPDATEs —
//     every landing key still holds a member row until that member moves —
//     and legacy replay would silently swap payloads while children stay
//     attached to the wrong lineage, so a cyclic plan ABORTS the apply
//     loudly before emission instead of degrading.
//  5. Each fold classifies its new-key victim claim: a source tombstone
//     at the new key authorizes removing the displaced key-holder (plain
//     merge delete semantics, just ordered before the UPDATE); a live
//     destination row with NO source tombstone at the new key is either
//     the conflict resolver's business (destination delta rows) or an
//     unchanged-since-branch occupant the apply must never silently
//     delete (flagged in unchangedOcc, victim authorization withheld).
//
// Anything unproven in a RELEVANT lineage (missing link, ambiguous
// evidence, raced claim) fails closed with the unresolved-provenance
// abort; proven absence keeps delete+insert meaning.
func resolveAndBuildPkMoveFolds(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	side int,
	provider *pkMoveLinkProvider,
	view *pkMoveHashmapView,
	tombstoneHashmap databranchutils.BranchHashmap,
	claimed *pkMoveClaimedKeys,
	branchTS timestamp.Timestamp,
) (plan *pkMoveFoldPlan, err error) {
	if provider == nil || view == nil || len(tblStuff.def.pkColIdxes) == 0 {
		return nil, nil
	}

	tombKeys := make([]string, 0, len(view.tombs))
	for ks := range view.tombs {
		tombKeys = append(tombKeys, ks)
	}
	sort.Strings(tombKeys)
	var cands []pkMoveFoldCandidate
	var prefKeys [][]byte
	for _, ks := range tombKeys {
		kb := []byte(ks)
		prefKeys = append(prefKeys, kb)
		for _, ref := range view.tombs[ks] {
			cands = append(cands, pkMoveFoldCandidate{key: kb, rid: ref.rowID})
		}
	}
	if len(cands) == 0 {
		if !view.capped {
			return nil, nil
		}
		// The capture budget cut the tombstone index off: it is empty
		// because evidence was dropped, not because the window was
		// quiet. A catalog proving NO move was recorded in window lets
		// every tombstone keep the proven-absence verdict — complete
		// evidence of no update intent — and legacy replay stays the
		// rows' own semantics. With links present, nothing distinguishes
		// a move's departure from an independent delete, and a
		// DELETE+INSERT replay could cascade a declared delete action
		// onto dependents of a row that MOVED: abort instead.
		var exists bool
		if exists, err = provider.anyPkMoveLinkInWindow(); err != nil {
			return nil, err
		} else if exists {
			return nil, moerr.NewInternalErrorNoCtxf(
				"data branch cannot apply this diff with complete primary-key update evidence: the tombstone capture exceeded its entry budget (%d rows) and recorded pk-update provenance exists in window; the apply is aborted so declared ON UPDATE actions are never replaced by destructive deletes",
				pkMoveEvidenceMaxEntries)
		}
		return nil, nil
	}
	if err = provider.prefetchContinuations(prefKeys); err != nil {
		return nil, err
	}

	// Link-free windows settle on the verdict walk alone: with no
	// recorded update intent anywhere in the window, a complete probe
	// space answers "no move" for every candidate without hydrating a
	// single LCA row. Anything less than a proven absence (disabled
	// provider, poisoned evidence) falls through to the general path
	// and is adjudicated there exactly as before.
	var linksExist bool
	if linksExist, err = provider.anyPkMoveLinkInWindow(); err != nil {
		return nil, err
	}
	if !linksExist {
		allNone := true
		for _, c := range cands {
			if _, _, v := provider.resolvePkMoveChainVerdict(view, c.key, c.rid); v != pkMoveProvenNone {
				allNone = false
				break
			}
		}
		if allNone {
			return nil, nil
		}
	}

	// Verdict walk before LCA hydration. Only an anchor whose recorded
	// intent needs adjudication — a resolved move or an unprovable one —
	// ever travels to the LCA probe; candidates with complete evidence
	// of no move intent keep the legacy DELETE+INSERT replay without
	// touching the destination snapshot, and PICK dependency-only keys
	// are admitted only to reassemble selected lineages, so they drop
	// here exactly as the post-probe filter used to drop them. The
	// abort below exists to protect destination lineages; relevance
	// (an LCA hit) still gates it, so a lineage the destination never
	// held cannot earn one.
	var survivors []pkMoveFoldCandidate
	for _, c := range cands {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if _, depOnly := tblStuff.pickDepKeys[string(c.key)]; depOnly {
			continue
		}
		var verdict pkMoveResolution
		c.finalPK, c.finalRow, verdict =
			provider.resolvePkMoveChainVerdict(view, c.key, c.rid)
		switch verdict {
		case pkMoveResolved:
			c.resolved = true
			survivors = append(survivors, c)
		case pkMoveProvenNone:
			// Complete evidence shows no update intent (or affirmatively
			// proves a foreign lineage): the legacy DELETE+INSERT replay
			// is the row's own semantics.
		case pkMoveProvenDeleted:
			// Recorded provenance proves the lineage moved and complete
			// stream evidence proves the moved lineage was terminated by
			// a plain deletion: the transaction's net effect is the
			// anchor tombstone's own DELETE, and declared ON DELETE
			// actions apply through the legacy replay path.
		case pkMoveUnresolved:
			// Recorded or plausibly-existing provenance could not be
			// proven (hop cap, probe budget, corrupt or ambiguous stream
			// evidence, poisoned key, disabled provider). Whether it can
			// abort is decided by relevance below: a source-only lineage
			// keeps legacy meaning exactly as before.
			survivors = append(survivors, c)
		}
	}
	if len(survivors) == 0 {
		return nil, nil
	}

	// Probe the LCA snapshot for the survivors in maxTombstoneBatchCnt-row
	// chunks — the same bounded shape the shard loop feeds
	// handleDelsOnLCA (pk value, row id, encoded key) — and keep only the
	// before-image rows of RESOLVED folds. A chunk's full row images are
	// released as soon as its hits are classified, so a window deleting
	// many wide rows pays one chunk of payload instead of the
	// whole-diff retention this pass used to require outside the
	// hashmap budget.
	maxChunk := tblStuff.maxTombstoneBatchCnt
	if maxChunk <= 0 {
		maxChunk = maxSqlBatchCnt
	}
	packer := types.NewPacker()
	defer packer.Close()
	var dBatAll *batch.Batch
	releaseKept := func() {
		if dBatAll != nil {
			tblStuff.retPool.releaseRetBatch(dBatAll, false)
			dBatAll = nil
		}
	}
	var resolved []pkMoveFoldCandidate
	unresolved := 0
	for start := 0; start < len(survivors); start += maxChunk {
		if ctx.Err() != nil {
			releaseKept()
			return nil, ctx.Err()
		}
		end := min(start+maxChunk, len(survivors))
		anchorBat := tblStuff.retPool.acquireRetBatch(tblStuff, true)
		for _, c := range survivors[start:end] {
			var tuple types.Tuple
			if tuple, _, err = tombstoneHashmap.DecodeRow(c.key); err != nil {
				tblStuff.retPool.releaseRetBatch(anchorBat, true)
				releaseKept()
				return nil, err
			}
			if err = vector.AppendAny(anchorBat.Vecs[0], tuple[0], false, ses.proc.Mp()); err != nil {
				tblStuff.retPool.releaseRetBatch(anchorBat, true)
				releaseKept()
				return nil, err
			}
			if err = vector.AppendFixed(anchorBat.Vecs[1], c.rid, false, ses.proc.Mp()); err != nil {
				tblStuff.retPool.releaseRetBatch(anchorBat, true)
				releaseKept()
				return nil, err
			}
			if err = vector.AppendBytes(anchorBat.Vecs[2], c.key, false, ses.proc.Mp()); err != nil {
				tblStuff.retPool.releaseRetBatch(anchorBat, true)
				releaseKept()
				return nil, err
			}
		}
		anchorBat.SetRowCount(anchorBat.Vecs[0].Length())
		var dBat *batch.Batch
		dBat, err = pkMoveLCAAnchorProbe(ctx, ses, bh, anchorBat, tblStuff, branchTS)
		tblStuff.retPool.releaseRetBatch(anchorBat, true)
		if err != nil {
			releaseKept()
			return nil, err
		}

		// Classify the chunk's hits: LCA-holding anchor keys, encoded the
		// way candidacy queries them. Only resolved folds keep their
		// before-image rows; unrelevant candidates and unresolvable hits
		// leave nothing behind.
		chunkHits := make(map[string]struct{}, dBat.RowCount())
		resolvedKeys := make(map[string]struct{})
		for _, c := range survivors[start:end] {
			if c.resolved {
				resolvedKeys[string(c.key)] = struct{}{}
			}
		}
		for i := 0; i < dBat.RowCount(); i++ {
			if dBat.Vecs[tblStuff.def.pkColIdx].IsNull(uint64(i)) {
				continue
			}
			packer.Reset()
			if err = vector.EncodeRowToPacker(packer, dBat.Vecs[tblStuff.def.pkColIdx], i); err != nil {
				tblStuff.retPool.releaseRetBatch(dBat, false)
				releaseKept()
				return nil, err
			}
			ks := string(packer.Bytes())
			chunkHits[ks] = struct{}{}
			if _, keep := resolvedKeys[ks]; keep {
				if dBatAll == nil {
					dBatAll = tblStuff.retPool.acquireRetBatch(tblStuff, false)
				}
				if err = dBatAll.UnionOne(dBat, int64(i), ses.proc.Mp()); err != nil {
					tblStuff.retPool.releaseRetBatch(dBat, false)
					releaseKept()
					return nil, err
				}
			}
		}
		tblStuff.retPool.releaseRetBatch(dBat, false)
		for _, c := range survivors[start:end] {
			if _, held := chunkHits[string(c.key)]; !held {
				continue
			}
			if c.resolved {
				resolved = append(resolved, c)
			} else {
				// The provenance of a destination-held lineage could
				// not be proven. Replaying this row as DELETE+INSERT
				// could cascade the declared DELETE action onto
				// dependents of a row that actually MOVED, so the whole
				// apply aborts atomically instead of downgrading it
				// silently.
				unresolved++
			}
		}
	}
	if dBatAll != nil {
		dBatAll.SetRowCount(dBatAll.Vecs[0].Length())
	}
	if unresolved > 0 {
		releaseKept()
		return nil, moerr.NewInternalErrorNoCtxf(
			"data branch cannot apply %d tombstone row(s): their recorded primary-key update provenance could not be resolved completely (evidence budget, chain hop limit, or ambiguous stream evidence); the apply is aborted so declared ON UPDATE actions are never replaced by destructive deletes", unresolved)
	}
	if len(resolved) == 0 {
		releaseKept()
		return nil, nil
	}

	// Live destination occupancy matters only on the apply side: the other
	// side's folds drive conflict bookkeeping, which has no victims. The
	// probe is batched (one chunked SQL covers every fold key) because a
	// per-fold round trip drowns merges at scale in point queries.
	var occupancyProbe func([][]byte, []types.Tuple) (map[string]bool, error)
	if side == diffSideTarget {
		occupancyProbe = func(keys [][]byte, tuples []types.Tuple) (map[string]bool, error) {
			return probeDstOccupancyAtFoldKeys(ctx, ses, bh, tblStuff, keys, tuples)
		}
	}
	defer releaseKept()
	return assemblePkMoveFoldPlan(
		ctx, ses, tblStuff, view, claimed, dBatAll, resolved, occupancyProbe,
	)
}

// pkMoveFoldHeap is the ready set of the fold plan's Kahn pass: fold
// indexes ordered by the plan's deterministic choice. The fold order it
// yields is identical to the lexicographic minimum over all ready folds.
type pkMoveFoldHeap struct {
	idx  []int
	less func(a, b int) bool
}

func (h pkMoveFoldHeap) Len() int { return len(h.idx) }
func (h pkMoveFoldHeap) Less(i, j int) bool {
	return h.less(h.idx[i], h.idx[j])
}
func (h pkMoveFoldHeap) Swap(i, j int) { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *pkMoveFoldHeap) Push(x any)   { h.idx = append(h.idx, x.(int)) }
func (h *pkMoveFoldHeap) Pop() any {
	old := h.idx
	n := len(old)
	v := old[n-1]
	h.idx = old[:n-1]
	return v
}

// assemblePkMoveFoldPlan is the SQL-free core of the fold pass: given the
// resolved anchor candidates and the LCA before-image rows they probed,
// emit the fully ordered fold plan. occupancyProbe batch-reports live
// destination occupancy at the requested fold keys (nil means "no live
// occupancy" and keeps every victim authorization at its proven default).
func assemblePkMoveFoldPlan(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	view *pkMoveHashmapView,
	claimed *pkMoveClaimedKeys,
	dBatAll *batch.Batch,
	resolved []pkMoveFoldCandidate,
	occupancyProbe func([][]byte, []types.Tuple) (map[string]bool, error),
) (plan *pkMoveFoldPlan, err error) {
	// hit keys -> the LCA before-image row index in dBatAll.
	hitIdxByKey := make(map[string]int, dBatAll.RowCount())
	packer := types.NewPacker()
	defer packer.Close()
	for i := 0; i < dBatAll.RowCount(); i++ {
		if dBatAll.Vecs[tblStuff.def.pkColIdx].IsNull(uint64(i)) {
			continue
		}
		packer.Reset()
		if err = vector.EncodeRowToPacker(packer, dBatAll.Vecs[tblStuff.def.pkColIdx], i); err != nil {
			return nil, err
		}
		ks := string(packer.Bytes())
		if _, dup := hitIdxByKey[ks]; !dup {
			hitIdxByKey[ks] = i
		}
	}

	type fold struct {
		oldKey     string
		anchorRID  types.Rowid
		finalPK    []byte
		finalRow   []byte
		finalTuple types.Tuple
		lcaIdx     int
		oldRow     []any
	}
	// Phase 1: candidate selection. Nothing here mutates the stream view
	// or the claimed/anchor maps: a candidate that does not survive the
	// dependency pass below leaves zero trace, so its anchor tombstone and
	// replacement row replay through the legacy path exactly as if the
	// fold had never been considered.
	var folds []fold
	seenCandKey := make(map[string]struct{})
	for _, c := range resolved {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		ks := string(c.key)
		lcaIdx, hit := hitIdxByKey[ks]
		if !hit {
			// Unreachable by construction (resolveAndBuildPkMoveFolds
			// pre-filters misses) but kept as a cheap invariant: a
			// source-only lineage has no destination lineage to cascade,
			// no fold to emit, nothing popped — born-died netting replays
			// it.
			continue
		}
		if _, depOnly := tblStuff.pickDepKeys[ks]; depOnly {
			// PICK selection is by proven row lineage, and a fold may only
			// be produced for a literally picked anchor. Dependency keys
			// admitted by the closure exist to reassemble the SELECTED
			// lineage; an unrelated lineage whose resurrected key collides
			// with a dependency key must not become an applied move.
			continue
		}
		if _, dup := seenCandKey[ks]; dup {
			// One candidate per old key: the earliest anchor (candidate
			// order is (key, cts, rowid)) owns the destination lineage;
			// later anchors of the same key keep legacy meaning and their
			// replacements flow on as ordinary inserts. The key stays owned
			// even if its candidate later fails materialization: a
			// contradicting stream fails the anchor closed instead of
			// letting a weaker duplicate take over.
			continue
		}
		seenCandKey[ks] = struct{}{}
		finalRow := append([]byte(nil), c.finalRow...)
		tuple, _, decErr := view.data.DecodeRow(finalRow)
		if decErr != nil {
			err = decErr
			return nil, err
		}
		oldRow := make([]any, len(tblStuff.def.colNames))
		for _, colIdx := range tblStuff.def.pkColIdxes {
			if err = extractDataBranchSQLRowValue(
				ctx, ses, dBatAll.Vecs[colIdx], colIdx, oldRow, lcaIdx,
			); err != nil {
				return nil, err
			}
		}
		folds = append(folds, fold{
			oldKey:     ks,
			anchorRID:  c.rid,
			finalPK:    c.finalPK,
			finalRow:   finalRow,
			finalTuple: tuple,
			lcaIdx:     lcaIdx,
			oldRow:     oldRow,
		})
	}
	if len(folds) == 0 {
		return nil, nil
	}

	// Phase 2: topological apply order over candidates. Move X precedes
	// move Y when Y lands on the key X vacates (Y.finalPK == X.oldKey):
	// replaying the vacating move first means a landing fold never meets,
	// at its new key, a row that is itself about to move away. Kahn with a
	// lexicographic choice for determinism; the ready set lives in a heap
	// because re-scanning all remaining folds per emitted fold is
	// quadratic and dominates merges at scale.
	//
	// Residue after the ready set drains is a genuine key-rotation cycle
	// or a fold depending on one. A cycle cannot be replayed as in-place
	// native UPDATEs: every landing key of the cycle still holds a member
	// row until that member itself moves. Legacy DELETE+INSERT replay is
	// not a safe fallback either: same-key matching would swap payloads
	// while each destination child stays attached to the row id at its
	// own key — the lineage that moved away — so dependents end on the
	// WRONG lineage without any FK violation staying loud. The apply
	// therefore aborts statement-atomically here instead of silently
	// emitting either replay shape.
	oldKeyIdx := make(map[string][]int, len(folds))
	finalPkIdx := make(map[string][]int, len(folds))
	for i := range folds {
		oldKeyIdx[folds[i].oldKey] = append(oldKeyIdx[folds[i].oldKey], i)
		finalPkIdx[string(folds[i].finalPK)] = append(finalPkIdx[string(folds[i].finalPK)], i)
	}
	indegree := make([]int, len(folds))
	for i := range folds {
		indegree[i] = len(oldKeyIdx[string(folds[i].finalPK)])
		if string(folds[i].finalPK) == folds[i].oldKey {
			// A fold landing back on its own anchor key applies as an
			// in-place update: no OTHER fold has to vacate anything for
			// it. The blanket count registered the fold as its own
			// prerequisite, which would deadlock the scheduler into a
			// false key-rotation cycle for any net no-op or payload-only
			// return to the original key (two committed moves 2->3->2
			// resolve to exactly this shape). The apply ranker
			// (keyMovingApplyRanks) already excludes the same self-edge.
			indegree[i]--
		}
	}
	less := func(a, b int) bool {
		c := bytes.Compare(folds[a].finalPK, folds[b].finalPK)
		if c != 0 {
			return c < 0
		}
		if folds[a].oldKey != folds[b].oldKey {
			return folds[a].oldKey < folds[b].oldKey
		}
		return bytes.Compare(folds[a].anchorRID[:], folds[b].anchorRID[:]) < 0
	}
	ready := pkMoveFoldHeap{less: less}
	for i := range folds {
		if indegree[i] == 0 {
			ready.idx = append(ready.idx, i)
		}
	}
	heap.Init(&ready)
	var topo []int
	for ready.Len() != 0 {
		picked := heap.Pop(&ready).(int)
		topo = append(topo, picked)
		// Emitting `picked` vacates picked.oldKey: every fold landing on
		// that key loses one unapplied prerequisite. A self-landing fold
		// is never its own prerequisite (see the count above), so the
		// pop must not decrement it here either — mirrors the apply
		// ranker's exclusion.
		for _, y := range finalPkIdx[folds[picked].oldKey] {
			if y == picked {
				continue
			}
			indegree[y]--
			if indegree[y] == 0 {
				heap.Push(&ready, y)
			}
		}
	}
	if len(topo) != len(folds) {
		// Cyclic residue: no apply order replays a key rotation as
		// native UPDATEs, and legacy replay silently mis-attaches
		// dependents. Abort the whole apply before any fold pops,
		// claims, or anchors — the statement rolls back atomically, and
		// the error names the safe rework (a delete plus re-insert
		// records no move provenance and merges as ordinary changes).
		return nil, moerr.NewInternalErrorNoCtxf(
			"data branch cannot apply %d primary-key update(s) whose landed keys participate in a key rotation cycle: a cycle cannot be replayed as native UPDATEs, and replaying it as DELETE+INSERT would silently detach dependents onto the wrong lineage; the apply is aborted statement-atomically so declared ON UPDATE actions are never silently lost. Rework the source branch so the lineage does not rotate keys (delete and re-insert the rows instead of updating their keys) and retry",
			len(folds)-len(topo))
	}

	// Phase 2.5: unique replacement ownership. Two provenance chains may
	// never claim the same replacement: a pair ending on one final row is
	// two anchors whose LCA lineages the verdict walk could not separate
	// (the recorded incoming provenance was insufficient to assign the
	// claim to one lineage), and a pair ending on one final key with
	// DIFFERENT rows contradicts the one-committed-image-per-key stream.
	// Consuming the row for the fold that happens to pop first here and
	// replaying the losing anchor's tombstone as a legacy DELETE would
	// rewrite one lineage as the other and cascade declared actions onto
	// the wrong dependents without any error. Abort the whole apply
	// statement-atomically BEFORE any fold pops, claims, or anchors —
	// the same contract the cyclic residue abort above establishes.
	claimOwners := make(map[string][]int, len(folds))
	for i := range folds {
		fpk := string(folds[i].finalPK)
		claimOwners[fpk] = append(claimOwners[fpk], i)
	}
	for fpk, owners := range claimOwners {
		if len(owners) < 2 {
			continue
		}
		for _, idx := range owners[1:] {
			if string(folds[idx].finalRow) != string(folds[owners[0]].finalRow) {
				return nil, moerr.NewInternalErrorNoCtxf(
					"data branch cannot apply %d primary-key update(s): their provenance chains claim conflicting replacement rows at the same final key x'%x'; the stream evidence is contradictory (one committed image per key per instant), so the apply is aborted statement-atomically instead of letting pop order pick a lineage",
					len(owners), []byte(fpk))
			}
		}
		return nil, moerr.NewInternalErrorNoCtxf(
			"data branch cannot apply %d primary-key update(s): their provenance chains claim the same replacement row at final key x'%x'; the anchors' LCA lineage could not be distinguished, so folding one chain and replaying the other anchor as DELETE would rewrite one lineage as the other; the apply is aborted statement-atomically so declared ON UPDATE actions are never silently lost. Rework the source branch so the competing lineage does not land on the same replacement (delete and re-insert instead of updating keys) and retry",
			len(owners), []byte(fpk))
	}

	// Phase 3: materialize the DAG-safe folds in apply order. Only now do
	// the stream-view side effects happen: popping the proven replacement
	// out of the data map and claiming its key, so the shard loop's legacy
	// matching operates only on rows provenance did not take. With unique
	// ownership enforced above, a pop that finds nothing can no longer be
	// consumption by a competing fold: the row the walk claimed is absent
	// from the very stream it was resolved from — contradictory evidence
	// — and degrading to legacy replay would DELETE the anchor's key.
	anchorKeys := make(map[string]struct{}, len(topo))
	materialized := make([]int, 0, len(topo))
	for _, idx := range topo {
		f := folds[idx]
		var removed int
		if removed, err = view.data.PopByEncodedKeyValue(f.finalPK, f.finalRow, false); err != nil {
			return nil, err
		}
		if removed == 0 {
			return nil, moerr.NewInternalErrorNoCtxf(
				"data branch cannot apply this primary-key update plan: one proven replacement row is absent from the diff stream (contradictory evidence); the apply is aborted statement-atomically instead of replaying its anchor as a destructive delete")
		}
		anchorKeys[f.oldKey] = struct{}{}
		claimed.add(f.finalPK)
		materialized = append(materialized, idx)
	}
	if len(materialized) == 0 {
		return nil, nil
	}

	// Victim authorization is needed before emission but probes in bulk:
	// folds without a selected source tombstone at the landed key go
	// through one batched occupancy read instead of one query per fold.
	tombAuth := make([]bool, len(materialized))
	for i, idx := range materialized {
		// An in-window source tombstone at the new key authorizes removing
		// the displaced key-holder — but only when that deletion is part of
		// THIS replay's selection. MERGE selects everything a source
		// tombstone deletes; PICK selects only literal keys, and a tombstone
		// sitting on a dependency (or unselected) key must never erase a
		// destination row whose lineage the pick leaves alone.
		tombAuth[i] = len(view.tombstonesAt(folds[idx].finalPK)) != 0
		if tombAuth[i] && len(tblStuff.pickLiteralKeys) != 0 {
			_, picked := tblStuff.pickLiteralKeys[string(folds[idx].finalPK)]
			tombAuth[i] = picked
		}
	}
	var occupied map[string]bool
	if occupancyProbe != nil {
		var probeKeys [][]byte
		var probeTuples []types.Tuple
		for i, idx := range materialized {
			if tombAuth[i] {
				continue
			}
			probeKeys = append(probeKeys, folds[idx].finalPK)
			probeTuples = append(probeTuples, folds[idx].finalTuple)
		}
		if len(probeKeys) != 0 {
			if occupied, err = occupancyProbe(probeKeys, probeTuples); err != nil {
				return nil, err
			}
		}
	}

	plan = &pkMoveFoldPlan{anchorKeys: anchorKeys}
	deleteBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	insertBat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	plan.deleteBat, plan.insertBat = deleteBat, insertBat
	releaseOnErr := func() {
		tblStuff.retPool.releaseRetBatch(deleteBat, false)
		tblStuff.retPool.releaseRetBatch(insertBat, false)
		plan.deleteBat, plan.insertBat = nil, nil
	}
	for i, idx := range materialized {
		f := folds[idx]
		if err = deleteBat.UnionOne(dBatAll, int64(f.lcaIdx), ses.proc.Mp()); err != nil {
			releaseOnErr()
			return nil, err
		}
		if err = appendTupleToBat(ses, insertBat, f.finalTuple, tblStuff); err != nil {
			releaseOnErr()
			return nil, err
		}
		plan.oldKeys = append(plan.oldKeys, f.oldRow)

		auth := true
		unchanged := false
		// No selected source tombstone covers the new key and the batched
		// probe found a live destination occupant there: classify the
		// occupancy. A delta-tracked row is the conflict resolver's
		// business (it grants authorization when ACCEPT preserves the
		// victim); an occupant the destination never changed since the
		// branch point must NEVER be silently deleted — authorization
		// stays withheld and the option logic downstream decides
		// (FAIL/bare error, SKIP drop, ACCEPT meets the raw duplicate-key
		// guard atomically).
		if !tombAuth[i] && occupied[string(f.finalPK)] {
			auth = false
			unchanged = true
		}
		plan.victimAuth = append(plan.victimAuth, auth)
		plan.unchangedOcc = append(plan.unchangedOcc, unchanged)
	}
	deleteBat.SetRowCount(deleteBat.Vecs[0].Length())
	insertBat.SetRowCount(insertBat.Vecs[0].Length())
	return plan, nil
}

// probeDstOccupancyAtFoldKeys reports, per requested fold key, whether the
// destination table holds a live row at that key right now. Probes are
// batched: each chunk packs the keys into one SQL as an OR of exact key
// predicates — the per-fold count(*) shape issued one round trip per
// moved row, which at diff scale (tens of thousands of moves) drowned the
// merge in point queries. The probe runs on the merge's own transaction,
// so it sees exactly the state the apply will meet; the whole merge
// applies single-statement afterwards.
//
// The result selects the physical primary-key column, so re-encoding each
// returned row reproduces the stream hash map's encoded key directly.
//
// tuples are decoded data-row tuples from the stream hash map: their
// width carries the rowid (and commit-ts) prefix/suffix the stream adds
// around the physical columns, so column lookup goes through
// getTupleColumnValue, never raw positions.
func probeDstOccupancyAtFoldKeys(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	keys [][]byte,
	tuples []types.Tuple,
) (map[string]bool, error) {
	occupied := make(map[string]bool, len(keys))
	if len(keys) != len(tuples) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"pk-move occupancy probe keys %d != tuples %d", len(keys), len(tuples))
	}
	if len(keys) == 0 {
		return occupied, nil
	}
	tableName := qualifiedTableName(
		tblStuff.baseRel.GetTableDef(ctx).DbName,
		tblStuff.baseRel.GetTableDef(ctx).Name,
	)
	pkColName := quoteIdentifierForSQL(tblStuff.def.baseColNames[tblStuff.def.pkColIdx])
	packer := types.NewPacker()
	defer packer.Close()
	for start := 0; start < len(keys); start += pkMoveOccupancyProbeChunk {
		end := start + pkMoveOccupancyProbeChunk
		if end > len(keys) {
			end = len(keys)
		}
		buf := acquireBuffer(tblStuff.bufPool)
		buf.WriteString("select ")
		buf.WriteString(pkColName)
		buf.WriteString(" from ")
		buf.WriteString(tableName)
		buf.WriteString(" where ")
		buildErr := error(nil)
		for i := start; i < end && buildErr == nil; i++ {
			if i > start {
				buf.WriteString(" or ")
			}
			row := make([]any, len(tblStuff.def.colNames))
			for _, colIdx := range tblStuff.def.pkColIdxes {
				if row[colIdx], buildErr = getTupleColumnValue(tuples[i], tblStuff, colIdx); buildErr != nil {
					break
				}
			}
			if buildErr == nil {
				buildErr = writeExactDataBranchKeyPredicate(ses, tblStuff, row, buf)
			}
		}
		sql := buf.String()
		releaseBuffer(tblStuff.bufPool, buf)
		if buildErr != nil {
			return nil, buildErr
		}
		ret, err := runSql(ctx, ses, bh, sql, nil, nil)
		if err != nil {
			return nil, err
		}
		var readErr error
		ret.ReadRows(func(rowCnt int, cols []*vector.Vector) bool {
			if len(cols) != 1 {
				readErr = moerr.NewInternalErrorNoCtxf(
					"unexpected pk-move occupancy probe width %d", len(cols))
				return false
			}
			for i := 0; i < rowCnt; i++ {
				packer.Reset()
				if e := vector.EncodeRowToPacker(packer, cols[0], i); e != nil {
					readErr = e
					return false
				}
				occupied[string(append([]byte(nil), packer.Bytes()...))] = true
			}
			return true
		})
		ret.Close()
		if readErr != nil {
			return nil, readErr
		}
	}
	return occupied, nil
}

// reorderKeyMovingBatch permutes a key-moving batch and every aligned
// sidecar (old keys, where-key overrides, victim authorization,
// unchanged-occupant flags) into one shared order. With rank == nil the
// order is ascending primary key (the merge-join order the conflict stage
// needs); with rank != nil it is the recorded topological apply rank per
// row, restored before emission so folds interacting through one another's
// keys replay in dependency order. A present sidecar must align with the
// batch rows; an absent (empty) one is skipped.
func reorderKeyMovingBatch(
	bat *batch.Batch,
	pkColIdx int,
	mp *mpool.MPool,
	rank []uint64,
	oldKeys, whereKeys [][]any,
	victimAuth, unchangedOcc []bool,
) error {
	if bat == nil {
		return nil
	}
	rowCnt := bat.Vecs[0].Length()
	for _, sc := range []struct {
		name string
		n    int
	}{
		{"old keys", len(oldKeys)},
		{"where keys", len(whereKeys)},
		{"victim authorization", len(victimAuth)},
		{"unchanged-occupant flags", len(unchangedOcc)},
	} {
		if sc.n != 0 && sc.n != rowCnt {
			return moerr.NewInternalErrorNoCtxf(
				"key-moving update batch rows %d != %s %d", rowCnt, sc.name, sc.n)
		}
	}
	if rowCnt <= 1 {
		return nil
	}

	cols := make([]*vector.Vector, 0, len(bat.Vecs)+3)
	cols = append(cols, bat.Vecs...)
	sortKeyCol := pkColIdx
	if rank != nil {
		if len(rank) != rowCnt {
			return moerr.NewInternalErrorNoCtxf(
				"key-moving update batch rows %d != apply ranks %d", rowCnt, len(rank))
		}
		rankVec := vector.NewVec(types.T_uint64.ToType())
		defer rankVec.Free(mp)
		for i := 0; i < rowCnt; i++ {
			if err := vector.AppendFixed(rankVec, rank[i], false, mp); err != nil {
				return err
			}
		}
		cols = append(cols, rankVec)
		sortKeyCol = len(cols) - 1
	} else if pkVec := bat.Vecs[pkColIdx]; isDataBranchFloatType(*pkVec.GetType()) {
		// Mirror sortDataBranchBatchByPrimaryKey: bit-distinct FLOAT/DOUBLE
		// keys sort by their identity column.
		identityVec := vector.NewVec(types.T_uint64.ToType())
		defer identityVec.Free(mp)
		for row := 0; row < pkVec.Length(); row++ {
			identity, isNull, err := dataBranchFloatPKIdentityAt(pkVec, row)
			if err != nil {
				return err
			}
			if err = vector.AppendFixed(identityVec, identity, isNull, mp); err != nil {
				return err
			}
		}
		cols = append(cols, identityVec)
		sortKeyCol = len(cols) - 1
	}

	indexVec := vector.NewVec(types.T_uint64.ToType())
	defer indexVec.Free(mp)
	for i := 0; i < rowCnt; i++ {
		if err := vector.AppendFixed(indexVec, uint64(i), false, mp); err != nil {
			return err
		}
	}
	cols = append(cols, indexVec)

	if err := mergeutil.SortColumnsByIndex(cols, sortKeyCol, mp); err != nil {
		return err
	}

	perm := vector.MustFixedColNoTypeCheck[uint64](indexVec)
	permuteAny := func(p [][]any) {
		if len(p) == 0 {
			return
		}
		sorted := make([][]any, len(p))
		for pos := range perm {
			sorted[pos] = p[perm[pos]]
		}
		copy(p, sorted)
	}
	permuteBool := func(p []bool) {
		if len(p) == 0 {
			return
		}
		sorted := make([]bool, len(p))
		for pos := range perm {
			sorted[pos] = p[perm[pos]]
		}
		copy(p, sorted)
	}
	permuteAny(oldKeys)
	permuteAny(whereKeys)
	permuteBool(victimAuth)
	permuteBool(unchangedOcc)
	return nil
}

// sortKeyMovingUpdateBat sorts a key-moving insert batch by pk while
// permuting the parallel key sidecars (old keys, and where-key overrides
// when the destination moved the same row) identically.
func sortKeyMovingUpdateBat(bat *batch.Batch, pkColIdx int, mp *mpool.MPool, parallels ...[][]any) error {
	var oldKeys, whereKeys [][]any
	if len(parallels) > 0 {
		oldKeys = parallels[0]
	}
	if len(parallels) > 1 {
		whereKeys = parallels[1]
	}
	return reorderKeyMovingBatch(bat, pkColIdx, mp, nil, oldKeys, whereKeys, nil, nil)
}

// keyMovingApplyRanks recomputes the deterministic topological apply rank
// of each surviving key-moving row: a move whose NEW key is another
// surviving move's EFFECTIVE WHERE key (destination-lineage override when
// installed, else the shared old key) applies after it, so no fold
// installs its row on top of a lineage that is itself about to move away.
// Ranking on the original old keys is unsafe once the conflict resolver
// retargets a move onto destination-moved lineage: two moves can then
// predicate on each other's new keys even though their source keys do not
// interact, and the wrong order makes one UPDATE rewrite a row the other
// just installed. Deterministic tie-breaks match the producer's emission
// order, so a batch the conflict stage never reordered keeps its arrival
// order exactly. Returns nil when there is nothing to restore.
func keyMovingApplyRanks(
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
) ([]uint64, error) {
	rowCnt := wrapped.batch.RowCount()
	if rowCnt <= 1 || len(wrapped.keyMovingOldKeys) != rowCnt {
		return nil, nil
	}
	if len(wrapped.keyMovingWhereKeys) != 0 &&
		len(wrapped.keyMovingWhereKeys) != rowCnt {
		return nil, moerr.NewInternalErrorNoCtxf(
			"data branch key-moving update has %d where keys for %d rows",
			len(wrapped.keyMovingWhereKeys), rowCnt)
	}
	oldKeyStrs := make([]string, rowCnt)
	newKeyStrs := make([]string, rowCnt)
	for i := range rowCnt {
		var err error
		// Rank on the key the apply actually predicates on: the
		// destination-lineage WHERE override when one is installed, else
		// the shared old key (mirrors pickKeyMovingWhereRow). Ranking on
		// the original old keys would order two retargeted moves by keys
		// neither one touches, letting a later UPDATE match a row an
		// earlier one just installed.
		effKey := pickKeyMovingWhereRow(wrapped, i)
		if effKey == nil {
			return nil, moerr.NewInternalErrorNoCtxf(
				"data branch key-moving update row %d misses its old key", i)
		}
		if oldKeyStrs[i], err = formatPKValuesAsString(ses, tblStuff, effKey); err != nil {
			return nil, err
		}
		if newKeyStrs[i], err = extractPKAsString(ses, tblStuff, wrapped.batch, i); err != nil {
			return nil, err
		}
	}
	oldKeyIdx := make(map[string][]int, rowCnt)
	newKeyIdx := make(map[string][]int, rowCnt)
	for i := range rowCnt {
		oldKeyIdx[oldKeyStrs[i]] = append(oldKeyIdx[oldKeyStrs[i]], i)
		newKeyIdx[newKeyStrs[i]] = append(newKeyIdx[newKeyStrs[i]], i)
	}
	indegree := make([]int, rowCnt)
	for i := range rowCnt {
		indegree[i] = len(oldKeyIdx[newKeyStrs[i]])
		if oldKeyStrs[i] == newKeyStrs[i] {
			// A row whose effective WHERE key equals its NEW key needs no
			// other row to vacate anything: source and destination both
			// moved this lineage to the same final key, so the apply is an
			// in-place update. The blanket count above registered the row
			// as its own prerequisite, which would deadlock the scheduler
			// into a false cycle.
			indegree[i]--
		}
	}
	less := func(a, b int) bool {
		if newKeyStrs[a] != newKeyStrs[b] {
			return newKeyStrs[a] < newKeyStrs[b]
		}
		return a < b
	}
	// Push-driven Kahn pass over the same edges: the ready set is seeded
	// once and each least-ready row pops from a min-heap ordered by the
	// same deterministic comparator, so a batch of N rows costs O(N log N)
	// heap operations instead of N full rescans of the remaining set. The
	// yielded order is identical: both forms always take the least element
	// of the same ready set at every step.
	ready := &pkMoveFoldHeap{less: less}
	for i := range rowCnt {
		if indegree[i] == 0 {
			heap.Push(ready, i)
		}
	}
	rank := make([]uint64, rowCnt)
	pos := 0
	popped := 0
	for ready.Len() > 0 {
		picked := heap.Pop(ready).(int)
		popped++
		rank[picked] = uint64(pos)
		pos++
		// Emitting `picked` vacates its OLD key: every move landing on
		// that key (its NEW key) loses one unapplied prerequisite. The
		// row itself is never its own prerequisite (see the count above),
		// so a same-key row must not be decremented here either.
		for _, y := range newKeyIdx[oldKeyStrs[picked]] {
			if y == picked {
				continue
			}
			indegree[y]--
			if indegree[y] == 0 {
				heap.Push(ready, y)
			}
		}
	}
	if popped != rowCnt {
		// A key-rotation cycle cannot be replayed as in-place native
		// UPDATEs, and the fold-plan pass already aborts cyclic plans
		// before emission — residue here means a producer bug.
		return nil, moerr.NewInternalErrorNoCtxf(
			"data branch key-moving update reached apply with a cyclic plan (%d rows)", rowCnt-popped)
	}
	return rank, nil
}

// removeKeyMovingOldKeys mirrors batch.Shrink(removed, true) for the sidecar
// old-key list of a key-moving update batch: conflict suppression drops rows
// from the batch, and the old keys must drop the same positions to stay
// aligned with the rows they describe.
func removeKeyMovingOldKeys(oldKeys [][]any, removed []int64) [][]any {
	drop := make(map[int64]struct{}, len(removed))
	for _, s := range removed {
		drop[s] = struct{}{}
	}
	kept := make([][]any, 0, len(oldKeys)-len(removed))
	for i, k := range oldKeys {
		if _, ok := drop[int64(i)]; !ok {
			kept = append(kept, k)
		}
	}
	return kept
}

// removeKeyMovingBoolFlags is removeKeyMovingOldKeys for the bool sidecars
// (victim authorization, unchanged-occupant flags): the same row positions
// drop so every sidecar stays aligned with the batch it describes.
func removeKeyMovingBoolFlags(flags []bool, removed []int64) []bool {
	if len(flags) == 0 {
		return flags
	}
	drop := make(map[int64]struct{}, len(removed))
	for _, s := range removed {
		drop[s] = struct{}{}
	}
	kept := make([]bool, 0, len(flags)-len(removed))
	for i, f := range flags {
		if _, ok := drop[int64(i)]; !ok {
			kept = append(kept, f)
		}
	}
	return kept
}

// keepKeyMovingBoolFlags mirrors batch.Shrink(keep, false) for the bool
// sidecars: only the listed row positions survive. Positions past the
// slice (absent authorization) keep the fail-closed zero value.
func keepKeyMovingBoolFlags(flags []bool, keep []int64) []bool {
	if len(flags) == 0 {
		return flags
	}
	kept := make([]bool, 0, len(keep))
	for _, s := range keep {
		if int(s) < len(flags) {
			kept = append(kept, flags[s])
		} else {
			kept = append(kept, false)
		}
	}
	return kept
}

// cascadeKeyMovingFoldKeep extends an initial folded-move drop set to a
// fixpoint over move dependencies. A folded move lands at the key the
// source vacated; when SKIP removes a move as a whole (conflict at its
// landed key, or its delete half losing a destination conflict), the
// surviving destination row at the move's OLD key stays — the lineage no
// longer frees that key for any other folded move that lands there. A
// dependent move whose landed (new) key equals a dropped move's old key
// must drop too, transitively, or its authorized victim delete would
// erase the very destination row the clause kept and its UPDATE would
// rewrite that lineage onto the kept row (r24 review F1). The old-key
// and new-key string forms are interchangeable here, exactly as the
// skippedKeyMovingOldKeys pairing above already assumes.
//
// cascadeKeyMovingFoldKeyOps counts the key-encoding operations
// cascadeKeyMovingFoldKeep performs (old-key formats plus new-key
// extractions) so the dependency-closure regression can bound the work
// deterministically instead of timing it (r54 review F4).
var cascadeKeyMovingFoldKeyOps atomic.Uint64

// Returns the surviving row indices in ascending order. initialDrops may
// name any folded-row positions; dependents are computed against the
// rows that still survive after each round.
func cascadeKeyMovingFoldKeep(
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	oldKeys [][]any,
	initialDrops []int64,
) (keep []int64, err error) {
	rowCount := bat.RowCount()
	dropped := make(map[int64]bool, len(initialDrops))
	formatOld := func(rowIdx int) (string, error) {
		cascadeKeyMovingFoldKeyOps.Add(1)
		if rowIdx >= len(oldKeys) || oldKeys[rowIdx] == nil {
			return "", moerr.NewInternalErrorNoCtxf(
				"data branch key-moving update row %d misses its old key", rowIdx)
		}
		return formatPKValuesAsString(ses, tblStuff, oldKeys[rowIdx])
	}
	vacated := make([]string, 0, len(initialDrops))
	seenVacated := make(map[string]struct{}, len(initialDrops))
	enqueueVacated := func(key string) {
		if _, ok := seenVacated[key]; ok {
			return
		}
		seenVacated[key] = struct{}{}
		vacated = append(vacated, key)
	}
	for _, s := range initialDrops {
		dropped[s] = true
		key, keyErr := formatOld(int(s))
		if keyErr != nil {
			return nil, keyErr
		}
		enqueueVacated(key)
	}
	// One extraction pass records where every still-surviving move LANDS:
	// reverse dependencies never change as rows drop, so the closure
	// below walks each vacated key once instead of rescanning the whole
	// batch after every dropped move (r54 review F4: an ascending chain
	// whose last move drops used to cost ~N(N-1)/2 key extractions).
	landedIndex := make(map[string][]int64, rowCount)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		if dropped[int64(rowIdx)] {
			continue
		}
		cascadeKeyMovingFoldKeyOps.Add(1)
		newKey, keyErr := extractPKAsString(ses, tblStuff, bat, rowIdx)
		if keyErr != nil {
			return nil, keyErr
		}
		landedIndex[newKey] = append(landedIndex[newKey], int64(rowIdx))
	}
	for head := 0; head < len(vacated); head++ {
		for _, rowIdx := range landedIndex[vacated[head]] {
			if dropped[rowIdx] {
				continue
			}
			dropped[rowIdx] = true
			key, keyErr := formatOld(int(rowIdx))
			if keyErr != nil {
				return nil, keyErr
			}
			enqueueVacated(key)
		}
	}
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		if !dropped[int64(rowIdx)] {
			keep = append(keep, int64(rowIdx))
		}
	}
	return keep, nil
}

// shrinkKeyMovingFoldCascade shrinks a folded key-moving insert batch and
// every aligned sidecar by the clause-driven drop set extended through
// move dependencies (cascadeKeyMovingFoldKeep). Returns false when no row
// survives, so the caller can release the batch and stop processing it.
func shrinkKeyMovingFoldCascade(
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	initialDrops []int64,
) (batchWithKind, bool, error) {
	keep, err := cascadeKeyMovingFoldKeep(
		ses, tblStuff, wrapped.batch, wrapped.keyMovingOldKeys, initialDrops)
	if err != nil {
		return wrapped, false, err
	}
	if len(keep) == 0 {
		wrapped.batch.Shrink(keep, false)
		wrapped.keyMovingOldKeys = nil
		wrapped.keyMovingWhereKeys = nil
		wrapped.keyMovingVictimAuth = nil
		wrapped.keyMovingUnchangedOcc = nil
		return wrapped, false, nil
	}
	wrapped.batch.Shrink(keep, false)
	wrapped.keyMovingOldKeys = keepKeyMovingOldKeys(wrapped.keyMovingOldKeys, keep)
	if len(wrapped.keyMovingWhereKeys) != 0 {
		wrapped.keyMovingWhereKeys = keepKeyMovingOldKeys(wrapped.keyMovingWhereKeys, keep)
	}
	wrapped.keyMovingVictimAuth = keepKeyMovingBoolFlags(wrapped.keyMovingVictimAuth, keep)
	wrapped.keyMovingUnchangedOcc = keepKeyMovingBoolFlags(wrapped.keyMovingUnchangedOcc, keep)
	return wrapped, true, nil
}

// keepKeyMovingOldKeys is keepKeyMovingBoolFlags for the old-key/where-key
// sidecars: only the listed row positions survive.
func keepKeyMovingOldKeys(oldKeys [][]any, keep []int64) [][]any {
	if len(oldKeys) == 0 {
		return oldKeys
	}
	kept := make([][]any, 0, len(keep))
	for _, s := range keep {
		if int(s) < len(oldKeys) {
			kept = append(kept, oldKeys[s])
		} else {
			kept = append(kept, nil)
		}
	}
	return kept
}

func appendTupleToBat(ses *Session, bat *batch.Batch, tuple types.Tuple, tblStuff tableStuff) error {
	if bat.VectorCount() != len(tblStuff.def.colNames) {
		return moerr.NewInternalErrorNoCtxf(
			"unexpected batch width %d for table %s with %d physical columns",
			bat.VectorCount(), tblStuff.tarRel.GetTableName(), len(tblStuff.def.colNames),
		)
	}
	for colIdx := range tblStuff.def.colNames {
		val, err := getTupleColumnValue(tuple, tblStuff, colIdx)
		if err != nil {
			return err
		}
		vec := bat.Vecs[colIdx]
		if err := appendTupleValueToVector(vec, val, ses.proc.Mp()); err != nil {
			return err
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func getTupleColumnValue(tuple types.Tuple, tblStuff tableStuff, colIdx int) (any, error) {
	totalColCnt := len(tblStuff.def.colTypes)
	if colIdx < 0 || colIdx >= totalColCnt {
		return nil, moerr.NewInternalErrorNoCtxf(
			"column index %d out of range for table %s with %d columns",
			colIdx, tblStuff.tarRel.GetTableName(), totalColCnt,
		)
	}
	switch len(tuple) {
	case totalColCnt:
		return normalizeTupleColumnValue(tuple[colIdx], tblStuff.def.colTypes[colIdx])
	case totalColCnt + 1, totalColCnt + 2:
		return normalizeTupleColumnValue(tuple[colIdx+1], tblStuff.def.colTypes[colIdx])
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected tuple width %d for table %s with %d visible columns",
			len(tuple), tblStuff.tarRel.GetTableName(), len(tblStuff.def.visibleIdxes),
		)
	}
}

func fakePKTupleKeyIdxes(tblStuff tableStuff) []int {
	keyColIdxes := tblStuff.def.commonVisibleIdxes
	if len(keyColIdxes) == 0 {
		keyColIdxes = tblStuff.def.visibleIdxes
	}

	idxes := make([]int, len(keyColIdxes))
	for i, colIdx := range keyColIdxes {
		idxes[i] = colIdx + 1
	}
	return idxes
}

func batchSampleRowsForLog(bat *batch.Batch, limit int) []string {
	if bat == nil || bat.RowCount() == 0 || limit <= 0 {
		return nil
	}
	if limit > bat.RowCount() {
		limit = bat.RowCount()
	}
	rows := make([]string, 0, limit)
	for rowIdx := 0; rowIdx < limit; rowIdx++ {
		cols := make([]string, 0, bat.VectorCount())
		for _, vec := range bat.Vecs {
			if vec == nil {
				cols = append(cols, "<nil>")
				continue
			}
			if rowIdx >= vec.Length() {
				cols = append(cols, "<oob>")
				continue
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				cols = append(cols, "NULL")
				continue
			}
			cols = append(cols, fmt.Sprintf("%v", types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)))
		}
		rows = append(rows, strings.Join(cols, ", "))
	}
	return rows
}

func validateLeadingRowID(side, tableName string, isTombstone bool, bat *batch.Batch) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	buildFields := func() []zap.Field {
		return []zap.Field{
			zap.String("side", side),
			zap.String("table-name", tableName),
			zap.Bool("tombstone", isTombstone),
			zap.Int("row-cnt", bat.RowCount()),
			zap.Int("vec-cnt", bat.VectorCount()),
			zap.Strings("attrs", append([]string(nil), bat.Attrs...)),
			zap.Strings("samples", batchSampleRowsForLog(bat, 4)),
		}
	}
	fail := func(msg string, extra ...zap.Field) error {
		logutil.Error(msg, append(buildFields(), extra...)...)
		return moerr.NewInternalErrorNoCtx(msg)
	}
	if bat.VectorCount() == 0 || bat.Vecs[0] == nil {
		return fail("DataBranch-CollectChanges-MissingRowID")
	}
	rowIDVec := bat.Vecs[0]
	if rowIDVec.GetType().Oid != types.T_Rowid {
		return fail("DataBranch-CollectChanges-InvalidRowIDVector",
			zap.String("rowid-vec-type", rowIDVec.GetType().String()),
		)
	}
	if rowIDVec.Length() != bat.RowCount() {
		return fail("DataBranch-CollectChanges-RowIDLenMismatch",
			zap.Int("rowid-vec-len", rowIDVec.Length()),
		)
	}
	rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](rowIDVec)
	for i := range rowIDs {
		if rowIDVec.GetNulls().Contains(uint64(i)) {
			return fail("DataBranch-CollectChanges-NullRowID", zap.Int("row-idx", i))
		}
		if rowIDs[i].EQ(&types.EmptyRowid) || rowIDs[i].BorrowBlockID().IsEmpty() {
			return fail("DataBranch-CollectChanges-InvalidRowID",
				zap.Int("row-idx", i),
				zap.String("rowid", rowIDs[i].String()),
			)
		}
	}
	return nil
}

func appendTupleValueToVector(vec *vector.Vector, val any, mp *mpool.MPool) error {
	val, err := normalizeTupleColumnValue(val, *vec.GetType())
	if err != nil {
		return err
	}
	if val == nil {
		return vector.AppendNull(vec, mp)
	}
	if raw, ok := val.([]byte); ok {
		return vector.AppendBytes(vec, raw, false, mp)
	}
	return vector.AppendAny(vec, val, false, mp)
}

func normalizeTupleColumnValue(val any, typ types.Type) (any, error) {
	if val == nil {
		return nil, nil
	}
	raw, ok := val.([]byte)
	if !ok {
		return val, nil
	}
	if typ.IsVarlen() {
		return raw, nil
	}
	fixedLen := typ.Oid.FixedLength()
	if fixedLen < 0 || len(raw) != fixedLen {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected byte slice for fixed-width column type %s",
			typ.String(),
		)
	}
	switch typ.Oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_uuid, types.T_TS, types.T_Rowid, types.T_Blockid,
		types.T_enum:
		return types.DecodeValue(raw, typ.Oid), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected byte slice for fixed-width column type %s",
			typ.String(),
		)
	}
}

func checkConflictAndAppendToBat(
	ses *Session, copt compositeOption, tblStuff tableStuff,
	tarBat, baseBat *batch.Batch, tarTuple, baseTuple types.Tuple,
) (err2 error) {
	if copt.conflictOpt != nil {
		switch copt.conflictOpt.Opt {
		case tree.CONFLICT_FAIL:
			buf := acquireBuffer(tblStuff.bufPool)
			for i, idx := range tblStuff.def.pkColIdxes {
				var val any
				if val, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if err2 = formatValIntoString(ses, val, tblStuff.def.colTypes[idx], buf); err2 != nil {
					releaseBuffer(tblStuff.bufPool, buf)
					return err2
				}
				if i < len(tblStuff.def.pkColIdxes)-1 {
					buf.WriteString(",")
				}
			}

			msg := buf.String()
			releaseBuffer(tblStuff.bufPool, buf)
			return moerr.NewDataBranchConflictNoCtxf(
				"%s %s and %s %s on pk(%v) with different values",
				tblStuff.tarRel.GetTableName(), diffInsert,
				tblStuff.baseRel.GetTableName(), diffInsert,
				msg,
			)
		case tree.CONFLICT_SKIP:
			return
		case tree.CONFLICT_ACCEPT:
			// accept the tar's insert
			return appendTupleToBat(ses, tarBat, tarTuple, tblStuff)
		}
	} else {
		if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
			return err2
		}
		if err2 = appendTupleToBat(ses, baseBat, baseTuple, tblStuff); err2 != nil {
			return err2
		}
	}
	return
}

func diffDataHelper(
	ctx context.Context,
	ses *Session,
	copt compositeOption,
	tblStuff tableStuff,
	emit emitFunc,
	tarDataHashmap databranchutils.BranchHashmap,
	baseDataHashmap databranchutils.BranchHashmap,
) (err error) {

	parallelism := -1
	if copt.onlineApply {
		// The inline appender and its transaction have one execution owner.
		parallelism = 1
	}

	// if no pk, we cannot use the fake pk to probe.
	// must probe with full columns
	var migratedHashmaps []databranchutils.BranchHashmap
	defer func() {
		for _, h := range migratedHashmaps {
			if closeErr := h.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
		}
	}()

	if tblStuff.def.pkKind == fakeKind {
		var (
			keyIdxes   = fakePKTupleKeyIdxes(tblStuff)
			newHashmap databranchutils.BranchHashmap
		)

		if newHashmap, err = migrateAndCloseSourceHashmap(baseDataHashmap, keyIdxes, -1); err != nil {
			return err
		}
		baseDataHashmap = newHashmap
		migratedHashmaps = append(migratedHashmaps, baseDataHashmap)

		if newHashmap, err = migrateAndCloseSourceHashmap(tarDataHashmap, keyIdxes, -1); err != nil {
			return err
		}
		tarDataHashmap = newHashmap
		migratedHashmaps = append(migratedHashmaps, tarDataHashmap)
	}

	if err = tarDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2               error
			tarBat             *batch.Batch
			baseBat            *batch.Batch
			baseDeleteBat      *batch.Batch
			tarUpdateBat       *batch.Batch
			tarNativeUpdateBat *batch.Batch
			tarTuple           types.Tuple
			baseTuple          types.Tuple
			checkRet           databranchutils.GetResult
		)

		tarBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		baseBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
		defer func() {
			if tarUpdateBat != nil {
				tblStuff.retPool.releaseRetBatch(tarUpdateBat, false)
			}
			if tarNativeUpdateBat != nil {
				tblStuff.retPool.releaseRetBatch(tarNativeUpdateBat, false)
			}
		}()

		if err2 = cursor.ForEach(func(key []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tblStuff.def.pkKind == fakeKind {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}
			} else {
				if checkRet, err2 = baseDataHashmap.PopByEncodedKey(key, false); err2 != nil {
					return err2
				}
			}

			if !checkRet.Exists {
				if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
					return err2
				}

				if err2 = appendTupleToBat(ses, tarBat, tarTuple, tblStuff); err2 != nil {
					return err2
				}

			} else {
				// both has the key, we continue compare the left columns,
				// if all columns are equal, exactly the same row, ignore.
				if tblStuff.def.pkKind == fakeKind {
					// all columns already compared.
					// ignore
				} else {
					if tarTuple, _, err2 = tarDataHashmap.DecodeRow(row); err2 != nil {
						return err2
					}

					if baseTuple, _, err2 = baseDataHashmap.DecodeRow(checkRet.Rows[0]); err2 != nil {
						return err2
					}

					notSame := false
					compareIdxes := tblStuff.def.commonIdxes
					if len(compareIdxes) == 0 {
						compareIdxes = tblStuff.def.visibleIdxes // backward compat when commonIdxes not set
					}
					for _, idx := range compareIdxes {
						if slices.Index(tblStuff.def.pkColIdxes, idx) != -1 {
							// pk columns already compared
							continue
						}
						var tarVal, baseVal any
						if tarVal, err2 = getTupleColumnValue(tarTuple, tblStuff, idx); err2 != nil {
							return err2
						}
						if baseVal, err2 = getTupleColumnValue(baseTuple, tblStuff, idx); err2 != nil {
							return err2
						}

						if types.CompareValue(
							tarVal, baseVal,
						) != 0 {
							notSame = true
							break
						}
					}

					if notSame {
						if copt.conflictOpt != nil &&
							copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT &&
							copt.expandUpdate {
							if baseDeleteBat == nil {
								baseDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
							}
							requiresNativeUpdate, updateErr := dataBranchUpdateRequiresNativeUpdateFromTuples(
								tblStuff, tarTuple, baseTuple,
							)
							if updateErr != nil {
								return updateErr
							}
							updateBat := tarUpdateBat
							if requiresNativeUpdate {
								updateBat = tarNativeUpdateBat
							}
							if updateBat == nil {
								updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false)
								if requiresNativeUpdate {
									tarNativeUpdateBat = updateBat
								} else {
									tarUpdateBat = updateBat
								}
							}
							if err2 = appendTupleToBat(ses, baseDeleteBat, baseTuple, tblStuff); err2 != nil {
								return err2
							}
							if err2 = appendTupleToBat(ses, updateBat, tarTuple, tblStuff); err2 != nil {
								return err2
							}
						} else {
							// conflict happened
							if err2 = checkConflictAndAppendToBat(
								ses, copt, tblStuff, tarBat, baseBat, tarTuple, baseTuple,
							); err2 != nil {
								return err2
							}
						}
					}
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if baseDeleteBat != nil {
			if stop, err3 := emitBatch(emit, batchWithKind{
				batch:      baseDeleteBat,
				kind:       diffDelete,
				name:       tblStuff.baseRel.GetTableName(),
				side:       diffSideBase,
				fromUpdate: true,
			}, false, tblStuff.retPool); err3 != nil {
				return err3
			} else if stop {
				return nil
			}
		}

		if tarUpdateBat != nil {
			stop, err3 := emitBatch(emit, batchWithKind{
				batch:      tarUpdateBat,
				kind:       diffInsert,
				name:       tblStuff.tarRel.GetTableName(),
				side:       diffSideTarget,
				fromUpdate: true,
			}, false, tblStuff.retPool)
			tarUpdateBat = nil
			if err3 != nil {
				return err3
			} else if stop {
				return nil
			}
		}

		if tarNativeUpdateBat != nil {
			stop, err3 := emitBatch(emit, batchWithKind{
				batch:                tarNativeUpdateBat,
				kind:                 diffInsert,
				name:                 tblStuff.tarRel.GetTableName(),
				side:                 diffSideTarget,
				fromUpdate:           true,
				requiresNativeUpdate: true,
			}, false, tblStuff.retPool)
			tarNativeUpdateBat = nil
			if err3 != nil {
				return err3
			} else if stop {
				return nil
			}
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: tarBat,
			kind:  diffInsert,
			name:  tblStuff.tarRel.GetTableName(),
			side:  diffSideTarget,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: baseBat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
			side:  diffSideBase,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}

		return nil
	}, parallelism); err != nil {
		return
	}

	if copt.conflictOpt != nil {
		if !copt.outputSQL {
			// merge doesnt need the base data
			return nil
		}
	}

	if err = baseDataHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			bat   *batch.Batch
		)

		bat = tblStuff.retPool.acquireRetBatch(tblStuff, false)

		if err2 = cursor.ForEach(func(_ []byte, row []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if tuple, _, err2 = baseDataHashmap.DecodeRow(row); err2 != nil {
				return err2
			}

			if err2 = appendTupleToBat(ses, bat, tuple, tblStuff); err2 != nil {
				return err2
			}
			return nil
		}); err2 != nil {
			return err2
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if stop, err3 := emitBatch(emit, batchWithKind{
			batch: bat,
			kind:  diffInsert,
			name:  tblStuff.baseRel.GetTableName(),
			side:  diffSideBase,
		}, false, tblStuff.retPool); err3 != nil {
			return err3
		} else if stop {
			return nil
		}
		return nil
	}, parallelism); err != nil {
		return
	}

	return nil
}

func migrateAndCloseSourceHashmap(
	source databranchutils.BranchHashmap,
	keyIdxes []int,
	parallelism int,
) (databranchutils.BranchHashmap, error) {
	newHashmap, err := source.Migrate(keyIdxes, parallelism)
	if err != nil {
		return nil, err
	}
	if err = source.Close(); err != nil {
		_ = newHashmap.Close()
		return nil, err
	}
	return newHashmap, nil
}

func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	tblStuff *tableStuff,
	handles []engine.ChangesHandle,
	side string,
	pickKeyHashmap databranchutils.BranchHashmap,
	wantPkMoveEvidence bool,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
	pkMoveEvidence *pkMoveStreamEvidence,
	err error,
) {
	var (
		atomicErr          atomic.Value
		dataBat            *batch.Batch
		tombstoneBat       *batch.Batch
		wg                 sync.WaitGroup
		totalRows          int64
		totalBytes         int64
		totalTombstoneRows int64
		start              = time.Now()
	)

	defer func() {
		wg.Wait()

		if dataBat != nil {
			dataBat.Clean(mp)
		}

		if tombstoneBat != nil {
			tombstoneBat.Clean(mp)
		}

		fields := []zap.Field{
			zap.String("side", side),
			zap.Int("handle-cnt", len(handles)),
			zap.Int64("data-row-cnt", totalRows),
			zap.Int64("data-bytes", totalBytes),
			zap.Int64("tombstone-row-cnt", totalTombstoneRows),
			zap.Duration("duration", time.Since(start)),
		}
		if dataHashmap != nil {
			fields = append(fields,
				zap.Int64("data-item-cnt", dataHashmap.ItemCount()),
				zap.Int("data-shard-cnt", dataHashmap.ShardCount()),
			)
		}
		if tombstoneHashmap != nil {
			fields = append(fields,
				zap.Int64("tombstone-item-cnt", tombstoneHashmap.ItemCount()),
				zap.Int("tombstone-shard-cnt", tombstoneHashmap.ShardCount()),
			)
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			logutil.Warn("DataBranch-Hashmap-Build-Error", fields...)
			return
		}
		logutil.Info("DataBranch-Hashmap-Build-Done", fields...)
	}()

	if dataHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	if tombstoneHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	); err != nil {
		return
	}

	putVectors := func(bat *batch.Batch, isTombstone bool) error {
		if bat == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			bat.Clean(mp)
			return ctx.Err()
		default:
		}

		wg.Add(1)

		if err = tblStuff.worker.Submit(func() {
			defer wg.Done()
			defer bat.Clean(mp)

			ll := bat.VectorCount()
			var taskErr error
			select {
			case <-ctx.Done():
				taskErr = ctx.Err()
			default:
				// When a pickKeyHashmap is provided (PICK operation), filter
				// the batch to keep only rows whose PK exists in the hashmap.
				// This moves the precise PK filtering from the consumer
				// (appendPickedBatchRows) into the producer, reducing the
				// data that flows through hashDiff and the channel.
				if pickKeyHashmap != nil {
					pkIdx := tblStuff.def.pkColIdx
					if isTombstone {
						// Data branch keeps rowid in Vec[0] while building the
						// hashmaps, so tombstone PK lives at Vec[1].
						pkIdx = 1
					} else {
						pkIdx++
					}
					var results []databranchutils.GetResult
					results, taskErr = pickKeyHashmap.GetByVectors(
						[]*vector.Vector{bat.Vecs[pkIdx]},
					)
					if taskErr != nil {
						atomicErr.Store(taskErr)
						return
					}
					var sels []int64
					for i, r := range results {
						if r.Exists {
							sels = append(sels, int64(i))
						}
					}
					if len(sels) == 0 {
						return // all rows filtered out
					}
					if len(sels) < bat.RowCount() {
						bat.Shrink(sels, false)
						ll = bat.VectorCount()
					}
				}

				if isTombstone {
					taskErr = tombstoneHashmap.PutByVectors(bat.Vecs[:ll], []int{1})
				} else {
					taskErr = dataHashmap.PutByVectors(bat.Vecs[:ll], []int{tblStuff.def.pkColIdx + 1})
				}
			}
			if taskErr != nil {
				atomicErr.Store(taskErr)
			}
		}); err != nil {
			wg.Done()
			bat.Clean(mp)
		}

		return err
	}

	// Synthetic-window certification (r53 D156(E) amendment): each
	// drained handle is certified against the three observable serving
	// modes whose rows carry the window's END instant instead of the
	// rows' recorded commit instants — (a) an empty lower bound routed
	// the drain to the end-stamping checkpoint state reader (recorded
	// by constructChangeHandles), (b) a batch served under the Snapshot
	// hint, (c) a visible-state recovery substitution counted by the
	// apply's recovery resources across this handle's CONSTRUCTION or
	// drain — the dispatcher can install the end-stamping reader while
	// the handle splits its range, before any drain baseline exists
	// (recorded per window by constructChangeHandles), and mid-drain
	// splits still surface as a counter delta across the drain. A
	// certified window's end instant is tainted: instant-bearing
	// extracts equal to it never bind pk-move provenance, while the
	// window's rows still serve net-effect replay unchanged. Window
	// metadata is appended under the same condition that appends the
	// handle, so the slices stay index-aligned.
	windowEnds := tblStuff.tarWindowEnds
	windowFromEmpty := tblStuff.tarWindowFromEmpty
	windowSubstConstruct := tblStuff.tarWindowSubstConstruct
	if side == "base" {
		windowEnds = tblStuff.baseWindowEnds
		windowFromEmpty = tblStuff.baseWindowFromEmpty
		windowSubstConstruct = tblStuff.baseWindowSubstConstruct
	}
	tainted := make(map[types.TS]struct{})
	for handleIdx, handle := range handles {
		substBefore := tblStuff.recoveryResources.recoverySubstitutions()
		snapshotHinted := false
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
			}
			var hint engine.ChangesHandle_Hint
			if dataBat, tombstoneBat, hint, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
			}
			if hint == engine.ChangesHandle_Snapshot {
				snapshotHinted = true
			}

			tableName := ""
			if side == "base" && tblStuff.baseRel != nil {
				tableName = tblStuff.baseRel.GetTableName()
			} else if side == "target" && tblStuff.tarRel != nil {
				tableName = tblStuff.tarRel.GetTableName()
			}
			if err = validateLeadingRowID(side, tableName, false, dataBat); err != nil {
				return
			}
			if err = validateLeadingRowID(side, tableName, true, tombstoneBat); err != nil {
				return
			}

			dataBat = projectBaseBatchToTargetIfNeeded(side, dataBat, tblStuff, mp)

			if dataBat != nil && dataBat.RowCount() > 0 {
				totalRows += int64(dataBat.RowCount())
				totalBytes += int64(dataBat.Size())
			}
			if tombstoneBat != nil && tombstoneBat.RowCount() > 0 {
				totalTombstoneRows += int64(tombstoneBat.RowCount())
			}

			if atomicErr.Load() != nil {
				err = atomicErr.Load().(error)
				return
			}

			if err = putVectors(dataBat, false); err != nil {
				return
			}

			if err = putVectors(tombstoneBat, true); err != nil {
				return
			}
		}
		if handleIdx < len(windowEnds) {
			synthetic := snapshotHinted ||
				(handleIdx < len(windowSubstConstruct) && windowSubstConstruct[handleIdx]) ||
				tblStuff.recoveryResources.recoverySubstitutions() != substBefore ||
				(handleIdx < len(windowFromEmpty) && windowFromEmpty[handleIdx])
			if synthetic {
				tainted[windowEnds[handleIdx]] = struct{}{}
			}
		}
	}

	wg.Wait()

	if atomicErr.Load() != nil {
		err = atomicErr.Load().(error)
	} else {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}

	if err != nil {
		return
	}

	if tombstoneHashmap.ItemCount() == 0 && dataHashmap.ItemCount() == 0 {
		return
	}

	if tblStuff.maxTombstoneBatchCnt == 0 {
		rowBytes := defaultRowBytes
		if totalRows > 0 {
			rowBytes = totalBytes / totalRows
			if rowBytes <= 0 {
				rowBytes = defaultRowBytes
			}
		}
		rowBytes = rowBytes * tombstoneRowMult / tombstoneRowDiv

		available := tblStuff.hashmapAllocator.Available() / 3

		shardCount := 0
		if tombstoneHashmap != nil {
			shardCount = tombstoneHashmap.ShardCount()
		} else if dataHashmap != nil {
			shardCount = dataHashmap.ShardCount()
		}

		maxByMem := int(available / int64(shardCount) / rowBytes)
		if maxByMem < 1 {
			maxByMem = 1
		} else if maxByMem > maxSqlBatchCnt {
			maxByMem = maxSqlBatchCnt
		}

		tblStuff.maxTombstoneBatchCnt = maxByMem

	}

	// For fake PK tables, each row version has a unique fake PK, so there are
	// no same-key duplicates. However, the dedup phase also pairs tombstones
	// with their corresponding data entries. For example, INSERT(1,1) with
	// FK_old then UPDATE→(1,11) produces tombstone FK_old + stale data FK_old
	// + live data FK_new. Without cleanup, the stale data (1,1) would survive
	// and appear in the diff output with the wrong value.

	/*
		case 1:
			     insert    update    update     update
				   pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
			1.1. the last one is an update: only keep the last update op
			1.2. the last one is a deletion: only keep the last deletion op

		case 2:
			           insert    update     update
				   		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
		     2.1. the last one is an update: only keep the last update op
		     2.2. the last one is a deletion: only keep the last deletion op

		case 3:
				 insert    delete    update     update
		           pk1		 pk1       pk1        pk1   ....
				|------|-------------------------------------|
		               |           collect range             |
				3.1. the last one is an update: only keep the last update op
				3.2. the last one is a deletion: only keep the last deletion op
	*/

	// Preserve identity evidence for pk-move chain walks BEFORE the
	// reconciliation below nets born-and-died generations out of the maps:
	// a version inserted and deleted inside this window ends with
	// staleDataCount >= totalTombstones and disappears entirely (data row
	// popped, tombstone consumed), which is the correct net effect for
	// plain replay but erases the tombstone+birth record a
	// cross-transaction chain hop binds to. Capturing here is one extra
	// pass over the in-window tombstones, gated on the diff being
	// apply-capable; the gated-off consumers (reads, fake-pk tables)
	// never consult links.
	if wantPkMoveEvidence && tombstoneHashmap.ItemCount() != 0 {
		pkMoveEvidence = &pkMoveStreamEvidence{tainted: tainted}
		if pkMoveEvidence.departed, pkMoveEvidence.tombs, pkMoveEvidence.capped, err = capturePkMoveStreamEvidence(
			ctx, dataHashmap, tombstoneHashmap, -1,
		); err != nil {
			return
		}
	}

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2    error
			err3    error
			maxTs   types.TS
			maxIdx  int
			tuple   types.Tuple
			dataRet databranchutils.GetResult
		)
		if err2 = cursor.ForEach(func(key []byte, _ []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var tombstoneRet databranchutils.GetResult
			if tombstoneRet, err3 = cursor.GetByEncodedKey(key); err3 != nil {
				return err3
			}
			if len(tombstoneRet.Rows) == 0 {
				return nil
			}

			maxIdx = 0
			maxTs = types.MinTs()
			for i := 0; i < len(tombstoneRet.Rows); i++ {
				if tuple, _, err3 = tombstoneHashmap.DecodeRow(tombstoneRet.Rows[i]); err3 != nil {
					return err3
				}
				cur := types.TS(tuple[len(tuple)-1].([]uint8))
				if cur.GT(&maxTs) {
					maxTs = cur
					maxIdx = i
				}
			}

			totalTombstones := len(tombstoneRet.Rows)

			// Remove stale tombstone entries (only needed when multiple exist)
			if len(tombstoneRet.Rows) > 1 {
				for i := range tombstoneRet.Rows {
					if i != maxIdx {
						if _, err3 = cursor.PopByEncodedKeyValue(
							key, tombstoneRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
					}
				}
			}

			// Remove data entries older than the latest tombstone.
			// Count how many stale data entries are removed — each one
			// "consumes" one tombstone from an intermediate operation
			// (e.g., insert→update creates 1 tombstone + 1 stale data entry).
			staleDataCount := 0
			if dataRet, err3 = dataHashmap.GetByEncodedKey(key); err3 != nil {
				return err3
			}

			if dataRet.Exists {
				for i := range dataRet.Rows {
					if tuple, _, err3 = dataHashmap.DecodeRow(dataRet.Rows[i]); err3 != nil {
						return err3
					}
					cur := types.TS(tuple[len(tuple)-1].([]uint8))
					if cur.LT(&maxTs) {
						if _, err3 = dataHashmap.PopByEncodedKeyValue(
							key, dataRet.Rows[i], false,
						); err3 != nil {
							return err3
						}
						staleDataCount++
					}
				}
			}

			// If all tombstones are consumed by stale data entries, the net
			// effect is a pure INSERT (not a delete). Remove the remaining
			// tombstone so that findDeleteAndUpdateBat doesn't incorrectly
			// cancel the surviving data entry.
			//
			// Example: insert(1,1) → update→(1,11) produces 1 tombstone +
			// 1 stale data (1,1) + 1 live data (1,11). staleDataCount=1
			// matches totalTombstones=1, so the tombstone is intermediate.
			//
			// Contrast: update LCA row (0,0)→(0,10) produces 1 tombstone +
			// 0 stale data (the original (0,0) is outside the change range).
			// staleDataCount=0 < totalTombstones=1, so the tombstone stays
			// and findDeleteAndUpdateBat correctly detects UPDATE on the LCA.
			if staleDataCount >= totalTombstones {
				if _, err3 = cursor.PopByEncodedKeyValue(
					key, tombstoneRet.Rows[maxIdx], false,
				); err3 != nil {
					return err3
				}
			}
			return nil
		}); err2 != nil {
			return err2
		}
		return nil
	}, -1); err != nil {
		return nil, nil, nil, err
	}

	return
}

// projectBaseBatchToTarget projects a base-side data batch to match the target
// column layout. Ownership of moved vectors is transferred to the returned
// batch; any vectors left behind on baseBat are cleaned before returning.
func projectBaseBatchToTarget(
	baseBat *batch.Batch,
	tblStuff *tableStuff,
	mp *mpool.MPool,
) *batch.Batch {
	return projectDataBranchBatchToTarget(
		baseBat, tblStuff, tblStuff.def.baseColToTarIdx, mp,
	)
}

func projectBaseBatchToTargetIfNeeded(
	side string,
	dataBat *batch.Batch,
	tblStuff *tableStuff,
	mp *mpool.MPool,
) *batch.Batch {
	if side != "base" || dataBat == nil || dataBat.RowCount() == 0 ||
		dataBranchBatchHasTargetLayout(dataBat, tblStuff) {
		return dataBat
	}
	// Legacy table metadata without a source-to-target mapping has no evidence
	// of a physical layout difference. Preserve the prior path.
	if len(tblStuff.def.baseColToTarIdx) == 0 {
		return dataBat
	}
	if !dataBranchNeedsHistoricalProjection(
		tblStuff.def.baseColToTarIdx,
		len(tblStuff.def.colNames),
	) {
		return dataBat
	}
	return projectBaseBatchToTarget(dataBat, tblStuff, mp)
}

func dataBranchSourceColToTargetIdx(
	sourceDef, targetDef *plan2.TableDef,
	targetColNames []string,
	targetOnlyIdxes []int,
) ([]int, error) {
	if sourceDef == nil || targetDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing schema for historical data branch projection")
	}
	if err := checkDataBranchPrimaryKeyCompatibilityWithResolver(
		targetDef,
		sourceDef,
		func(targetCol *plan2.ColDef) *plan2.ColDef {
			return dataBranchEndpointColumnDef(sourceDef, targetCol)
		},
	); err != nil {
		return nil, moerr.NewNotSupportedNoCtxf(
			"historical data branch primary key is incompatible with the endpoint schema: %s",
			err.Error(),
		)
	}
	if sourceDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		sourceNames := make([]string, 0, len(sourceDef.Cols))
		targetNames := make([]string, 0, len(targetDef.Cols))
		for _, col := range sourceDef.Cols {
			if col.Name != catalog.Row_ID {
				sourceNames = append(sourceNames, col.Name)
			}
		}
		for _, col := range targetDef.Cols {
			if col.Name != catalog.Row_ID {
				targetNames = append(targetNames, col.Name)
			}
		}
		if len(sourceNames) != len(targetNames) {
			return nil, moerr.NewNotSupportedNoCtx(
				"historical data branch fake primary key schema differs from the endpoint schema",
			)
		}
		for i := range sourceNames {
			if !strings.EqualFold(sourceNames[i], targetNames[i]) {
				return nil, moerr.NewNotSupportedNoCtx(
					"historical data branch fake primary key schema differs from the endpoint schema",
				)
			}
		}
	}
	targetOnly := make(map[int]struct{}, len(targetOnlyIdxes))
	for _, idx := range targetOnlyIdxes {
		targetOnly[idx] = struct{}{}
	}
	mapping := make([]int, 0, len(sourceDef.Cols))
	for _, col := range sourceDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}
		targetCol := dataBranchEndpointColumnDef(targetDef, col)
		targetIdx := -1
		if targetCol != nil {
			targetIdx = dataBranchColumnIndexByName(targetColNames, targetCol.Name)
		}
		mapping = append(mapping, targetIdx)
		if targetIdx < 0 {
			continue
		}
		derivedCompositePK :=
			isDataBranchDerivedCompositePKColumn(sourceDef, targetDef, col, targetCol)
		sameType := col.Typ.Id == targetCol.Typ.Id &&
			(dataBranchColumnTypeAttributesEqual(col.Typ, targetCol.Typ) || derivedCompositePK)
		if !sameType {
			if _, isTargetOnly := targetOnly[targetIdx]; isTargetOnly {
				// This column does not exist on the base endpoint and is excluded
				// from comparison and MERGE apply. Its old physical representation
				// cannot be moved into the endpoint-typed batch; project it as NULL
				// and let endpoint hydration restore a current value when needed.
				mapping[len(mapping)-1] = -1
				continue
			}
			return nil, moerr.NewNotSupportedNoCtxf(
				"historical data branch column %s has a different type from the endpoint schema",
				col.Name,
			)
		}
	}
	return mapping, nil
}

// isDataBranchDerivedCompositePKColumn reports whether the two columns are the
// hidden serialization of the same logical composite primary key. COPY ALTER
// rebuilds this derived column and may assign it a new Seqnum even though the
// component-column identities, which schema reconciliation has already
// validated, remain unchanged.
func isDataBranchDerivedCompositePKColumn(
	sourceDef, targetDef *plan2.TableDef,
	sourceCol, targetCol *plan2.ColDef,
) bool {
	if sourceDef == nil || targetDef == nil || sourceCol == nil || targetCol == nil ||
		sourceDef.Pkey == nil || targetDef.Pkey == nil ||
		!strings.EqualFold(sourceDef.Pkey.PkeyColName, catalog.CPrimaryKeyColName) ||
		!strings.EqualFold(targetDef.Pkey.PkeyColName, catalog.CPrimaryKeyColName) ||
		!strings.EqualFold(sourceCol.Name, catalog.CPrimaryKeyColName) ||
		!strings.EqualFold(targetCol.Name, catalog.CPrimaryKeyColName) ||
		len(sourceDef.Pkey.Names) != len(targetDef.Pkey.Names) {
		return false
	}
	for i, sourceName := range sourceDef.Pkey.Names {
		targetName := targetDef.Pkey.Names[i]
		if !strings.EqualFold(sourceName, targetName) {
			return false
		}
		sourcePart := dataBranchColumnDefByName(sourceDef, sourceName)
		targetPart := dataBranchColumnDefByName(targetDef, targetName)
		if sourcePart == nil || targetPart == nil ||
			sourcePart.Typ.Id != targetPart.Typ.Id ||
			!dataBranchColumnTypeAttributesEqual(sourcePart.Typ, targetPart.Typ) ||
			sourcePart.NotNull != targetPart.NotNull {
			return false
		}
	}
	return true
}

// dataBranchNeedsHistoricalProjection reports whether change rows from an
// older physical table generation have a different data-column layout from
// the endpoint. A different table ID alone can also mean an ordinary branch
// ancestor with an identical schema; projecting and hydrating those rows would
// collapse intermediate UPDATE versions to endpoint values.
func dataBranchNeedsHistoricalProjection(sourceToTarget []int, targetColCount int) bool {
	if len(sourceToTarget) != targetColCount {
		return true
	}
	for sourceIdx, targetIdx := range sourceToTarget {
		if sourceIdx != targetIdx {
			return true
		}
	}
	return false
}

func dataBranchTargetLayoutAttrs(tblStuff *tableStuff, hasCommitTS bool) []string {
	attrs := make([]string, 0, len(tblStuff.def.colNames)+2)
	attrs = append(attrs, catalog.Row_ID)
	attrs = append(attrs, tblStuff.def.colNames...)
	if hasCommitTS {
		attrs = append(attrs, objectio.DefaultCommitTS_Attr)
	}
	return attrs
}

func dataBranchBatchHasTargetLayout(bat *batch.Batch, tblStuff *tableStuff) bool {
	if bat == nil || len(bat.Attrs) != bat.VectorCount() {
		return false
	}
	hasCommitTS := bat.VectorCount() == len(tblStuff.def.colNames)+2
	if !hasCommitTS && bat.VectorCount() != len(tblStuff.def.colNames)+1 {
		return false
	}
	want := dataBranchTargetLayoutAttrs(tblStuff, hasCommitTS)
	for i := range want {
		if !strings.EqualFold(bat.Attrs[i], want[i]) {
			return false
		}
	}
	return true
}

func overlayDataBranchProbeResult(
	projected *batch.Batch,
	probe executor.Result,
	pkTargetIdx int,
	mp *mpool.MPool,
) (err error) {
	if projected == nil || projected.RowCount() == 0 {
		return nil
	}
	prepared := false
	probe.ReadRows(func(rowCount int, cols []*vector.Vector) bool {
		if len(cols) < 2 || pkTargetIdx < 0 || pkTargetIdx+1 >= len(cols) {
			err = moerr.NewInternalErrorNoCtx("invalid endpoint probe layout for historical data branch batch")
			return false
		}
		targetColCount := len(cols) - 1
		if projected.VectorCount() < targetColCount+1 {
			err = moerr.NewInternalErrorNoCtxf(
				"historical data branch batch has %d vectors for %d target columns",
				projected.VectorCount(), targetColCount,
			)
			return false
		}
		if !prepared {
			// Projection represents columns absent from the historical schema as
			// constant-NULL vectors. Vector.Copy requires a writable flat
			// destination, so materialize only those constant data columns before
			// overlaying endpoint values. RowID and commit_ts stay untouched.
			for targetIdx := 0; targetIdx < targetColCount; targetIdx++ {
				dst := projected.Vecs[targetIdx+1]
				if !dst.IsConst() {
					continue
				}
				flat := vector.NewVec(*dst.GetType())
				if err = flat.UnionBatch(dst, 0, projected.RowCount(), nil, mp); err != nil {
					flat.Free(mp)
					return false
				}
				dst.Free(mp)
				projected.Vecs[targetIdx+1] = flat
			}
			prepared = true
		}
		for row := 0; row < rowCount; row++ {
			projectedRow := vector.GetFixedAtNoTypeCheck[int64](cols[0], row)
			if projectedRow < 0 || projectedRow >= int64(projected.RowCount()) {
				err = moerr.NewInternalErrorNoCtxf(
					"endpoint probe row index %d out of range %d",
					projectedRow, projected.RowCount(),
				)
				return false
			}
			if cols[pkTargetIdx+1].IsNull(uint64(row)) {
				continue
			}
			for targetIdx := 0; targetIdx < targetColCount; targetIdx++ {
				if err = projected.Vecs[targetIdx+1].Copy(
					cols[targetIdx+1], projectedRow, int64(row), mp,
				); err != nil {
					return false
				}
			}
		}
		return true
	})
	return err
}

func dataBranchHistoricalProbeStuff(
	ctx context.Context,
	tblStuff tableStuff,
	endpointRel engine.Relation,
) tableStuff {
	probeStuff := tblStuff
	probeStuff.lcaRel = endpointRel
	endpointTableID := endpointRel.GetTableID(ctx)
	if endpointTableID == tblStuff.tarRel.GetTableID(ctx) {
		// Target-side hydration restores the current values of columns that
		// were absent or incompatible in an older physical generation.
		probeStuff.tarRel = endpointRel
		probeStuff.def.tarOnlyIdxes = nil
		probeStuff.def.lcaColNames = nil
	} else if endpointTableID == tblStuff.baseRel.GetTableID(ctx) {
		// Base-side hydration emits SQL against the base endpoint, so map the
		// target batch layout to the base endpoint's visible column names.
		probeStuff.def.lcaColNames = probeStuff.def.baseColNames
	} else {
		// A bounded target-side PICK can hydrate from an older physical target
		// generation. Its relation definition already carries that generation's
		// exact names; base endpoint names are neither valid nor needed here.
		probeStuff.def.lcaColNames = nil
	}
	return probeStuff
}

func hydrateHistoricalDataBranchBatch(
	ctx context.Context,
	ses *Session,
	projected *batch.Batch,
	tblStuff tableStuff,
	endpointIdent branchIdentity,
	endpointRel engine.Relation,
	endpointSnapshot types.TS,
) (err error) {
	if projected == nil || projected.RowCount() == 0 {
		return nil
	}
	pkVecIdx := tblStuff.def.pkColIdx + 1 // projected data keeps RowID at Vec[0]
	if pkVecIdx <= 0 || pkVecIdx >= projected.VectorCount() {
		return moerr.NewInternalErrorNoCtxf(
			"historical data branch PK index %d out of range", pkVecIdx,
		)
	}
	tBat := batch.NewWithSize(1)
	tBat.Vecs[0] = vector.NewVec(*projected.Vecs[pkVecIdx].GetType())
	defer tBat.Clean(ses.proc.Mp())
	if err = tBat.Vecs[0].UnionBatch(
		projected.Vecs[pkVecIdx], 0, projected.RowCount(), nil, ses.proc.Mp(),
	); err != nil {
		return err
	}
	tBat.SetRowCount(projected.RowCount())

	probeStuff := dataBranchHistoricalProbeStuff(ctx, tblStuff, endpointRel)
	probeStuff.lcaIdent = &endpointIdent
	probe, err := runLCAProbeWithReaderFallback(
		ctx, ses, tBat, probeStuff, endpointSnapshot,
	)
	if err != nil {
		return err
	}
	defer probe.Close()
	return overlayDataBranchProbeResult(
		projected, probe, tblStuff.def.pkColIdx, ses.proc.Mp(),
	)
}

// ownerScopedChangesHandle re-attaches the physical segment owner identity to
// the live caller context on every Next (design §6). Construction-time
// identity is not enough: replay runs inside Next, and the engine's lost
// partition-state recovery reopens the physical table by id through
// Engine.GetRelationById, which resolves names in the *context's* account.
// Forwarding the statement context unchanged routes that recovery to the
// subscriber namespace, where the publisher-owned table cannot resolve.
// Deriving from the live context (identityCtx) keeps cancellation and every
// later value attachment intact; for a segment owned by the statement account
// the derivation is a no-op and local-local diffs pass their context through
// byte-identical.
type ownerScopedChangesHandle struct {
	inner engine.ChangesHandle
	ident branchIdentity
}

// scopeChangesHandleToSegmentOwner wraps a segment change handle so every
// Next runs under the segment's physical owner identity.
func scopeChangesHandleToSegmentOwner(
	handle engine.ChangesHandle,
	ident branchIdentity,
) engine.ChangesHandle {
	return &ownerScopedChangesHandle{inner: handle, ident: ident}
}

func (h *ownerScopedChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	return h.inner.Next(identityCtx(ctx, h.ident), mp)
}

func (h *ownerScopedChangesHandle) Close() error {
	return h.inner.Close()
}

type historicalDataBranchChangesHandle struct {
	inner            engine.ChangesHandle
	sourceMapping    []int
	tblStuff         tableStuff
	ses              *Session
	endpointRel      engine.Relation
	endpointSnapshot types.TS
	// endpointIdent is the physical owner identity endpointRel was opened
	// under (design §6); hydration probes the endpoint under it.
	endpointIdent branchIdentity
	hydrate       bool
}

func (h *historicalDataBranchChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	data, tombstone, hint, err := h.inner.Next(ctx, mp)
	if err != nil || data == nil || data.RowCount() == 0 {
		return data, tombstone, hint, err
	}
	data = projectDataBranchBatchToTarget(data, &h.tblStuff, h.sourceMapping, mp)
	if !h.hydrate {
		return data, tombstone, hint, nil
	}
	if err = hydrateHistoricalDataBranchBatch(
		ctx, h.ses, data, h.tblStuff, h.endpointIdent, h.endpointRel, h.endpointSnapshot,
	); err != nil {
		data.Clean(mp)
		if tombstone != nil {
			tombstone.Clean(mp)
		}
		return nil, nil, hint, err
	}
	return data, tombstone, hint, nil
}

func (h *historicalDataBranchChangesHandle) Close() error {
	return h.inner.Close()
}

func projectDataBranchBatchToTarget(
	sourceBat *batch.Batch,
	tblStuff *tableStuff,
	sourceColToTargetIdx []int,
	mp *mpool.MPool,
) *batch.Batch {
	// CollectChanges data batches are laid out as [RowID, data..., commit_ts].
	// Identify the optional trailing vector from the schema-derived data-column
	// count so that it cannot be mistaken for a column that needs projection.
	hasCommitTS := sourceBat.VectorCount() == len(sourceColToTargetIdx)+2
	outColCount := len(tblStuff.def.colNames) + 1
	if hasCommitTS {
		outColCount++
	}
	out := batch.NewWithSize(outColCount)
	out.Vecs[0] = sourceBat.Vecs[0] // RowID
	sourceBat.Vecs[0] = nil
	sourceColCount := sourceBat.VectorCount() - 1 // subtract RowID
	if hasCommitTS {
		sourceColCount--
		commitTSIdx := sourceBat.VectorCount() - 1
		out.Vecs[out.VectorCount()-1] = sourceBat.Vecs[commitTSIdx]
		sourceBat.Vecs[commitTSIdx] = nil
	}
	if sourceColCount > len(sourceColToTargetIdx) {
		sourceColCount = len(sourceColToTargetIdx)
	}
	for sourceColIdx := 0; sourceColIdx < sourceColCount; sourceColIdx++ {
		tarColIdx := sourceColToTargetIdx[sourceColIdx]
		if tarColIdx >= 0 && tarColIdx < len(tblStuff.def.colNames) {
			out.Vecs[tarColIdx+1] = sourceBat.Vecs[sourceColIdx+1]
			sourceBat.Vecs[sourceColIdx+1] = nil
		}
	}
	for i := range tblStuff.def.colNames {
		if out.Vecs[i+1] == nil {
			out.Vecs[i+1] = vector.NewConstNull(tblStuff.def.colTypes[i], sourceBat.RowCount(), mp)
		}
	}
	out.SetRowCount(sourceBat.RowCount())
	out.SetAttributes(dataBranchTargetLayoutAttrs(tblStuff, hasCommitTS))
	sourceBat.Clean(mp)
	return out
}
