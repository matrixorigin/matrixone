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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

	identityVec := vector.NewVec(types.T_uint64.ToType(), mp)
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

// buildLCAProbeColumnLayout selects compatible common columns from the LCA and
// target layouts. The caller reconstructs target-only columns as NULL because
// they are excluded from endpoint comparison and MERGE apply even when an
// ancestor generation still contains an older representation of the column.
func buildLCAProbeColumnLayout(
	lcaDef, targetDef *plan2.TableDef,
	targetColNames []string,
	targetColTypes []types.Type,
	targetOnlyIdxes []int,
	lcaColNames []string,
	resolvedSnapshot bool,
) (lcaProbeLayout, error) {
	if lcaDef == nil {
		return lcaProbeLayout{}, moerr.NewInternalErrorNoCtx("DATA BRANCH LCA table definition is unavailable")
	}
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
		if resolvedSnapshot || lcaAttrName == "" {
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

func lcaProbeTableDefAtSnapshot(
	ctx context.Context, ses *Session, tblStuff tableStuff, snapshotTS types.TS,
) (rel engine.Relation, def *plan2.TableDef, sqlReady bool, err error) {
	scanCtx := identityCtx(ctx, tblStuff.resolvedLCAIdentity(ctx, ses))
	var lookupErr error
	if handler := ses.GetTxnHandler(); handler != nil {
		storage, txnOp := handler.GetStorage(), handler.GetTxn()
		if storage != nil && txnOp != nil {
			if !snapshotTS.IsEmpty() {
				txnOp = txnOp.CloneSnapshotOp(snapshotTS.ToTimestamp())
			}
			_, _, snapshotRel, lookupErrAtSnapshot := storage.GetRelationById(scanCtx, txnOp, tblStuff.lcaProbeTableID(scanCtx))
			lookupErr = lookupErrAtSnapshot
			if lookupErr == nil && snapshotRel != nil {
				if snapshotDef := snapshotRel.GetTableDef(scanCtx); snapshotDef != nil {
					return snapshotRel, snapshotDef, true, nil
				}
			}
		}
	}
	// A relation already opened at this exact snapshot can feed the reader.
	// Only a successful historical lookup enables the SQL path.
	if fallbackRel := tblStuff.lcaFallbackRangeRel(snapshotTS); fallbackRel != nil {
		if def := fallbackRel.GetTableDef(scanCtx); def != nil {
			return fallbackRel, def, false, nil
		}
	}
	if lookupErr != nil {
		return nil, nil, false, lookupErr
	}
	return nil, nil, false, moerr.NewInternalErrorNoCtxf(
		"DATA BRANCH LCA table definition is unavailable at snapshot %s", snapshotTS.ToString(),
	)
}

// Resolve tombstones against the LCA, retaining only misses in tombstones.
func handleDelsOnLCA(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tombstones *batch.Batch,
	tblStuff tableStuff,
	snapshot timestamp.Timestamp,
) (*batch.Batch, error) {
	var keys *vector.Vector
	if tombstones != nil {
		keys = tombstones.Vecs[0]
	}
	rows, missing, err := probeDataBranchRows(ctx, ses, bh, keys, tblStuff, snapshot)
	if err != nil {
		return nil, err
	}
	if err := tombstones.Shrink(missing, false, ses.proc.Mp()); err != nil {
		tblStuff.retPool.releaseRetBatch(rows, false)
		return nil, err
	}
	return rows, nil
}

// Probe keys in input order. The result contains the found rows in that order
// and the input indexes for missing keys. The caller owns the result batch.
func probeDataBranchRows(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	keys *vector.Vector,
	tblStuff tableStuff,
	snapshot timestamp.Timestamp,
) (dBat *batch.Batch, missing []int64, err error) {

	if snapshot.PhysicalTime == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtxf("invalid branch ts: %s", snapshot.DebugString())
	}
	if keys == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("data branch probe is missing its primary key vector")
	}

	var (
		sqlRet           executor.Result
		fullTargetLayout bool
		lcaLayout        lcaProbeLayout
		lcaTableName     string

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
	// The SQL probe resolves db.table by name under the statement account.
	// A lineage node owned by another account (design §6.4, e.g. reopened
	// under a subscription source owner) would resolve that name to a
	// same-named local decoy or fail outright, so it is always scanned by
	// id under its own owner identity via the reader fallback.
	var lcaSnapshotRel engine.Relation
	var lcaTblDef *plan2.TableDef
	readerProbe := func() (executor.Result, error) {
		keyBatch := batch.NewWithSize(1)
		keyBatch.Vecs[0] = keys
		keyBatch.SetRowCount(keys.Length())
		return runLCAProbeWithReaderFallbackWithPreopened(ctx, ses, keyBatch, tblStuff, snapshotTS, lcaSnapshotRel, lcaTblDef)
	}
	lcaOwnerMismatch := tblStuff.resolvedLCAIdentity(ctx, ses).account != ses.GetAccountId()
	forceReaderProbe := lcaOwnerMismatch ||
		(tblStuff.lcaReaderProbeMode != nil && tblStuff.lcaReaderProbeMode.Load())
	var sqlReady bool
	if !forceReaderProbe {
		lcaSnapshotRel, lcaTblDef, sqlReady, err = lcaProbeTableDefAtSnapshot(ctx, ses, tblStuff, snapshotTS)
		if err != nil {
			return nil, nil, err
		}
	}
	if !sqlReady {
		sqlRet, err = readerProbe()
		if err != nil {
			return nil, nil, err
		}
		fullTargetLayout = true
	} else {
		lcaLayout, err = buildLCAProbeColumnLayout(
			lcaTblDef, tblStuff.tarRel.GetTableDef(ctx), tblStuff.def.colNames, tblStuff.def.colTypes,
			tblStuff.def.tarOnlyIdxes, tblStuff.def.lcaColNames, true,
		)
		if err != nil {
			return nil, nil, err
		}
		lcaTableName = lcaTblDef.Name
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
		mots := fmt.Sprintf("{MO_TS='%s'} ", snapshotTS.ToString())
		var pkNames []string
		if tblStuff.def.pkKind == fakeKind {
			// The synthetic hash column is internal and cannot be renamed.
			pkNames = []string{catalog.FakePrimaryKeyColName}
		} else {
			pkNames, err = lcaLayout.columnNamesForTargetIndexes(probePKColIdxes)
			if err != nil {
				return nil, nil, err
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
			cols, area := vector.MustVarlenaRawData(keys)
			for i := range cols {
				b := cols[i].GetByteSlice(area)
				if tuple, err = types.Unpack(b); err != nil {
					return nil, nil, err
				}

				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				for j := range tuple {
					if err = formatValIntoStringWithFloatCast(
						ses, tuple[j], colTypes[probePKColIdxes[j]], valsBuf, true,
					); err != nil {
						return nil, nil, err
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
			pks := vector.MustFixedColNoTypeCheck[uint64](keys)
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
			for i := range keys.Length() {
				valsBuf.WriteString(fmt.Sprintf("row(%d,", i))
				b := keys.GetRawBytesAt(i)
				val := types.DecodeValue(b, keys.GetType().Oid)
				if err = formatValIntoStringWithFloatCast(ses, val, pkType, valsBuf, true); err != nil {
					return nil, nil, err
				}
				valsBuf.WriteString(")")
				if i != keys.Length()-1 {
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
			zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
			zap.String("db-name", lcaTblDef.DbName),
			zap.String("table-name", lcaTblDef.Name),
			zap.Int("input-pk-rows", keys.Length()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("sql-len", sqlBuf.Len()),
			zap.String("sql-preview", sqlPreview),
		)

		if sqlRet, err = runSql(ctx, ses, bh, sqlBuf.String(), nil, nil); err != nil {
			logutil.Error(
				"DataBranch-LCA-SQL-Error",
				zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
				zap.String("db-name", lcaTblDef.DbName),
				zap.String("table-name", lcaTblDef.Name),
				zap.Int("input-pk-rows", keys.Length()),
				zap.String("snapshot-ts", snapshotTS.ToString()),
				zap.Error(err),
			)
			if shouldUseLCAReaderFallback(err) {
				if tblStuff.lcaReaderProbeMode != nil {
					tblStuff.lcaReaderProbeMode.Store(true)
				}
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Start",
					zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", keys.Length()),
					zap.String("snapshot-ts", snapshotTS.ToString()),
					zap.Error(err),
				)
				sqlRet, err = readerProbe()
				if err != nil {
					logutil.Error(
						"DataBranch-LCA-SQL-Fallback-Error",
						zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
						zap.String("table-name", lcaTblDef.Name),
						zap.Int("input-pk-rows", keys.Length()),
						zap.String("snapshot-ts", snapshotTS.ToString()),
						zap.Error(err),
					)
					return nil, nil, err
				}
				fullTargetLayout = true
				logutil.Info(
					"DataBranch-LCA-SQL-Fallback-Done",
					zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
					zap.String("table-name", lcaTblDef.Name),
					zap.Int("input-pk-rows", keys.Length()),
					zap.Int("fallback-batches", len(sqlRet.Batches)),
				)
			} else {
				return nil, nil, err
			}
		}
	}
	defer sqlRet.Close()

	if forceReaderProbe {
		logutil.Debug(
			"DataBranch-LCA-ReaderProbe-Direct",
			zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
			zap.String("table-name", lcaTableName),
			zap.Int("input-pk-rows", keys.Length()),
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

	dBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
	defer func(rows *batch.Batch) {
		if err != nil {
			tblStuff.retPool.releaseRetBatch(rows, false)
		}
	}(dBat)

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
		return nil, nil, err
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
			return nil, nil, err
		}
		nullVec.Free(ses.proc.Mp())
	}
	dBat.SetRowCount(lcaHitRows)
	logutil.Debug(
		"DataBranch-LCA-Join-Result",
		zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
		zap.String("table-name", lcaTableName),
		zap.Int("joined-rows", joinedRows),
		zap.Int("lca-hit-rows", lcaHitRows),
		zap.Int("lca-miss-rows", lcaMissRows),
		zap.Int("delete-candidate-rows", len(sels)),
		zap.Int("output-dbat-rows", dBat.RowCount()),
	)

	return dBat, sels, nil
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
) (executor.Result, error) {
	return runLCAProbeWithReaderFallbackWithPreopened(ctx, ses, tBat, tblStuff, snapshotTS, nil, nil)
}

func runLCAProbeWithReaderFallbackWithPreopened(
	ctx context.Context,
	ses *Session,
	tBat *batch.Batch,
	tblStuff tableStuff,
	snapshotTS types.TS,
	preopenedRel engine.Relation,
	preopenedDef *plan2.TableDef,
) (sqlRet executor.Result, err error) {
	start := time.Now()
	sqlRet.Mp = ses.proc.Mp()
	if tBat == nil {
		return sqlRet, nil
	}
	rowCount := tBat.RowCount()
	if rowCount == 0 {
		bat := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), ses.proc.Mp())
		for i, typ := range tblStuff.def.colTypes {
			bat.Vecs[i+1] = vector.NewVec(typ, ses.proc.Mp())
		}
		bat.SetRowCount(0)
		sqlRet.Batches = []*batch.Batch{bat}
		return sqlRet, nil
	}

	lcaSnapshotRel, lcaTblDef := preopenedRel, preopenedDef
	if lcaTblDef == nil {
		lcaSnapshotRel, lcaTblDef, _, err = lcaProbeTableDefAtSnapshot(ctx, ses, tblStuff, snapshotTS)
		if err != nil {
			return executor.Result{}, err
		}
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
	lcaLayout, err := buildLCAProbeColumnLayout(
		lcaTblDef, tblStuff.tarRel.GetTableDef(ctx), tblStuff.def.colNames, tblStuff.def.colTypes,
		tblStuff.def.tarOnlyIdxes, tblStuff.def.lcaColNames, true,
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
	filterVec := vector.NewVec(*pkVec.GetType(), mp)
	defer filterVec.Free(mp)
	if err = filterVec.UnionBatch(pkVec, 0, rowCount, nil, mp); err != nil {
		return executor.Result{}, err
	}
	if err := filterVec.InplaceSort(mp); err != nil {
		return executor.Result{}, err
	}
	pkFilterExpr, err := readutil.ConstructInExpr(ctx, lcaPKName, filterVec)
	if err != nil {
		return executor.Result{}, err
	}
	prepareCost = time.Since(start)

	tmp := batch.NewWithSize(len(lcaLayout.attrs))
	for i, typ := range lcaLayout.types {
		tmp.Vecs[i] = vector.NewVec(typ, mp)
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
		zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("input-pk-rows", rowCount),
		zap.Int("input-unique-keys", len(inputKeys)),
		zap.String("snapshot-ts", snapshotTS.ToString()),
	)

	defer func() {
		fields := []zap.Field{
			zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
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
	err = scanSnapshotRelationWithPreopened(
		scanCtx,
		"lca-reader-fallback",
		ses,
		tblStuff.lcaProbeTableID(ctx),
		snapshotTS,
		lcaSnapshotRel,
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
		zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
		zap.String("table-name", lcaTblDef.Name),
		zap.Int("scan-batch-cnt", scannedBatchCnt),
		zap.Int("scan-row-cnt", scannedRowCnt),
		zap.Int("accepted-row-cnt", acceptedRowCnt),
		zap.Int("dedup-row-cnt", duplicateRows),
		zap.Int("accepted-key-cnt", len(tmpRowByKey)),
	)

	out := batch.NewWithSize(len(tblStuff.def.colNames) + 1)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	for i, typ := range tblStuff.def.colTypes {
		out.Vecs[i+1] = vector.NewVec(typ, mp)
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
			// Even an all-hit scan may return rows in a different order than orderedIdx.
			if err = out.Vecs[targetIdx+1].Union(tmp.Vecs[sourceIdx], sels, mp); err != nil {
				return executor.Result{}, err
			}
			for _, rowIdx := range missedRows {
				out.Vecs[targetIdx+1].GetNulls().Add(rowIdx)
			}
		}
	}
	out.SetRowCount(rowCount)
	sqlRet.Batches = []*batch.Batch{out}
	outputCost = time.Since(outputStart)
	logutil.Info(
		"DataBranch-LCA-ReaderFallback-Output",
		zap.Uint64("table-id", tblStuff.lcaProbeTableID(ctx)),
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

		tarDataHashmap      databranchutils.BranchHashmap
		tarTombstoneHashmap databranchutils.BranchHashmap
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

	buildBaseStart := time.Now()
	if baseDataHashmap, baseTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, baseHandle, "base", pickKeyHashmap,
	); err != nil {
		return
	}
	buildBaseCost = time.Since(buildBaseStart)

	buildTargetStart := time.Now()
	if tarDataHashmap, tarTombstoneHashmap, err = buildHashmapForTable(
		ctx, ses.proc.Mp(), &tblStuff, tarHandle, "target", pickKeyHashmap,
	); err != nil {
		return
	}
	buildTargetCost = time.Since(buildTargetStart)

	diffStart := time.Now()
	// The source window has a committed before-image even when the common
	// ancestor has none. It can classify deletes at any destination depth.
	useWindowBefore := copt.preservePickConflicts && tblStuff.boundedPick &&
		tblStuff.pickWindowStart != nil && tblStuff.def.pkKind != fakeKind
	if !dagInfo.hasLCA() || (tblStuff.lcaHasZeroHistory && !useWindowBefore) {
		if tblStuff.pickWindowStart != nil && tblStuff.def.pkKind != fakeKind &&
			tarDataHashmap.ItemCount() > 0 && tarTombstoneHashmap.ItemCount() > 0 {
			if err = pruneUnchangedPickRowsWithoutLCA(
				ctx, ses, bh, tblStuff, dagInfo, tarDataHashmap, tarTombstoneHashmap,
			); err != nil {
				return
			}
		}
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
			tarDataHashmap, tarTombstoneHashmap,
			baseDataHashmap, baseTombstoneHashmap,
		); err != nil {
			return
		}
	}
	diffCost = time.Since(diffStart)

	return
}

func sideTableStuffForLCAProbe(
	tblStuff tableStuff, copt compositeOption, dagInfo branchMetaInfo, tarSP, baseSP types.TS, forBase bool,
) tableStuff {
	if copt.preservePickConflicts {
		_, _, bothForked := dagInfo.lcaProbeAnchors(tarSP, baseSP)
		if bothForked {
			tarTableID, baseTableID := dagInfo.lcaProbeTableIDs(true)
			probeTableID := tarTableID
			names := tblStuff.def.tarLCAColNames
			if forBase {
				probeTableID = baseTableID
				names = tblStuff.def.baseLCAColNames
			}
			if probeTableID != 0 && probeTableID != dagInfo.lcaTableId {
				tblStuff.lcaProbeTableId = probeTableID
				tblStuff.lcaRelSnapshot = types.TS{}
			}
			if len(names) > 0 {
				tblStuff.def.lcaColNames = names
			}
		}
	}
	return tblStuff
}

// dataBranchPickWindowProbe binds the source before image to the schema
// actually visible at lo. The probe's output still uses the endpoint layout.
func dataBranchPickWindowProbe(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	dagInfo branchMetaInfo,
	tarSP types.TS,
) (tableStuff, error) {
	start := tblStuff.pickWindowStart
	rel, ident, err := reopenDataBranchNodeByID(
		ctx, ses, bh, []branchIdentity{start.identity}, start.tableID, start.snapshot,
	)
	if err != nil {
		return tableStuff{}, err
	}
	loDef := rel.GetTableDef(identityCtx(ctx, ident))
	tarDef := tblStuff.tarRel.GetTableDef(ctx)
	pathDefs := []*plan2.TableDef{tarDef}
	lineageOnly := []bool{false}
	var creationDefs []*plan2.TableDef
	crossesCollapsedFork := false
	if len(tblStuff.pickForkColumns) > 0 {
		defs, edges := dataBranchSchemaPathFrom(loDef, tblStuff.pickSourceDefs, tblStuff.pickSourceEdges)
		for i := 1; i < len(defs); i++ {
			if tblStuff.pickForkColumns[[2]*plan2.TableDef{defs[i-1], defs[i]}] != nil {
				pathDefs, lineageOnly = defs, edges
				crossesCollapsedFork = true
				break
			}
		}
	}
	if !crossesCollapsedFork && start.tableID != tblStuff.tarRel.GetTableID(ctx) {
		// A COPY ALTER can place the endpoint in a different generation.
		// Resolve each edge before using the starting generation's own
		// physical column identity to walk from that edge back to lo.
		i := slices.Index(dagInfo.pathFromLCAToTar, start.tableID)
		if i < 0 {
			return tableStuff{}, moerr.NewInternalErrorNoCtx("PICK window start is missing from the source lineage")
		}
		tarIdent, baseIdent := tblStuff.endpointIdentities(ctx, ses)
		candidates := dedupeBranchIdentities(tarIdent, baseIdent)
		path := dagInfo.pathFromLCAToTar[i:]
		pathTS := dagInfo.pathFromLCAToTarTS[i:]
		pathDefs, _, err = dataBranchPathTableDefs(
			ctx, ses, bh, candidates, tblStuff.tarRel, tarSP, path, pathTS,
		)
		if err != nil {
			return tableStuff{}, err
		}
		lineageOnly = dagInfo.pathFromLCAToTarLineageOnly[i:]
		creationDefs = slices.Clone(pathDefs)
		for j := len(pathDefs) - 1; j > 0; j-- {
			if pathDefs[j].Version == 0 {
				continue
			}
			copyRel, copyIdent, copySP := tblStuff.tarRel, tarIdent, tarSP
			if j < len(pathDefs)-1 {
				copySP = pathTS[j+1]
				copyRel, copyIdent, err = reopenDataBranchNodeByID(ctx, ses, bh, candidates, path[j], copySP)
				if err != nil {
					return tableStuff{}, err
				}
			}
			creationDefs[j], err = dataBranchPickWindowCreation(ctx, ses, bh, copyRel, copyIdent, copySP)
			if err != nil {
				return tableStuff{}, err
			}
			if lineageOnly[j] && creationDefs[j].Version != 0 {
				return tableStuff{}, moerr.NewNotSupportedNoCtxf(
					"DATA BRANCH PICK has ambiguous COPY ALTER column lineage for table %d at the window start", path[j])
			}
		}
	}

	tblStuff.lcaRel = rel
	tblStuff.lcaIdent = &ident
	tblStuff.lcaRelSnapshot = start.snapshot
	tblStuff.lcaProbeTableId = 0
	tblStuff.def.lcaColNames = make([]string, len(tblStuff.def.colNames))
	tblStuff.def.tarOnlyIdxes = slices.Clone(tblStuff.def.tarOnlyIdxes)
	renameProof := newDataBranchRenameProof(tblStuff.pickForkColumns)
	for i, name := range tblStuff.def.colNames {
		if slices.Contains(tblStuff.def.tarOnlyIdxes, i) {
			continue
		}
		tarCol := dataBranchColumnDefByName(tarDef, name)
		startCol := tarCol
		if crossesCollapsedFork {
			_, startCol, _ = dataBranchColumnReachesLCA(pathDefs, lineageOnly, startCol, renameProof)
		}
		for j := len(pathDefs) - 1; !crossesCollapsedFork && j > 0 && startCol != nil; j-- {
			prev := pathDefs[j-1]
			if isDataBranchUserVisibleColumn(startCol) {
				created := creationDefs[j]
				createdCol := dataBranchColumnDefByIdentity(created, startCol)
				if createdCol == nil && dataBranchColumnDefByName(created, startCol.Name) == nil {
					startCol = nil // Added within the copied generation.
					break
				}
				if !lineageOnly[j] {
					if createdCol == nil {
						return tableStuff{}, moerr.NewNotSupportedNoCtxf(
							"DATA BRANCH PICK cannot establish fork column identity for %q", name)
					}
					// A logical fork copies its creation names. Later
					// in-place names cannot identify the parent's column.
					startCol = dataBranchColumnDefByLogicalName(prev, createdCol)
					continue
				}
				if createdCol == nil ||
					(!strings.EqualFold(createdCol.Name, startCol.Name) &&
						dataBranchColumnDefByName(pathDefs[j], createdCol.Name) != nil) ||
					!isDataBranchLogicalTypeEquivalent(createdCol.Typ, startCol.Typ) {
					return tableStuff{}, moerr.NewNotSupportedNoCtxf(
						"DATA BRANCH PICK has ambiguous COPY ALTER column lineage for %q at the window start",
						name,
					)
				}
				var proven bool
				startCol, proven = dataBranchPickWindowCopyColumn(prev, created, createdCol, renameProof)
				if !proven {
					return tableStuff{}, moerr.NewNotSupportedNoCtxf(
						"DATA BRANCH PICK has ambiguous COPY ALTER column lineage for %q at the window start", name)
				}
				continue
			}
			startCol = dataBranchColumnDefByLogicalName(prev, startCol)
		}
		// Both definitions are for the starting physical generation. Its
		// current or edge name can belong to a different column at lo.
		loCol := dataBranchColumnDefByIdentity(loDef, startCol)
		if loCol == nil {
			if isDataBranchUserVisibleColumn(tarCol) && !slices.Contains(tblStuff.def.pkColIdxes, i) &&
				dataBranchColumnDefByName(loDef, name) == nil &&
				(startCol == nil || dataBranchColumnDefByLogicalName(loDef, startCol) == nil) {
				// A column introduced after lo has no before-image. Exclude
				// it from both SQL and reader projections so they pad NULL.
				tblStuff.def.tarOnlyIdxes = append(tblStuff.def.tarOnlyIdxes, i)
				continue
			}
			return tableStuff{}, moerr.NewInternalErrorNoCtxf(
				"DATA BRANCH PICK cannot resolve column %q at the window start (table %d, snapshot %s)",
				name, start.tableID, start.snapshot.ToString(),
			)
		}
		tblStuff.def.lcaColNames[i] = loCol.Name
	}
	return tblStuff, nil
}

func dataBranchPickWindowCreation(
	ctx context.Context, ses *Session, bh BackgroundExec,
	copyRel engine.Relation, ident branchIdentity, snapshot types.TS,
) (*plan2.TableDef, error) {
	// Coordinates are table-local. Bind the versioned definition to the
	// same physical table at creation before crossing a lineage edge.
	tableID := copyRel.GetTableID(identityCtx(ctx, ident))
	cts, err := getTablesCreationCommitTS(ctx, ses, ident, copyRel, ident, copyRel, []types.TS{snapshot, snapshot})
	if err != nil {
		return nil, err
	}
	createdRel, _, err := reopenDataBranchNodeByID(ctx, ses, bh, []branchIdentity{ident}, tableID, cts[0])
	if err != nil {
		return nil, err
	}
	return createdRel.GetTableDef(identityCtx(ctx, ident)), nil
}

func dataBranchPickWindowCopyColumn(
	previousDef, copyDef *plan2.TableDef, col *plan2.ColDef,
	renameProof dataBranchRenameProof,
) (*plan2.ColDef, bool) {
	previous := dataBranchColumnDefByIdentity(previousDef, col)
	if !isDataBranchUserVisibleColumn(previous) || !isDataBranchLogicalTypeEquivalent(previous.Typ, col.Typ) {
		// ADD FIRST can shift every coordinate. Names can still prove
		// identity when the old columns are all carried over in order.
		if dataBranchCopyPreservesColumnOrder(previousDef, copyDef) {
			if named := dataBranchColumnDefByName(previousDef, col.Name); named == nil ||
				isDataBranchLogicalTypeEquivalent(named.Typ, col.Typ) {
				return named, true
			}
		}
		return nil, false
	}
	if strings.EqualFold(previous.Name, col.Name) {
		return previous, true
	}

	if dataBranchCopyPreservesColumnOrder(previousDef, copyDef) {
		if named := dataBranchColumnDefByName(previousDef, col.Name); named == nil ||
			isDataBranchLogicalTypeEquivalent(named.Typ, col.Typ) {
			return named, true
		}
	}

	// An isolated rename is unambiguous when all visible coordinates are
	// preserved and every other name still belongs to the same column.
	if dataBranchColumnDefByRenameIdentity(previousDef, copyDef, col, renameProof) == nil {
		return nil, false
	}
	for _, candidate := range previousDef.Cols {
		if candidate != previous && isDataBranchUserVisibleColumn(candidate) &&
			dataBranchColumnDefByName(copyDef, candidate.Name) == nil {
			return nil, false
		}
	}
	return previous, true
}

func dataBranchCopyPreservesColumnOrder(previousDef, copyDef *plan2.TableDef) bool {
	remaining := copyDef.Cols
	for _, col := range previousDef.Cols {
		if !isDataBranchUserVisibleColumn(col) {
			continue
		}
		i := slices.IndexFunc(remaining, func(next *plan2.ColDef) bool {
			return isDataBranchUserVisibleColumn(next) && strings.EqualFold(col.Name, next.Name)
		})
		if i < 0 {
			return false
		}
		remaining = remaining[i+1:]
	}
	return true
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
	baseDataHashmap databranchutils.BranchHashmap,
	baseTombstoneHashmap databranchutils.BranchHashmap,
) (err error) {

	tarSP, baseSP := tblStuff.resolvedSnapshots(ses)
	tarLCAProbe, baseLCAProbe := dagInfo.lcaProbeSnapshots(tarSP, baseSP, copt.preservePickConflicts)
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
	tarProbeStuff := tblStuff
	if tblStuff.pickWindowStart != nil && tarTombstoneHashmap.ItemCount() > 0 {
		// A row can change within the PICK window back to its value at the
		// fork. Comparing against the fork would silently discard that update.
		// The destination still uses its own fork for conflict detection.
		tarProbeStuff, err = dataBranchPickWindowProbe(ctx, ses, bh, tblStuff, dagInfo, tarSP)
		if err != nil {
			return err
		}
	}
	var (
		wg        sync.WaitGroup
		atomicErr atomic.Value

		baseDeleteBatches  []batchWithKind
		baseUpdateBatches  []batchWithKind
		restoreMissingKeys = make(map[string]struct{})
		// A source update reaches this matcher as a delete of its LCA row followed
		// by an insert of its replacement row. Record destination updates that
		// changed an indexed special column so their source replacement can use a
		// full-row native UPDATE even after the destination batch is consumed.
		destinationChangedIndexedSpecialKeys = make(map[string]struct{})
		sourceNativeUpdateKeys               = make(map[string]struct{})
	)

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
		if err2 := sortDataBranchBatchByPrimaryKey(
			wrapped.batch, tblStuff.def.pkColIdx, ses.proc.Mp(),
		); err2 != nil {
			return err2
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

	// A later-forked destination can collect parent inserts that were
	// already visible at its fork. Window deletes reconcile those separately.
	var inheritedAt types.TS
	baseProbeStuff := sideTableStuffForLCAProbe(tblStuff, copt, dagInfo, tarSP, baseSP, true)
	if tblStuff.lcaHasZeroHistory {
		// The destination's first committed image can classify its later
		// changes and prove an absent row was deleted. It cannot establish
		// that an existing row was inherited from the source.
		_, ident := tblStuff.endpointIdentities(ctx, ses)
		cts, err := getTablesCreationCommitTS(ctx, ses, ident, tblStuff.baseRel, ident, tblStuff.baseRel,
			[]types.TS{baseSP, baseSP})
		if err != nil {
			return err
		}
		baseLCAProbe, inheritedAt = cts[0], cts[0]
		baseProbeStuff = dataBranchHistoricalProbeStuff(ctx, tblStuff, tblStuff.baseRel)
		baseProbeStuff.lcaIdent = &ident
	} else if tblStuff.boundedPick && tarLCAProbe.LT(&baseLCAProbe) {
		if _, hasBaseFork := firstDataBranchForkTS(dagInfo.pathFromLCAToBaseTS, dagInfo.pathFromLCAToBaseLineageOnly); hasBaseFork {
			inheritedAt = baseLCAProbe
		}
	}

	forkHistory := pickForkHistory{branches: dagInfo.pathFromLCAToBase, forks: dagInfo.baseForks}
	if copt.protectMissingPickHistory && tblStuff.boundedPick && tblStuff.def.pkKind != fakeKind && len(forkHistory.forks) != 0 {
		write := emit
		emit = func(wrapped batchWithKind) (bool, error) {
			if wrapped.side == diffSideTarget && wrapped.kind == diffInsert && !wrapped.fromUpdate && wrapped.batch.RowCount() != 0 {
				keys, err := wrapped.batch.Vecs[tblStuff.def.pkColIdx].Dup(ses.proc.Mp())
				if err != nil {
					return false, err
				}
				defer keys.Free(ses.proc.Mp())
				if stop, err := forkHistory.emitMissingConflicts(ctx, ses, bh, tblStuff, wrapped.batch, keys, write); err != nil || stop {
					return stop, err
				}
			}
			return write(wrapped)
		}
	}

	handleTarDeleteAndUpdates := func(wrapped batchWithKind) (err2 error) {
		wrapped.side = diffSideTarget
		if (tblStuff.boundedPick || tblStuff.pickWindowStart != nil) && copt.conflictOpt != nil &&
			((wrapped.kind == diffDelete && !wrapped.fromUpdate) ||
				(wrapped.kind == diffInsert && wrapped.fromUpdate)) &&
			wrapped.batch.RowCount() > 0 &&
			(baseDataHashmap.ItemCount() > 0 || wrapped.fromUpdate &&
				(tblStuff.lcaHasZeroHistory || copt.protectMissingPickHistory && len(forkHistory.forks) != 0)) {
			stop, err := reconcilePickWindowDestinationInsert(ctx, ses, bh, baseProbeStuff, tarProbeStuff,
				copt, emit, wrapped, baseDataHashmap, inheritedAt, &forkHistory)
			if stop || err != nil || wrapped.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return err
			}
		}
		var pickConflictBat *batch.Batch
		if wrapped.kind == diffInsert && wrapped.fromUpdate && len(restoreMissingKeys) > 0 {
			var keep []int64
			restoreBat := tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
			for rowIdx := range wrapped.batch.RowCount() {
				key, keyErr := extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
				if keyErr != nil {
					tblStuff.retPool.releaseRetBatch(restoreBat, false)
					return keyErr
				}
				if _, restore := restoreMissingKeys[key]; !restore {
					keep = append(keep, int64(rowIdx))
					continue
				}
				if err2 = restoreBat.UnionOne(wrapped.batch, int64(rowIdx), ses.proc.Mp()); err2 != nil {
					tblStuff.retPool.releaseRetBatch(restoreBat, false)
					return err2
				}
				delete(restoreMissingKeys, key)
			}
			if restoreBat.Vecs[0].Length() > 0 {
				restoreBat.SetRowCount(restoreBat.Vecs[0].Length())
				out := wrapped
				out.batch, out.restoreMissing = restoreBat, true
				if stop, e := emitBatch(emit, out, false, tblStuff.retPool); e != nil {
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return e
				} else if stop {
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					return nil
				}
			} else {
				tblStuff.retPool.releaseRetBatch(restoreBat, false)
			}
			if len(keep) == 0 {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return nil
			}
			if err := wrapped.batch.Shrink(keep, false, ses.proc.Mp()); err != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				return err
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
		if len(baseUpdateBatches) == 0 && len(baseDeleteBatches) == 0 && len(sourceNativeUpdateKeys) == 0 {
			// no need to check conflict
			if stop, e := emitBatch(emit, wrapped, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
			}
			return nil
		}

		if err2 = sortDataBranchBatchByPrimaryKey(
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
				pickConflictBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
			}
			return pickConflictBat.UnionOne(baseWrapped.batch, int64(rowIdx), ses.proc.Mp())
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
						// only keep the rows from tar
						sels2 = append(sels2, int64(j))
						i++
						j++
					} else if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
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
						if err3 = wrapped.batch.Shrink(sels1, true, ses.proc.Mp()); err3 != nil {
							return
						}
					}

					if len(sels2) != 0 {
						if err3 = baseWrapped.batch.Shrink(sels2, true, ses.proc.Mp()); err3 != nil {
							return
						}
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

		baseDeleteBatches = slices.DeleteFunc(baseDeleteBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if err2 = foo(baseUpdateBatches); err2 != nil {
			return
		}

		baseUpdateBatches = slices.DeleteFunc(baseUpdateBatches, func(t batchWithKind) bool {
			if t.batch.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(t.batch, false)
				return true
			}
			return false
		})

		if pickConflictBat != nil {
			if stop, e := emitBatch(emit, batchWithKind{
				batch:          pickConflictBat,
				kind:           diffDelete,
				name:           tblStuff.baseRel.GetTableName(),
				side:           diffSideBase,
				hasReplacement: wrapped.kind == diffInsert || wrapped.fromUpdate,
			}, false, tblStuff.retPool); e != nil {
				return e
			} else if stop {
				return nil
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

	// Select a relation, layout and snapshot together for either execution path.
	stepHandler := func(forBase bool) (err2 error) {
		sideStuff := baseProbeStuff
		name, side, branchTS := tblStuff.baseRel.GetTableName(), diffSideBase, baseLCAProbe
		hashmap1, hashmap2, handle := baseDataHashmap, baseTombstoneHashmap, handleBaseDeleteAndUpdates
		var windowStart *tableStuff
		if !forBase {
			sideStuff = sideTableStuffForLCAProbe(tblStuff, copt, dagInfo, tarSP, baseSP, false)
			name, side, branchTS = tblStuff.tarRel.GetTableName(), diffSideTarget, tarLCAProbe
			hashmap1, hashmap2, handle = tarDataHashmap, tarTombstoneHashmap, handleTarDeleteAndUpdates
			if tblStuff.pickWindowStart != nil {
				windowStart = &tarProbeStuff
			}
		}
		if copt.onlineApply {
			// Delivery owns the batch even on failure. Held batches remain in
			// retPool until the agency unwinds; handlers release emitted failures.
			return findDeleteAndUpdateBat(ctx, ses, bh, sideStuff, windowStart, name, side,
				handle, branchTS, copt.expandUpdate, hashmap1, hashmap2, 1)
		}

		tmpCh := make(chan batchWithKind, 1)
		wg.Add(1)
		if err2 = tblStuff.worker.Submit(func() {
			defer wg.Done()
			defer close(tmpCh)
			if err3 := findDeleteAndUpdateBat(
				newCtx, ses, bh, sideStuff, windowStart, name, side,
				func(wrapped batchWithKind) error {
					select {
					case <-newCtx.Done():
						tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
						return newCtx.Err()
					case tmpCh <- wrapped:
						return nil
					}
				}, branchTS, copt.expandUpdate, hashmap1, hashmap2, -1,
			); err3 != nil {
				atomicErr.Store(err3)
			}
		}); err2 != nil {
			wg.Done()
			return err2
		}

		var first error
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

	if copt.preservePickConflicts && tblStuff.pickOrigin != nil && !tblStuff.lcaHasZeroHistory {
		if baseUpdateBatches, baseDeleteBatches, err = discardSharedForkChanges(ctx, ses, bh, tblStuff,
			baseUpdateBatches, baseDeleteBatches); err != nil {
			return err
		}
	}

	// phase2: handle tar dels and updates on lca
	if err = stepHandler(false); err != nil {
		return
	}
	if len(restoreMissingKeys) != 0 {
		return moerr.NewInternalErrorNoCtxf(
			"data branch source update is missing %d replacement row(s)",
			len(restoreMissingKeys),
		)
	}

	if copt.preservePickConflicts && tblStuff.boundedPick && tblStuff.def.pkKind != fakeKind &&
		copt.conflictOpt != nil && copt.conflictOpt.Opt == tree.CONFLICT_ACCEPT && tarDataHashmap.ItemCount() != 0 {
		// A window INSERT has no old source row to match during phase 2.
		// Emit conflicts for its remaining destination deletions before
		// the writer checks the live destination and inserts the key.
		for _, deleted := range baseDeleteBatches {
			if deleted.fromUpdate {
				continue
			}
			matches, err := tarDataHashmap.GetByVectors([]*vector.Vector{deleted.batch.Vecs[tblStuff.def.pkColIdx]})
			if err != nil {
				return err
			}
			var selected []int64
			for i, match := range matches {
				if match.Exists {
					selected = append(selected, int64(i))
				}
			}
			if len(selected) == 0 {
				continue
			}
			markers := tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
			if err := markers.Union(deleted.batch, selected, ses.proc.Mp()); err != nil {
				tblStuff.retPool.releaseRetBatch(markers, false)
				return err
			}
			// It is only a marker, not a DELETE or an instruction to UPDATE
			// a missing destination row. ACCEPT still takes the INSERT path.
			if stop, err := emitBatch(emit, batchWithKind{batch: markers, name: deleted.name,
				kind: diffDelete, side: diffSideBase, fromUpdate: true}, false, tblStuff.retPool); err != nil || stop {
				return err
			}
		}
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

// A destination insert may survive the fork probe in baseDataHashmap,
// outside the destination delete/update batches. Match it against both
// deletes and window replacements before emitting their writes.
func reconcilePickWindowDestinationInsert(
	ctx context.Context, ses *Session, bh BackgroundExec, tables, sourceBefore tableStuff, copt compositeOption,
	emit emitFunc, src batchWithKind, baseDataHashmap databranchutils.BranchHashmap, inheritedAt types.TS, forkHistory *pickForkHistory,
) (bool, error) {
	var matches *batch.Batch
	defer func() {
		if matches != nil {
			tables.retPool.releaseRetBatch(matches, false)
		}
	}()
	var srcRows []int64
	pkIdx := tables.def.pkColIdx
	_, err := baseDataHashmap.PopByVectorsStream([]*vector.Vector{src.batch.Vecs[pkIdx]}, false,
		func(rowIdx int, _ []byte, row []byte) error {
			tuple, _, err := baseDataHashmap.DecodeRow(row)
			if err != nil {
				return err
			}
			if matches == nil {
				matches = tables.retPool.acquireRetBatch(tables, false, ses.proc.Mp())
			}
			srcRows = append(srcRows, int64(rowIdx))
			return appendTupleToBat(ses, matches, tuple, tables)
		},
	)
	if err != nil {
		return false, err
	}
	protectForks := copt.protectMissingPickHistory && forkHistory != nil && len(forkHistory.forks) != 0 &&
		tables.boundedPick && tables.def.pkKind != fakeKind
	if (tables.lcaHasZeroHistory || protectForks) && src.fromUpdate && len(srcRows) < src.batch.RowCount() {
		// The destination's live image has no tombstones. Use the first
		// committed image only to distinguish a proven deletion from a key
		// that was already absent, never to classify an existing row.
		live := make([]bool, src.batch.RowCount())
		for _, row := range srcRows {
			live[row] = true
		}
		srcKeys := src.batch.Vecs[pkIdx]
		missingKeys := vector.NewVec(*srcKeys.GetType(), ses.proc.Mp())
		defer missingKeys.Free(ses.proc.Mp())
		for row, found := range live {
			if !found {
				if err := missingKeys.UnionOne(srcKeys, int64(row), ses.proc.Mp()); err != nil {
					return false, err
				}
			}
		}
		if tables.lcaHasZeroHistory {
			if stop, err := emitPreviouslyPresentPickKeys(ctx, ses, bh, tables, missingKeys, inheritedAt, emit); err != nil || stop {
				return stop, err
			}
		}
		if protectForks && missingKeys.Length() != 0 {
			if stop, err := forkHistory.emitMissingConflicts(ctx, ses, bh, tables, src.batch, missingKeys, emit); err != nil || stop {
				return stop, err
			}
		}
	}
	if matches == nil {
		return false, nil
	}
	matches.SetRowCount(len(srcRows))
	conflicting := make([]int64, 0, len(srcRows))
	if tables.lcaHasZeroHistory {
		// A destination row is safe to change when it equals the source
		// before-image. An equal update result has already converged.
		var beforeRows *batch.Batch
		var missing []int64
		if src.fromUpdate {
			beforeRows, missing, err = probeDataBranchRows(ctx, ses, bh, matches.Vecs[pkIdx],
				sourceBefore, sourceBefore.pickWindowStart.snapshot.ToTimestamp())
			if err != nil {
				return false, err
			}
			defer tables.retPool.releaseRetBatch(beforeRows, false)
		}
		beforeRow, miss := 0, 0
		for i, srcRow := range srcRows {
			cmp, err := compareRowInWrappedBatches(ctx, ses, tables, int(srcRow), i, true,
				src, batchWithKind{batch: matches})
			if err != nil {
				return false, err
			}
			if src.fromUpdate {
				if miss < len(missing) && missing[miss] == int64(i) {
					miss++
				} else {
					if cmp != 0 {
						cmp, err = compareRowInWrappedBatches(ctx, ses, sourceBefore, beforeRow, i, true,
							batchWithKind{batch: beforeRows}, batchWithKind{batch: matches})
						if err != nil {
							return false, err
						}
					}
					beforeRow++
				}
			}
			if cmp != 0 {
				conflicting = append(conflicting, int64(i))
			}
		}
	} else if src.fromUpdate {
		for i, srcRow := range srcRows {
			cmp, err := compareRowInWrappedBatches(ctx, ses, tables, int(srcRow), i, true,
				src, batchWithKind{batch: matches})
			if err != nil {
				return false, err
			}
			if cmp != 0 {
				conflicting = append(conflicting, int64(i))
			}
		}
	} else if inheritedAt.IsEmpty() {
		for i := range srcRows {
			conflicting = append(conflicting, int64(i))
		}
	} else {
		forkRows, missing, err := probeDataBranchRows(ctx, ses, bh, matches.Vecs[pkIdx], tables, inheritedAt.ToTimestamp())
		if err != nil {
			return false, err
		}
		defer tables.retPool.releaseRetBatch(forkRows, false)
		forkRow, miss := 0, 0
		for i := range srcRows {
			if miss < len(missing) && missing[miss] == int64(i) {
				conflicting = append(conflicting, int64(i))
				miss++
				continue
			}
			cmp, err := compareRowInWrappedBatches(ctx, ses, tables, i, forkRow, true,
				batchWithKind{batch: matches}, batchWithKind{batch: forkRows})
			if err != nil {
				return false, err
			}
			if cmp != 0 {
				conflicting = append(conflicting, int64(i))
			}
			forkRow++
		}
	}
	if tables.pickOrigin != nil && len(conflicting) != 0 {
		conflicting, err = excludeInheritedPickRows(ctx, ses, bh, tables, matches, conflicting)
		if err != nil {
			return false, err
		}
	}
	if len(conflicting) == 0 {
		return false, nil
	}
	baseName := tables.baseRel.GetTableName()
	switch copt.conflictOpt.Opt {
	case tree.CONFLICT_SKIP:
	case tree.CONFLICT_ACCEPT:
		if !copt.preservePickConflicts {
			return false, nil
		}
	default:
		key, err := extractPKAsString(ses, tables, src.batch, int(srcRows[conflicting[0]]))
		if err != nil {
			return false, err
		}
		if src.fromUpdate {
			return false, moerr.NewDataBranchConflictNoCtxf(
				"%s INSERT and %s INSERT on pk(%v) with different values", src.name, baseName, key)
		}
		return false, moerr.NewDataBranchConflictNoCtxf(
			"%s DELETE and %s INSERT on pk(%v)", src.name, baseName, key,
		)
	}
	mp := ses.proc.Mp()
	if !src.fromUpdate || copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
		remove := make([]int64, 0, len(conflicting))
		for _, i := range conflicting {
			remove = append(remove, srcRows[i])
		}
		slices.Sort(remove) // Hashmap callbacks are grouped by shard.
		if err := src.batch.Shrink(remove, true, mp); err != nil {
			return false, err
		}
	}
	if copt.conflictOpt.Opt == tree.CONFLICT_SKIP {
		return false, nil
	}
	if err := matches.Shrink(conflicting, false, mp); err != nil {
		return false, err
	}
	wrapped := batchWithKind{
		name: baseName, kind: diffDelete, side: diffSideBase, batch: matches,
		hasReplacement:           src.fromUpdate,
		fromUpdate:               src.fromUpdate,
		pickDeleteInsertConflict: !src.fromUpdate,
	}
	matches = nil // emitBatch transfers or releases the batch.
	return emitBatch(emit, wrapped, false, tables.retPool)
}

// Both branches committed with the retained ancestor. Their first committed
// images establish the values they agreed on at that point, including when
// the ancestor itself was changed after the forks.
func excludeInheritedPickRows(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff,
	matches *batch.Batch, conflicts []int64,
) ([]int64, error) {
	origin := tables.pickOrigin
	proof := newDataBranchRenameProof(tables.pickForkColumns)
	keys := vector.NewVec(*matches.Vecs[tables.def.pkColIdx].GetType(), ses.proc.Mp())
	defer keys.Free(ses.proc.Mp())
	inherited := slices.Clone(conflicts)
	sourceIdent, baseIdent := tables.endpointIdentities(ctx, ses)
	sourceDef := tables.tarRel.GetTableDef(identityCtx(ctx, sourceIdent))
	baseDef := tables.baseRel.GetTableDef(identityCtx(ctx, baseIdent))
	for _, image := range []dataBranchPickWindowStart{origin.source, origin.destination} {
		rel, ident, err := reopenDataBranchNodeByID(
			ctx, ses, bh, []branchIdentity{image.identity}, image.tableID, image.snapshot,
		)
		if err != nil {
			if isMissingDataBranchRelationByID(err) {
				return conflicts, nil
			}
			return nil, err
		}
		def := rel.GetTableDef(identityCtx(ctx, ident))
		endpoint := sourceDef
		path, edges, names := tables.pickSourceDefs, tables.pickSourceEdges, tables.def.colNames
		if image.tableID == origin.destination.tableID {
			endpoint = baseDef
			path, edges, names = tables.pickBaseDefs, tables.pickBaseEdges, tables.def.baseColNames
		}
		if len(historicalPickPrimaryKeyNames(def, path, edges, proof)) == 0 {
			return conflicts, nil
		}
		columns, err := pickSegmentColumns(def, endpoint, path, edges, names, proof)
		if err != nil {
			return nil, err
		}
		probe := tables
		probe.lcaRel, probe.lcaIdent, probe.lcaRelSnapshot = rel, &ident, image.snapshot
		probe.lcaProbeTableId = 0
		probe.def.lcaColNames = make([]string, len(columns))
		probe.def.tarOnlyIdxes = nil
		for i, col := range columns {
			if !slices.Contains(tables.def.commonIdxes, i) &&
				!slices.Contains(tables.def.pkColIdxes, i) && i != tables.def.pkColIdx {
				probe.def.tarOnlyIdxes = append(probe.def.tarOnlyIdxes, i)
				continue
			}
			targetCol := dataBranchColumnDefByName(sourceDef, tables.def.colNames[i])
			if col == nil || targetCol == nil || !isDataBranchLogicalTypeEquivalent(col.Typ, targetCol.Typ) {
				return conflicts, nil
			}
			probe.def.lcaColNames[i] = col.Name
		}

		keys.CleanOnlyData()
		for _, row := range inherited {
			if err := keys.UnionOne(matches.Vecs[tables.def.pkColIdx], row, ses.proc.Mp()); err != nil {
				return nil, err
			}
		}
		rows, missing, err := probeDataBranchRows(ctx, ses, bh, keys, probe, image.snapshot.ToTimestamp())
		if err != nil {
			return nil, err
		}
		kept := inherited[:0]
		row, miss := 0, 0
		for i, candidate := range inherited {
			if miss < len(missing) && missing[miss] == int64(i) {
				miss++
				continue
			}
			cmp, err := compareRowInWrappedBatches(ctx, ses, tables, int(candidate), row, true,
				batchWithKind{batch: matches}, batchWithKind{batch: rows})
			if err != nil {
				tables.retPool.releaseRetBatch(rows, false)
				return nil, err
			}
			row++
			if cmp == 0 {
				kept = append(kept, candidate)
			}
		}
		tables.retPool.releaseRetBatch(rows, false)
		inherited = kept
		if len(inherited) == 0 {
			return conflicts, nil
		}
	}
	return slices.DeleteFunc(conflicts, func(row int64) bool {
		_, found := slices.BinarySearch(inherited, row)
		return found
	}), nil
}

// A comparison against the committed parent can produce an UPDATE for a
// value both branches inherited from the parent's workspace. Remove the
// synthetic UPDATE pair while retaining real changes to the destination.
func discardSharedForkChanges(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff,
	updates, deletes []batchWithKind,
) ([]batchWithKind, []batchWithKind, error) {
	inherited := make(map[string]struct{})
	mp := ses.proc.Mp()
	keptUpdates := updates[:0]
	for _, update := range updates {
		if !update.fromUpdate {
			keptUpdates = append(keptUpdates, update)
			continue
		}
		rows := make([]int64, update.batch.RowCount())
		for i := range rows {
			rows[i] = int64(i)
		}
		keep, err := excludeInheritedPickRows(ctx, ses, bh, tables, update.batch, rows)
		if err != nil {
			return nil, nil, err
		}
		if len(keep) == len(rows) {
			keptUpdates = append(keptUpdates, update)
			continue
		}
		for i := range rows {
			if _, kept := slices.BinarySearch(keep, int64(i)); kept {
				continue
			}
			key, err := extractPKAsString(ses, tables, update.batch, i)
			if err != nil {
				return nil, nil, err
			}
			inherited[key] = struct{}{}
		}
		if len(keep) == 0 {
			tables.retPool.releaseRetBatch(update.batch, false)
			continue
		}
		if err := update.batch.Shrink(keep, false, mp); err != nil {
			return nil, nil, err
		}
		keptUpdates = append(keptUpdates, update)
	}
	if len(inherited) == 0 {
		return keptUpdates, deletes, nil
	}
	keptDeletes := deletes[:0]
	for _, deletion := range deletes {
		if !deletion.fromUpdate {
			keptDeletes = append(keptDeletes, deletion)
			continue
		}
		var keep []int64
		for i := range deletion.batch.RowCount() {
			key, err := extractPKAsString(ses, tables, deletion.batch, i)
			if err != nil {
				return nil, nil, err
			}
			if _, shared := inherited[key]; !shared {
				keep = append(keep, int64(i))
			}
		}
		if len(keep) == 0 {
			tables.retPool.releaseRetBatch(deletion.batch, false)
			continue
		}
		if len(keep) != deletion.batch.RowCount() {
			if err := deletion.batch.Shrink(keep, false, mp); err != nil {
				return nil, nil, err
			}
		}
		keptDeletes = append(keptDeletes, deletion)
	}
	return keptUpdates, keptDeletes, nil
}

// A currently absent key that existed in a committed destination generation
// is a deletion even if a later destination fork inherited that absence.
// A row under a different historical key is not evidence of a deletion.
// Resolve each current key component by column identity before probing.
func historicalPickPrimaryKeyNames(
	historical *plan2.TableDef, defs []*plan2.TableDef, edges []bool, proof dataBranchRenameProof,
) []string {
	path, pathEdges := dataBranchSchemaPathFrom(historical, defs, edges)
	if path == nil {
		return nil
	}
	current := path[len(path)-1]
	oldKind, oldNames := dataBranchPrimaryKeyColumns(historical)
	kind, names := dataBranchPrimaryKeyColumns(current)
	if oldKind != kind || len(oldNames) != len(names) {
		return nil
	}
	for i, name := range names {
		col := dataBranchColumnDefByName(current, name)
		reaches, oldCol, redefined := dataBranchColumnReachesLCA(path, pathEdges, col, proof)
		if !reaches || redefined || oldCol == nil || oldCol.Name != oldNames[i] ||
			!isDataBranchLogicalTypeEquivalent(oldCol.Typ, col.Typ) {
			return nil
		}
	}
	return oldNames
}

// probePickKeyMembership probes a historical generation only if its primary
// key still denotes the same columns as the current key. Nil means the key
// has changed, so this generation cannot provide membership evidence.
func probePickKeyMembership(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff,
	keys *vector.Vector, start dataBranchPickWindowStart, defs []*plan2.TableDef, edges []bool,
	proof dataBranchRenameProof,
) (*batch.Batch, []int64, error) {
	rel, ident, err := reopenDataBranchNodeByID(ctx, ses, bh, []branchIdentity{start.identity}, start.tableID, start.snapshot)
	if err != nil {
		return nil, nil, err
	}
	names := historicalPickPrimaryKeyNames(rel.GetTableDef(identityCtx(ctx, ident)), defs, edges, proof)
	if len(names) == 0 {
		return nil, nil, nil
	}

	probe := tables
	probe.lcaRel, probe.lcaIdent, probe.lcaRelSnapshot = rel, &ident, start.snapshot
	probe.lcaProbeTableId = 0
	probe.def.lcaColNames = make([]string, len(probe.def.colNames))
	probe.def.tarOnlyIdxes = nil
	for i, idx := range probe.def.pkColIdxes {
		probe.def.lcaColNames[idx] = names[i]
	}
	for idx, name := range probe.def.lcaColNames {
		if name == "" && idx != probe.def.pkColIdx {
			probe.def.tarOnlyIdxes = append(probe.def.tarOnlyIdxes, idx)
		}
	}
	return probeDataBranchRows(ctx, ses, bh, keys, probe, start.snapshot.ToTimestamp())
}

func emitPreviouslyPresentPickKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff,
	keys *vector.Vector, creation types.TS, emit emitFunc,
) (bool, error) {
	starts := append([]dataBranchPickWindowStart{
		{tableID: tables.baseRel.GetTableID(ctx), snapshot: creation, identity: tables.resolvedLCAIdentity(ctx, ses)},
	}, tables.zeroHistoryBaseStarts...)
	proof := newDataBranchRenameProof(tables.pickForkColumns)
	emitDeletion := func(prior *batch.Batch) (bool, error) {
		return emitBatch(emit, batchWithKind{
			name: tables.baseRel.GetTableName(), kind: diffDelete, side: diffSideBase, batch: prior,
			hasReplacement: true, fromUpdate: true,
		}, false, tables.retPool)
	}
	for _, start := range starts {
		prior, missing, err := probePickKeyMembership(ctx, ses, bh, tables, keys, start,
			tables.pickBaseDefs, tables.pickBaseEdges, proof)
		if err != nil {
			return false, err
		}
		if prior == nil {
			continue
		}
		if prior.RowCount() == 0 {
			tables.retPool.releaseRetBatch(prior, false)
			continue
		}
		if stop, err := emitDeletion(prior); err != nil || stop {
			return stop, err
		}
		if len(missing) == 0 {
			keys.CleanOnlyData()
			return false, nil
		}
		if err = keys.Shrink(missing, false, ses.proc.Mp()); err != nil {
			return false, err
		}
	}

	return false, nil
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

	nativeBat := tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
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
	if err := bat.Shrink(nativeRowIdxes, true, ses.proc.Mp()); err != nil {
		tblStuff.retPool.releaseRetBatch(nativeBat, false)
		return nil, err
	}
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

// Keep window deletes and fork-existing updates in before. Suppress live rows
// equal to lo. A changed row present at lo is a window UPDATE even if it
// did not exist at the source fork. Target existence is resolved at apply.
func pruneUnchangedPickWindowRows(
	ses *Session, tables tableStuff,
	before *batch.Batch, data databranchutils.BranchHashmap,
) error {
	if before.RowCount() == 0 || data.ItemCount() == 0 {
		return nil
	}
	pkVec := before.Vecs[tables.def.pkColIdx]
	live, err := data.GetByVectors([]*vector.Vector{pkVec})
	if err != nil {
		return err
	}
	mp := ses.proc.Mp()
	var drop []int64
	for i, result := range live {
		if !result.Exists {
			continue
		}
		row := result.Rows[0]
		tuple, _, err := data.DecodeRow(row)
		if err != nil {
			return err
		}
		cmp, err := compareTupleWithBatchRow(tables, tuple, before, i, tables.def.pkKind != fakeKind)
		if err != nil {
			return err
		}
		if cmp != 0 {
			continue
		}
		if _, err := data.PopByEncodedFullValueExact(row, false); err != nil {
			return err
		}
		drop = append(drop, int64(i))
	}
	if len(drop) == 0 {
		return nil
	}
	slices.Sort(drop)
	return before.Shrink(drop, true, mp)
}

// Unrelated operands retain their existing conflict semantics, but a source
// replacement that equals its own window-start row is not a candidate change.
func pruneUnchangedPickRowsWithoutLCA(
	ctx context.Context, ses *Session, bh BackgroundExec, tables tableStuff, dagInfo branchMetaInfo,
	data, tombstones databranchutils.BranchHashmap,
) error {
	mp := ses.proc.Mp()
	keys := vector.NewVec(tables.def.colTypes[tables.def.pkColIdx], mp)
	defer keys.Free(mp)
	limit := tables.maxTombstoneBatchCnt
	if limit <= 0 {
		limit = maxSqlBatchCnt
	}
	var before tableStuff
	flush := func() error {
		if keys.Length() == 0 {
			return nil
		}
		if before.lcaRel == nil {
			tarSP, _ := tables.resolvedSnapshots(ses)
			layout, err := dataBranchPickWindowProbe(ctx, ses, bh, tables, dagInfo, tarSP)
			if err != nil {
				return err
			}
			before = layout
		}
		rows, _, err := probeDataBranchRows(ctx, ses, bh, keys, before, before.pickWindowStart.snapshot.ToTimestamp())
		if err != nil {
			return err
		}
		defer before.retPool.releaseRetBatch(rows, false)
		keys.CleanOnlyData()
		return pruneUnchangedPickWindowRows(ses, before, rows, data)
	}
	err := tombstones.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		return cursor.ForEach(func(key []byte, _ []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			live, err := data.GetByEncodedKey(key)
			if err != nil || !live.Exists {
				return err
			}
			tuple, _, err := tombstones.DecodeRow(key)
			if err != nil {
				return err
			}
			if err := appendTupleValueToVector(keys, tuple[0], mp); err != nil {
				return err
			}
			if keys.Length() >= limit {
				return flush()
			}
			return nil
		})
	}, 1)
	if err != nil {
		return err
	}
	return flush()
}

func findDeleteAndUpdateBat(
	ctx context.Context, ses *Session, bh BackgroundExec,
	tblStuff tableStuff, windowStart *tableStuff, tblName string, side int, deliver func(batchWithKind) error, branchTS types.TS,
	expandUpdate bool,
	dataHashmap, tombstoneHashmap databranchutils.BranchHashmap,
	parallelism int,
) (err error) {

	maxTombstoneBatchCnt := tblStuff.maxTombstoneBatchCnt
	if maxTombstoneBatchCnt <= 0 {
		maxTombstoneBatchCnt = maxSqlBatchCnt
	}

	if err = tombstoneHashmap.ForEachShardParallel(func(cursor databranchutils.ShardCursor) error {
		var (
			err2  error
			tuple types.Tuple
			tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true, ses.proc.Mp())
		)

		processBatch := func(tombBat *batch.Batch) error {
			if tombBat.RowCount() == 0 {
				tblStuff.retPool.releaseRetBatch(tombBat, true)
				return nil
			}

			probeStuff, probeTS := tblStuff, branchTS
			if windowStart != nil {
				probeStuff, probeTS = *windowStart, windowStart.pickWindowStart.snapshot
			}
			dBat, err2 := handleDelsOnLCA(ctx, ses, bh, tombBat, probeStuff, probeTS.ToTimestamp())
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

			if windowStart != nil {
				if err2 = pruneUnchangedPickWindowRows(ses, tblStuff, dBat, dataHashmap); err2 != nil {
					tblStuff.retPool.releaseRetBatch(dBat, false)
					return err2
				}
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
				tBat2 := tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
				seen := make([]bool, dBat.RowCount())
				var stagedUpdateBat *batch.Batch
				var nativeUpdateBat *batch.Batch
				var updateDeleteBat *batch.Batch
				if _, err2 = dataHashmap.PopByVectorsStream(
					[]*vector.Vector{dBat.Vecs[tblStuff.def.pkColIdx]}, false,
					func(idx int, _ []byte, row []byte) error {
						seen[idx] = true

						if tuple, _, err2 = dataHashmap.DecodeRow(row); err2 != nil {
							return err2
						}

						if windowStart == nil {
							cmp, err := compareTupleWithBatchRow(tblStuff, tuple, dBat, idx, tblStuff.def.pkKind != fakeKind)
							if err != nil || cmp == 0 {
								return err
							}
						}

						// delete on lca and insert into tar ==> update
						requiresNativeUpdate := false
						if expandUpdate {
							var updateErr error
							requiresNativeUpdate, updateErr = dataBranchUpdateRequiresNativeUpdateFromBatch(
								tblStuff, tuple, dBat, idx,
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
							updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
							if requiresNativeUpdate {
								nativeUpdateBat = updateBat
							} else {
								stagedUpdateBat = updateBat
							}
						}
						if expandUpdate && updateDeleteBat == nil {
							updateDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
						}
						if expandUpdate {
							if err2 = updateDeleteBat.UnionOne(dBat, int64(idx), ses.proc.Mp()); err2 != nil {
								return err2
							}
						}
						if err2 = appendTupleToBat(ses, updateBat, tuple, tblStuff); err2 != nil {
							return err2
						}
						return nil
					},
				); err2 != nil {
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
				tBat1 = tblStuff.retPool.acquireRetBatch(tblStuff, true, ses.proc.Mp())
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
		return
	}

	return nil
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
		types.T_bf16, types.T_float16, types.T_float8, types.T_float4,
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

		tarBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
		baseBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
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
								baseDeleteBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
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
								updateBat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())
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

		bat = tblStuff.retPool.acquireRetBatch(tblStuff, false, ses.proc.Mp())

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

func dataBranchPickedKeyRows(keys databranchutils.BranchHashmap, pk *vector.Vector) ([]int64, error) {
	results, err := keys.GetByVectors([]*vector.Vector{pk})
	if err != nil {
		return nil, err
	}
	var rows []int64
	for i, result := range results {
		if result.Exists {
			rows = append(rows, int64(i))
		}
	}
	return rows, nil
}

func buildHashmapForTable(
	ctx context.Context,
	mp *mpool.MPool,
	tblStuff *tableStuff,
	handles []engine.ChangesHandle,
	side string,
	pickKeyHashmap databranchutils.BranchHashmap,
) (
	dataHashmap databranchutils.BranchHashmap,
	tombstoneHashmap databranchutils.BranchHashmap,
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
					var sels []int64
					sels, taskErr = dataBranchPickedKeyRows(pickKeyHashmap, bat.Vecs[pkIdx])
					if taskErr != nil {
						atomicErr.Store(taskErr)
						return
					}
					if len(sels) == 0 {
						return // all rows filtered out
					}
					if len(sels) < bat.RowCount() {
						if taskErr = bat.Shrink(sels, false, mp); taskErr != nil {
							atomicErr.Store(taskErr)
							return
						}
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

	for _, handle := range handles {
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				return
			default:
			}
			if dataBat, tombstoneBat, _, err = handle.Next(
				ctx, mp,
			); err != nil {
				return
			} else if dataBat == nil && tombstoneBat == nil {
				// out of data
				break
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
			// In bounded PICK the destination range may start before its
			// own fork. An insert in that prefix was inherited by the
			// destination, and must not cancel its post-fork delete. Keep
			// the tombstone for the destination-fork probe; a true net-zero
			// insert/delete will not find a before-image there.
			if staleDataCount >= totalTombstones &&
				!(side == "base" && tblStuff.boundedPick && tblStuff.def.pkKind != fakeKind) {
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
		return nil, nil, err
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
	if side != "base" || dataBat == nil || dataBat.RowCount() == 0 {
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
	excludeTargetOnly bool,
	historical ...[]*plan2.ColDef,
) ([]int, error) {
	if sourceDef == nil || targetDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing schema for historical data branch projection")
	}
	var sourceColumns map[*plan2.ColDef]int
	if len(historical) != 0 {
		sourceColumns = make(map[*plan2.ColDef]int, len(historical[0]))
		for idx, col := range historical[0] {
			if col != nil {
				sourceColumns[col] = idx
			}
		}
	}
	if err := checkDataBranchPrimaryKeyCompatibilityWithResolver(
		targetDef,
		sourceDef,
		func(targetCol *plan2.ColDef) *plan2.ColDef {
			if sourceColumns != nil {
				if idx := dataBranchColumnIndexByName(targetColNames, targetCol.Name); idx >= 0 {
					return historical[0][idx]
				}
				return nil
			}
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
		var targetCol *plan2.ColDef
		targetIdx := -1
		if sourceColumns != nil {
			if idx, ok := sourceColumns[col]; ok {
				targetIdx = idx
				targetCol = dataBranchColumnDefByName(targetDef, targetColNames[idx])
			}
		} else {
			targetCol = dataBranchEndpointColumnDef(targetDef, col)
			if targetCol != nil {
				targetIdx = dataBranchColumnIndexByName(targetColNames, targetCol.Name)
			}
		}
		_, isTargetOnly := targetOnly[targetIdx]
		if excludeTargetOnly && isTargetOnly {
			// A historical base value is absent from the current base schema.
			targetIdx = -1
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
			if isTargetOnly {
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
				flat := vector.NewVec(*dst.GetType(), mp)
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
	probeStuff.lcaProbeTableId = 0
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
	endpointSnapshot, endpointRelSnapshot types.TS,
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
	tBat.Vecs[0] = vector.NewVec(*projected.Vecs[pkVecIdx].GetType(), ses.proc.Mp())
	defer tBat.Clean(ses.proc.Mp())
	if err = tBat.Vecs[0].UnionBatch(
		projected.Vecs[pkVecIdx], 0, projected.RowCount(), nil, ses.proc.Mp(),
	); err != nil {
		return err
	}
	tBat.SetRowCount(projected.RowCount())

	probeStuff := dataBranchHistoricalProbeStuff(ctx, tblStuff, endpointRel)
	probeStuff.lcaIdent = &endpointIdent
	probeStuff.lcaRelSnapshot = endpointRelSnapshot
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
	inner               engine.ChangesHandle
	sourceMapping       []int
	tblStuff            tableStuff
	ses                 *Session
	endpointRel         engine.Relation
	endpointSnapshot    types.TS
	endpointRelSnapshot types.TS
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
		ctx, h.ses, data, h.tblStuff, h.endpointIdent, h.endpointRel, h.endpointSnapshot, h.endpointRelSnapshot,
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

// currentBaseChangesHandle projects batches of the current base relation,
// read in the base table's layout, into the target layout. Only that
// relation's handle is wrapped: a historical relation's handle already
// yields the target layout (projected by historicalDataBranchChangesHandle,
// or identical to it), and a batch carries no names to tell them apart.
type currentBaseChangesHandle struct {
	inner    engine.ChangesHandle
	tblStuff tableStuff
}

func (h *currentBaseChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	data, tombstone, hint, err := h.inner.Next(ctx, mp)
	if err != nil {
		return data, tombstone, hint, err
	}
	data = projectBaseBatchToTargetIfNeeded("base", data, &h.tblStuff, mp)
	return data, tombstone, hint, nil
}

func (h *currentBaseChangesHandle) Close() error {
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
	sourceBat.Clean(mp)
	return out
}
