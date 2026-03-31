// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func handleBranchPick(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchPick,
) (err error) {
	if stmt.ConflictOpt == nil {
		stmt.ConflictOpt = &tree.ConflictOpt{
			Opt: tree.CONFLICT_FAIL,
		}
	}

	return diffMergeAgency(ses, execCtx, stmt)
}

// pickMergeDiffs is the consumer goroutine for PICK. It receives diff batches
// from the hashDiff producer, filters rows whose PK is in the KEYS set,
// and applies INSERT/DELETE SQL to the destination table.
func pickMergeDiffs(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		insertCnt int
		deleteCnt int

		insertIntoVals = acquireBuffer(tblStuff.bufPool)
		deleteFromVals = acquireBuffer(tblStuff.bufPool)
		firstErr       error
		tmpValsBuffer  = acquireBuffer(tblStuff.bufPool)
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, insertIntoVals)
		releaseBuffer(tblStuff.bufPool, deleteFromVals)
		releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
	}()

	defer func() {
		cancel()
	}()

	// Materialize the KEYS clause into a PK set for fast lookup.
	keySet, err := materializePickKeys(ctx, ses, bh, stmt, tblStuff)
	if err != nil {
		return err
	}

	appender := sqlValuesAppender{
		ctx:             ctx,
		ses:             ses,
		bh:              bh,
		tblStuff:        tblStuff,
		deleteByFullRow: tblStuff.def.pkKind == fakeKind,
		pkInfo:          newPKBatchInfo(ctx, ses, tblStuff),
		deleteCnt:       &deleteCnt,
		deleteBuf:       deleteFromVals,
		insertCnt:       &insertCnt,
		insertBuf:       insertIntoVals,
	}
	if err = initPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err != nil {
		return err
	}
	defer func() {
		if err2 := dropPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err2 != nil && err == nil {
			err = err2
		}
	}()

	for wrapped := range retCh {
		if firstErr != nil || ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		if err = appendPickedBatchRows(
			ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender, keySet,
		); err != nil {
			firstErr = err
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
	}

	if err = appender.flushAll(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return
}

// appendPickedBatchRows filters rows from a diff batch by PK key set,
// then appends matching rows to the SQL appender.
func appendPickedBatchRows(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
	keySet map[string]struct{},
) (err error) {
	row := make([]any, len(tblStuff.def.colNames))

	for rowIdx := range wrapped.batch.RowCount() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Extract PK value for this row and check membership.
		pkKey, err2 := extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
		if err2 != nil {
			return err2
		}

		if _, ok := keySet[pkKey]; !ok {
			continue // skip rows not in the KEYS set
		}

		// Row is in KEYS set — extract all visible column values.
		for _, colIdx := range tblStuff.def.visibleIdxes {
			vec := wrapped.batch.Vecs[colIdx]
			if rowIdx >= vec.Length() {
				return fmt.Errorf(
					"data branch pick batch shape mismatch: row=%d batchRows=%d col=%d vecLen=%d",
					rowIdx, wrapped.batch.RowCount(), colIdx, vec.Length(),
				)
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colIdx] = nil
				continue
			}

			switch vec.GetType().Oid {
			case types.T_datetime, types.T_timestamp, types.T_decimal64,
				types.T_decimal128, types.T_time:
				row[colIdx] = types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)
			default:
				if err = extractRowFromVector(
					ctx, ses, vec, colIdx, row, rowIdx, false,
				); err != nil {
					return
				}
			}
		}

		tmpValsBuffer.Reset()
		if wrapped.kind == diffDelete {
			if appender.deleteByFullRow {
				if err = writeDeleteRowSQLFull(ctx, ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else if appender.pkInfo != nil {
				if err = writeDeleteRowValuesAsTuple(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else {
				if err = writeDeleteRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			}
		} else {
			if err = writeInsertRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
				return
			}
		}

		if tmpValsBuffer.Len() == 0 {
			continue
		}

		if err = appender.appendRow(wrapped.kind, tmpValsBuffer.Bytes()); err != nil {
			return
		}
	}

	return nil
}

// extractPKAsString serializes the PK column(s) of a batch row into a
// canonical string for map-based key matching.
func extractPKAsString(
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	rowIdx int,
) (string, error) {
	var buf bytes.Buffer

	for i, colIdx := range tblStuff.def.pkColIdxes {
		vec := bat.Vecs[colIdx]
		if vec.GetNulls().Contains(uint64(rowIdx)) {
			buf.WriteString("NULL")
		} else {
			val := extractPKVal(vec, rowIdx)
			if err := formatValIntoString(ses, val, tblStuff.def.colTypes[colIdx], &buf); err != nil {
				return "", err
			}
		}
		if i < len(tblStuff.def.pkColIdxes)-1 {
			buf.WriteByte(',')
		}
	}

	return buf.String(), nil
}

// extractPKVal extracts the Go value from a vector at the given row index.
func extractPKVal(vec *vector.Vector, rowIdx int) any {
	switch vec.GetType().Oid {
	case types.T_datetime, types.T_timestamp, types.T_decimal64,
		types.T_decimal128, types.T_time:
		return types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, rowIdx)
	case types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, rowIdx)
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, rowIdx)
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, rowIdx)
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, rowIdx)
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, rowIdx)
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, rowIdx)
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, rowIdx)
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, rowIdx)
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, rowIdx)
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, rowIdx)
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, rowIdx)
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, rowIdx)
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, rowIdx)
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, rowIdx)
	default:
		return vec.GetBytesAt(rowIdx)
	}
}

// materializePickKeys converts the KEYS clause (values or subquery) into
// a map[string]struct{} for O(1) PK membership testing.
func materializePickKeys(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
) (map[string]struct{}, error) {
	if stmt.Keys == nil {
		return nil, fmt.Errorf("DATA BRANCH PICK requires a KEYS clause")
	}

	switch stmt.Keys.Type {
	case tree.PickKeysValues:
		return materializePickKeysFromValues(ses, stmt, tblStuff)
	case tree.PickKeysSubquery:
		return materializePickKeysFromSubquery(ctx, ses, bh, stmt, tblStuff)
	default:
		return nil, fmt.Errorf("unsupported KEYS type: %d", stmt.Keys.Type)
	}
}

// materializePickKeysFromValues converts literal key expressions to string keys.
func materializePickKeysFromValues(
	ses *Session,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
) (map[string]struct{}, error) {
	keySet := make(map[string]struct{}, len(stmt.Keys.KeyExprs))
	var buf bytes.Buffer

	for _, expr := range stmt.Keys.KeyExprs {
		buf.Reset()

		switch e := expr.(type) {
		case *tree.Tuple:
			// Composite PK: (val1, val2, ...)
			for i, sub := range e.Exprs {
				if err := formatExprIntoString(ses, sub, &buf); err != nil {
					return nil, err
				}
				if i < len(e.Exprs)-1 {
					buf.WriteByte(',')
				}
			}
		default:
			// Single PK value
			if err := formatExprIntoString(ses, expr, &buf); err != nil {
				return nil, err
			}
		}

		keySet[buf.String()] = struct{}{}
	}

	return keySet, nil
}

// materializePickKeysFromSubquery executes the subquery and collects PK values.
func materializePickKeysFromSubquery(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
) (map[string]struct{}, error) {
	if stmt.Keys.Select == nil {
		return nil, fmt.Errorf("KEYS subquery is nil")
	}

	// Format the subquery to SQL.
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
	stmt.Keys.Select.Format(fmtCtx)
	sql := fmtCtx.String()

	if err := bh.Exec(ctx, sql); err != nil {
		return nil, fmt.Errorf("failed to execute KEYS subquery: %w", err)
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, fmt.Errorf("failed to get KEYS subquery results: %w", err)
	}

	keySet := make(map[string]struct{})
	var buf bytes.Buffer

	for _, rs := range erArray {
		for rowIdx := uint64(0); rowIdx < rs.GetRowCount(); rowIdx++ {
			buf.Reset()
			colCnt := rs.GetColumnCount()

			for colIdx := uint64(0); colIdx < colCnt; colIdx++ {
				val, err2 := rs.GetString(ctx, rowIdx, colIdx)
				if err2 != nil {
					return nil, fmt.Errorf("failed to read KEYS subquery result: %w", err2)
				}
				buf.WriteString(quoteStringForKey(val))
				if colIdx < colCnt-1 {
					buf.WriteByte(',')
				}
			}

			keySet[buf.String()] = struct{}{}
		}
	}

	return keySet, nil
}

// formatExprIntoString converts an AST expression (literal) to its canonical
// string form for key matching.
func formatExprIntoString(ses *Session, expr tree.Expr, buf *bytes.Buffer) error {
	switch e := expr.(type) {
	case *tree.NumVal:
		buf.WriteString(e.String())
		return nil
	case *tree.StrVal:
		buf.WriteString(quoteStringForKey(e.String()))
		return nil
	case *tree.UnresolvedName:
		buf.WriteString(e.ColName())
		return nil
	default:
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
		expr.Format(fmtCtx)
		buf.WriteString(fmtCtx.String())
		return nil
	}
}

// quoteStringForKey wraps a string value in single quotes for canonical key matching.
func quoteStringForKey(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// buildPKFilterForPick constructs an engine.PKFilter from literal KEYS values.
// For subquery-based KEYS, returns nil (engine-level pruning is skipped;
// the consumer-level string filter still guarantees correctness).
// The returned vector is sorted, as required by ZoneMap.AnyIn().
func buildPKFilterForPick(
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
	mp *mpool.MPool,
) (*engine.PKFilter, error) {
	if stmt.Keys == nil || stmt.Keys.Type != tree.PickKeysValues {
		return nil, nil
	}
	// Only support single-column PK for engine-level pruning.
	// Composite PK uses __mo_cpkey_col which is opaque at the engine layer.
	if tblStuff.def.pkKind != normalKind {
		return nil, nil
	}

	pkType := tblStuff.def.colTypes[tblStuff.def.pkColIdx]
	vec := vector.NewVec(pkType)

	for _, expr := range stmt.Keys.KeyExprs {
		if err := appendExprToVec(vec, expr, pkType, mp); err != nil {
			vec.Free(mp)
			return nil, err
		}
	}

	// Sort the vector — required by ZoneMap.AnyIn() which uses binary search.
	vec.InplaceSort()

	return &engine.PKFilter{
		Vec:           vec,
		PrimarySeqnum: tblStuff.def.pkColIdx,
	}, nil
}

// appendExprToVec appends a literal AST expression value to a typed vector.
func appendExprToVec(vec *vector.Vector, expr tree.Expr, pkType types.Type, mp *mpool.MPool) error {
	switch e := expr.(type) {
	case *tree.NumVal:
		return appendNumValToVec(vec, e, pkType, mp)
	case *tree.StrVal:
		return vector.AppendBytes(vec, []byte(e.String()), false, mp)
	default:
		// For complex expressions, skip engine-level filtering.
		return fmt.Errorf("unsupported expression type for PK filter: %T", expr)
	}
}

// appendNumValToVec converts a numeric literal to the correct typed value
// and appends it to the vector.
func appendNumValToVec(vec *vector.Vector, val *tree.NumVal, pkType types.Type, mp *mpool.MPool) error {
	s := val.String()
	switch pkType.Oid {
	case types.T_int8:
		v, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int8(v), false, mp)
	case types.T_int16:
		v, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int16(v), false, mp)
	case types.T_int32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int32(v), false, mp)
	case types.T_int64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_uint8:
		v, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint8(v), false, mp)
	case types.T_uint16:
		v, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint16(v), false, mp)
	case types.T_uint32:
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint32(v), false, mp)
	case types.T_uint64:
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_float32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, float32(v), false, mp)
	case types.T_float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_varchar, types.T_char, types.T_text, types.T_blob:
		return vector.AppendBytes(vec, []byte(s), false, mp)
	default:
		// For other types (decimal, date, etc.), skip engine-level filtering.
		return fmt.Errorf("unsupported PK type for engine filter: %s", pkType.Oid.String())
	}
}

// freePKFilter safely frees the PK filter vector.
func freePKFilter(filter *engine.PKFilter, mp *mpool.MPool) {
	if filter != nil && filter.Vec != nil {
		filter.Vec.Free(mp)
		filter.Vec = nil
	}
}
