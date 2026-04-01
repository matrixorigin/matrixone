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
	"errors"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
// from the hashDiff producer (which uses ACCEPT internally), filters rows whose
// PK is in the KEYS set, and applies INSERT/DELETE SQL to the destination table.
//
// The user's conflict option is enforced here:
//   - ACCEPT: base DELETE + target INSERT are both applied (source value wins).
//   - SKIP:   base DELETE rows mark keys to skip; corresponding target INSERTs are ignored.
//   - FAIL:   base DELETE for a picked key means conflict → return error.
//
// Within each hashDiff shard, base DELETE batches are emitted before target INSERT
// batches, so the skipSet is populated before the corresponding INSERTs arrive.
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

	// skipSet tracks picked keys that should be skipped (SKIP conflict mode).
	var skipSet map[string]struct{}
	if stmt.ConflictOpt != nil && stmt.ConflictOpt.Opt == tree.CONFLICT_SKIP {
		skipSet = make(map[string]struct{})
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
			ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
			keySet, stmt.ConflictOpt, skipSet,
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
// then applies the user's conflict semantics before appending to the SQL appender.
//
// hashDiff uses ACCEPT internally, so:
//   - base DELETE batches (side=base, kind=DELETE) represent conflict victims
//     (old base rows being replaced by source values).
//   - target INSERT batches (side=target, kind=INSERT) contain both new rows
//     and conflict-winning source rows.
//
// The consumer enforces the user's actual conflict choice:
//   - ACCEPT: apply both DELETE and INSERT (source wins). Same as hashDiff output.
//   - SKIP:   record conflicting keys in skipSet; skip both DELETE and INSERT.
//   - FAIL:   return error when a base DELETE exists for a picked key.
func appendPickedBatchRows(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
	keySet map[string]struct{},
	userConflictOpt *tree.ConflictOpt,
	skipSet map[string]struct{},
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

		// Handle conflict semantics for picked keys.
		if wrapped.side == diffSideBase && wrapped.kind == diffDelete {
			// A base DELETE for a picked key means the key exists in both
			// source and destination with different values (a conflict).
			if userConflictOpt != nil {
				switch userConflictOpt.Opt {
				case tree.CONFLICT_FAIL:
					return moerr.NewInternalErrorNoCtxf(
						"conflict: %s %s and %s %s on pk(%v) with different values",
						tblStuff.tarRel.GetTableName(), diffInsert,
						tblStuff.baseRel.GetTableName(), diffInsert,
						pkKey,
					)
				case tree.CONFLICT_SKIP:
					skipSet[pkKey] = struct{}{}
					continue // do not apply the DELETE
				case tree.CONFLICT_ACCEPT:
					// fall through — apply the DELETE (source value wins)
				}
			}
		}

		// For SKIP mode: skip target INSERTs for conflicting keys.
		if skipSet != nil {
			if _, skipped := skipSet[pkKey]; skipped {
				continue
			}
		}

		// Row is in KEYS set and passed conflict checks — extract all visible column values.
		for _, colIdx := range tblStuff.def.visibleIdxes {
			vec := wrapped.batch.Vecs[colIdx]
			if rowIdx >= vec.Length() {
				return moerr.NewInternalErrorNoCtxf(
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
		return nil, moerr.NewInvalidInputNoCtx("DATA BRANCH PICK requires a KEYS clause")
	}

	switch stmt.Keys.Type {
	case tree.PickKeysValues:
		return materializePickKeysFromValues(ses, stmt, tblStuff)
	case tree.PickKeysSubquery:
		return materializePickKeysFromSubquery(ctx, ses, bh, stmt, tblStuff)
	default:
		return nil, moerr.NewInvalidInputNoCtxf("unsupported KEYS type: %d", stmt.Keys.Type)
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
		if err := formatPKExprIntoCanonicalString(ses, expr, tblStuff, &buf); err != nil {
			return nil, err
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
		return nil, moerr.NewInvalidInputNoCtx("KEYS subquery is nil")
	}

	// Format the subquery to SQL.
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
	stmt.Keys.Select.Format(fmtCtx)
	sql := fmtCtx.String()

	if err := bh.Exec(ctx, sql); err != nil {
		return nil, errors.New("failed to execute KEYS subquery: " + err.Error())
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, errors.New("failed to get KEYS subquery results: " + err.Error())
	}

	keySet := make(map[string]struct{})
	var buf bytes.Buffer

	for _, rs := range erArray {
		for rowIdx := uint64(0); rowIdx < rs.GetRowCount(); rowIdx++ {
			buf.Reset()
			if err := formatPKResultRowIntoCanonicalString(ctx, ses, rs, rowIdx, tblStuff, &buf); err != nil {
				return nil, err
			}
			keySet[buf.String()] = struct{}{}
		}
	}

	return keySet, nil
}

func formatPKExprIntoCanonicalString(
	ses *Session,
	expr tree.Expr,
	tblStuff tableStuff,
	buf *bytes.Buffer,
) error {
	pkColIdxes := tblStuff.def.pkColIdxes
	pkTypes := tblStuff.def.colTypes
	switch e := expr.(type) {
	case *tree.Tuple:
		if len(e.Exprs) != len(pkColIdxes) {
			return moerr.NewInvalidInputNoCtxf("invalid composite KEYS width: got %d want %d", len(e.Exprs), len(pkColIdxes))
		}
		for i, sub := range e.Exprs {
			if err := formatSinglePKExprIntoCanonicalString(ses, sub, pkTypes[pkColIdxes[i]], buf); err != nil {
				return err
			}
			if i < len(e.Exprs)-1 {
				buf.WriteByte(',')
			}
		}
	default:
		if len(pkColIdxes) != 1 {
			return moerr.NewInvalidInputNoCtxf("invalid scalar KEYS width for composite PK: got 1 want %d", len(pkColIdxes))
		}
		if err := formatSinglePKExprIntoCanonicalString(ses, expr, pkTypes[pkColIdxes[0]], buf); err != nil {
			return err
		}
	}
	return nil
}

func formatSinglePKExprIntoCanonicalString(
	ses *Session,
	expr tree.Expr,
	pkType types.Type,
	buf *bytes.Buffer,
) error {
	vec := vector.NewVec(pkType)
	defer vec.Free(ses.proc.Mp())
	if err := appendExprToVec(vec, expr, pkType, ses.proc.Mp()); err != nil {
		return err
	}
	val := extractPKVal(vec, 0)
	return formatValIntoString(ses, val, pkType, buf)
}

func isStringLikePKType(t types.Type) bool {
	switch t.Oid {
	case types.T_varchar, types.T_char, types.T_text, types.T_blob, types.T_binary, types.T_varbinary:
		return true
	default:
		return false
	}
}

func unescapeMySQLString(s string) string {
	if strings.IndexByte(s, '\\') < 0 && strings.Index(s, "''") < 0 {
		return s
	}
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case '0':
				buf = append(buf, 0)
			case 'b':
				buf = append(buf, '\b')
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case 'Z':
				buf = append(buf, 0x1a)
			default:
				buf = append(buf, s[i])
			}
			continue
		}
		if s[i] == '\'' && i+1 < len(s) && s[i+1] == '\'' {
			buf = append(buf, '\'')
			i++
			continue
		}
		buf = append(buf, s[i])
	}
	return string(buf)
}

func getExecResultPKValue(
	ctx context.Context,
	rs ExecResult,
	rowIdx, colIdx uint64,
	pkType types.Type,
) (any, error) {
	if typedRS, ok := rs.(ResultSet); ok {
		val, err := typedRS.GetValue(ctx, rowIdx, colIdx)
		if err != nil {
			return nil, err
		}
		if !isStringLikePKType(pkType) {
			return val, nil
		}
		switch x := val.(type) {
		case string:
			return unescapeMySQLString(x), nil
		case []byte:
			return []byte(unescapeMySQLString(string(x))), nil
		default:
			return val, nil
		}
	}
	switch pkType.Oid {
	case types.T_bool:
		v, err := rs.GetInt64(ctx, rowIdx, colIdx)
		return v != 0, err
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return rs.GetUint64(ctx, rowIdx, colIdx)
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return rs.GetInt64(ctx, rowIdx, colIdx)
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary:
		s, err := rs.GetString(ctx, rowIdx, colIdx)
		if err != nil {
			return nil, err
		}
		return unescapeMySQLString(s), nil
	case types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_year, types.T_json, types.T_array_float32, types.T_array_float64:
		return rs.GetString(ctx, rowIdx, colIdx)
	default:
		return rs.GetString(ctx, rowIdx, colIdx)
	}
}

func formatPKResultRowIntoCanonicalString(
	ctx context.Context,
	ses *Session,
	rs ExecResult,
	rowIdx uint64,
	tblStuff tableStuff,
	buf *bytes.Buffer,
) error {
	pkColIdxes := tblStuff.def.pkColIdxes
	if rs.GetColumnCount() != uint64(len(pkColIdxes)) {
		return moerr.NewInvalidInputNoCtxf("invalid KEYS subquery column count: got %d want %d", rs.GetColumnCount(), len(pkColIdxes))
	}
	for i, pkColIdx := range pkColIdxes {
		pkType := tblStuff.def.colTypes[pkColIdx]
		val, err := getExecResultPKValue(ctx, rs, rowIdx, uint64(i), pkType)
		if err != nil {
			return errors.New("failed to read KEYS subquery result: " + err.Error())
		}
		if err := formatValIntoString(ses, val, pkType, buf); err != nil {
			return err
		}
		if i < len(pkColIdxes)-1 {
			buf.WriteByte(',')
		}
	}
	return nil
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
	case *tree.UnaryExpr:
		num, ok := e.Expr.(*tree.NumVal)
		if !ok {
			return moerr.NewInvalidInputNoCtxf("unsupported unary expression type for PK filter: %T", e.Expr)
		}
		s := num.String()
		switch e.Op {
		case tree.UNARY_MINUS:
			s = "-" + s
		case tree.UNARY_PLUS:
			s = "+" + s
		default:
			return moerr.NewInvalidInputNoCtxf("unsupported unary operator for PK filter: %v", e.Op)
		}
		return appendNumericStringToVec(vec, s, pkType, mp)
	case *tree.StrVal:
		return vector.AppendBytes(vec, []byte(unescapeMySQLString(e.String())), false, mp)
	default:
		// For complex expressions, skip engine-level filtering.
		return moerr.NewInvalidInputNoCtxf("unsupported expression type for PK filter: %T", expr)
	}
}

// appendNumValToVec converts a numeric literal to the correct typed value
// and appends it to the vector.
func appendNumValToVec(vec *vector.Vector, val *tree.NumVal, pkType types.Type, mp *mpool.MPool) error {
	return appendNumericStringToVec(vec, val.String(), pkType, mp)
}

func appendNumericStringToVec(vec *vector.Vector, s string, pkType types.Type, mp *mpool.MPool) error {
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
		return moerr.NewInvalidInputNoCtxf("unsupported PK type for engine filter: %s", pkType.Oid.String())
	}
}

// freePKFilter safely frees the PK filter vector.
func freePKFilter(filter *engine.PKFilter, mp *mpool.MPool) {
	if filter != nil && filter.Vec != nil {
		filter.Vec.Free(mp)
		filter.Vec = nil
	}
}
