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
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

func containsDataBranchTempTableName(sqlLower string) bool {
	return containsTempTableMarker(sqlLower, "__mo_diff_del_") ||
		containsTempTableMarker(sqlLower, "__mo_diff_ins_")
}

func containsTempTableMarker(sqlLower, marker string) bool {
	searchFrom := 0
	for {
		offset := strings.Index(sqlLower[searchFrom:], marker)
		if offset < 0 {
			return false
		}
		idx := searchFrom + offset
		if idx == 0 {
			return true
		}
		switch sqlLower[idx-1] {
		case ' ', '\t', '\n', '\r', '.', '(', ',':
			return true
		}
		searchFrom = idx + len(marker)
	}
}

func acquireBuffer(pool *sync.Pool) *bytes.Buffer {
	if pool == nil {
		return &bytes.Buffer{}
	}
	buf := pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func releaseBuffer(pool *sync.Pool, buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	if pool != nil {
		pool.Put(buf)
	}
}

func newEmitter(
	ctx context.Context, stopCh <-chan struct{}, retCh chan batchWithKind,
) emitFunc {
	return func(wrapped batchWithKind) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-stopCh:
			return true, nil
		default:
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-stopCh:
			return true, nil
		case retCh <- wrapped:
			return false, nil
		}
	}
}

func emitBatch(
	emit emitFunc, wrapped batchWithKind, forTombstone bool, pool *retBatchList,
) (bool, error) {
	stop, err := emit(wrapped)
	if stop || err != nil {
		pool.releaseRetBatch(wrapped.batch, forTombstone)
		return stop, err
	}
	return false, nil
}

func runSql(
	ctx context.Context, ses *Session, bh BackgroundExec, sql string,
	streamChan chan executor.Result, errChan chan error,
) (sqlRet executor.Result, err error) {

	useBackExec := false
	trimmedLower := strings.ToLower(strings.TrimSpace(sql))
	if strings.HasPrefix(trimmedLower, "drop database") {
		// Internal executor does not support DROP DATABASE (IsPublishing panics).
		useBackExec = true
	} else if containsDataBranchTempTableName(trimmedLower) {
		// Branch diff/merge/pick temp tables do repeated DDL/DML in one shared txn.
		// The internal SQL fast path skips per-statement workspace increments and can
		// hit ErrTxnNeedRetryWithDefChanged in RC mode while these temp definitions churn.
		useBackExec = true
	} else if strings.Contains(strings.ToLower(snapConditionRegex.FindString(sql)), "snapshot") {
		// SQLExecutor cannot resolve snapshot by name.
		useBackExec = true
	}

	var exec executor.SQLExecutor
	if !useBackExec {
		rt := moruntime.ServiceRuntime(ses.service)
		if rt == nil {
			useBackExec = true
		} else {
			val, exist := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			if !exist {
				useBackExec = true
			} else {
				exec = val.(executor.SQLExecutor)
			}
		}
	}

	if useBackExec {
		// export as CSV need this
		// bh.(*backExec).backSes.SetMysqlResultSet(&MysqlResultSet{})
		//for range tblStuff.def.visibleIdxes {
		//	bh.(*backExec).backSes.mrs.AddColumn(&MysqlColumn{})
		//}
		if err = bh.Exec(ctx, sql); err != nil {
			return
		}
		if _, ok := bh.(*backExec); ok {
			bh.ClearExecResultSet()
			sqlRet.Mp = ses.proc.Mp()
			sqlRet.Batches = bh.GetExecResultBatches()
			return
		}

		rs := bh.GetExecResultSet()
		bh.ClearExecResultSet()
		if len(rs) == 0 || rs[0] == nil {
			return
		}
		mrs, ok := rs[0].(*MysqlResultSet)
		if !ok {
			return sqlRet, moerr.NewInternalError(ctx, "unexpected result set type")
		}
		if len(mrs.Columns) == 0 {
			return
		}
		var bat *batch.Batch
		bat, _, err = convertRowsIntoBatch(ses.proc.Mp(), mrs.Columns, mrs.Data)
		if err != nil {
			return sqlRet, err
		}
		sqlRet.Mp = ses.proc.Mp()
		sqlRet.Batches = []*batch.Batch{bat}
		return
	}

	// we do not use the bh.Exec here, it's too slow.
	// use internal sql instead.

	backSes := bh.(*backExec).backSes

	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(backSes.GetTxnHandler().GetTxn()).
		WithKeepTxnAlive().
		WithTimeZone(ses.GetTimeZone()).
		WithDatabase(ses.GetDatabaseName())

	if streamChan != nil && errChan != nil {
		opts = opts.WithStreaming(streamChan, errChan)
	}

	if sqlRet, err = exec.Exec(ctx, sql, opts); err != nil {
		logutil.Error(
			"DataBranch-RunSQL-Error",
			zap.Bool("use-back-exec", useBackExec),
			zap.String("txn", backSes.GetTxnInfo()),
			zap.String("sql", sql),
			zap.Error(err),
		)
		return sqlRet, err
	}

	return sqlRet, nil
}

// shouldUseLCAReaderFallback returns true only for recoverable snapshot-read
// failures where an engine reader based fallback can preserve LCA semantics.
// After GC, time travelling by account/db/table name can fail if no snapshot or
// PITR history was created for the corresponding account/db/table and the
// historical catalog can no longer resolve that name. In that case, branch diff
// should fall back to table-id based readers.
//
// ErrParseError is included because the SQL compiler emits it as
// "table X does not exist" when getRelation() returns nil after catalog
// name lookup fails at an old snapshot (the original ErrNoSuchTable is
// swallowed in sql_executor_context.getRelation).
func shouldUseLCAReaderFallback(err error) bool {
	if err == nil {
		return false
	}
	return moerr.IsMoErrCode(err, moerr.ErrFileNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrStaleRead) ||
		moerr.IsMoErrCode(err, moerr.ErrBadDB) ||
		moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
		moerr.IsMoErrCode(err, moerr.ErrParseError)
}

// scanSnapshotRelationByID scans a relation with a split-view strategy:
//   - current relation: resolved at the current transaction view to collect
//     only currently valid physical objects (avoid stale/GC'ed object names).
//   - snapshot reader: rebuilt directly from the requested snapshot timestamp
//     and the current relation's stable table handle, without re-resolving the
//     historical relation through catalog name lookup.
//
// After GC, time travelling by account/db/table name can fail if no snapshot or
// PITR history was created for the corresponding account/db/table. This helper
// keeps object selection and row visibility decoupled so data-branch fallback
// paths can still probe old snapshots without requiring snapshotRelation lookup.
func scanSnapshotRelationByID(
	ctx context.Context,
	caller string,
	ses *Session,
	tableID uint64,
	snapshotTS types.TS,
	attrs []string,
	colTypes []types.Type,
	filterExpr *plan.Expr,
	scanParallelism int,
	onBatch func(*batch.Batch) error,
) error {
	if len(attrs) == 0 {
		return nil
	}
	if len(attrs) != len(colTypes) {
		return moerr.NewInternalErrorNoCtxf(
			"scanSnapshotRelationByID: attrs/colTypes length mismatch, attrs=%d colTypes=%d",
			len(attrs),
			len(colTypes),
		)
	}

	storage := ses.GetTxnHandler().GetStorage()
	baseTxnOp := ses.GetTxnHandler().GetTxn()
	rangeTS := types.TimestampToTS(baseTxnOp.SnapshotTS())
	logutil.Info(
		"DataBranch-SnapshotScan-Start",
		zap.String("caller", caller),
		zap.Uint64("table-id", tableID),
		zap.String("range-ts", rangeTS.ToString()),
		zap.String("snapshot-ts", snapshotTS.ToString()),
		zap.Bool("has-filter-expr", filterExpr != nil),
		zap.Int("attr-cnt", len(attrs)),
		zap.Int("scan-parallelism", scanParallelism),
	)

	_, _, rangeRel, err := storage.GetRelationById(ctx, baseTxnOp, tableID)
	if err != nil {
		return err
	}
	if rangeRel == nil {
		return moerr.NewInternalErrorNoCtxf(
			"scanSnapshotRelationByID: cannot resolve range relation by id %d at current txn view",
			tableID,
		)
	}

	rangesParam := engine.DefaultRangesParam
	if filterExpr != nil {
		rangesParam.BlockFilters = []*plan.Expr{filterExpr}
	}

	relData, err := rangeRel.Ranges(ctx, rangesParam)
	if err != nil {
		logutil.Error(
			"DataBranch-SnapshotScan-Ranges-Error",
			zap.String("caller", caller),
			zap.Uint64("table-id", tableID),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Error(err),
		)
		return err
	}
	rangeBlocks := relData.GetBlockInfoSlice()
	rangeBlockCnt := rangeBlocks.Len()
	logutil.Info(
		"DataBranch-SnapshotScan-Ranges-Done",
		zap.String("caller", caller),
		zap.Uint64("table-id", tableID),
		zap.String("range-ts", rangeTS.ToString()),
		zap.String("snapshot-ts", snapshotTS.ToString()),
		zap.Int("rel-data-type", int(relData.GetType())),
		zap.Int("range-data-cnt", relData.DataCnt()),
		zap.Int("range-block-cnt", rangeBlockCnt),
	)

	var (
		readBatchCnt atomic.Int64
		readRowCnt   atomic.Int64
	)

	if err = disttae.ScanSnapshotWithCurrentRanges(
		ctx,
		caller,
		rangeRel,
		relData,
		snapshotTS,
		attrs,
		colTypes,
		filterExpr,
		scanParallelism,
		ses.proc.Mp(),
		func(readBatch *batch.Batch) error {
			readBatchCnt.Add(1)
			readRowCnt.Add(int64(readBatch.RowCount()))
			return onBatch(readBatch)
		},
	); err != nil {
		logutil.Error(
			"DataBranch-SnapshotScan-Reader-Error",
			zap.String("caller", caller),
			zap.Uint64("table-id", tableID),
			zap.String("range-ts", rangeTS.ToString()),
			zap.String("snapshot-ts", snapshotTS.ToString()),
			zap.Int("range-data-cnt", relData.DataCnt()),
			zap.Int("range-block-cnt", rangeBlockCnt),
			zap.Int64("read-batch-cnt", readBatchCnt.Load()),
			zap.Int64("read-row-cnt", readRowCnt.Load()),
			zap.Error(err),
		)
		return err
	}
	logutil.Info(
		"DataBranch-SnapshotScan-Done",
		zap.String("caller", caller),
		zap.Uint64("table-id", tableID),
		zap.String("range-ts", rangeTS.ToString()),
		zap.String("snapshot-ts", snapshotTS.ToString()),
		zap.Int64("read-batch-cnt", readBatchCnt.Load()),
		zap.Int64("read-row-cnt", readRowCnt.Load()),
	)
	return nil
}

func formatValIntoString(ses *Session, val any, t types.Type, buf *bytes.Buffer) error {
	if val == nil {
		buf.WriteString("NULL")
		return nil
	}

	var scratch [64]byte

	writeInt := func(v int64) {
		buf.Write(strconv.AppendInt(scratch[:0], v, 10))
	}

	writeUint := func(v uint64) {
		buf.Write(strconv.AppendUint(scratch[:0], v, 10))
	}

	writeFloat := func(v float64, bitSize int) {
		buf.Write(strconv.AppendFloat(scratch[:0], v, 'g', -1, bitSize))
	}

	writeBool := func(v bool) {
		buf.WriteString(strconv.FormatBool(v))
	}

	switch t.Oid {
	case types.T_varchar, types.T_text, types.T_json, types.T_char, types.
		T_varbinary, types.T_binary:
		if t.Oid == types.T_json {
			var strVal string
			switch x := val.(type) {
			case bytejson.ByteJson:
				strVal = x.String()
			case *bytejson.ByteJson:
				if x == nil {
					return moerr.NewInternalErrorNoCtx("formatValIntoString: nil *bytejson.ByteJson")
				}
				strVal = x.String()
			case []byte:
				strVal = string(x)
			case string:
				strVal = x
			default:
				return moerr.NewInternalErrorNoCtxf("formatValIntoString: unexpected json type %T", val)
			}
			jsonLiteral := escapeJSONControlBytes([]byte(strVal))
			if !json.Valid(jsonLiteral) {
				return moerr.NewInternalErrorNoCtxf("formatValIntoString: invalid json input %q", strVal)
			}
			writeEscapedSQLString(buf, jsonLiteral)
			return nil
		}
		switch x := val.(type) {
		case []byte:
			writeEscapedSQLString(buf, x)
		case string:
			writeEscapedSQLString(buf, []byte(x))
		default:
			return moerr.NewInternalErrorNoCtxf("formatValIntoString: unexpected string type %T", val)
		}
	case types.T_timestamp:
		buf.WriteString("'")
		buf.WriteString(val.(types.Timestamp).String2(ses.timeZone, t.Scale))
		buf.WriteString("'")
	case types.T_datetime:
		buf.WriteString("'")
		buf.WriteString(val.(types.Datetime).String2(t.Scale))
		buf.WriteString("'")
	case types.T_time:
		buf.WriteString("'")
		buf.WriteString(val.(types.Time).String2(t.Scale))
		buf.WriteString("'")
	case types.T_date:
		buf.WriteString("'")
		buf.WriteString(val.(types.Date).String())
		buf.WriteString("'")
	case types.T_year:
		buf.WriteString(val.(types.MoYear).String())
	case types.T_decimal64:
		buf.WriteString(val.(types.Decimal64).Format(t.Scale))
	case types.T_decimal128:
		buf.WriteString(val.(types.Decimal128).Format(t.Scale))
	case types.T_decimal256:
		buf.WriteString(val.(types.Decimal256).Format(t.Scale))
	case types.T_bool:
		writeBool(val.(bool))
	case types.T_uint8:
		writeUint(uint64(val.(uint8)))
	case types.T_uint16:
		writeUint(uint64(val.(uint16)))
	case types.T_uint32:
		writeUint(uint64(val.(uint32)))
	case types.T_uint64:
		writeUint(val.(uint64))
	case types.T_int8:
		writeInt(int64(val.(int8)))
	case types.T_int16:
		writeInt(int64(val.(int16)))
	case types.T_int32:
		writeInt(int64(val.(int32)))
	case types.T_int64:
		writeInt(val.(int64))
	case types.T_float32:
		writeFloat(float64(val.(float32)), 32)
	case types.T_float64:
		writeFloat(val.(float64), 64)
	case types.T_array_float32:
		buf.WriteString("'")
		buf.WriteString(types.ArrayToString[float32](val.([]float32)))
		buf.WriteString("'")
	case types.T_array_float64:
		buf.WriteString("'")
		buf.WriteString(types.ArrayToString[float64](val.([]float64)))
		buf.WriteString("'")
	default:
		return moerr.NewNotSupportedNoCtxf("formatValIntoString: not support type %v", t.Oid)
	}

	return nil
}

// writeEscapedSQLString escapes special and control characters for SQL literal output.
func writeEscapedSQLString(buf *bytes.Buffer, b []byte) {
	buf.WriteByte('\'')
	for _, c := range b {
		switch c {
		case '\'':
			buf.WriteString("\\'")
		//case '"':
		//	buf.WriteString("\\\"")
		case '\\':
			buf.WriteString("\\\\")
		case 0:
			buf.WriteString("\\0")
		case '\b':
			buf.WriteString("\\b")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\t':
			buf.WriteString("\\t")
		case 0x1A:
			buf.WriteString("\\Z")
		default:
			if c < 0x20 || c == 0x7f {
				buf.WriteString("\\x")
				buf.WriteString(hex.EncodeToString([]byte{c}))
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('\'')
}

// escapeJSONControlChars converts control characters to JSON-compliant \u00XX escapes.
func escapeJSONControlBytes(b []byte) []byte {
	var out []byte
	hexDigits := "0123456789abcdef"

	for i, c := range b {
		if c < 0x20 || c == 0x7f {
			if out == nil {
				out = make([]byte, 0, len(b)+8)
				out = append(out, b[:i]...)
			}
			out = append(out, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			continue
		}
		if out != nil {
			out = append(out, c)
		}
	}

	if out == nil {
		return b
	}
	return out
}

func compareSingleValInVector(
	ctx context.Context,
	ses *Session,
	rowIdx1 int,
	rowIdx2 int,
	vec1 *vector.Vector,
	vec2 *vector.Vector,
) (int, error) {

	if !vec1.GetType().Eq(*vec2.GetType()) {
		return 0, moerr.NewInternalErrorNoCtxf(
			"type not matched: %v <-> %v",
			vec1.GetType().String(), vec2.GetType().String(),
		)
	}

	if vec1.IsConst() {
		rowIdx1 = 0
	}
	if vec2.IsConst() {
		rowIdx2 = 0
	}

	// Treat NULL as equal only when both sides are NULL.
	if vec1.IsNull(uint64(rowIdx1)) || vec2.IsNull(uint64(rowIdx2)) {
		if vec1.IsNull(uint64(rowIdx1)) && vec2.IsNull(uint64(rowIdx2)) {
			return 0, nil
		}
		return 1, nil
	}

	// Use raw values to avoid format conversions in extractRowFromVector.
	switch vec1.GetType().Oid {
	case types.T_json:
		return bytejson.CompareByteJson(
			types.DecodeJson(vec1.GetBytesAt(rowIdx1)),
			types.DecodeJson(vec2.GetBytesAt(rowIdx2)),
		), nil
	case types.T_bool:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[bool](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[bool](vec2, rowIdx2),
		), nil
	case types.T_bit:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint64](vec2, rowIdx2),
		), nil
	case types.T_int8:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int8](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int8](vec2, rowIdx2),
		), nil
	case types.T_uint8:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint8](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint8](vec2, rowIdx2),
		), nil
	case types.T_int16:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int16](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int16](vec2, rowIdx2),
		), nil
	case types.T_uint16:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint16](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint16](vec2, rowIdx2),
		), nil
	case types.T_int32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int32](vec2, rowIdx2),
		), nil
	case types.T_uint32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint32](vec2, rowIdx2),
		), nil
	case types.T_int64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[int64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[int64](vec2, rowIdx2),
		), nil
	case types.T_uint64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[uint64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[uint64](vec2, rowIdx2),
		), nil
	case types.T_float32:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[float32](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[float32](vec2, rowIdx2),
		), nil
	case types.T_float64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[float64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[float64](vec2, rowIdx2),
		), nil
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink, types.T_geometry:
		return bytes.Compare(
			vec1.GetBytesAt(rowIdx1),
			vec2.GetBytesAt(rowIdx2),
		), nil
	case types.T_array_float32:
		return types.CompareValue(
			vector.GetArrayAt[float32](vec1, rowIdx1),
			vector.GetArrayAt[float32](vec2, rowIdx2),
		), nil
	case types.T_array_float64:
		return types.CompareValue(
			vector.GetArrayAt[float64](vec1, rowIdx1),
			vector.GetArrayAt[float64](vec2, rowIdx2),
		), nil
	case types.T_date:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Date](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Date](vec2, rowIdx2),
		), nil
	case types.T_datetime:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Datetime](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Datetime](vec2, rowIdx2),
		), nil
	case types.T_time:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Time](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Time](vec2, rowIdx2),
		), nil
	case types.T_timestamp:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Timestamp](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Timestamp](vec2, rowIdx2),
		), nil
	case types.T_year:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.MoYear](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.MoYear](vec2, rowIdx2),
		), nil
	case types.T_decimal64:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Decimal64](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Decimal64](vec2, rowIdx2),
		), nil
	case types.T_decimal128:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Decimal128](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Decimal128](vec2, rowIdx2),
		), nil
	case types.T_uuid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Uuid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Uuid](vec2, rowIdx2),
		), nil
	case types.T_Rowid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Rowid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Rowid](vec2, rowIdx2),
		), nil
	case types.T_Blockid:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Blockid](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Blockid](vec2, rowIdx2),
		), nil
	case types.T_TS:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.TS](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.TS](vec2, rowIdx2),
		), nil
	case types.T_enum:
		return types.CompareValue(
			vector.GetFixedAtNoTypeCheck[types.Enum](vec1, rowIdx1),
			vector.GetFixedAtNoTypeCheck[types.Enum](vec2, rowIdx2),
		), nil
	default:
		return 0, moerr.NewInternalErrorNoCtxf("compareSingleValInVector : unsupported type %d", vec1.GetType().Oid)
	}
}
