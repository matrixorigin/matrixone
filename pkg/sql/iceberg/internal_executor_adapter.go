// Copyright 2026 Matrix Origin
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

package iceberg

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	internalexecutor "github.com/matrixorigin/matrixone/pkg/util/executor"
)

type InternalSQLExecutorAdapter struct {
	Executor        internalexecutor.SQLExecutor
	Options         internalexecutor.Options
	StatementOption internalexecutor.StatementOption
}

func (a InternalSQLExecutorAdapter) Exec(ctx context.Context, sql string) (uint64, error) {
	if a.Executor == nil {
		return 0, moerr.NewInvalidInput(ctx, "iceberg internal SQL executor adapter requires an executor")
	}
	result, err := a.Executor.Exec(ctx, sql, a.execOptions())
	defer result.Close()
	if err != nil {
		return 0, err
	}
	return result.AffectedRows, nil
}

func (a InternalSQLExecutorAdapter) QueryRow(ctx context.Context, sql string) RowScanner {
	rows, err := a.Query(ctx, sql)
	return &internalSQLRow{rows: rows, err: err}
}

func (a InternalSQLExecutorAdapter) Query(ctx context.Context, sql string) (RowsScanner, error) {
	if a.Executor == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg internal SQL executor adapter requires an executor")
	}
	result, err := a.Executor.Exec(ctx, sql, a.execOptions())
	if err != nil {
		return nil, err
	}
	return &internalSQLRows{result: result}, nil
}

func (a InternalSQLExecutorAdapter) execOptions() internalexecutor.Options {
	stmt := a.StatementOption.WithDisableLog()
	return a.Options.WithStatementOption(stmt)
}

type internalSQLRow struct {
	rows RowsScanner
	err  error
}

func (r *internalSQLRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return moerr.NewInternalErrorNoCtx("iceberg DAO query returned no rows")
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return moerr.NewInternalErrorNoCtx("iceberg DAO query returned no rows")
	}
	return r.rows.Scan(dest...)
}

type internalSQLRows struct {
	result       internalexecutor.Result
	batchIdx     int
	rowIdx       int
	currentBatch *batch.Batch
	currentRow   int
	closed       bool
	err          error
}

func (r *internalSQLRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.result.Close()
	return nil
}

func (r *internalSQLRows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	for r.batchIdx < len(r.result.Batches) {
		bat := r.result.Batches[r.batchIdx]
		if bat == nil || bat.RowCount() == 0 {
			r.batchIdx++
			r.rowIdx = 0
			continue
		}
		if r.rowIdx < bat.RowCount() {
			r.currentBatch = bat
			r.currentRow = r.rowIdx
			r.rowIdx++
			return true
		}
		r.batchIdx++
		r.rowIdx = 0
	}
	r.currentBatch = nil
	return false
}

func (r *internalSQLRows) Scan(dest ...any) error {
	if r.currentBatch == nil {
		return moerr.NewInternalErrorNoCtx("iceberg DAO rows Scan called before Next")
	}
	if len(dest) > len(r.currentBatch.Vecs) {
		return moerr.NewInternalErrorNoCtx("iceberg DAO scan destination count exceeds result columns")
	}
	for i := range dest {
		if err := scanVectorValue(dest[i], r.currentBatch.Vecs[i], r.currentRow); err != nil {
			r.err = err
			return err
		}
	}
	return nil
}

func (r *internalSQLRows) Err() error {
	return r.err
}

func scanVectorValue(dest any, vec *vector.Vector, row int) error {
	if vec == nil {
		return moerr.NewInternalErrorNoCtx("iceberg DAO scan encountered nil vector")
	}
	if vec.GetNulls().Contains(uint64(row)) {
		return nil
	}
	switch ptr := dest.(type) {
	case *string:
		value, err := scanVectorString(vec, row)
		if err != nil {
			return err
		}
		*ptr = value
	case *uint32:
		value, err := scanVectorUint64(vec, row)
		if err != nil {
			return err
		}
		if value > math.MaxUint32 {
			return moerr.NewInternalErrorNoCtx("iceberg DAO scan uint32 overflow")
		}
		*ptr = uint32(value)
	case *uint64:
		value, err := scanVectorUint64(vec, row)
		if err != nil {
			return err
		}
		*ptr = value
	case *time.Time:
		value, err := scanVectorTime(vec, row)
		if err != nil {
			return err
		}
		*ptr = value
	default:
		return moerr.NewInternalErrorNoCtx("iceberg DAO scan destination type is unsupported")
	}
	return nil
}

func scanVectorString(vec *vector.Vector, row int) (string, error) {
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_json, types.T_blob, types.T_varbinary, types.T_binary:
		return vec.GetStringAt(row), nil
	default:
		return "", moerr.NewInternalErrorNoCtx("iceberg DAO scan expected string vector")
	}
}

func scanVectorUint64(vec *vector.Vector, row int) (uint64, error) {
	switch vec.GetType().Oid {
	case types.T_uint8:
		return uint64(vector.GetFixedAtWithTypeCheck[uint8](vec, row)), nil
	case types.T_uint16:
		return uint64(vector.GetFixedAtWithTypeCheck[uint16](vec, row)), nil
	case types.T_uint32:
		return uint64(vector.GetFixedAtWithTypeCheck[uint32](vec, row)), nil
	case types.T_uint64:
		return vector.GetFixedAtWithTypeCheck[uint64](vec, row), nil
	case types.T_int8:
		return nonNegativeInt64ToUint64(int64(vector.GetFixedAtWithTypeCheck[int8](vec, row)))
	case types.T_int16:
		return nonNegativeInt64ToUint64(int64(vector.GetFixedAtWithTypeCheck[int16](vec, row)))
	case types.T_int32:
		return nonNegativeInt64ToUint64(int64(vector.GetFixedAtWithTypeCheck[int32](vec, row)))
	case types.T_int64:
		return nonNegativeInt64ToUint64(vector.GetFixedAtWithTypeCheck[int64](vec, row))
	default:
		return 0, moerr.NewInternalErrorNoCtx("iceberg DAO scan expected integer vector")
	}
}

func nonNegativeInt64ToUint64(value int64) (uint64, error) {
	if value < 0 {
		return 0, moerr.NewInternalErrorNoCtx("iceberg DAO scan negative integer for unsigned destination")
	}
	return uint64(value), nil
}

func scanVectorTime(vec *vector.Vector, row int) (time.Time, error) {
	switch vec.GetType().Oid {
	case types.T_datetime:
		return vector.GetFixedAtWithTypeCheck[types.Datetime](vec, row).ConvertToGoTime(time.UTC), nil
	case types.T_timestamp:
		return vector.GetFixedAtWithTypeCheck[types.Timestamp](vec, row).ToDatetime(time.UTC).ConvertToGoTime(time.UTC), nil
	case types.T_char, types.T_varchar, types.T_text:
		raw := vec.GetStringAt(row)
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02"} {
			if ts, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
				return ts.UTC(), nil
			}
		}
		if unixMS, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.UnixMilli(unixMS).UTC(), nil
		}
		return time.Time{}, moerr.NewInternalErrorNoCtx("iceberg DAO scan invalid timestamp string")
	default:
		return time.Time{}, moerr.NewInternalErrorNoCtx("iceberg DAO scan expected time vector")
	}
}
