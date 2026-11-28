// Copyright 2024 Matrix Origin
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

package iscp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// bat columns layout:
// 1. data: user defined cols | cpk (if needed) | commit-ts
// 2. tombstone: pk/cpk | commit-ts
// return user defined cols for data or only one cpk column for tombstone
func extractRowFromEveryVector(
	ctx context.Context,
	dataSet *batch.Batch,
	rowIndex int,
	row []any,
) error {
	for i := 0; i < len(row); i++ {
		vec := dataSet.Vecs[i]
		rowIndexBackup := rowIndex
		if vec.IsConstNull() {
			row[i] = nil
			continue
		}
		if vec.IsConst() {
			rowIndex = 0
		}

		if err := extractRowFromVector(ctx, vec, i, row, rowIndex); err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector sets row[i] to row rowIndex of vec, as vector.ExportValue
// returns it.
func extractRowFromVector(ctx context.Context, vec *vector.Vector, i int, row []any, rowIndex int) error {
	if !vector.SupportsAny(vec.GetType().Oid) {
		logutil.Error(
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalErrorf(ctx, "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	row[i] = vector.ExportValue(vec, rowIndex)
	return nil
}

// SqlLiteral returns the SQL literal of row i of vec.
func SqlLiteral(ctx context.Context, vec *vector.Vector, i int) (string, error) {
	if !vector.SupportsAny(vec.GetType().Oid) {
		return "", moerr.NewInternalErrorf(ctx, "SqlLiteral : unsupported type %d", vec.GetType().Oid)
	}
	buf, err := convertColIntoSql(ctx, vector.ExportValue(vec, i), vec.GetType(), nil)
	return string(buf), err
}

// convertColIntoSql appends data, a value of type typ as vector.ExportValue
// returns it, as a literal of the SQL MatrixOne runs (types.AppendMOSQLLiteral),
// and NULL as a typed NULL.
func convertColIntoSql(
	ctx context.Context,
	data any,
	typ *types.Type,
	sqlBuff []byte) ([]byte, error) {
	if data == nil {
		// a typed NULL keeps the column of a VALUES list typed
		typstr := typ.DescString()
		if typ.Oid == types.T_enum {
			// The source projection restores the ENUM domain. Its transport type
			// must be valid SQL without member metadata in the runtime type.
			typstr = types.T_uint16.ToType().DescString()
		}
		return fmt.Appendf(sqlBuff, "CAST(NULL as %s)", typstr), nil
	}
	return types.AppendMOSQLLiteral(sqlBuff, *typ, data)
}

// DescString omits temporal scale. Intermediate casts must retain it: casting
// a TIMESTAMP(6) to the default TIMESTAMP(0) loses precision before assignment
// to the hidden index, even if the destination column still has scale 6.
func sqlCastTypeName(typ *types.Type) string {
	switch typ.Oid {
	case types.T_time, types.T_datetime, types.T_timestamp:
		return fmt.Sprintf("%s(%d)", typ.Oid.String(), typ.Scale)
	default:
		return typ.DescString()
	}
}

func appendBytes(buf []byte, data []byte) []byte {
	return append(buf, data...)
}

func appendString(buf []byte, s string) []byte {
	return appendBytes(buf, []byte(s))
}

// CollectChanges reads one ISCP iteration's change range for index maintenance.
// The caller's ctx carries the read policy: collectChangesCtxForJobs decides
// whether this range may keep the trailing DELETE of an insert-then-delete key.
var CollectChanges = func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	return rel.CollectChanges(ctx, fromTs, toTs, false, mp)
}

// finishISCPTransaction is the single commit/rollback boundary for ISCP-owned
// transactions. Cleanup must remain possible after the operation context is
// canceled, and neither commit nor rollback failures may be converted to
// success.
func finishISCPTransaction(ctx context.Context, txnOp client.TxnOperator, err error) error {
	if txnOp == nil {
		return err
	}
	// Cancellation before the commit point is a failed operation, even when the
	// last statement happened to return first. Use the detached cleanup context
	// below to roll back instead of accidentally committing canceled work.
	if err == nil {
		err = ctx.Err()
	}
	cleanupCtx, cancel := context.WithTimeoutCause(
		context.WithoutCancel(ctx),
		time.Minute*5,
		moerr.CauseISCPTransactionFinishTimeout,
	)
	defer cancel()
	if err != nil {
		if rollbackErr := txnOp.Rollback(cleanupCtx); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return txnOp.Commit(cleanupCtx)
}

func batchRowCount(bat *batch.Batch) int {
	if bat == nil || len(bat.Vecs) == 0 {
		return 0
	}
	return bat.Vecs[0].Length()
}

func getTxn(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	info string,
) (client.TxnOperator, error) {
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		info,
		0)
	op, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		return nil, finishISCPTransaction(ctx, op, err)
	}
	err = cnEngine.New(ctx, op)
	if err != nil {
		return nil, finishISCPTransaction(ctx, op, err)
	}
	return op, nil
}

var CheckLeaseWithRetry = func(
	ctx context.Context,
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) (ok bool, err error) {
	defer func() {
		if err != nil || !ok {
			logutil.Errorf(
				"ISCP-Task check lease failed, err=%v, ok=%v, cnUUID=%v",
				zap.Error(err),
				zap.Bool("ok", ok),
				zap.String("cnUUID", cnUUID),
			)
		}
	}()
	err = retry(
		ctx,
		func() error {
			ok, err = checkLease(ctx, cnUUID, txnEngine, cnTxnClient)
			return err
		},
		DefaultRetryTimes,
		DefaultRetryInterval,
		DefaultRetryDuration,
	)
	return
}

func checkLease(
	ctx context.Context,
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) (ok bool, err error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txn, err := getTxn(ctxWithTimeout, txnEngine, cnTxnClient, "iscp check lease")
	if err != nil {
		return
	}
	defer func() {
		err = finishISCPTransaction(ctxWithTimeout, txn, err)
		if err != nil {
			ok = false
		}
	}()

	var runner string
	runner, err = GetTaskRunner(ctxWithTimeout, cnUUID, txn)
	if err != nil {
		return
	}
	if runner == "" {
		return
	}
	if runner == cnUUID {
		ok = true
	} else {
		logutil.Errorf(
			"ISCP-Task check lease failed, runner: %s, expected: %s",
			runner,
			cnUUID,
		)
	}
	return
}

func GetTaskRunner(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
) (string, error) {
	ctxWithSysAccount := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctxWithTimeout, cancel := context.WithTimeoutCause(ctxWithSysAccount, time.Minute*5, moerr.CauseISCPGetTaskRunnerTimeout)
	defer cancel()

	sql := `select task_runner from mo_task.sys_daemon_task where task_type = "ISCP" and task_runner is not null`
	result, err := ExecWithResult(ctxWithTimeout, sql, cnUUID, txn)
	if err != nil {
		return "", err
	}
	defer result.Close()
	return readSingleTaskRunner(result)
}

// Result callbacks carry logical cardinality; a constant vector can encode
// several rows in one physical value. Do not infer lease uniqueness from it.
func readSingleTaskRunner(result executor.Result) (string, error) {
	var runner string
	var rowCount int
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		rowCount += rows
		if rowCount != 1 {
			return false
		}
		if !cols[0].IsNull(0) {
			runner = cols[0].GetStringAt(0)
		}
		return true
	})
	if rowCount == 0 {
		return "", nil
	}
	if rowCount != 1 {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("unexpected rows count: %d", rowCount))
	}
	if runner == "" {
		return "", moerr.NewInternalErrorNoCtx("task runner is null")
	}
	return runner, nil
}
