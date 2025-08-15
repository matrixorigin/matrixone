// Copyright 2022 Matrix Origin
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
	"encoding/json"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func MarshalJobSpec(jobSpec *JobSpec) (string, error) {
	jsonBytes, err := json.Marshal(jobSpec)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func UnmarshalJobSpec(jsonByte []byte) (*JobSpec, error) {
	byteJson := types.DecodeJson(jsonByte)
	var jobSpec JobSpec
	err := json.Unmarshal([]byte(byteJson.String()), &jobSpec)
	if err != nil {
		return nil, err
	}
	return &jobSpec, nil
}

func ExecWithResult(
	ctx context.Context,
	sql string,
	cnUUID string,
	txn client.TxnOperator,
) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	return exec.Exec(ctx, sql, opts)
}

// return true if create, return false if task already exists, return error when error
func RegisterJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	pitr_name string,
	jobSpec *JobSpec,
	jobID *JobID,
) (ok bool, err error) {
	return ok, retry(
		func() error {
			ok, err = registerJob(ctx, cnUUID, txn, pitr_name, jobSpec, jobID)
			return err
		},
		DefaultRetryTimes,
	)
}

func registerJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	pitr_name string,
	jobSpec *JobSpec,
	jobID *JobID,
) (ok bool, err error) {
	ctxWithSysAccount := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctxWithSysAccount, cancel := context.WithTimeout(ctxWithSysAccount, time.Minute*5)
	defer cancel()
	var tenantId uint32
	var tableID uint64
	var dropped bool
	var internalJobID uint64
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task RegisterJob",
			zap.Uint32("tenantID", tenantId),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobID.JobName),
			zap.Uint64("jobID", internalJobID),
			zap.Bool("create new", ok),
			zap.Bool("dropped", dropped),
			zap.Error(err),
		)
	}()
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}
	if jobSpec.TriggerSpec.JobType == 0 {
		jobSpec.TriggerSpec.JobType = TriggerType_Default
	}
	tableID, err = getTableID(
		ctxWithSysAccount,
		cnUUID,
		txn,
		tenantId,
		jobID.DBName,
		jobID.TableName,
	)
	if err != nil {
		return
	}
	exist, dropped, prevID, err := queryIndexLog(
		ctxWithSysAccount,
		cnUUID,
		txn,
		tenantId,
		tableID,
		jobID.JobName,
	)
	if err != nil {
		return
	}
	if exist && !dropped {
		return
	}
	ok = true
	jobSpecJson, err := MarshalJobSpec(jobSpec)
	if err != nil {
		return
	}
	emptyJobStatus := JobStatus{}
	jobStatusJson, err := json.Marshal(emptyJobStatus)
	if err != nil {
		return
	}
	internalJobID = prevID + 1
	sql := cdc.CDCSQLBuilder.ISCPLogInsertSQL(
		tenantId,
		tableID,
		jobID.JobName,
		internalJobID,
		jobSpecJson,
		ISCPJobState_Completed,
		string(jobStatusJson),
	)
	_, err = ExecWithResult(ctxWithSysAccount, sql, cnUUID, txn)
	if err != nil {
		return
	}
	return
}

// return true if delete success, return false if no task found, return error when delete failed.
func UnregisterJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	jobID *JobID,
) (ok bool, err error) {
	return ok, retry(
		func() error {
			ok, err = unregisterJob(ctx, cnUUID, txn, jobID)
			return err
		},
		DefaultRetryTimes,
	)
}

func UpdateJobSpec(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	jobID *JobID,
	jobSpec *JobSpec,
) (err error) {
	return retry(
		func() error {
			return updateJobSpec(ctx, cnUUID, txn, jobID, jobSpec)
		},
		DefaultRetryTimes,
	)
}

func updateJobSpec(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	jobID *JobID,
	jobSpec *JobSpec,
) (err error) {
	var tenantId uint32
	var tableID uint64
	var jobSpecJson string
	var internalJobID uint64
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task UnregisterJob",
			zap.Uint32("tenantID", tenantId),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobID.JobName),
			zap.Uint64("jobID", internalJobID),
			zap.String("jobSpec", jobSpecJson),
			zap.Error(err),
		)
	}()
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}
	if jobSpec.TriggerSpec.JobType == 0 {
		jobSpec.TriggerSpec.JobType = TriggerType_Default
	}
	jobSpecJson, err = MarshalJobSpec(jobSpec)
	if err != nil {
		return
	}
	tableID, err = getTableID(
		ctx,
		cnUUID,
		txn,
		tenantId,
		jobID.DBName,
		jobID.TableName,
	)
	if err != nil {
		return
	}
	exist, dropped, internalJobID, err := queryIndexLog(
		ctx,
		cnUUID,
		txn,
		tenantId,
		tableID,
		jobID.JobName,
	)
	if err != nil {
		return
	}
	if !exist || dropped {
		err = moerr.NewInternalErrorNoCtx("job not found")
		return
	}
	sql := cdc.CDCSQLBuilder.ISCPLogUpdateJobSpecSQL(
		tenantId,
		tableID,
		jobID.JobName,
		internalJobID,
		jobSpecJson,
	)
	_, err = ExecWithResult(ctx, sql, cnUUID, txn)
	return
}
func unregisterJob(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	jobID *JobID,
) (ok bool, err error) {
	var tenantId uint32
	var tableID uint64
	var dropped bool
	var internalJobID uint64
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task UnregisterJob",
			zap.Uint32("tenantID", tenantId),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobID.JobName),
			zap.Uint64("jobID", internalJobID),
			zap.Bool("delete", ok),
			zap.Bool("dropped", dropped),
			zap.Error(err),
		)
	}()
	if tenantId, err = defines.GetAccountId(ctx); err != nil {
		return
	}
	tableID, err = getTableID(
		ctx,
		cnUUID,
		txn,
		tenantId,
		jobID.DBName,
		jobID.TableName,
	)
	if err != nil {
		return
	}
	exist, dropped, internalJobID, err := queryIndexLog(
		ctx,
		cnUUID,
		txn,
		tenantId,
		tableID,
		jobID.JobName,
	)
	if err != nil {
		return
	}
	if !exist || dropped {
		return
	}
	ok = true
	sql := cdc.CDCSQLBuilder.ISCPLogUpdateDropAtSQL(
		tenantId,
		tableID,
		jobID.JobName,
		internalJobID,
	)
	_, err = ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		return
	}
	return
}

func getTableID(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tenantId uint32,
	dbName string,
	tableName string,
) (tableID uint64, err error) {
	tableIDSql := cdc.CDCSQLBuilder.GetTableIDSQL(
		tenantId,
		dbName,
		tableName,
	)
	result, err := ExecWithResult(ctx, tableIDSql, cnUUID, txn)
	if err != nil {
		return
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 1 {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid rows %d", rows))
			return false
		}
		for i := 0; i < rows; i++ {
			tableID = vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if tableID == 0 {
		return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("tableID is 0, tableIDSql %s", tableIDSql))
	}
	return
}

func queryIndexLog(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tenantId uint32,
	tableID uint64,
	jobName string,
) (exist, dropped bool, prevID uint64, err error) {
	selectSql := cdc.CDCSQLBuilder.ISCPLogSelectByTableSQL(
		tenantId,
		tableID,
		jobName,
	)
	result, err := ExecWithResult(ctx, selectSql, cnUUID, txn)
	if err != nil {
		return
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 0 {
			exist = true
		}
		dropped = true
		ids := vector.MustFixedColWithTypeCheck[uint64](cols[1])
		for i := 0; i < rows; i++ {
			if cols[0].IsNull(0) {
				dropped = false
				prevID = ids[i]
				return false
			}
			if ids[i] > prevID {
				prevID = ids[i]
			}
		}
		return true
	})
	return
}
