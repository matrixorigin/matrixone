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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

func (iter *Iteration) GetFrom() types.TS {
	return iter.status[0].From
}
func (iter *Iteration) GetTo() types.TS {
	return iter.status[0].To
}

func NewIteration(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	accountID uint32,
	dbName string,
	tableName string,
	jobNames []string,
	fromTS, toTS types.TS,
	mp *mpool.MPool,
) (iter *Iteration, err error) {

	rel, txnOp, err := GetRelation(
		ctx,
		cnEngine,
		cnTxnClient,
		accountID,
		dbName,
		tableName,
	)
	if err != nil {
		return
	}
	jobSpecs, err := GetJobSpecs(
		ctx,
		cnUUID,
		txnOp,
		accountID,
		rel.GetTableID(ctx),
		jobNames,
	)

	if fromTS.IsEmpty() {
		toTS = types.TimestampToTS(txnOp.SnapshotTS())
	}
	iter = &Iteration{
		accountID:   accountID,
		rel:         rel,
		txnReader:   txnOp,
		jobNames:    jobNames,
		jobSpecs:    jobSpecs,
		cnUUID:      cnUUID,
		cnEngine:    cnEngine,
		cnTxnClient: cnTxnClient,
		packer:      types.NewPacker(),
		mp:          mp,
	}
	iter.status = make([]*JobStatus, len(jobSpecs))
	startAt := types.BuildTS(time.Now().UnixNano(), 0)
	for i := range jobSpecs {
		iter.status[i] = &JobStatus{
			From: fromTS,
			To:   toTS,
		}
		iter.status[i].StartAt = startAt
	}
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"iscp iteration",
		0)
	txnWriter, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, txnWriter.Rollback(ctx))
		} else {
			err = txnWriter.Commit(ctx)
		}
	}()
	for i := range jobSpecs {
		err = FlushStatus(
			ctx,
			cnUUID,
			txnWriter,
			accountID,
			rel.GetTableID(ctx),
			jobNames[i],
			iter.status[i],
			ISCPJobState_Running,
		)
		if err != nil {
			return nil, err
		}
	}
	return iter, nil
}

func FlushStatus(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tenantId uint32,
	tableID uint64,
	jobName string,
	jobStatus *JobStatus,
	state int8,
) (err error) {
	statusJson, err := MarshalJobStatus(jobStatus)
	if err != nil {
		return
	}
	sql := cdc.CDCSQLBuilder.ISCPLogUpdateResultSQL(
		tenantId,
		tableID,
		jobName,
		jobStatus.To,
		statusJson,
		state,
	)
	_, err = ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		return
	}
	return
}

func GetJobSpecs(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tenantId uint32,
	tableID uint64,
	jobName []string,
) (jobSpec []*JobSpec, err error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT job_config FROM mo_catalog.mo_iscp_log WHERE")
	for i, jobName := range jobName {
		if i > 0 {
			buf.WriteString(" OR")
		}
		buf.WriteString(fmt.Sprintf(" account_id = %d AND table_id = %d AND job_name = '%s'", tenantId, tableID, jobName))
	}
	sql := buf.String()
	execResult, err := ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		return
	}
	defer execResult.Close()
	jobSpec = make([]*JobSpec, len(jobName))
	execResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != len(jobName) {
			panic(fmt.Sprintf("invalid rows %d, expected %d", rows, len(jobName)))
		}
		for i := 0; i < rows; i++ {
			jobSpec[i], err = UnmarshalJobSpec(string(vector.MustFixedColWithTypeCheck[[]byte](cols[0])[i]))
			if err != nil {
				return false
			}
		}
		return true
	})
	return
}

func GetRelation(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	accountID uint32,
	dbName string,
	tableName string,
) (rel engine.Relation, txn client.TxnOperator, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"iscp iteration",
		0)
	txnOp, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		return
	}
	err = cnEngine.New(ctx, txnOp)
	if err != nil {
		return
	}
	var db engine.Database
	if db, err = cnEngine.Database(ctx, dbName, txnOp); err != nil {
		return
	}

	if rel, err = db.Relation(ctx, tableName, nil); err != nil {
		return
	}
	return
}

func (iter *Iteration) Run() {
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, iter.accountID)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	defer func() {
		iter.packer.Close()
		end := types.BuildTS(time.Now().UnixNano(), 0)
		for i, status := range iter.status {
			if status.ErrorCode != 0 {
				nowTs := iter.cnEngine.LatestLogtailAppliedTime()
				createByOpt := client.WithTxnCreateBy(
					0,
					"",
					"iscp iteration",
					0)
				txnOp, err := iter.cnTxnClient.New(ctx, nowTs, createByOpt)
				if err != nil {
					return
				}
				err = iter.cnEngine.New(ctx, txnOp)
				if err != nil {
					return
				}
				iter.status[i].EndAt = end
				state := ISCPJobState_Completed
				if iter.status[i].PermanentlyFailed() {
					state = ISCPJobState_Error
				}
				err = FlushStatus(
					ctx,
					iter.cnUUID,
					txnOp,
					iter.accountID,
					iter.rel.GetTableID(ctx),
					iter.jobNames[i],
					iter.status[i],
					state,
				)
				if err == nil {
					err = txnOp.Commit(ctx)
				} else {
					err = errors.Join(err, txnOp.Rollback(ctx))
				}
				logutil.Error(
					"ISCP-Task iteration failed",
					zap.Uint32("tenantID", iter.accountID),
					zap.Uint64("tableID", iter.rel.GetTableID(ctx)),
					zap.String("jobName", iter.jobNames[i]),
					zap.Any("status", status),
					zap.Error(err),
				)
			}
		}
	}()
	CollectChangesForIteration(
		ctx,
		iter,
	)
}

// TODO
func isPermanentError(err error) bool {
	return err.Error() == "permanent error"
}

func (status *JobStatus) SetError(err error) {
	if err == nil {
		return
	}
	if isPermanentError(err) {
		status.ErrorCode = PermanentErrorThreshold
	} else {
		status.ErrorCode = 1
	}
	status.ErrorMsg = err.Error()
}

func (status *JobStatus) PermanentlyFailed() bool {
	return status.ErrorCode >= PermanentErrorThreshold
}
