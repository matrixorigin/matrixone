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
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

type DataRetrieverConsumer interface {
	DataRetriever
	SetNextBatch(*ISCPData)
	SetError(error)
	Close()
}

func ExecuteIteration(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	iterCtx *IterationContext,
	mp *mpool.MPool,
) (err error) {
	packer := types.NewPacker()
	defer packer.Close()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"iscp iteration",
		0)
	txnOp, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if txnOp != nil {
		defer txnOp.Commit(ctx)
	}
	if err != nil {
		return
	}
	err = cnEngine.New(ctx, txnOp)
	if err != nil {
		return
	}

	jobSpecs, err := GetJobSpecs(
		ctx,
		cnUUID,
		txnOp,
		iterCtx.accountID,
		iterCtx.tableID,
		iterCtx.jobNames,
		iterCtx.jobIDs,
	)
	if err != nil {
		return
	}
	dbName := jobSpecs[0].ConsumerInfo.SrcTable.DBName
	tableName := jobSpecs[0].ConsumerInfo.SrcTable.TableName
	db, err := cnEngine.Database(ctx, dbName, txnOp)
	if err != nil {
		return
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return
	}
	tableDef := rel.CopyTableDef(ctx)
	if rel.GetTableID(ctx) != iterCtx.tableID {
		return moerr.NewInternalErrorNoCtx("table id mismatch")
	}

	if iterCtx.fromTS.IsEmpty() {
		iterCtx.toTS = types.TimestampToTS(txnOp.SnapshotTS())
	}
	statuses := make([]*JobStatus, len(jobSpecs))
	startAt := types.BuildTS(time.Now().UnixNano(), 0)
	for i := range jobSpecs {
		statuses[i] = &JobStatus{
			From: iterCtx.fromTS,
			To:   iterCtx.toTS,
		}
		statuses[i].StartAt = startAt
	}

	changes, err := CollectChanges(ctx, rel, iterCtx.fromTS, iterCtx.toTS, mp)
	if changes != nil {
		defer changes.Close()
	}
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "collectChanges" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if msg, injected := objectio.ISCPExecutorInjected(); injected && strings.HasPrefix(msg, "iteration:") {
		strs := strings.Split(msg, ":")
		for i := 1; i < len(strs); i++ {
			if strs[i] == tableName {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
		}
	}
	if err != nil {
		return
	}

	consumers := make([]Consumer, len(jobSpecs))
	for i := range jobSpecs {
		jobID := JobID{
			JobName:   iterCtx.jobNames[i],
			DBName:    dbName,
			TableName: tableName,
		}
		consumers[i], err = NewConsumer(cnUUID, rel.CopyTableDef(ctx), jobID, &jobSpecs[i].ConsumerInfo)
		if err != nil {
			return
		}
	}

	insTSColIdx := len(tableDef.Cols) - 1
	insCompositedPkColIdx := len(tableDef.Cols) - 2
	delTSColIdx := 1
	delCompositedPkColIdx := 0
	if len(tableDef.Pkey.Names) == 1 {
		insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}
	allocateAtomicBatchIfNeed := func(atomicBatch *AtomicBatch) *AtomicBatch {
		if atomicBatch == nil {
			atomicBatch = NewAtomicBatch(mp)
		}
		return atomicBatch
	}

	dataRetrievers := make([]DataRetrieverConsumer, len(consumers))
	typ := ISCPDataType_Tail
	if iterCtx.fromTS.IsEmpty() {
		typ = ISCPDataType_Snapshot
	}
	waitGroups := make([]sync.WaitGroup, len(consumers))
	for i := range consumers {
		if consumers[i] == nil {
			continue
		}
		dataRetrievers[i] = NewDataRetriever(
			ctx,
			iterCtx.accountID,
			iterCtx.tableID,
			iterCtx.jobNames[i],
			iterCtx.jobIDs[i],
			statuses[i],
			typ,
		)
		defer dataRetrievers[i].Close()
	}

	err = FlushJobStatusOnIterationState(
		ctx,
		cnUUID,
		cnEngine,
		cnTxnClient,
		iterCtx.accountID,
		iterCtx.tableID,
		iterCtx.jobNames,
		iterCtx.jobIDs,
		statuses,
		iterCtx.fromTS,
		ISCPJobState_Running,
	)
	if err != nil {
		return
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	changeHandelWg := sync.WaitGroup{}
	go func() {
		defer cancel()
		defer changeHandelWg.Done()
		changeHandelWg.Add(1)
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
			}
			var data *ISCPData
			insertData, deleteData, currentHint, err := changes.Next(ctxWithCancel, mp)
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "changesNext" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			if err != nil {
				jobNames := ""
				for _, jobName := range iterCtx.jobNames {
					jobNames = fmt.Sprintf("%s%s, ", jobNames, jobName)
				}
				logutil.Error(
					"ISCP-Task sink iteration failed",
					zap.Uint32("tenantID", iterCtx.accountID),
					zap.Uint64("tableID", iterCtx.tableID),
					zap.String("jobName", jobNames),
					zap.Error(err),
					zap.String("from", iterCtx.fromTS.ToString()),
					zap.String("to", iterCtx.toTS.ToString()),
				)
				data = NewISCPData(true, nil, nil, err)
			} else {
				// both nil denote no more data (end of this tail)
				if insertData == nil && deleteData == nil {
					data = NewISCPData(true, nil, nil, err)
				} else {
					var insertAtmBatch *AtomicBatch
					var deleteAtmBatch *AtomicBatch
					switch currentHint {
					case engine.ChangesHandle_Snapshot:
						if typ != ISCPDataType_Snapshot {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						data = NewISCPData(false, insertAtmBatch, nil, nil)
					case engine.ChangesHandle_Tail_wip:
						panic("logic error")
					case engine.ChangesHandle_Tail_done:
						if typ != ISCPDataType_Tail {
							panic("logic error")
						}
						insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
						deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
						insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
						deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)
						data = NewISCPData(false, insertAtmBatch, deleteAtmBatch, nil)

					}
				}
			}

			noMoreData := data.noMoreData
			data.Set(len(consumers))
			for i := range consumers {
				dataRetrievers[i].SetNextBatch(data)
			}

			if noMoreData {
				return
			}
		}
	}()

	for i, consumerEntry := range consumers {
		if dataRetrievers[i] == nil {
			continue
		}
		waitGroups[i].Add(1)
		go func(i int) {
			defer waitGroups[i].Done()
			err := consumerEntry.Consume(context.Background(), dataRetrievers[i])
			if err != nil {
				logutil.Error(
					"ISCP-Task sink consume failed",
					zap.Uint32("tenantID", iterCtx.accountID),
					zap.Uint64("tableID", iterCtx.tableID),
					zap.String("jobName", iterCtx.jobNames[i]),
					zap.Error(err),
					zap.String("from", iterCtx.fromTS.ToString()),
					zap.String("to", iterCtx.toTS.ToString()),
				)
				dataRetrievers[i].SetError(err)
				statuses[i].SetError(err)
			}
		}(i)
	}
	for i := range waitGroups {
		waitGroups[i].Wait()
	}

	cancel()
	changeHandelWg.Wait()
	for i, status := range statuses {
		if status.ErrorCode != 0 || typ == ISCPDataType_Snapshot {
			state := ISCPJobState_Completed
			if status.PermanentlyFailed() {
				state = ISCPJobState_Error
			}
			watermark := status.From
			if status.ErrorCode == 0 {
				watermark = status.To
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				err = FlushJobStatusOnIterationState(
					ctx,
					cnUUID,
					cnEngine,
					cnTxnClient,
					iterCtx.accountID,
					iterCtx.tableID,
					[]string{iterCtx.jobNames[i]},
					[]uint64{iterCtx.jobIDs[i]},
					[]*JobStatus{status},
					watermark,
					state,
				)
				if err == nil {
					break
				}
			}
		}
	}

	return nil
}

func FlushJobStatusOnIterationState(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	accountID uint32,
	tableID uint64,
	jobNames []string,
	jobIDs []uint64,
	jobStatuses []*JobStatus,
	watermark types.TS,
	state int8,
) (err error) {

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	txnWriter, err := getTxn(ctx, cnEngine, cnTxnClient, "iscp iteration")
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, txnWriter.Rollback(ctx))
		} else {
			err = txnWriter.Commit(ctx)
		}
		if err != nil {
			logutil.Error(
				"ISCP-Task flush job status failed",
				zap.Uint32("tenantID", accountID),
				zap.Uint64("tableID", tableID),
				zap.Strings("jobNames", jobNames),
				zap.Int8("state", state),
				zap.Error(err),
			)
		}
	}()
	for i := range jobNames {
		err = FlushStatus(
			ctx,
			cnUUID,
			txnWriter,
			accountID,
			tableID,
			jobNames[i],
			jobIDs[i],
			jobStatuses[i],
			watermark,
			state,
		)
		if err != nil {
			return
		}
	}
	return
}

func FlushStatus(
	ctx context.Context,
	cnUUID string,
	txn client.TxnOperator,
	tenantId uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
	jobStatus *JobStatus,
	watermark types.TS,
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
		jobID,
		watermark,
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
	jobIDs []uint64,
) (jobSpec []*JobSpec, err error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT job_spec FROM mo_catalog.mo_iscp_log WHERE")
	for i, jobName := range jobName {
		if i > 0 {
			buf.WriteString(" OR")
		}
		buf.WriteString(fmt.Sprintf(" account_id = %d AND table_id = %d AND job_name = '%s' AND job_id = %d",
			tenantId, tableID, jobName, jobIDs[i]))
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
			jobSpec[i], err = UnmarshalJobSpec(cols[0].GetBytesAt(i))
			if err != nil {
				return false
			}
		}
		return true
	})
	return
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
