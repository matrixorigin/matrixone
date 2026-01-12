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
	"encoding/base64"
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

func (iterCtx *IterationContext) String() string {
	return fmt.Sprintf("%d-%d-%v, %v->%v, lsn=%v, ids=%v",
		iterCtx.accountID, iterCtx.tableID, iterCtx.jobNames, iterCtx.fromTS.ToString(), iterCtx.toTS.ToString(), iterCtx.lsn, iterCtx.jobIDs)
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
	ctxWithoutTimeout := ctx
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

	statuses := make([]*JobStatus, len(iterCtx.jobNames))
	jobSpecs, prevStatus, err := GetJobSpecs(
		ctx,
		cnUUID,
		cnTxnClient,
		cnEngine,
		txnOp,
		iterCtx.accountID,
		iterCtx.tableID,
		iterCtx.jobNames,
		iterCtx.lsn,
		iterCtx.fromTS,
		statuses,
		iterCtx.jobIDs,
	)
	if err != nil {
		return
	}
	preLSN := make([]uint64, len(iterCtx.jobNames))
	for i := range iterCtx.jobNames {
		preLSN[i] = iterCtx.lsn[i] - 1
	}

	var needInit bool
	for i := range prevStatus {
		if prevStatus[i].Stage == JobStage_Init && jobSpecs[i].ConsumerInfo.InitSQL != "" {
			if len(iterCtx.jobNames) != 1 {
				errMsg := "init sql is not supported for multiple jobs"
				FlushPermanentErrorMessage(
					ctx,
					cnUUID,
					cnEngine,
					cnTxnClient,
					iterCtx.accountID,
					iterCtx.tableID,
					iterCtx.jobNames,
					iterCtx.jobIDs,
					iterCtx.lsn,
					statuses,
					types.MaxTs(),
					errMsg,
					preLSN,
				)
			}
			needInit = true
			break
		}
	}
	if needInit {
		ctxWithAccount := context.WithValue(ctx, defines.TenantIDKey{}, iterCtx.accountID)
		err = ProcessInitSQL(ctxWithAccount, cnUUID, cnEngine, cnTxnClient, jobSpecs[0].ConsumerInfo.InitSQL)
		if err != nil {
			return
		}
		statuses[0] = prevStatus[0]
		statuses[0].Stage = JobStage_Running
		err = retry(
			ctx,
			func() error {
				return FlushJobStatusOnIterationState(
					ctx,
					cnUUID,
					cnEngine,
					cnTxnClient,
					iterCtx.accountID,
					iterCtx.tableID,
					iterCtx.jobNames,
					iterCtx.jobIDs,
					iterCtx.lsn,
					statuses,
					iterCtx.fromTS,
					ISCPJobState_Completed,
					preLSN,
				)
			},
			SubmitRetryTimes,
			DefaultRetryInterval,
			SubmitRetryDuration,
		)
		if err != nil {
			return
		}
		return nil
	}
	dbName := jobSpecs[0].ConsumerInfo.SrcTable.DBName
	tableName := jobSpecs[0].ConsumerInfo.SrcTable.TableName
	ctxWithAccount := context.WithValue(ctx, defines.TenantIDKey{}, iterCtx.accountID)
	db, err := cnEngine.Database(ctxWithAccount, dbName, txnOp)
	if err != nil {
		return
	}
	rel, err := db.Relation(ctxWithAccount, tableName, nil)
	if err != nil {
		return
	}
	tableDef := rel.CopyTableDef(ctxWithAccount)
	if rel.GetTableID(ctxWithAccount) != iterCtx.tableID {
		return moerr.NewInternalErrorNoCtx("table id mismatch")
	}

	if iterCtx.fromTS.IsEmpty() {
		iterCtx.toTS = types.TimestampToTS(txnOp.SnapshotTS())
	}

	startAt := types.BuildTS(time.Now().UnixNano(), 0)
	for i := range iterCtx.jobNames {
		statuses[i] = &JobStatus{
			From:  iterCtx.fromTS,
			To:    iterCtx.toTS,
			Stage: prevStatus[i].Stage,
		}
		statuses[i].StartAt = startAt
	}
	changes, err := CollectChanges(ctxWithoutTimeout, rel, iterCtx.fromTS, iterCtx.toTS, mp)
	if err != nil {
		return
	}
	if changes != nil {
		defer changes.Close()
	}
	// injection is for ut
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "collectChanges" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	// injection is for ut
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
		consumers[i], err = NewConsumer(cnUUID, cnEngine, cnTxnClient, rel.CopyTableDef(ctx), jobID, &jobSpecs[i].ConsumerInfo)
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
			ctxWithoutTimeout,
			iterCtx.accountID,
			iterCtx.tableID,
			iterCtx.jobNames[i],
			iterCtx.jobIDs[i],
			statuses[i],
			iterCtx.lsn[i],
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
		iterCtx.lsn,
		statuses,
		iterCtx.fromTS,
		ISCPJobState_Running,
		preLSN,
	)
	if err != nil {
		return
	}

	ctxWithCancel, cancel := context.WithCancel(ctxWithoutTimeout)
	changeHandelWg := sync.WaitGroup{}
	changeHandelWg.Add(1)
	go func() {
		var dataLength int
		defer func() {
			logutil.Infof("ISCP-Task iteration %s, data length %d", iterCtx.String(), dataLength)
		}()
		defer cancel()
		defer changeHandelWg.Done()
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
			}
			var data *ISCPData
			insertData, deleteData, currentHint, err := changes.Next(ctxWithCancel, mp)
			if insertData != nil {
				dataLength += insertData.RowCount()
			}
			// injection is for ut
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
							err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid type %d, should be snapshot", typ))
							data = NewISCPData(true, nil, nil, err)
						} else {
							insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
							insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
							data = NewISCPData(false, insertAtmBatch, nil, nil)
						}
					case engine.ChangesHandle_Tail_wip:
						err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid hint %d", currentHint))
						data = NewISCPData(true, nil, nil, err)
					case engine.ChangesHandle_Tail_done:
						if typ != ISCPDataType_Tail {
							err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid type %d, should be tail", typ))
							data = NewISCPData(true, nil, nil, err)
						} else {
							insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
							deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
							insertAtmBatch.Append(packer, insertData, insTSColIdx, insCompositedPkColIdx)
							deleteAtmBatch.Append(packer, deleteData, delTSColIdx, delCompositedPkColIdx)
							data = NewISCPData(false, insertAtmBatch, deleteAtmBatch, nil)
						}

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
			ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)
			err := consumerEntry.Consume(ctx, dataRetrievers[i])
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
			err = retry(
				ctx,
				func() error {
					return FlushJobStatusOnIterationState(
						ctx,
						cnUUID,
						cnEngine,
						cnTxnClient,
						iterCtx.accountID,
						iterCtx.tableID,
						[]string{iterCtx.jobNames[i]},
						[]uint64{iterCtx.jobIDs[i]},
						[]uint64{iterCtx.lsn[i]},
						[]*JobStatus{status},
						watermark,
						state,
						iterCtx.lsn,
					)
				},
				SubmitRetryTimes,
				DefaultRetryInterval,
				SubmitRetryDuration,
			)
			if err != nil {
				logutil.Error(
					"ISCP-Task iteration flush job status failed",
					zap.Error(err),
				)
			}
		}
	}

	return nil
}

var FlushJobStatusOnIterationState = func(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	accountID uint32,
	tableID uint64,
	jobNames []string,
	jobIDs []uint64,
	lsns []uint64,
	jobStatuses []*JobStatus,
	watermark types.TS,
	state int8,
	prevLSN []uint64,
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
		jobStatuses[i].LSN = lsns[i]
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
			prevLSN[i],
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
	prevLSN uint64,
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
		prevLSN,
	)
	result, err := ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		return
	}
	result.Close()
	return
}

var GetJobSpecs = func(
	ctx context.Context,
	cnUUID string,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	txn client.TxnOperator,
	tenantId uint32,
	tableID uint64,
	jobName []string,
	lsns []uint64,
	watermark types.TS,
	jobStatuses []*JobStatus,
	jobIDs []uint64,
) (jobSpec []*JobSpec, prevStatus []*JobStatus, err error) {
	var buf bytes.Buffer
	buf.WriteString("SELECT job_spec, job_status FROM mo_catalog.mo_iscp_log WHERE")
	for i, jobName := range jobName {
		if i > 0 {
			buf.WriteString(" OR")
		}
		buf.WriteString(fmt.Sprintf(" (account_id = %d AND table_id = %d AND job_name = '%s' AND job_id = %d)",
			tenantId, tableID, jobName, jobIDs[i]))
	}
	sql := buf.String()
	execResult, err := ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		return
	}
	prevLSNs := make([]uint64, len(jobName))
	for i := range jobName {
		prevLSNs[i] = lsns[i] - 1
	}
	defer execResult.Close()
	jobSpec = make([]*JobSpec, len(jobName))
	prevStatus = make([]*JobStatus, len(jobName))
	execResult.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != len(jobName) {
			errMsg := fmt.Sprintf("invalid rows %d, expected %d", rows, len(jobName))
			FlushPermanentErrorMessage(
				ctx,
				cnUUID,
				cnEngine,
				cnTxnClient,
				tenantId,
				tableID,
				jobName,
				jobIDs,
				lsns,
				jobStatuses,
				types.MaxTs(),
				errMsg,
				prevLSNs,
			)
		}
		for i := 0; i < rows; i++ {
			jobSpec[i], err = UnmarshalJobSpec([]byte(cols[0].GetStringAt(i)))
			if err != nil {
				return false
			}
			prevStatus[i], err = UnmarshalJobStatus([]byte(cols[1].GetStringAt(i)))
			if err != nil {
				return false
			}
		}
		return true
	})
	return
}

func FlushPermanentErrorMessage(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	accountID uint32,
	tableID uint64,
	jobNames []string,
	jobIDs []uint64,
	lsns []uint64,
	jobStatuses []*JobStatus,
	watermark types.TS,
	errMsg string,
	prevLSN []uint64,
) (err error) {
	logutil.Error(
		"ISCP-Task Flush Permanent Error Message",
		zap.Uint32("accountID", accountID),
		zap.Uint64("tableID", tableID),
		zap.Strings("jobNames", jobNames),
		zap.Any("jobIDs", jobIDs),
		zap.String("errMsg", errMsg),
	)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	return FlushJobStatusOnIterationState(
		ctx,
		cnUUID,
		cnEngine,
		cnTxnClient,
		accountID,
		tableID,
		jobNames,
		jobIDs,
		lsns,
		jobStatuses,
		watermark,
		ISCPJobState_Error,
		prevLSN,
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

func NewIterationContext(accountID uint32, tableID uint64, jobNames []string, jobIDs []uint64, lsn []uint64, fromTS types.TS, toTS types.TS) *IterationContext {
	return &IterationContext{
		accountID: accountID,
		tableID:   tableID,
		jobNames:  jobNames,
		jobIDs:    jobIDs,
		lsn:       lsn,
		fromTS:    fromTS,
		toTS:      toTS,
	}
}

func ProcessInitSQL(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	sql string,
) (err error) {
	decoded, err := base64.StdEncoding.DecodeString(sql)
	if err != nil {
		return
	}
	sql = string(decoded)
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"iscp process init sql",
		0)
	txnOp, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if txnOp != nil {
		defer txnOp.Commit(ctx)
	}
	// injection is for ut

	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "processInitSQLNewTxn" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		return
	}
	err = cnEngine.New(ctx, txnOp)
	if err != nil {
		return
	}
	result, err := ExecWithResult(ctx, sql, cnUUID, txnOp)
	if err != nil {
		return
	}
	defer result.Close()
	return
}
