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
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
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
		err = ProcessInitSQL(
			ctxWithAccount, cnUUID, cnEngine, cnTxnClient,
			jobSpecs[0].ConsumerInfo.InitSQL,
			jobSpecs[0].ConsumerInfo.SrcTable.DBName,
			jobSpecs[0].ConsumerInfo.SrcTable.TableName,
			jobSpecs[0].ConsumerInfo.IndexName,
		)
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

// initSQLSessionVars loads the source table's def and returns the target
// index's captured session_vars blob (algo_params.session_vars, JSON text), or
// nil if absent. Load failures (Database/Relation) and the IndexParamsSessionVars
// parse error are returned so the caller can fail the InitSQL rather than
// silently rebuild with defaults; an absent blob (no index match, or no
// session_vars key) is (nil, nil) — legitimate, skip the overlay.
func initSQLSessionVars(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, dbName, tableName, indexName string) ([]byte, error) {
	if dbName == "" || tableName == "" || indexName == "" {
		return nil, nil
	}
	db, err := cnEngine.Database(ctx, dbName, txnOp)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(ctx, tableName, nil)
	if err != nil {
		return nil, err
	}
	tableDef := rel.CopyTableDef(ctx)
	if tableDef == nil {
		return nil, nil
	}
	for _, idx := range tableDef.Indexes {
		if idx.IndexName != indexName {
			continue
		}
		sv, serr := catalog.IndexParamsSessionVars(idx.IndexAlgoParams)
		if serr != nil {
			return nil, serr
		}
		return sv, nil
	}
	return nil, nil
}

// initSQLResolver builds the system-variable resolver for an InitSQL build.
// When sessionVars (the index's captured algo_params.session_vars) is present,
// its typed values overlay the process defaults so the background build
// (cagra_create/ivfpq_create/...) reproduces the create-time config (e.g.
// kmeans_train_percent); every other var falls through to
// executor.DefaultResolveVariable. With no sessionVars it IS
// DefaultResolveVariable (may be nil — caller guards), preserving prior
// behaviour. Note md.ResolveVariableFunc errors on a var it doesn't hold, so
// the wrapper falls back on error rather than propagating it.
func initSQLResolver(sessionVars []byte) (func(string, bool, bool) (interface{}, error), error) {
	def := executor.DefaultResolveVariable
	if len(sessionVars) == 0 {
		// No captured session_vars (old index / non-vector / no build vars):
		// skip the overlay and resolve through the process default. Not an error.
		return def, nil
	}
	// session_vars is JSON *text* (from algo_params), so parse it with the text
	// parser (NewMetadataFromJson → bytejson.ParseFromString). NewMetadata uses
	// bj.Unmarshal, which expects the binary bytejson encoding and would silently
	// mis-parse the text → md.bj garbage → every lookup falls back.
	md, err := sqlexec.NewMetadataFromJson(string(sessionVars))
	if err != nil {
		// session_vars is present but unparseable — surface the error rather than
		// silently rebuilding with defaults (which would corrupt the index).
		return nil, err
	}
	if md == nil {
		return def, nil
	}
	return func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		// Captured vars resolve from the overlay; everything else (timezone,
		// sql_mode, …) falls through to the process default — that per-variable
		// fallback is expected, unlike the parse-failure case above.
		if v, verr := md.ResolveVariableFunc(name, isSystemVar, isGlobalVar); verr == nil && v != nil {
			return v, nil
		}
		if def != nil {
			return def(name, isSystemVar, isGlobalVar)
		}
		return nil, nil
	}, nil
}

func ProcessInitSQL(
	ctx context.Context,
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	sql string,
	dbName string,
	tableName string,
	indexName string,
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

	// Fetch the target index's captured build-time session vars from
	// algo_params.session_vars (recorded at CREATE INDEX). The InitSQL build
	// (cagra_create/ivfpq_create/...) resolves vars like kmeans_train_percent
	// through this process's resolver; overlaying the captured values lets the
	// background rebuild reproduce the create-time config instead of process
	// defaults. A load/parse failure is surfaced; an absent blob yields nil.
	sessionVars, svErr := initSQLSessionVars(ctx, cnEngine, txnOp, dbName, tableName, indexName)
	if svErr != nil {
		err = svErr
		return
	}

	// Run the InitSQL through sqlexec's background SqlContext rather than the
	// frontend back-exec. sqlexec.RunSql (SqlContext path) runs IsFrontend=false
	// and passes the SqlContext's ResolveVariableFunc straight into the executor
	// opts, so the overlay built from algo_params.session_vars
	// (kmeans_train_percent etc.) actually reaches the cuvs build. The frontend
	// back-exec instead resets the proc resolver to the back session's default
	// (back_exec.go), silently dropping the overlay. initSQLResolver overlays the
	// captured session_vars on top of executor.DefaultResolveVariable.
	accountId, aerr := defines.GetAccountId(ctx)
	if aerr != nil {
		err = aerr
		return
	}
	resolver, rerr := initSQLResolver(sessionVars)
	if rerr != nil {
		err = rerr
		return
	}
	sqlctx := sqlexec.NewSqlContext(ctx, cnUUID, txnOp, accountId, resolver)
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlctx)
	result, err := sqlexec.RunSql(sqlproc, sql)
	if err != nil {
		return
	}
	defer result.Close()
	return
}
