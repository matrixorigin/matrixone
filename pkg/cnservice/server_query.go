// Copyright 2021 - 2023 Matrix Origin
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

package cnservice

import (
	"context"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	commonUtil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"go.uber.org/zap"
)

var (
	iscpExecutorReadyTimeout = 2 * time.Second
	iscpGetExecutorRuntimeFn = iscp.GetExecutorRuntime
)

type queryWorkLifecycle struct {
	sync.Mutex
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closing   bool
	closeOnce sync.Once
	closeErr  error
}

func (l *queryWorkLifecycle) beginClose() {
	l.Lock()
	l.closing = true
	if l.cancel != nil {
		l.cancel()
	}
	l.Unlock()
}

func (l *queryWorkLifecycle) admit() (func(), bool) {
	l.Lock()
	defer l.Unlock()
	if l.closing {
		return nil, false
	}
	l.wg.Add(1)
	return l.wg.Done, true
}

func (l *queryWorkLifecycle) launch(executor taskservice.TaskExecutor, asyncTask task.Task) bool {
	l.Lock()
	if l.closing {
		l.Unlock()
		return false
	}
	if l.cancel == nil {
		l.ctx, l.cancel = context.WithCancel(context.Background())
	}
	ctx := l.ctx
	l.wg.Add(1)
	l.Unlock()

	go func() {
		defer l.wg.Done()
		_ = executor(ctx, asyncTask)
	}()
	return true
}

func (l *queryWorkLifecycle) close(closeIngress func() error) error {
	l.closeOnce.Do(func() {
		l.beginClose()
		l.closeErr = closeIngress()
		l.wg.Wait()
	})
	return l.closeErr
}

func (s *service) initQueryService() error {
	if s.gossipNode != nil {
		s.gossipNode.SetListenAddrFn(s.gossipListenAddr)
		s.gossipNode.SetServiceAddrFn(s.gossipServiceAddr)
		s.gossipNode.SetCacheServerAddrFn(s.queryServiceServiceAddr)
		if err := s.gossipNode.Create(); err != nil {
			return err
		}
	}

	var err error
	s.queryService, err = queryservice.NewQueryService(s.cfg.UUID,
		s.queryServiceListenAddr(), s.cfg.RPC)
	if err != nil {
		return err
	}
	s.initQueryCommandHandler()

	s.queryClient, err = qclient.NewQueryClient(s.cfg.UUID, s.cfg.RPC)
	if err != nil {
		return err
	}
	return nil
}

func (s *service) initQueryCommandHandler() {
	s.addQueryCommandHandler(query.CmdMethod_KillConn, s.handleKillConn)
	s.addQueryCommandHandler(query.CmdMethod_AlterAccount, s.handleAlterAccount)
	s.addQueryCommandHandler(query.CmdMethod_TraceSpan, s.handleTraceSpan)
	s.addQueryCommandHandler(query.CmdMethod_GetLockInfo, s.handleGetLockInfo)
	s.addQueryCommandHandler(query.CmdMethod_GetTxnInfo, s.handleGetTxnInfo)
	s.addQueryCommandHandler(query.CmdMethod_GetCacheInfo, s.handleGetCacheInfo)
	s.addQueryCommandHandler(query.CmdMethod_SyncCommit, s.handleSyncCommit)
	s.addQueryCommandHandler(query.CmdMethod_GetCommit, s.handleGetCommit)
	s.addQueryCommandHandler(query.CmdMethod_ShowProcessList, s.handleShowProcessList)
	s.addQueryCommandHandler(query.CmdMethod_RunTask, s.handleRunTask)
	s.addQueryCommandHandler(query.CmdMethod_RemoveRemoteLockTable, s.handleRemoveRemoteLockTable)
	s.addQueryCommandHandler(query.CmdMethod_UnsubscribeTable, s.handleUnsubscribeTable)
	s.addQueryCommandHandler(query.CmdMethod_GetCacheData, s.handleGetCacheData)
	s.addQueryCommandHandler(query.CmdMethod_GetStatsInfo, s.handleGetStatsInfo)
	s.addQueryCommandHandler(query.CmdMethod_GetPipelineInfo, s.handleGetPipelineInfo)
	s.addQueryCommandHandler(query.CmdMethod_MigrateConnFrom, s.handleMigrateConnFrom)
	s.addQueryCommandHandler(query.CmdMethod_MigrateConnTo, s.handleMigrateConnTo)
	s.addQueryCommandHandler(query.CmdMethod_ReloadAutoIncrementCache, s.handleReloadAutoIncrementCache)
	s.addQueryCommandHandler(query.CmdMethod_GetReplicaCount, s.handleGetReplicaCount)
	s.addQueryCommandHandler(query.CmdMethod_CtlReader, s.handleCtlReader)
	s.addQueryCommandHandler(query.CmdMethod_ResetSession, s.handleResetSession)
	s.addQueryCommandHandler(query.CmdMethod_GOMAXPROCS, s.handleGoMaxProcs)
	s.addQueryCommandHandler(query.CmdMethod_GOMEMLIMIT, s.handleGoMemLimit)
	s.addQueryCommandHandler(query.CmdMethod_GOGCPercent, s.handleGoGCPercent)
	s.addQueryCommandHandler(query.CmdMethod_FileServiceCache, s.handleFileServiceCacheRequest)
	s.addQueryCommandHandler(query.CmdMethod_FileServiceCacheEvict, s.handleFileServiceCacheEvictRequest)
	s.addQueryCommandHandler(query.CmdMethod_MetadataCache, s.handleMetadataCacheRequest)
	s.addQueryCommandHandler(query.CmdMethod_FaultInject, s.handleFaultInjection)
	s.addQueryCommandHandler(query.CmdMethod_CtlMoTableStats, s.handleMoTableStats)
	s.addQueryCommandHandler(query.CmdMethod_WorkspaceThreshold, s.handleWorkspaceThresholdRequest)
	s.addQueryCommandHandler(query.CmdMethod_MinTimestamp, s.handleGetMinTimestamp)
	s.addQueryCommandHandler(query.CmdMethod_CtlPrefetchOnSubscribed, s.handleCtlPrefetchOnSubscribed)
	s.addQueryCommandHandler(query.CmdMethod_ISCPDrainConsumer, s.handleISCPDrainConsumer)
	s.addQueryCommandHandler(query.CmdMethod_IcebergCacheInvalidate, s.handleIcebergCacheInvalidate)
	s.addQueryCommandHandler(query.CmdMethod_MongoDBClientRetire, s.handleMongoDBClientRetire)
}

func (s *service) addQueryCommandHandler(
	method query.CmdMethod,
	handler func(context.Context, *query.Request, *query.Response, *morpc.Buffer) error,
) {
	s.queryService.AddHandleFunc(
		method,
		func(ctx context.Context, req *query.Request, resp *query.Response, buf *morpc.Buffer) error {
			release, ok := s.queryWork.admit()
			if !ok {
				return moerr.NewServiceUnavailableNoCtx("CN query service is closing")
			}
			defer release()
			return handler(ctx, req, resp, buf)
		},
		false,
	)
}

func (s *service) closeQueryService() error {
	return s.queryWork.close(func() error {
		if s.queryService != nil {
			return s.queryService.Close()
		}
		return nil
	})
}

func (s *service) handleKillConn(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req == nil || req.KillConnRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	if rm == nil {
		return moerr.NewInternalError(ctx, "routine manager not initialized")
	}
	accountMgr := rm.GetAccountRoutineManager()
	if accountMgr == nil {
		return moerr.NewInternalError(ctx, "account routine manager not initialized")
	}
	logutil.Infof("[handle kill request] handle kill conn, add account id %d, version %d to kill queue", req.KillConnRequest.AccountID, req.KillConnRequest.Version)
	accountMgr.EnKillQueue(req.KillConnRequest.AccountID, req.KillConnRequest.Version)

	resp.KillConnResponse = &query.KillConnResponse{
		Success: true,
	}
	return nil
}

func (s *service) handleAlterAccount(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req == nil || req.AlterAccountRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	if rm == nil {
		return moerr.NewInternalError(ctx, "routine manager not initialized")
	}
	accountMgr := rm.GetAccountRoutineManager()
	if accountMgr == nil {
		return moerr.NewInternalError(ctx, "account routine manager not initialized")
	}
	logutil.Infof("[handle alter request] handle alter conn, account id %d to status %s", req.AlterAccountRequest.TenantId, req.AlterAccountRequest.Status)
	accountMgr.AlterRoutineStatue(req.AlterAccountRequest.TenantId, req.AlterAccountRequest.Status)
	resp.AlterAccountResponse = &query.AlterAccountResponse{
		AlterSuccess: true,
	}
	return nil
}

func (s *service) handleTraceSpan(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.TraceSpanResponse = new(query.TraceSpanResponse)
	resp.TraceSpanResponse.Resp = ctl.UpdateCurrentCNTraceSpan(
		req.TraceSpanRequest.Cmd, req.TraceSpanRequest.Spans, req.TraceSpanRequest.Threshold)
	return nil
}

func (s *service) handleFaultInjection(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.FaultInjectResponse = new(query.FaultInjectResponse)
	resp.FaultInjectResponse.Resp = fault.HandleFaultInject(
		ctx, req.FaultInjectRequest.Method, req.FaultInjectRequest.Parameters,
	)
	return nil
}

func (s *service) handleMoTableStats(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	e := s.storeEngine.(*disttae.Engine)
	ret := e.HandleMoTableStatsCtl(req.CtlMoTableStatsRequest.Cmd)
	resp.CtlMoTableStatsResponse = query.CtlMoTableStatsResponse{
		Resp: ret,
	}
	return nil
}

func (s *service) handleCtlReader(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.CtlReaderResponse = new(query.CtlReaderResponse)

	extra := strings.Split(types.DecodeStringSlice(req.CtlReaderRequest.Extra)[0], ":")
	extra = append([]string{req.CtlReaderRequest.Cfg}, extra...)

	resp.CtlReaderResponse.Resp = ctl.UpdateCurrentCNReader(
		req.CtlReaderRequest.Cmd, extra...)

	return nil
}

func (s *service) handleCtlPrefetchOnSubscribed(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req == nil || req.CtlPrefetchOnSubscribedRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	resp.CtlPrefetchOnSubscribedResponse = new(query.CtlPrefetchOnSubscribedResponse)
	resp.CtlPrefetchOnSubscribedResponse.Resp = ctl.UpdateCurrentCNPrefetchOnSubscribed(
		req.CtlPrefetchOnSubscribedRequest.Patterns,
	)
	return nil
}

func (s *service) handleISCPDrainConsumer(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req == nil || req.ISCPDrainConsumerRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	r := req.ISCPDrainConsumerRequest
	key := iscp.NewJobRuntimeKey(r.AccountID, r.TableID, r.JobName, r.JobID)
	if r.RemoveFenceOnly {
		if _, msg, injected := fault.TriggerFault(objectio.FJ_ISCPCancelRemoveFenceError); injected {
			if msg == "" {
				msg = objectio.FJ_ISCPCancelRemoveFenceError
			}
			return moerr.NewInternalErrorNoCtxf("injected ISCP remove fence error: %s", msg)
		}
		iscp.RemoveCNJobFence(s.cfg.UUID, key)
		resp.ISCPDrainConsumerResponse = &query.ISCPDrainConsumerResponse{Success: true}
		return nil
	}
	if r.RenewFenceOnly {
		ttl := iscp.RollbackFenceTTL()
		// Renewal must never create a fence. A delayed renew can be processed
		// after rollback cleanup; requiring the CN fence to exist makes remove
		// terminal even when RPC handling is reordered.
		if !iscp.RenewCNJobFence(s.cfg.UUID, key, ttl) {
			return moerr.NewInternalErrorf(
				ctx,
				"cannot renew ISCP consumer quiescence fence on CN %s for tableID=%d jobName=%s jobID=%d",
				s.cfg.UUID,
				r.TableID,
				r.JobName,
				r.JobID,
			)
		}
		resp.ISCPDrainConsumerResponse = &query.ISCPDrainConsumerResponse{Success: true}
		return nil
	}

	// Install the CN-scoped fence before looking up the executor. This closes
	// the task-assignment/readiness gap: a replacement executor generation on
	// this CN observes the fence even if it is published after this request.
	iscp.InstallCNJobFence(s.cfg.UUID, key, iscp.RollbackFenceTTL())
	// A daemon task publishes task_runner before its executor has completed
	// recovery and registered its runtime.
	readyCtx, cancel := context.WithTimeout(ctx, iscpExecutorReadyTimeout)
	defer cancel()
	exec, ok := getISCPExecutorRuntime(readyCtx, s.cfg.UUID)
	if !ok || exec == nil {
		exec, ok = waitISCPExecutorRuntime(readyCtx, s.cfg.UUID)
	}
	if !ok || exec == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// This code is preserved by queryservice's response error envelope and
		// is retried by the compile-side drain path only.
		return moerr.NewRetryForCNRollingRestart()
	}
	if err := exec.CancelAndDrainJobConsumer(ctx, r.AccountID, r.TableID, r.JobName, r.JobID); err != nil {
		exec.RemoveJobFence(key)
		return err
	}
	resp.ISCPDrainConsumerResponse = &query.ISCPDrainConsumerResponse{Success: true}
	return nil
}

func getISCPExecutorRuntime(ctx context.Context, cnUUID string) (*iscp.ISCPTaskExecutor, bool) {
	if _, _, injected := fault.TriggerFaultWithContext(ctx, objectio.FJ_ISCPCancelExecutorNotReady); injected {
		return nil, false
	}
	return iscpGetExecutorRuntimeFn(cnUUID)
}

func waitISCPExecutorRuntime(ctx context.Context, cnUUID string) (*iscp.ISCPTaskExecutor, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	if exec, ok := getISCPExecutorRuntime(ctx, cnUUID); ok && exec != nil {
		return exec, true
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-ticker.C:
			if exec, ok := getISCPExecutorRuntime(ctx, cnUUID); ok && exec != nil {
				return exec, true
			}
		}
	}
}

// handleGetLockInfo sends the lock info on current cn to another cn that needs.
func (s *service) handleGetLockInfo(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.GetLockInfoResponse = new(query.GetLockInfoResponse)

	//get lock info from lock service in current cn
	locks := make([]*query.LockInfo, 0)
	getAllLocks := func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
		//need copy keys
		info := &query.LockInfo{
			TableId:     tableID,
			Keys:        copyKeys(keys),
			LockMode:    lock.GetLockMode(),
			IsRangeLock: lock.IsRangeLock(),
		}

		lock.IterHolders(func(holder pblock.WaitTxn) bool {
			info.Holders = append(info.Holders, copyWaitTxn(holder))
			return true
		})

		lock.IterWaiters(func(waiter pblock.WaitTxn) bool {
			info.Waiters = append(info.Waiters, copyWaitTxn(waiter))
			return true
		})

		locks = append(locks, info)
		return true
	}

	s.lockService.IterLocks(getAllLocks)

	// fill the response
	resp.GetLockInfoResponse.CnId = s.metadata.UUID
	resp.GetLockInfoResponse.LockInfoList = locks
	return nil
}

func (s *service) handleGetTxnInfo(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.GetTxnInfoResponse = new(query.GetTxnInfoResponse)
	txns := make([]*query.TxnInfo, 0)

	s._txnClient.IterTxns(func(view client.TxnOverview) bool {
		info := &query.TxnInfo{
			CreateAt:  view.CreateAt,
			AccountID: view.AccountID,
			Meta:      copyTxnMeta(view.Meta),
			UserTxn:   view.UserTxn,
		}

		for _, lock := range view.WaitLocks {
			info.WaitLocks = append(info.WaitLocks, copyTxnInfo(lock))
		}
		txns = append(txns, info)
		return true
	})

	resp.GetTxnInfoResponse.CnId = s.metadata.UUID
	resp.GetTxnInfoResponse.TxnInfoList = txns
	return nil
}

func (s *service) handleSyncCommit(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	s._txnClient.SyncLatestCommitTS(req.SycnCommit.LatestCommitTS)
	return nil
}

func (s *service) handleGetMinTimestamp(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.MinTimestampResponse = new(query.MinTimestampResponse)
	resp.MinTimestampResponse.MinTimestamp = s._txnClient.MinTimestamp()
	return nil
}

func (s *service) handleGetCommit(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.GetCommit = new(query.GetCommitResponse)
	resp.GetCommit.CurrentCommitTS = s._txnClient.GetLatestCommitTS()
	return nil
}

func (s *service) handleShowProcessList(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.ShowProcessListRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	sessions, err := s.processList(req.ShowProcessListRequest.Tenant,
		req.ShowProcessListRequest.SysTenant)
	if err != nil {
		resp.WrapError(err)
		return nil
	}
	resp.ShowProcessListResponse = &query.ShowProcessListResponse{
		Sessions: sessions,
	}
	return nil
}

func (s *service) handleRunTask(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.RunTask == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	s.task.Lock()
	defer s.task.Unlock()

	code := task.TaskCode(req.RunTask.TaskCode)
	if s.task.runner == nil {
		resp.RunTask = &query.RunTaskResponse{
			Result: "Task Runner Not Ready",
		}
		return nil
	}
	exec := s.task.runner.GetExecutor(code)
	if exec == nil {
		resp.RunTask = &query.RunTaskResponse{
			Result: "Task Not Found",
		}
		return nil
	}
	if !s.queryWork.launch(exec, &task.AsyncTask{
		ID:       0,
		Metadata: task.TaskMetadata{ID: code.String(), Executor: code},
	}) {
		return moerr.NewServiceUnavailableNoCtx("CN query service is closing")
	}
	resp.RunTask = &query.RunTaskResponse{
		Result: "OK",
	}
	return nil
}

// processList returns all the sessions. For sys tenant, return all sessions; but for common
// tenant, just return the sessions belong to the tenant.
// It is called "processList" is because it is used in "SHOW PROCESSLIST" statement.
func (s *service) processList(tenant string, sysTenant bool) ([]*status.Session, error) {
	if sysTenant {
		return s.sessionMgr.GetAllStatusSessions(), nil
	}
	return s.sessionMgr.GetStatusSessionsByTenant(tenant), nil
}

func copyKeys(src [][]byte) [][]byte {
	dst := make([][]byte, 0, len(src))
	for _, s := range src {
		d := make([]byte, len(s))
		copy(d, s)
		dst = append(dst, s)
	}
	return dst
}

func copyWaitTxn(src pblock.WaitTxn) *pblock.WaitTxn {
	dst := &pblock.WaitTxn{}
	dst.TxnID = make([]byte, len(src.TxnID))
	copy(dst.TxnID, src.GetTxnID())
	dst.CreatedOn = src.GetCreatedOn()
	return dst
}

func copyTxnMeta(src txn.TxnMeta) *txn.TxnMeta {
	dst := &txn.TxnMeta{
		ID:         commonUtil.CloneBytes(src.GetID()),
		Status:     src.GetStatus(),
		SnapshotTS: src.GetSnapshotTS(),
		PreparedTS: src.GetPreparedTS(),
		CommitTS:   src.GetCommitTS(),
		Mode:       src.GetMode(),
		Isolation:  src.GetIsolation(),
	}
	return dst
}

func copyLockOptions(src pblock.LockOptions) *pblock.LockOptions {
	dst := &pblock.LockOptions{
		Granularity: src.GetGranularity(),
		Mode:        src.GetMode(),
	}
	return dst
}

func copyTxnInfo(src client.Lock) *query.TxnLockInfo {
	dst := &query.TxnLockInfo{
		TableId: src.TableID,
		Rows:    copyKeys(src.Rows),
		Options: copyLockOptions(src.Options),
	}
	return dst
}

func (s *service) handleGetCacheInfo(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.GetCacheInfoResponse = new(query.GetCacheInfoResponse)

	perfcounter.GetCacheStats(func(infos []*query.CacheInfo) {
		for _, info := range infos {
			if info != nil {
				resp.GetCacheInfoResponse.CacheInfoList = append(resp.GetCacheInfoResponse.CacheInfoList, info)
			}
		}
	})

	return nil
}

func (s *service) handleRemoveRemoteLockTable(
	ctx context.Context,
	req *query.Request,
	resp *query.Response,
	_ *morpc.Buffer,
) error {
	removed, err := s.lockService.CloseRemoteLockTable(
		req.RemoveRemoteLockTable.GroupID,
		req.RemoveRemoteLockTable.TableID,
		req.RemoveRemoteLockTable.Version)
	if err != nil {
		return err
	}

	resp.RemoveRemoteLockTable = &query.RemoveRemoteLockTableResponse{}
	if removed {
		resp.RemoveRemoteLockTable.Count = 1
	}
	return nil
}

func (s *service) handleUnsubscribeTable(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.UnsubscribeTable == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	err := s.storeEngine.UnsubscribeTable(ctx, 0, req.UnsubscribeTable.DatabaseID, req.UnsubscribeTable.TableID)
	if err != nil {
		resp.WrapError(err)
		return nil
	}
	resp.UnsubscribeTable = &query.UnsubscribeTableResponse{
		Success: true,
	}
	return nil
}

// handleGetCacheData reads the cache data from the local data cache in fileservice.
func (s *service) handleGetCacheData(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	sharedFS, err := fileservice.Get[fileservice.FileService](s.fileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	wr := &query.WrappedResponse{
		Response: resp,
	}
	err = fileservice.HandleRemoteRead(ctx, sharedFS, req, wr)
	if err != nil {
		return err
	}
	s.queryService.SetReleaseFunc(resp, wr.ReleaseFunc)
	return nil
}

func (s *service) handleGetStatsInfo(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.GetStatsInfoRequest == nil || req.GetStatsInfoRequest.StatsInfoKey == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	// The parameter sync is false, as the read request is from remote node,
	// and we do not need wait for the data sync.
	key := *req.GetStatsInfoRequest.StatsInfoKey
	var info *statsinfo.StatsInfo
	if exporter, ok := s.storeEngine.(engine.RemoteStatsExporter); ok {
		info = exporter.StatsForRemote(ctx, key)
	} else {
		info = s.storeEngine.Stats(ctx, key, false)
	}
	resp.GetStatsInfoResponse = &query.GetStatsInfoResponse{
		StatsInfo: info,
	}
	return nil
}

// handleGetPipelineInfo handles the GetPipelineInfoRequest and respond with
// the pipeline info in the server.
func (s *service) handleGetPipelineInfo(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	if req.GetPipelineInfoRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	count := s.pipelines.counter.Load()
	resp.GetPipelineInfoResponse = &query.GetPipelineInfoResponse{
		Count: count,
	}
	return nil
}

func (s *service) handleMigrateConnFrom(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	if req.MigrateConnFromRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	resp.MigrateConnFromResponse = &query.MigrateConnFromResponse{}
	if err := rm.MigrateConnectionFromWithContext(ctx, req.MigrateConnFromRequest, resp.MigrateConnFromResponse); err != nil {
		logutil.Errorf("failed to migrate conn from: %v", err)
		return err
	}
	return nil
}

func (s *service) handleMigrateConnTo(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	if req.MigrateConnToRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	if err := rm.MigrateConnectionTo(ctx, req.MigrateConnToRequest); err != nil {
		logutil.Errorf("failed to migrate conn to: %v", err)
		return err
	}
	logutil.Infof("migrate ok, conn ID: %d, DB: %s, prepared stmt count: %d",
		req.MigrateConnToRequest.ConnID, req.MigrateConnToRequest.DB, len(req.MigrateConnToRequest.PrepareStmts))
	for _, stmt := range req.MigrateConnToRequest.PrepareStmts {
		logutil.Infof("migrated prepare stmt on conn %d, %s, %s", req.MigrateConnToRequest.ConnID, stmt.Name, stmt.SQL)
	}
	resp.MigrateConnToResponse = &query.MigrateConnToResponse{
		Success: true,
	}
	return nil
}

func (s *service) handleReloadAutoIncrementCache(
	ctx context.Context,
	req *query.Request,
	resp *query.Response,
	_ *morpc.Buffer,
) error {
	return s.incrservice.Reload(
		ctx,
		req.ReloadAutoIncrementCache.TableID,
	)
}

func (s *service) handleGetReplicaCount(
	ctx context.Context,
	req *query.Request,
	resp *query.Response,
	_ *morpc.Buffer,
) error {
	if s.shardService != nil {
		resp.GetReplicaCount.Count = s.shardService.ReplicaCount()
	}
	return nil
}

func (s *service) handleResetSession(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	if req.ResetSessionRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	rm := s.mo.GetRoutineManager()
	resp.ResetSessionResponse = &query.ResetSessionResponse{}
	if err := rm.ResetSessionWithContext(ctx, req.ResetSessionRequest, resp.ResetSessionResponse); err != nil {
		logutil.Errorf("failed to reset session: %v", err)
		return err
	}
	resp.ResetSessionResponse.Success = true
	return nil
}

func (s *service) handleGoMaxProcs(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	resp.GoMaxProcsResponse.MaxProcs = int32(system.SetGoMaxProcs(int(req.GoMaxProcsRequest.MaxProcs)))
	logutil.Info("QueryService::GoMaxProcs",
		zap.String("op", "set"),
		zap.Int32("in", req.GoMaxProcsRequest.MaxProcs),
		zap.Int32("out", resp.GoMaxProcsResponse.MaxProcs),
	)
	return nil
}

func (s *service) handleGoMemLimit(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	resp.GoMemLimitResponse.MemLimitBytes = int64(debug.SetMemoryLimit(req.GoMemLimitRequest.MemLimitBytes))
	logutil.Info("QueryService::GoMemLimit",
		zap.String("op", "set"),
		zap.Int64("in", req.GoMemLimitRequest.MemLimitBytes),
		zap.Int64("out", resp.GoMemLimitResponse.MemLimitBytes),
	)
	return nil
}

func (s *service) handleGoGCPercent(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	resp.GoGCPercentResponse.Percent = int32(debug.SetGCPercent(int(req.GoGCPercentRequest.Percent)))
	logutil.Info("QueryService::GOGCPercent",
		zap.Int32("in", req.GoGCPercentRequest.Percent),
		zap.Int32("out", resp.GoGCPercentResponse.Percent),
	)
	return nil
}

func (s *service) handleFileServiceCacheRequest(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {

	if n := req.FileServiceCacheRequest.CacheSize; n > 0 {
		switch req.FileServiceCacheRequest.Type {
		case query.FileServiceCacheType_Disk:
			fileservice.GlobalDiskCacheSizeHint.Store(n)
		case query.FileServiceCacheType_Memory:
			fileservice.GlobalMemoryCacheSizeHint.Store(n)
		}
		logutil.Info("cache size adjusted",
			zap.Any("type", req.FileServiceCacheRequest.Type),
			zap.Any("size", n),
		)
	}

	return nil
}

func (s *service) handleFileServiceCacheEvictRequest(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {

	logutil.Info("file service cache evict",
		zap.String("type", req.FileServiceCacheEvictRequest.Type.String()))

	var ret map[string]int64
	switch req.FileServiceCacheEvictRequest.Type {
	case query.FileServiceCacheType_Disk:
		ret = fileservice.EvictDiskCaches(ctx)
	case query.FileServiceCacheType_Memory:
		ret = fileservice.EvictMemoryCaches(ctx)
	}

	for name, target := range ret {
		logutil.Info("file service cache evict",
			zap.String("type", req.FileServiceCacheEvictRequest.Type.String()),
			zap.Int64("size", target),
			zap.String("name", name),
		)
		resp.FileServiceCacheEvictResponse.CacheSize = target
		resp.FileServiceCacheEvictResponse.CacheCapacity = target
		// usually one instance
		break
	}

	return nil
}

func (s *service) handleMetadataCacheRequest(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {

	logutil.Info("metadata cache", zap.Int64("size", req.MetadataCacheRequest.CacheSize))

	// set capacity hint
	objectio.GlobalCacheCapacityHint.Store(req.MetadataCacheRequest.CacheSize)
	// evict
	target := objectio.EvictCache(ctx)
	// response
	resp.MetadataCacheResponse.CacheCapacity = target

	return nil
}

func (s *service) handleIcebergCacheInvalidate(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	hook, ok := moruntime.ServiceRuntime(s.serviceID()).GetGlobalVariables(api.CacheInvalidatorRuntimeKey)
	if !ok || hook == nil {
		resp.IcebergCacheInvalidateResponse.RemovedEntries = 0
		return nil
	}
	invalidator, ok := hook.(api.CacheInvalidationHandler)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid Iceberg cache invalidator runtime hook")
	}
	payload := req.GetIcebergCacheInvalidateRequest()
	removed, err := invalidator.InvalidateIcebergCache(ctx, api.CacheInvalidationRequest{
		AccountID:            payload.AccountID,
		CatalogID:            payload.CatalogID,
		Namespace:            payload.Namespace,
		Table:                payload.Table,
		SnapshotID:           payload.SnapshotID,
		MetadataLocationHash: payload.MetadataLocationHash,
		CommitID:             payload.CommitID,
	})
	if err != nil {
		return err
	}
	resp.IcebergCacheInvalidateResponse.RemovedEntries = int64(removed)
	return nil
}

func (s *service) handleMongoDBClientRetire(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {
	if req == nil {
		return moerr.NewInternalError(ctx, "invalid MongoDB client retirement request")
	}
	value, ok := moruntime.ServiceRuntime(s.serviceID()).GetGlobalVariables(sqlmongodb.RuntimeDependenciesKey)
	if !ok || value == nil {
		resp.MongoDBClientRetireResponse.Success = true
		return nil
	}
	dependencies, ok := value.(*sqlmongodb.RuntimeDependencies)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid MongoDB runtime dependencies")
	}
	payload := req.GetMongoDBClientRetireRequest()
	if err := (sqlmongodb.ClientRetirement{
		AccountID: payload.AccountID, ConnectionID: payload.ConnectionID,
		VersionExclusive: payload.VersionExclusive,
	}).Apply(dependencies.Pool); err != nil {
		return err
	}
	resp.MongoDBClientRetireResponse.Success = true
	return nil
}

func (s *service) serviceID() string {
	if s == nil || s.cfg == nil {
		return ""
	}
	return s.cfg.UUID
}

func (s *service) handleWorkspaceThresholdRequest(
	ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer,
) error {

	logutil.Info(
		"WORKSPACE-THRESHOLD-CHANGED",
		zap.Uint64("commit-threshold", req.WorkspaceThresholdRequest.CommitThreshold),
		zap.Uint64("write-threshold", req.WorkspaceThresholdRequest.WriteThreshold),
	)

	e := s.storeEngine.(*disttae.Engine)
	commit, write := e.SetWorkspaceThreshold(
		req.WorkspaceThresholdRequest.CommitThreshold,
		req.WorkspaceThresholdRequest.WriteThreshold,
	)

	resp.WorkspaceThresholdResponse = &query.WorkspaceThresholdResponse{
		CommitThreshold: commit,
		WriteThreshold:  write,
	}

	return nil
}
