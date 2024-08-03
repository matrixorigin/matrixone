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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

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
	s.queryService.AddHandleFunc(query.CmdMethod_KillConn, s.handleKillConn, false)
	s.queryService.AddHandleFunc(query.CmdMethod_AlterAccount, s.handleAlterAccount, false)
	s.queryService.AddHandleFunc(query.CmdMethod_TraceSpan, s.handleTraceSpan, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetLockInfo, s.handleGetLockInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetTxnInfo, s.handleGetTxnInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetCacheInfo, s.handleGetCacheInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_SyncCommit, s.handleSyncCommit, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetCommit, s.handleGetCommit, false)
	s.queryService.AddHandleFunc(query.CmdMethod_ShowProcessList, s.handleShowProcessList, false)
	s.queryService.AddHandleFunc(query.CmdMethod_RunTask, s.handleRunTask, false)
	s.queryService.AddHandleFunc(query.CmdMethod_RemoveRemoteLockTable, s.handleRemoveRemoteLockTable, false)
	s.queryService.AddHandleFunc(query.CmdMethod_UnsubscribeTable, s.handleUnsubscribeTable, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetCacheData, s.handleGetCacheData, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetStatsInfo, s.handleGetStatsInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetPipelineInfo, s.handleGetPipelineInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_MigrateConnFrom, s.handleMigrateConnFrom, false)
	s.queryService.AddHandleFunc(query.CmdMethod_MigrateConnTo, s.handleMigrateConnTo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_ReloadAutoIncrementCache, s.handleReloadAutoIncrementCache, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetReplicaCount, s.handleGetReplicaCount, false)
	s.queryService.AddHandleFunc(query.CmdMethod_CtlReader, s.handleCtlReader, false)
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

	accountMgr.EnKillQueue(req.KillConnRequest.AccountID, req.KillConnRequest.Version)
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

	accountMgr.AlterRoutineStatue(req.AlterAccountRequest.TenantId, req.AlterAccountRequest.Status)
	return nil
}

func (s *service) handleTraceSpan(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.TraceSpanResponse = new(query.TraceSpanResponse)
	resp.TraceSpanResponse.Resp = ctl.UpdateCurrentCNTraceSpan(
		req.TraceSpanRequest.Cmd, req.TraceSpanRequest.Spans, req.TraceSpanRequest.Threshold)
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
			CreateAt: view.CreateAt,
			Meta:     copyTxnMeta(view.Meta),
			UserTxn:  view.UserTxn,
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
	go func() {
		_ = exec(context.Background(), &task.AsyncTask{
			ID:       0,
			Metadata: task.TaskMetadata{ID: code.String(), Executor: code},
		})
	}()
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

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func copyTxnMeta(src txn.TxnMeta) *txn.TxnMeta {
	dst := &txn.TxnMeta{
		ID:         copyBytes(src.GetID()),
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
	err := s.storeEngine.UnsubscribeTable(ctx, req.UnsubscribeTable.DatabaseID, req.UnsubscribeTable.TableID)
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
	if req.GetStatsInfoRequest == nil {
		return moerr.NewInternalError(ctx, "bad request")
	}
	// The parameter sync is false, as the read request is from remote node,
	// and we do not need wait for the data sync.
	resp.GetStatsInfoResponse = &query.GetStatsInfoResponse{
		StatsInfo: s.storeEngine.Stats(ctx, *req.GetStatsInfoRequest.StatsInfoKey, false),
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
	if err := rm.MigrateConnectionFrom(req.MigrateConnFromRequest, resp.MigrateConnFromResponse); err != nil {
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
	resp.GetReplicaCount.Count = s.shardService.ReplicaCount()
	return nil
}
