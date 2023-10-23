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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func (s *service) initQueryService() {
	svc, err := queryservice.NewQueryService(s.cfg.UUID,
		s.queryServiceListenAddr(), s.cfg.RPC, s.sessionMgr)
	if err != nil {
		panic(err)
	}
	s.queryService = svc
	s.initQueryCommandHandler()
}

func (s *service) initQueryCommandHandler() {
	s.queryService.AddHandleFunc(query.CmdMethod_KillConn, s.handleKillConn, false)
	s.queryService.AddHandleFunc(query.CmdMethod_AlterAccount, s.handleAlterAccount, false)
	s.queryService.AddHandleFunc(query.CmdMethod_TraceSpan, s.handleTraceSpan, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetLockInfo, s.handleGetLockInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetTxnInfo, s.handleGetTxnInfo, false)
	s.queryService.AddHandleFunc(query.CmdMethod_GetCacheInfo, s.handleGetCacheInfo, false)
}

func (s *service) handleKillConn(ctx context.Context, req *query.Request, resp *query.Response) error {
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

func (s *service) handleAlterAccount(ctx context.Context, req *query.Request, resp *query.Response) error {
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

func (s *service) handleTraceSpan(ctx context.Context, req *query.Request, resp *query.Response) error {
	resp.TraceSpanResponse = new(query.TraceSpanResponse)
	resp.TraceSpanResponse.Resp = ctl.SelfProcess(
		req.TraceSpanRequest.Cmd, req.TraceSpanRequest.Spans, req.TraceSpanRequest.Threshold)
	return nil
}

// handleGetLockInfo sends the lock info on current cn to another cn that needs.
func (s *service) handleGetLockInfo(ctx context.Context, req *query.Request, resp *query.Response) error {
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

func (s *service) handleGetTxnInfo(ctx context.Context, req *query.Request, resp *query.Response) error {
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

func (s *service) handleGetCacheInfo(ctx context.Context, req *query.Request, resp *query.Response) error {
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
