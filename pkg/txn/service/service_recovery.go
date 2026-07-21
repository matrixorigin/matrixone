// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

func (s *service) startRecovery() {
	if err := s.stopper.RunTask(s.doRecovery); err != nil {
		s.logger.Fatal("start recover task failed",
			zap.Error(err))
	}
	s.storage.StartRecovery(s.recoveryCtx, s.txnC)
	s.waitRecoveryCompleted()
}

func (s *service) doRecovery(ctx context.Context) {
	defer s.finishRecovery()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.recoveryCtx.Done():
			return
		case txn, ok := <-s.txnC:
			if !ok {
				s.end()
				return
			}
			s.addLog(txn)
		}
	}
}

func (s *service) resolveRecoveryTNShards(
	ctx context.Context,
	txnMeta txn.TxnMeta,
) (txn.TxnMeta, bool) {
	const retryInterval = 100 * time.Millisecond
	var cluster clusterservice.MOCluster
	for cluster == nil {
		var err error
		cluster, err = clusterservice.GetMOClusterWithContext(ctx, s.sid)
		if err == nil {
			break
		}
		if !waitRecoveryRouteRetry(ctx, retryInterval) {
			return txnMeta, false
		}
	}

	for {
		// A complete cached route can still name a replaced replica. Establish
		// freshness before accepting ReplicaID and Address for recovery RPCs.
		if refresher, ok := cluster.(clusterservice.AuthoritativeRefresher); ok {
			if err := refresher.Refresh(ctx); err != nil {
				if ctx.Err() != nil || !waitRecoveryRouteRetry(ctx, retryInterval) {
					return txnMeta, false
				}
				continue
			}
		}
		services, err := clusterservice.GetAllTNServicesWithContext(ctx, cluster)
		if err != nil {
			if !waitRecoveryRouteRetry(ctx, retryInterval) {
				return txnMeta, false
			}
			continue
		}
		if shards, ok := s.resolveRecoveryTNShardsFromSnapshot(txnMeta.TNShards, services); ok {
			txnMeta.TNShards = shards
			return txnMeta, true
		}

		if !waitRecoveryRouteRetry(ctx, retryInterval) {
			return txnMeta, false
		}
	}
}

func waitRecoveryRouteRetry(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *service) resolveRecoveryTNShardsFromSnapshot(
	participants []metadata.TNShard,
	services []metadata.TNService,
) ([]metadata.TNShard, bool) {
	routes := make(map[uint64]metadata.TNShard)
	for _, service := range services {
		for _, shard := range service.Shards {
			shard.Address = service.TxnServiceAddress
			if recoveryTNShardRouteComplete(shard) {
				routes[shard.ShardID] = shard
			}
		}
	}

	resolved := make([]metadata.TNShard, len(participants))
	for i, participant := range participants {
		if participant.ShardID == s.shard.ShardID {
			if !recoveryTNShardRouteComplete(s.shard) {
				return nil, false
			}
			resolved[i] = s.shard
			continue
		}
		route, ok := routes[participant.ShardID]
		if !ok {
			return nil, false
		}
		if route.LogShardID == 0 {
			route.LogShardID = participant.LogShardID
		}
		resolved[i] = route
	}
	return resolved, true
}

func recoveryTNShardRouteComplete(shard metadata.TNShard) bool {
	// LogShardID is storage metadata, not part of txn RPC routing or TN shard
	// identity. Dynamic HAKeeper TN snapshots currently expose only ShardID and
	// ReplicaID, so preserve LogShardID when supplied but do not wait forever
	// when a current route legitimately has it unset.
	return shard.ShardID != 0 &&
		shard.ReplicaID != 0 &&
		shard.Address != ""
}

func (s *service) addLog(txnMeta txn.TxnMeta) {
	if len(txnMeta.TNShards) <= 1 {
		return
	}

	switch txnMeta.Status {
	case txn.TxnStatus_Committing:
		s.checkRecoveryStatus(txnMeta)
		txnCtx := s.getTxnContext(txnMeta.ID)
		if txnCtx == nil {
			s.maybeAddTxn(txnMeta)
		} else {
			if txnCtx.getTxn().Status != txn.TxnStatus_Prepared &&
				txnCtx.getTxn().Status != txn.TxnStatus_Committing {
				s.logger.Fatal("invalid txn status before committing",
					zap.String("prev-status", txnCtx.getTxn().Status.String()),
					util.TxnField(txnMeta))
			}
			txnCtx.updateTxn(txnMeta)
		}
	case txn.TxnStatus_Prepared:
		s.checkRecoveryStatus(txnMeta)
		txnCtx := s.getTxnContext(txnMeta.ID)
		if txnCtx == nil {
			s.maybeAddTxn(txnMeta)
			break
		}

		if txnCtx.getTxn().Status != txn.TxnStatus_Prepared {
			s.logger.Fatal("invalid txn status before prepare status",
				zap.String("prev-status", txnCtx.getTxn().Status.String()),
				util.TxnField(txnMeta))
		}
		txnCtx.updateTxn(txnMeta)
	case txn.TxnStatus_Committed:
		s.checkRecoveryStatus(txnMeta)
		s.removeTxn(txnMeta.ID)
	default:
		s.logger.Fatal("invalid recovery status",
			util.TxnField(txnMeta))
	}
}

func (s *service) end() {
	defer s.finishRecovery()
	s.transactions.Range(func(_, value any) bool {
		txnCtx := value.(*txnContext)
		txnMeta := txnCtx.getTxn()
		if len(txnMeta.TNShards) == 0 ||
			txnMeta.TNShards[0].ShardID != s.shard.ShardID {
			return true
		}
		// Fold the complete log stream before waiting for live routes. A later
		// terminal record may remove an obsolete prepared transaction entirely.
		var resolved bool
		txnMeta, resolved = s.resolveRecoveryTNShards(s.recoveryCtx, txnMeta)
		if !resolved {
			return false
		}
		txnCtx.updateTxn(txnMeta)

		switch txnMeta.Status {
		case txn.TxnStatus_Prepared:
			if err := s.startAsyncCheckCommitTask(txnCtx); err != nil {
				s.logger.Error("start check commit task failed during recovery",
					zap.Error(err),
					util.TxnField(txnMeta))
			}
		case txn.TxnStatus_Committing:
			s.validTNShard(txnMeta.TNShards[0])
			if err := s.startAsyncCommitTask(txnCtx); err != nil {
				s.logger.Error("start commit task failed during recovery",
					zap.Error(err),
					util.TxnField(txnMeta))
				return true
			}
			s.removeTxn(txnMeta.ID)
		}
		return true
	})
}

func (s *service) finishRecovery() {
	s.recoveryOnce.Do(func() {
		close(s.recoveryC)
	})
}

func (s *service) waitRecoveryCompleted() {
	<-s.recoveryC
}

func (s *service) startAsyncCheckCommitTask(txnCtx *txnContext) error {
	return s.stopper.RunTask(func(ctx context.Context) {
		txnMeta := txnCtx.getTxn()

		requests := make([]txn.TxnRequest, 0, len(txnMeta.TNShards)-1)
		for _, tn := range txnMeta.TNShards[1:] {
			requests = append(requests, txn.TxnRequest{
				Txn:              txnMeta,
				Method:           txn.TxnMethod_GetStatus,
				GetStatusRequest: &txn.TxnGetStatusRequest{TNShard: tn},
			})
		}

		result := s.parallelSendWithRetry(ctx, requests, prepareIgnoreErrorCodes)
		if result == nil {
			return
		}
		defer result.Release()

		prepared := 1
		txnMeta.CommitTS = txnMeta.PreparedTS
		for _, resp := range result.Responses {
			if resp.Txn != nil && resp.Txn.Status == txn.TxnStatus_Prepared {
				prepared++
				if txnMeta.CommitTS.Less(resp.Txn.PreparedTS) {
					txnMeta.PreparedTS = resp.Txn.PreparedTS
				}
			}
		}

		if prepared == len(txnMeta.TNShards) {
			txnCtx.updateTxnLocked(txnMeta)
			s.removeTxn(txnMeta.ID)
			if err := s.startAsyncCommitTask(txnCtx); err != nil {
				s.logger.Error("start commit task failed",
					zap.Error(err),
					util.TxnField(txnMeta))
			}
		} else {
			s.startAsyncRollbackTask(txnMeta)
		}
	})
}

func (s *service) checkRecoveryStatus(txnMeta txn.TxnMeta) {
	if txnMeta.PreparedTS.IsEmpty() ||
		(txnMeta.Status != txn.TxnStatus_Prepared &&
			txnMeta.CommitTS.IsEmpty()) {
		s.logger.Fatal("invalid preparedTS or commitTS",
			util.TxnField(txnMeta))
	}

}
