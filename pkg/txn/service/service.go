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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/zap"
)

var _ TxnService = (*service)(nil)

type service struct {
	logger    *log.MOLogger
	shard     metadata.DNShard
	storage   storage.TxnStorage
	sender    rpc.TxnSender
	stopper   *stopper.Stopper
	allocator lockservice.LockTableAllocator

	// TxnService maintains a sync.Map in memory to record all running transactions. The metadata for each write
	// transaction is initialized when the TxnService receives its first write operation and written to the map.
	// The transaction is removed from the map after it has been committed or aborted.
	//
	// When a transaction's Read operation encounters data written by transactions in Committing and Prepared,
	// it needs to wait for these transactions to reach the final state as it is not sure if the data is visible
	// for the current read transaction. So we need to keep track of all running write transactions and notify the
	// blocked Read operation when the transaction is committed or aborted.
	//
	// In some cases, after a transaction has been committed or rolled back, a previous write request is received
	// due to the network, resulting in the transaction information being written back to the map.
	// We use the zombieTimeout setting to solve this problem, so that when a transaction exceeds the zombieTimeout
	// threshold in the map, it is cleaned up.
	transactions  sync.Map // string(txn.id) -> txnContext
	zombieTimeout time.Duration
	pool          sync.Pool
	recoveryC     chan struct{}
	txnC          chan txn.TxnMeta
}

// NewTxnService create TxnService
func NewTxnService(
	shard metadata.DNShard,
	storage storage.TxnStorage,
	sender rpc.TxnSender,
	zombieTimeout time.Duration,
	allocator lockservice.LockTableAllocator) TxnService {
	s := &service{
		logger:  util.GetLogger(),
		shard:   shard,
		sender:  sender,
		storage: storage,
		pool: sync.Pool{
			New: func() any {
				return &txnContext{}
			}},
		stopper: stopper.NewStopper(fmt.Sprintf("txn-service-%d-%d",
			shard.ShardID,
			shard.ReplicaID),
			stopper.WithLogger(util.GetLogger().RawLogger())),
		zombieTimeout: zombieTimeout,
		recoveryC:     make(chan struct{}),
		txnC:          make(chan txn.TxnMeta, 16),
		allocator:     allocator,
	}
	if err := s.stopper.RunTask(s.gcZombieTxn); err != nil {
		s.logger.Fatal("start gc zombie txn failed",
			zap.Error(err))
	}
	return s
}

func (s *service) Shard() metadata.DNShard {
	return s.shard
}

func (s *service) Start() error {
	if err := s.storage.Start(); err != nil {
		return err
	}
	s.startRecovery()
	return nil
}

func (s *service) Close(destroy bool) error {
	s.waitRecoveryCompleted()
	s.stopper.Stop()
	closer := s.storage.Close
	if destroy {
		closer = s.storage.Destroy
	}
	// FIXME: all context.TODO() need to use tracing context
	return errors.Join(closer(context.TODO()), s.sender.Close())
}

func (s *service) gcZombieTxn(ctx context.Context) {
	s.logger.Info("gc zombie txn task started")
	defer s.logger.Info("gc zombie txn task stopped")

	timer := time.NewTicker(s.zombieTimeout)
	defer timer.Stop()

	var cleanTxns []txn.TxnMeta
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.transactions.Range(func(_, value any) bool {
				txnCtx := value.(*txnContext)
				txnMeta := txnCtx.getTxn()
				// if a txn is not a distributed txn coordinator, wait coordinator dnshard.
				if len(txnMeta.DNShards) == 0 ||
					(len(txnMeta.DNShards) > 0 && s.shard.ShardID != txnMeta.DNShards[0].ShardID) {
					return true
				}

				now := time.Now()
				if now.Sub(txnCtx.createAt) > s.zombieTimeout {
					cleanTxns = append(cleanTxns, txnMeta)
				}
				return true
			})
			for _, txnMeta := range cleanTxns {
				req := &txn.TxnRequest{
					Method:          txn.TxnMethod_Rollback,
					Txn:             txnMeta,
					RollbackRequest: &txn.TxnRollbackRequest{},
				}
				resp := &txn.TxnResponse{}
				if err := s.Rollback(ctx, req, resp); err != nil || resp.TxnError != nil {
					txnError := ""
					if resp.TxnError != nil {
						txnError = resp.TxnError.DebugString()
					}
					s.logger.Error("rollback zombie txn failed",
						util.TxnField(txnMeta),
						zap.String("txn-err", txnError),
						zap.Error(err))
				}
			}
			cleanTxns = cleanTxns[:0]
		}
	}
}

func (s *service) maybeAddTxn(meta txn.TxnMeta) (*txnContext, bool) {
	id := hack.SliceToString(meta.ID)
	if v, ok := s.transactions.Load(id); ok {
		return v.(*txnContext), false
	}

	txnCtx := s.acquireTxnContext()
	v, loaded := s.transactions.LoadOrStore(id, txnCtx)
	if loaded {
		s.releaseTxnContext(txnCtx)
		return v.(*txnContext), false
	}

	// 1. first transaction write request at current DNShard
	// 2. transaction already committed or aborted, the transaction context will be removed by gcZombieTxn.
	txnCtx.init(meta, acquireNotifier())
	util.LogTxnCreateOn(meta, s.shard)
	return txnCtx, true
}

func (s *service) removeTxn(txnID []byte) {
	s.transactions.Delete(hack.SliceToString(txnID))
}

func (s *service) getTxnContext(txnID []byte) *txnContext {
	id := hack.SliceToString(txnID)
	v, ok := s.transactions.Load(id)
	if !ok {
		return nil
	}
	return v.(*txnContext)
}

func (s *service) validDNShard(dn metadata.DNShard) bool {
	if !s.shard.Equal(dn) {
		// DNShard not match, so cn need to fetch latest DNShards from hakeeper.
		s.logger.Error("DN metadata not match",
			zap.String("request-dn", dn.DebugString()),
			zap.String("local-dn", s.shard.DebugString()))
		return false
	}
	return true
}

func (s *service) acquireTxnContext() *txnContext {
	return s.pool.Get().(*txnContext)
}

func (s *service) releaseTxnContext(txnCtx *txnContext) {
	txnCtx.resetLocked()
	s.pool.Put(txnCtx)
}

func (s *service) parallelSendWithRetry(
	ctx context.Context,
	requests []txn.TxnRequest,
	ignoreTxnErrorCodes map[uint16]struct{}) *rpc.SendResult {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			util.LogTxnSendRequests(requests)
			result, err := s.sender.Send(ctx, requests)
			if err != nil {
				util.LogTxnSendRequestsFailed(requests, err)
				continue
			}
			util.LogTxnReceivedResponses(result.Responses)
			hasError := false
			for _, resp := range result.Responses {
				if resp.TxnError != nil {
					_, ok := ignoreTxnErrorCodes[uint16(resp.TxnError.Code)]
					if !ok {
						hasError = true
					}
				}
			}
			if !hasError {
				return result
			}
			result.Release()
		}
	}
}

type txnContext struct {
	nt       *notifier
	createAt time.Time

	mu struct {
		sync.RWMutex
		requests []txn.TxnRequest
		txn      txn.TxnMeta
	}
}

func (c *txnContext) addWaiter(txnID []byte, w *waiter, waitStatus txn.TxnStatus) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !bytes.Equal(c.mu.txn.ID, txnID) {
		return false
	}

	util.LogTxnWaiterAdded(c.mu.txn, waitStatus)
	c.nt.addWaiter(w, waitStatus)
	return true
}

func (c *txnContext) init(txn txn.TxnMeta, nt *notifier) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.txn = txn
	c.nt = nt
	c.createAt = time.Now()
}

func (c *txnContext) getTxn() txn.TxnMeta {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.getTxnLocked()
}

func (c *txnContext) updateTxn(txn txn.TxnMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateTxnLocked(txn)
}

func (c *txnContext) getTxnLocked() txn.TxnMeta {
	return c.mu.txn
}

func (c *txnContext) updateTxnLocked(txn txn.TxnMeta) {
	c.mu.txn = txn
	util.LogTxnUpdated(c.mu.txn)
}

func (c *txnContext) resetLocked() {
	c.nt.close(c.mu.txn.Status)
	c.nt = nil
	c.mu.requests = c.mu.requests[:0]
	c.mu.txn = txn.TxnMeta{}
}

func (c *txnContext) changeStatusLocked(status txn.TxnStatus) {
	if c.mu.txn.Status != status {
		c.mu.txn.Status = status
		util.LogTxnUpdated(c.mu.txn)
		c.nt.notify(status)
	}
}
