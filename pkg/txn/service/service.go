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
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var _ TxnService = (*service)(nil)

type service struct {
	logger  *zap.Logger
	shard   metadata.DNShard
	storage storage.TxnStorage
	sender  rpc.TxnSender
	clocker clock.Clock
	stopper *stopper.Stopper

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
func NewTxnService(logger *zap.Logger,
	shard metadata.DNShard,
	storage storage.TxnStorage,
	sender rpc.TxnSender,
	clocker clock.Clock,
	zombieTimeout time.Duration) TxnService {
	logger = logutil.Adjust(logger).With(util.TxnDNShardField(shard))
	s := &service{
		logger:  logger,
		shard:   shard,
		sender:  sender,
		storage: storage,
		clocker: clocker,
		pool: sync.Pool{
			New: func() any {
				return &txnContext{
					logger: logger,
				}
			}},
		stopper: stopper.NewStopper(fmt.Sprintf("txn-service-%d-%d",
			shard.ShardID,
			shard.ReplicaID), stopper.WithLogger(logger)),
		zombieTimeout: zombieTimeout,
		recoveryC:     make(chan struct{}),
		txnC:          make(chan txn.TxnMeta, 16),
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
	if err := closer(); err != nil {
		return multierr.Append(err, s.sender.Close())
	}
	return s.sender.Close()
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
		case now := <-timer.C:
			s.transactions.Range(func(_, value any) bool {
				txnCtx := value.(*txnContext)
				if now.Sub(txnCtx.createAt) > s.zombieTimeout {
					cleanTxns = append(cleanTxns, txnCtx.getTxn())
				}
				return true
			})
			for _, txn := range cleanTxns {
				s.removeTxn(txn.ID)
				if err := s.storage.Rollback(txn); err != nil {
					s.logger.Error("start rollback task failed",
						util.TxnIDFieldWithID(txn.ID),
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
	// 2. transaction already committed or aborted, the transcation context will removed by gcZombieTxn.
	txnCtx.init(meta, acquireNotifier())
	util.LogTxnCreateOn(s.logger, meta, s.shard)
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

func (s *service) validDNShard(dn metadata.DNShard) {
	if !s.shard.Equal(dn) {
		s.logger.Fatal("DN metadata not match",
			zap.String("request-dn", dn.DebugString()),
			zap.String("local-dn", s.shard.DebugString()))
	}
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
	txnMeta txn.TxnMeta,
	requests []txn.TxnRequest,
	ignoreTxnErrorCodes map[txn.ErrorCode]struct{}) *rpc.SendResult {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			util.LogTxnSendRequests(s.logger, requests)
			result, err := s.sender.Send(ctx, requests)
			if err != nil {
				util.LogTxnSendRequestsFailed(s.logger, requests, err)
				continue
			}
			util.LogTxnReceivedResponses(s.logger, result.Responses)
			hasError := false
			for _, resp := range result.Responses {
				if resp.TxnError != nil {
					_, ok := ignoreTxnErrorCodes[resp.TxnError.Code]
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
	logger   *zap.Logger
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

	util.LogTxnWaiterAdded(c.logger, c.mu.txn, waitStatus)
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
	util.LogTxnUpdated(c.logger, c.mu.txn)
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
		util.LogTxnUpdated(c.logger, c.mu.txn)
		c.nt.notify(status)
	}
}

func (s *service) mustGetTimeoutAtFromContext(ctx context.Context) int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		s.logger.Fatal("context deadline not set")
	}
	return deadline.UnixNano()
}
