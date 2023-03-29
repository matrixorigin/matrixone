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

package lockservice

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type service struct {
	cfg              Config
	tables           sync.Map // tableid -> locktable
	activeTxnHolder  activeTxnHolder
	fsp              *fixedSlicePool
	deadlockDetector *detector
	clock            clock.Clock
	stopper          *stopper.Stopper
	stopOnce         sync.Once

	remote struct {
		client Client
		server Server
		keeper LockTableKeeper
	}
}

// NewLockService create a lock service instance
func NewLockService(cfg Config) LockService {
	cfg.Validate()
	s := &service{
		cfg: cfg,
		fsp: newFixedSlicePool(int(cfg.MaxFixedSliceSize)),
		stopper: stopper.NewStopper("lock-service",
			stopper.WithLogger(getLogger().RawLogger())),
	}
	s.activeTxnHolder = newMapBasedTxnHandler(s.cfg.ServiceID, s.fsp)
	s.deadlockDetector = newDeadlockDetector(
		s.cfg.ServiceID,
		s.fetchTxnWaitingList,
		s.abortDeadlockTxn)
	s.clock = runtime.ProcessLevelRuntime().Clock()
	s.initRemote()
	return s
}

func (s *service) Lock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options LockOptions) (pb.Result, error) {
	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock")
	defer span.End()

	txn := s.activeTxnHolder.getActiveTxn(txnID, true, "")
	l, err := s.getLockTable(tableID)
	if err != nil {
		return pb.Result{}, err
	}
	return l.lock(ctx, txn, rows, options)
}

func (s *service) Unlock(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp) error {
	// FIXME(fagongzi): too many mem alloc in trace
	_, span := trace.Debug(ctx, "lockservice.unlock")
	defer span.End()

	txn := s.activeTxnHolder.deleteActiveTxn(txnID)
	if txn == nil {
		return nil
	}
	defer logUnlockTxn(s.cfg.ServiceID, txn)()
	txn.close(s.cfg.ServiceID, txnID, commitTS, s.getLockTable)
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	return nil
}

func (s *service) GetConfig() Config {
	return s.cfg
}

func (s *service) Close() error {
	var err error
	s.stopOnce.Do(func() {
		s.stopper.Stop()
		s.tables.Range(func(key, value any) bool {
			value.(lockTable).close()
			return true
		})
		if err = s.remote.client.Close(); err != nil {
			return
		}
		s.deadlockDetector.close()
		if err = s.remote.keeper.Close(); err != nil {
			return
		}
		if err = s.remote.client.Close(); err != nil {
			return
		}
		if err = s.remote.server.Close(); err != nil {
			return
		}
	})
	return err
}

func (s *service) fetchTxnWaitingList(txn pb.WaitTxn, waiters *waiters) (bool, error) {
	if txn.CreatedOn == s.cfg.ServiceID {
		txn := s.activeTxnHolder.getActiveTxn(txn.TxnID, false, "")
		// the active txn closed
		if txn == nil {
			return true, nil
		}
		return txn.fetchWhoWaitingMe(
			s.cfg.ServiceID,
			txn.txnID,
			s.activeTxnHolder,
			waiters.add,
			s.getLockTable), nil
	}

	waitingList, err := s.getTxnWaitingList(txn.TxnID, txn.CreatedOn)
	if err != nil {
		return false, err
	}
	for _, v := range waitingList {
		if !waiters.add(v) {
			return false, nil
		}
	}
	return true, nil
}

func (s *service) abortDeadlockTxn(wait pb.WaitTxn) {
	// this wait txn must be hold by current service, because
	// all transactions found to be deadlocked by the deadlock
	// detector must be held by the current service
	txn := s.activeTxnHolder.getActiveTxn(wait.TxnID, false, "")
	// the active txn closed
	if txn == nil {
		return
	}
	txn.abort(s.cfg.ServiceID, wait.TxnID)
}

func (s *service) getLockTable(tableID uint64) (lockTable, error) {
	return s.getLockTableWithCreate(tableID, true)
}

func (s *service) getLockTableWithCreate(tableID uint64, create bool) (lockTable, error) {
	if v, ok := s.tables.Load(tableID); ok {
		return v.(lockTable), nil
	}
	if !create {
		return nil, nil
	}

	bind, err := getLockTableBind(
		s.remote.client,
		tableID,
		s.cfg.ServiceID)
	if err != nil {
		return nil, err
	}

	l := s.createLockTableByBind(bind)
	if v, loaded := s.tables.LoadOrStore(tableID, l); loaded {
		l.close()
		return v.(lockTable), nil
	}
	return l, nil
}

func (s *service) handleBindChanged(newBind pb.LockTable) {
	// TODO(fagongzi): replace with swap if upgrade to go1.20
	old, loaded := s.tables.LoadAndDelete(newBind.Table)
	if !loaded {
		panic("missing lock table")
	}
	s.tables.Store(
		newBind.Table,
		s.createLockTableByBind(newBind))
	logRemoteBindChanged(s.cfg.ServiceID, old.(lockTable).getBind(), newBind)
	old.(lockTable).close()
}

func (s *service) createLockTableByBind(bind pb.LockTable) lockTable {
	defer logLockTableCreated(
		s.cfg.ServiceID,
		bind,
		bind.ServiceID != s.cfg.ServiceID)

	if bind.ServiceID == s.cfg.ServiceID {
		return newLocalLockTable(
			bind,
			s.fsp,
			s.deadlockDetector,
			s.clock)
	} else {
		return newRemoteLockTable(
			s.cfg.ServiceID,
			bind,
			s.remote.client,
			s.handleBindChanged)
	}
}

type activeTxnHolder interface {
	getActiveTxn(txnID []byte, create bool, remoteService string) *activeTxn
	deleteActiveTxn(txnID []byte) *activeTxn
	keepRemoteActiveTxn(remoteService string)
	getTimeoutRemoveTxn(
		timeoutServices map[string]struct{},
		timeoutTxns [][]byte,
		maxKeepInterval time.Duration) ([][]byte, time.Duration)
}

type mapBasedTxnHolder struct {
	serviceID string
	fsp       *fixedSlicePool
	mu        struct {
		sync.RWMutex
		// remoteServices known remote service
		remoteServices map[string]*list.Element[remote]
		// head(oldest) -> tail (newest)
		dequeue    list.Deque[remote]
		activeTxns map[string]*activeTxn
	}
}

func newMapBasedTxnHandler(
	serviceID string,
	fsp *fixedSlicePool) activeTxnHolder {
	h := &mapBasedTxnHolder{}
	h.fsp = fsp
	h.serviceID = serviceID
	h.mu.activeTxns = make(map[string]*activeTxn, 1024)
	h.mu.remoteServices = make(map[string]*list.Element[remote])
	h.mu.dequeue = list.New[remote]()
	return h
}

func (h *mapBasedTxnHolder) getActiveTxn(
	txnID []byte,
	create bool,
	remoteService string) *activeTxn {
	txnKey := unsafeByteSliceToString(txnID)
	h.mu.RLock()
	if v, ok := h.mu.activeTxns[txnKey]; ok {
		h.mu.RUnlock()
		return v
	}
	h.mu.RUnlock()
	if !create {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	txn := newActiveTxn(txnID, txnKey, h.fsp, remoteService)
	h.mu.activeTxns[txnKey] = txn

	if remoteService != "" {
		if _, ok := h.mu.remoteServices[remoteService]; !ok {
			h.mu.remoteServices[remoteService] = h.mu.dequeue.PushBack(remote{
				id:   remoteService,
				time: time.Now(),
			})

		}
	}
	logTxnCreated(h.serviceID, txn)
	return txn
}

func (h *mapBasedTxnHolder) deleteActiveTxn(txnID []byte) *activeTxn {
	txnKey := unsafeByteSliceToString(txnID)
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.mu.activeTxns[txnKey]
	if ok {
		delete(h.mu.activeTxns, txnKey)
	}
	return v
}

func (h *mapBasedTxnHolder) keepRemoteActiveTxn(remoteService string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.mu.remoteServices[remoteService]; ok {
		e.Value.time = time.Now()
		h.mu.dequeue.MoveToBack(e)
	}
}

func (h *mapBasedTxnHolder) getTimeoutRemoveTxn(
	timeoutServices map[string]struct{},
	timeoutTxns [][]byte,
	maxKeepInterval time.Duration) ([][]byte, time.Duration) {
	timeoutTxns = timeoutTxns[:0]
	for k := range timeoutServices {
		delete(timeoutServices, k)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	idx := 0
	wait := time.Duration(0)
	h.mu.dequeue.Iter(0, func(r remote) bool {
		v := now.Sub(r.time)
		if v < maxKeepInterval {
			wait = maxKeepInterval - v
			return false
		}
		idx++
		return true
	})
	if removed := h.mu.dequeue.Drain(0, idx); removed != nil {
		removed.Iter(0, func(r remote) bool {
			timeoutServices[r.id] = struct{}{}
			return true
		})

		for _, txn := range h.mu.activeTxns {
			txn.Lock()
			if _, ok := timeoutServices[txn.remoteService]; ok {
				timeoutTxns = append(timeoutTxns, txn.txnID)
			}
			txn.Unlock()
		}
	}
	return timeoutTxns, wait
}

type remote struct {
	id   string
	time time.Time
}

func unsafeByteSliceToString(key []byte) string {
	return *(*string)(unsafe.Pointer(&key))
}

func unsafeStringToByteSlice(key string) []byte {
	return *(*[]byte)(unsafe.Pointer(&key))
}
