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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

var (
	// All shared tables is shared by all tenants. We will use 1+TableID(high 4 bytes) + tenantID(low 4 bytes)
	// as table id to avoid conflict.
	sharedTables = map[uint64]uint64{
		// can not use catalog.MO_TABLES_ID, because cycle import.
		1: 1<<63 | 1<<32,
		2: 1<<63 | 2<<32,
		3: 1<<63 | 3<<32,
	}
)

type service struct {
	cfg                  Config
	serviceID            string
	tables               *lockTableHolder
	activeTxnHolder      activeTxnHolder
	fsp                  *fixedSlicePool
	deadlockDetector     *detector
	events               *waiterEvents
	clock                clock.Clock
	stopper              *stopper.Stopper
	stopOnce             sync.Once
	fetchWhoWaitingListC chan who

	remote struct {
		client Client
		server Server
		keeper LockTableKeeper
	}

	mu struct {
		sync.RWMutex
		allocating map[uint64]chan struct{}
	}
}

// NewLockService create a lock service instance
func NewLockService(cfg Config) LockService {
	cfg.Validate()
	s := &service{
		// If a cn with the same uuid is restarted within a short period of time, it will lead to
		// the possibility that the remote locks will not be released, because the heartbeat timeout
		// of a remote lockservice cannot be detected. To solve this problem we use uuid+create-time as
		// service id, then a cn reboot with the same uuid will also be considered as not a lockservice.
		serviceID: getServiceIdentifier(cfg.ServiceID, time.Now().UnixNano()),
		cfg:       cfg,
		fsp:       newFixedSlicePool(int(cfg.MaxFixedSliceSize)),
		stopper: stopper.NewStopper("lock-service",
			stopper.WithLogger(getLogger().RawLogger())),
		fetchWhoWaitingListC: make(chan who, 10240),
	}
	s.tables = &lockTableHolder{id: s.serviceID, tables: make(map[uint64]lockTable)}
	s.mu.allocating = make(map[uint64]chan struct{})
	s.activeTxnHolder = newMapBasedTxnHandler(s.serviceID, s.fsp)
	s.deadlockDetector = newDeadlockDetector(
		s.fetchTxnWaitingList,
		s.abortDeadlockTxn)
	s.events = newWaiterEvents(eventsWorkers, s.deadlockDetector)
	s.clock = runtime.ProcessLevelRuntime().Clock()
	s.initRemote()
	s.events.start()
	for i := 0; i < fetchWhoWaitingListTaskCount; i++ {
		_ = s.stopper.RunTask(s.handleFetchWhoWaitingMe)
	}
	return s
}

func (s *service) Lock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options pb.LockOptions) (pb.Result, error) {
	v2.TxnLockTotalCounter.Inc()

	start := time.Now()
	defer func() {
		v2.TxnAcquireLockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock")
	defer span.End()

	tableID = encodeSharedTableID(ctx, tableID)
	if options.ForwardTo != "" {
		return s.forwardLock(ctx, tableID, rows, txnID, options)
	}

	txn := s.activeTxnHolder.getActiveTxn(txnID, true, "")
	l, err := s.getLockTableWithCreate(tableID, true)
	if err != nil {
		return pb.Result{}, err
	}

	// All txn lock op must be serial. And avoid dead lock between doAcquireLock
	// and getLock. The doAcquireLock and getLock operations of the same transaction
	// will be concurrent (deadlock detection), which may lead to a deadlock in mutex.
	txn.Lock()
	defer txn.Unlock()
	if !bytes.Equal(txn.txnID, txnID) {
		return pb.Result{}, ErrTxnNotFound
	}
	if txn.deadlockFound {
		return pb.Result{}, ErrDeadLockDetected
	}

	var result pb.Result
	l.lock(
		ctx,
		txn,
		rows,
		LockOptions{LockOptions: options},
		func(r pb.Result, e error) {
			result = r
			err = e
		})
	return result, err
}

func (s *service) Unlock(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp) error {
	start := time.Now()
	defer func() {
		v2.TxnUnlockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// FIXME(fagongzi): too many mem alloc in trace
	_, span := trace.Debug(ctx, "lockservice.unlock")
	defer span.End()

	txn := s.activeTxnHolder.deleteActiveTxn(txnID)
	if txn == nil {
		return nil
	}
	txn.Lock()
	defer txn.Unlock()
	if !bytes.Equal(txn.txnID, txnID) {
		return nil
	}

	defer logUnlockTxn(s.serviceID, txn)()
	txn.close(s.serviceID, txnID, commitTS, s.getLockTable)
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	return nil
}

func (s *service) GetServiceID() string {
	return s.serviceID
}

func (s *service) GetConfig() Config {
	return s.cfg
}

func (s *service) Close() error {
	var err error
	s.stopOnce.Do(func() {
		s.stopper.Stop()
		s.tables.removeWithFilter(func(_ uint64, _ lockTable) bool { return true })
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
		s.events.close()
		close(s.fetchWhoWaitingListC)
	})
	return err
}

func (s *service) fetchTxnWaitingList(txn pb.WaitTxn, waiters *waiters) (bool, error) {
	if txn.CreatedOn == s.serviceID {
		activeTxn := s.activeTxnHolder.getActiveTxn(txn.TxnID, false, "")
		// the active txn closed
		if activeTxn == nil {
			return true, nil
		}
		txnID := activeTxn.getID()
		if !bytes.Equal(txnID, txn.TxnID) {
			return true, nil
		}
		return activeTxn.fetchWhoWaitingMe(
			s.serviceID,
			txnID,
			s.activeTxnHolder,
			waiters.add,
			s.getLockTable), nil
	}

	waitingList, err := s.getTxnWaitingListOnRemote(txn.TxnID, txn.CreatedOn)
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

func (s *service) abortDeadlockTxn(wait pb.WaitTxn, err error) {
	// this wait activeTxn must be hold by current service, because
	// all transactions found to be deadlocked by the deadlock
	// detector must be held by the current service
	activeTxn := s.activeTxnHolder.getActiveTxn(wait.TxnID, false, "")
	// the active txn closed
	if activeTxn == nil {
		return
	}
	activeTxn.abort(s.serviceID, wait, err)
}

func (s *service) getLockTable(tableID uint64) (lockTable, error) {
	return s.getLockTableWithCreate(tableID, false)
}

func (s *service) getAllocatingC(
	tableID uint64,
	locked bool) chan struct{} {
	if !locked {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	return s.mu.allocating[tableID]
}

func (s *service) waitLockTableBind(
	tableID uint64,
	locked bool) lockTable {
	c := s.getAllocatingC(tableID, locked)
	if c != nil {
		<-c
	}
	return s.tables.get(tableID)
}

func (s *service) getLockTableWithCreate(tableID uint64, create bool) (lockTable, error) {
	if v := s.tables.get(tableID); v != nil {
		return v, nil
	}
	if !create {
		return s.waitLockTableBind(tableID, false), nil
	}

	var c chan struct{}
	fn := func() lockTable {
		s.mu.Lock()
		waitC := s.getAllocatingC(tableID, true)
		if waitC != nil {
			s.mu.Unlock()
			<-waitC
			s.mu.Lock()
		}

		v := s.tables.get(tableID)
		if v == nil {
			c = make(chan struct{})
			s.mu.allocating[tableID] = c
		}
		s.mu.Unlock()
		return v
	}
	if v := fn(); v != nil {
		return v, nil
	}

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.mu.allocating, tableID)
		close(c)
	}()
	bind, err := getLockTableBind(
		s.remote.client,
		tableID,
		s.serviceID)
	if err != nil {
		return nil, err
	}

	v := s.tables.set(tableID, s.createLockTableByBind(bind))
	return v, nil
}

func (s *service) handleBindChanged(newBind pb.LockTable) {
	new := s.createLockTableByBind(newBind)
	s.tables.set(newBind.Table, new)
}

func (s *service) createLockTableByBind(bind pb.LockTable) lockTable {
	defer logLockTableCreated(
		s.serviceID,
		bind,
		bind.ServiceID != s.serviceID)

	if bind.ServiceID == s.serviceID {
		return newLocalLockTable(
			bind,
			s.fsp,
			s.events,
			s.clock)
	} else {
		return newRemoteLockTable(
			s.serviceID,
			s.cfg.RemoteLockTimeout.Duration,
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
		dequeue           list.Deque[remote]
		activeTxns        map[string]*activeTxn
		activeTxnServices map[string]string
	}
}

func newMapBasedTxnHandler(
	serviceID string,
	fsp *fixedSlicePool) activeTxnHolder {
	h := &mapBasedTxnHolder{}
	h.fsp = fsp
	h.serviceID = serviceID
	h.mu.activeTxns = make(map[string]*activeTxn, 1024)
	h.mu.activeTxnServices = make(map[string]string)
	h.mu.remoteServices = make(map[string]*list.Element[remote])
	h.mu.dequeue = list.New[remote]()
	return h
}

func (h *mapBasedTxnHolder) getActiveLocked(txnKey string) *activeTxn {
	if v, ok := h.mu.activeTxns[txnKey]; ok {
		return v
	}
	return nil
}

func (h *mapBasedTxnHolder) getActiveTxn(
	txnID []byte,
	create bool,
	remoteService string) *activeTxn {
	txnKey := util.UnsafeBytesToString(txnID)
	h.mu.RLock()
	v := h.getActiveLocked(txnKey)
	if v != nil {
		h.mu.RUnlock()
		return v
	}
	h.mu.RUnlock()
	if !create {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if v := h.getActiveLocked(txnKey); v != nil {
		return v
	}

	txn := newActiveTxn(txnID, txnKey, h.fsp, remoteService)
	h.mu.activeTxns[txnKey] = txn
	h.mu.activeTxnServices[txnKey] = txn.remoteService

	if remoteService != "" {
		if _, ok := h.mu.remoteServices[remoteService]; !ok {
			h.mu.remoteServices[remoteService] = h.mu.dequeue.PushBack(remote{
				id:   remoteService,
				time: time.Now(),
			})

		}
	}
	logTxnCreated(txn)
	return txn
}

func (h *mapBasedTxnHolder) deleteActiveTxn(txnID []byte) *activeTxn {
	txnKey := util.UnsafeBytesToString(txnID)
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.mu.activeTxns[txnKey]
	if ok {
		delete(h.mu.activeTxns, txnKey)
		delete(h.mu.activeTxnServices, txnKey)
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

		for txnKey := range h.mu.activeTxns {
			remoteService := h.mu.activeTxnServices[txnKey]
			if _, ok := timeoutServices[remoteService]; ok {
				timeoutTxns = append(timeoutTxns, util.UnsafeStringToBytes(txnKey))
			}
		}

		for k := range timeoutServices {
			delete(h.mu.remoteServices, k)
		}
	}
	return timeoutTxns, wait
}

type remote struct {
	id   string
	time time.Time
}

func getServiceIdentifier(id string, version int64) string {
	return fmt.Sprintf("%19d%s", version, id)
}

func getUUIDFromServiceIdentifier(id string) string {
	if len(id) <= 19 {
		return id
	}
	return id[19:]
}

func encodeSharedTableID(
	ctx context.Context,
	tableID uint64) uint64 {
	v, ok := sharedTables[tableID]
	if !ok {
		return tableID
	}

	tenantID, ok := ctx.Value(defines.TenantIDKey{}).(uint32)
	if !ok {
		tenantID = 0
	}

	return v | uint64(tenantID)
}

func decodeSharedTableID(tableID uint64) (tenantID uint32, sharedTableID uint64, ok bool) {
	if tableID&(1<<63) == 0 {
		return 0, 0, false
	}
	tableID = tableID << 1 >> 1
	return uint32(tableID << 32 >> 32), tableID >> 32, true
}

func isSharedTable(id uint64) bool {
	_, ok := sharedTables[id]
	return ok
}

type lockTableHolder struct {
	sync.RWMutex
	id     string
	tables map[uint64]lockTable
}

func (m *lockTableHolder) get(id uint64) lockTable {
	m.RLock()
	defer m.RUnlock()
	return m.tables[id]
}

func (m *lockTableHolder) set(id uint64, new lockTable) lockTable {
	m.Lock()
	defer m.Unlock()

	old, ok := m.tables[id]

	if !ok {
		m.tables[id] = new
		return new
	}

	oldBind := old.getBind()
	newBind := new.getBind()
	if oldBind.Changed(newBind) {
		old.close()
		m.tables[id] = new
		logRemoteBindChanged(m.id, oldBind, newBind)
		return new
	}
	new.close()
	return old
}

func (m *lockTableHolder) iter(fn func(uint64, lockTable) bool) {
	m.RLock()
	defer m.RUnlock()
	for id, v := range m.tables {
		if !fn(id, v) {
			return
		}
	}
}

func (m *lockTableHolder) removeWithFilter(filter func(uint64, lockTable) bool) {
	m.Lock()
	defer m.Unlock()
	for id, v := range m.tables {
		if filter(id, v) {
			v.close()
			delete(m.tables, id)
		}
	}
}
