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
	"hash/crc64"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// WithWait setup wait func to wait some condition ready
func WithWait(wait func()) Option {
	return func(s *service) {
		s.option.wait = wait
	}
}

type service struct {
	cfg                  Config
	serviceID            string
	tableGroups          *lockTableHolders
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
		restartTime  timestamp.Timestamp
		status       pb.Status
		groupTables  [][]pb.LockTable
		lockTableRef map[uint32]map[uint64]uint64
		allocating   map[uint32]map[uint64]chan struct{}
	}

	option struct {
		wait       func()
		serverOpts []ServerOption
	}
}

// NewLockService create a lock service instance
func NewLockService(
	cfg Config,
	opts ...Option) LockService {
	cfg.Validate()
	s := &service{
		// If a cn with the same uuid is restarted within a short period of time, it will lead to
		// the possibility that the remote locks will not be released, because the heartbeat timeout
		// of a remote lockservice cannot be detected. To solve this problem we use uuid+create-time
		// as service id, then a cn reboot with the same uuid will also be considered as not a same
		// lockservice.
		serviceID: getServiceIdentifier(cfg.ServiceID, time.Now().UnixNano()),
		cfg:       cfg,
		fsp:       newFixedSlicePool(int(cfg.MaxFixedSliceSize)),
		stopper: stopper.NewStopper("lock-service",
			stopper.WithLogger(getLogger().RawLogger())),
		fetchWhoWaitingListC: make(chan who, 10240),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.tableGroups = &lockTableHolders{service: s.serviceID, holders: map[uint32]*lockTableHolder{}}
	s.mu.allocating = make(map[uint32]map[uint64]chan struct{})
	s.mu.lockTableRef = make(map[uint32]map[uint64]uint64)
	s.deadlockDetector = newDeadlockDetector(
		s.fetchTxnWaitingList,
		s.abortDeadlockTxn)
	s.clock = runtime.ProcessLevelRuntime().Clock()

	s.initRemote()
	s.events = newWaiterEvents(eventsWorkers, s.deadlockDetector, s.activeTxnHolder, s.Unlock)
	s.events.start()
	for i := 0; i < fetchWhoWaitingListTaskCount; i++ {
		_ = s.stopper.RunTask(s.handleFetchWhoWaitingMe)
	}
	logLockServiceStartSucc(s.serviceID)
	return s
}

func (s *service) Lock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options pb.LockOptions) (pb.Result, error) {

	if !s.canLockOnServiceStatus(txnID, options, tableID, rows) {
		return pb.Result{}, moerr.NewNewTxnInCNRollingRestart()
	}

	v2.TxnLockTotalCounter.Inc()
	options.Validate(rows)

	start := time.Now()
	defer func() {
		v2.TxnAcquireLockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	s.wait()

	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock")
	defer span.End()

	if options.ForwardTo != "" {
		return s.forwardLock(ctx, tableID, rows, txnID, options)
	}

	txn := s.activeTxnHolder.getActiveTxn(txnID, true, "")
	l, err := s.getLockTableWithCreate(options.Group, tableID, rows, options.Sharding)
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
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	start := time.Now()
	defer func() {
		v2.TxnUnlockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	s.wait()

	txn := s.activeTxnHolder.deleteActiveTxn(txnID)
	if txn == nil {
		return nil
	}

	txn.Lock()
	defer txn.Unlock()
	if !bytes.Equal(txn.txnID, txnID) {
		return nil
	}

	if !s.isStatus(pb.Status_ServiceLockEnable) {
		s.reduceCanMoveGroupTables(txn)
		if s.isStatus(pb.Status_ServiceLockWaiting) &&
			s.activeTxnHolder.empty() {
			s.setStatus(pb.Status_ServiceUnLockSucc)
		}
	}

	defer logUnlockTxn(s.serviceID, txn)()
	txn.close(s.serviceID, txnID, commitTS, s.getLockTable, mutations...)
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	return nil
}

func (s *service) reduceCanMoveGroupTables(txn *activeTxn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.lockTableRef) == 0 {
		return
	}

	var res []pb.LockTable

	for group, h := range txn.lockHolders {
		for table, bind := range h.tableBinds {
			if bind.ServiceID == s.serviceID {
				if _, ok := s.mu.lockTableRef[group][table]; ok {
					s.mu.lockTableRef[group][table]--
					if s.mu.lockTableRef[group][table] == 0 {
						delete(s.mu.lockTableRef[group], table)
						res = append(res, bind)
					}
				}
			}
		}
	}
	if len(res) > 0 {
		s.mu.groupTables = append(s.mu.groupTables, res)
	}
}

func (s *service) checkCanMoveGroupTables() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.activeTxnHolder.incLockTableRef(s.mu.lockTableRef, s.serviceID)
	var res []pb.LockTable
	s.tableGroups.iter(func(_ uint64, v lockTable) bool {
		bind := v.getBind()
		if bind.ServiceID == s.serviceID {
			if _, ok := s.mu.lockTableRef[bind.Group][bind.Table]; !ok {
				res = append(res, bind)
			}
		}
		return true
	})
	if len(res) > 0 {
		s.mu.groupTables = append(s.mu.groupTables, res)
	}
}

func (s *service) canLockOnServiceStatus(
	txnID []byte,
	opts pb.LockOptions,
	tableID uint64,
	rows [][]byte) bool {
	if s.isStatus(pb.Status_ServiceLockEnable) {
		return true
	}
	if opts.Sharding == pb.Sharding_ByRow {
		tableID = shardingByRow(rows[0])
	}
	if !s.validGroupTable(opts.Group, tableID) {
		return false
	}
	if s.activeTxnHolder.empty() {
		return false
	}
	if s.activeTxnHolder.hasActiveTxn(txnID) {
		return true
	}
	if opts.SnapShotTs.LessEq(s.getRestartTime()) {
		return true
	}
	return false
}

func (s *service) validGroupTable(group uint32, tableID uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.mu.lockTableRef) == 0 {
		return true
	}
	_, ok := s.mu.lockTableRef[group][tableID]
	return ok
}

func (s *service) setRestartTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.restartTime, _ = s.clock.Now()
}

func (s *service) getRestartTime() timestamp.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.restartTime
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
		s.tableGroups.removeWithFilter(func(_ uint64, _ lockTable) bool { return true })
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
		s.activeTxnHolder.close()
		close(s.fetchWhoWaitingListC)
	})
	return err
}

func (s *service) setStatus(status pb.Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.status = status
}

func (s *service) getStatus() pb.Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.status
}

func (s *service) popGroupTables() []pb.LockTable {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.groupTables) == 0 {
		return nil
	}
	g := s.mu.groupTables[0]
	s.mu.groupTables = s.mu.groupTables[1:]
	return g
}

func (s *service) isStatus(status pb.Status) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.status == status
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

func (s *service) getLockTable(
	group uint32,
	tableID uint64) (lockTable, error) {
	if v := s.tableGroups.get(group, tableID); v != nil {
		return v, nil
	}
	return s.waitLockTableBind(
		group,
		tableID,
		false), nil
}

func (s *service) getAllocatingC(
	group uint32,
	tableID uint64,
	locked bool) chan struct{} {
	if !locked {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	if m, ok := s.mu.allocating[group]; ok {
		return m[tableID]
	}
	return nil
}

func (s *service) waitLockTableBind(
	group uint32,
	tableID uint64,
	locked bool) lockTable {
	c := s.getAllocatingC(group, tableID, locked)
	if c != nil {
		<-c
	}
	return s.tableGroups.get(group, tableID)
}

func (s *service) getLockTableWithCreate(
	group uint32,
	tableID uint64,
	rows [][]byte,
	sharding pb.Sharding) (lockTable, error) {
	originTableID := tableID
	if sharding == pb.Sharding_ByRow {
		tableID = shardingByRow(rows[0])
	}

	if v := s.tableGroups.get(group, tableID); v != nil {
		return v, nil
	}

	var c chan struct{}
	fn := func() lockTable {
		s.mu.Lock()
		waitC := s.getAllocatingC(group, tableID, true)
		if waitC != nil {
			s.mu.Unlock()
			<-waitC
			s.mu.Lock()
		}

		v := s.tableGroups.get(group, tableID)
		if v == nil {
			c = make(chan struct{})
			m, ok := s.mu.allocating[group]
			if !ok {
				m = make(map[uint64]chan struct{})
				s.mu.allocating[group] = m
			}
			m[tableID] = c
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
		delete(s.mu.allocating[group], tableID)
		close(c)
	}()
	bind, err := getLockTableBind(
		s.remote.client,
		group,
		tableID,
		originTableID,
		s.serviceID,
		sharding)
	if err != nil {
		return nil, err
	}

	v := s.tableGroups.set(group, tableID, s.createLockTableByBind(bind))
	return v, nil
}

func (s *service) handleBindChanged(newBind pb.LockTable) {
	new := s.createLockTableByBind(newBind)
	s.tableGroups.set(newBind.Group, newBind.Table, new)
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
			s.clock,
			s.activeTxnHolder)
	} else {
		remote := newRemoteLockTable(
			s.serviceID,
			s.cfg.RemoteLockTimeout.Duration,
			bind,
			s.remote.client,
			s.handleBindChanged)
		if !s.cfg.EnableRemoteLocalProxy {
			return remote
		}
		return newLockTableProxy(s.serviceID, remote)
	}
}

func (s *service) wait() {
	if s.option.wait == nil {
		return
	}
	s.option.wait()
}

type activeTxnHolder interface {
	close()
	empty() bool
	getAllTxnID() [][]byte
	incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string)
	getActiveTxn(txnID []byte, create bool, remoteService string) *activeTxn
	hasActiveTxn(txnID []byte) bool
	deleteActiveTxn(txnID []byte) *activeTxn
	keepRemoteActiveTxn(remoteService string)
	getTimeoutRemoveTxn(
		timeoutServices map[string]struct{},
		timeoutTxns [][]byte,
		maxKeepInterval time.Duration) [][]byte
	validTimeoutRemoteTxn(pb.WaitTxn) bool
}

type mapBasedTxnHolder struct {
	serviceID string
	fsp       *fixedSlicePool
	validTxn  func(txn pb.WaitTxn) (bool, error)
	valid     func(sid string) (bool, error)
	notify    func([]pb.OrphanTxn) ([][]byte, error)
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
	fsp *fixedSlicePool,
	valid func(sid string) (bool, error),
	notify func([]pb.OrphanTxn) ([][]byte, error),
	validTxn func(txn pb.WaitTxn) (bool, error),
) activeTxnHolder {
	h := &mapBasedTxnHolder{}
	h.fsp = fsp
	h.valid = valid
	h.notify = notify
	h.validTxn = validTxn
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

func (h *mapBasedTxnHolder) hasActiveTxn(txnID []byte) bool {
	txnKey := util.UnsafeBytesToString(txnID)
	h.mu.RLock()
	defer h.mu.RUnlock()
	if v := h.getActiveLocked(txnKey); v != nil {
		return true
	}
	return false
}

func (h *mapBasedTxnHolder) empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.mu.activeTxns) == 0
}

func (h *mapBasedTxnHolder) getAllTxnID() [][]byte {
	h.mu.RLock()
	defer h.mu.RUnlock()
	txns := make([][]byte, len(h.mu.activeTxns))
	i := 0
	for k := range h.mu.activeTxns {
		txns[i] = util.UnsafeStringToBytes(k)
		i++
	}
	return txns
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
	needRemoved [][]byte,
	maxKeepInterval time.Duration,
) [][]byte {
	needRemoved = needRemoved[:0]
	for k := range timeoutServices {
		delete(timeoutServices, k)
	}
	h.mu.Lock()
	now := time.Now()
	h.mu.dequeue.Iter(0, func(r remote) bool {
		v := now.Sub(r.time)
		if v < maxKeepInterval {
			return false
		}
		timeoutServices[r.id] = struct{}{}
		return true
	})
	h.mu.Unlock()

	var cannotCommit []pb.OrphanTxn
	cannotCommitServices := make(map[string]int)
	for sid := range timeoutServices {
		// skip maybe valid services
		if ok, err := h.valid(sid); ok && err == nil {
			delete(timeoutServices, sid)
		} else {
			// any error will be considered the txn cannot commit.
			delete(timeoutServices, sid)
			cannotCommit = append(cannotCommit, pb.OrphanTxn{Service: sid})
			cannotCommitServices[sid] = len(cannotCommit) - 1
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// all txns in the timeout services need to be removed
	for txnKey := range h.mu.activeTxns {
		remoteService := h.mu.activeTxnServices[txnKey]

		if idx, ok := cannotCommitServices[remoteService]; ok {
			cannotCommit[idx].Txn = append(cannotCommit[idx].Txn, util.UnsafeStringToBytes(txnKey))
			continue
		}

		if _, ok := timeoutServices[remoteService]; ok {
			needRemoved = append(needRemoved, util.UnsafeStringToBytes(txnKey))
		}
	}
	h.mu.Unlock()

	if len(cannotCommit) > 0 {
		// found txn1 cannot commit, but txn1 is still running in other cn.
		// There are 2 possible timings here:
		// 1. txn1's commit request arrive TN before cannot commit request
		// 2. txn1's commit request arrive TN after cannot commit request
		//
		// In case1: we cannot make txn1 as timeout txn.
		// In case2: txn1'commit request will failed, and we can make txn1 as
		//           timeout txn.
		if committing, err := h.notify(cannotCommit); err == nil {
			for sid, idx := range cannotCommitServices {
				if len(committing) == 0 {
					needRemoved = append(needRemoved, cannotCommit[idx].Txn...)
					timeoutServices[sid] = struct{}{}
				} else {
					m := make(map[string]struct{}, len(committing))
					for _, v := range committing {
						m[util.UnsafeBytesToString(v)] = struct{}{}
					}
					for _, v := range cannotCommit[idx].Txn {
						if _, ok := m[util.UnsafeBytesToString(v)]; !ok {
							needRemoved = append(needRemoved, v)
						}
					}
				}
			}
		}
	}

	// clear
	h.mu.Lock()
	for k := range timeoutServices {
		if e, ok := h.mu.remoteServices[k]; ok {
			delete(h.mu.remoteServices, k)
			h.mu.dequeue.Remove(e)
		}
	}
	return needRemoved
}

func (h *mapBasedTxnHolder) validTimeoutRemoteTxn(txn pb.WaitTxn) bool {
	if txn.CreatedOn == h.serviceID {
		return false
	}

	valid, err := h.validTxn(txn)
	if err == nil {
		return !valid
	}

	cannotCommit := []pb.OrphanTxn{
		{
			Service: txn.CreatedOn,
			Txn:     [][]byte{txn.TxnID},
		},
	}

	committing, err := h.notify(cannotCommit)
	if err != nil {
		return false
	}
	return len(committing) == 0
}

func (h *mapBasedTxnHolder) close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for k, txn := range h.mu.activeTxns {
		reuse.Free(txn, nil)
		delete(h.mu.activeTxns, k)
	}
}

func (h *mapBasedTxnHolder) incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, txn := range h.mu.activeTxns {
		txn.incLockTableRef(m, serviceID)
	}
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

func shardingByRow(row []byte) uint64 {
	return crc64.Checksum(row, crc64.MakeTable(crc64.ECMA))
}

type lockTableHolders struct {
	sync.RWMutex
	service string
	holders map[uint32]*lockTableHolder
}

func (m *lockTableHolders) get(group uint32, id uint64) lockTable {
	return m.mustGetHolder(group).get(id)
}

func (m *lockTableHolders) set(group uint32, id uint64, new lockTable) lockTable {
	return m.mustGetHolder(group).set(id, new)
}

func (m *lockTableHolders) mustGetHolder(group uint32) *lockTableHolder {
	m.RLock()
	h, ok := m.holders[group]
	m.RUnlock()
	if ok {
		return h
	}

	m.Lock()
	defer m.Unlock()
	if h, ok := m.holders[group]; ok {
		return h
	}
	h = &lockTableHolder{
		service: m.service,
		tables:  map[uint64]lockTable{},
	}
	m.holders[group] = h
	return h
}

func (m *lockTableHolders) iter(fn func(uint64, lockTable) bool) {
	m.RLock()
	defer m.RUnlock()
	for _, h := range m.holders {
		if !h.iter(fn) {
			return
		}
	}
}

func (m *lockTableHolders) removeWithFilter(filter func(uint64, lockTable) bool) {
	m.RLock()
	defer m.RUnlock()

	for _, h := range m.holders {
		h.removeWithFilter(filter)
	}
}

type lockTableHolder struct {
	sync.RWMutex
	service string
	tables  map[uint64]lockTable
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
		logRemoteBindChanged(m.service, oldBind, newBind)
		return new
	}
	new.close()
	return old
}

func (m *lockTableHolder) iter(fn func(uint64, lockTable) bool) bool {
	m.RLock()
	defer m.RUnlock()
	for id, v := range m.tables {
		if !fn(id, v) {
			return false
		}
	}
	return true
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
