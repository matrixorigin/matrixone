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
	"errors"
	"fmt"
	"hash/crc64"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
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
	"go.uber.org/zap"
)

// WithWait setup wait func to wait some condition ready
func WithWait(wait func(context.Context) error) Option {
	return func(s *service) {
		s.option.wait = wait
	}
}

type service struct {
	cfg                   Config
	serviceID             string
	tableGroups           *lockTableHolders
	activeTxnHolder       activeTxnHolder
	fsp                   *fixedSlicePool
	deadlockDetector      *detector
	events                *waiterEvents
	unknownCommitResolver *unknownCommitResolver
	clock                 clock.Clock
	stopper               *stopper.Stopper
	stopOnce              sync.Once
	closeErr              error
	lifecycle             struct {
		sync.RWMutex
		closing            bool
		ctx                context.Context
		cancel             context.CancelFunc
		operations         sync.WaitGroup
		resolverAdmissions sync.WaitGroup
	}
	// lockWaitCeilingWarned prevents a large explicit timeout from logging on
	// every lock operation. The metric still counts every clamped request.
	lockWaitCeilingWarned atomic.Bool
	bindChangeMu          sync.RWMutex
	fetchWhoWaitingListC  chan who
	logger                *log.MOLogger
	commitSequence        atomic.Uint64
	txnClosureAdmissions  [txnClosureAdmissionShards]txnClosureAdmissionShard

	allocatorVersionMu         sync.Mutex
	lastAllocatorVersion       uint64
	lastAllocatorID            string
	supersededAllocatorIDs     map[string]struct{}
	supersededAllocatorIDOrder []string

	remote struct {
		client Client
		server Server
		keeper LockTableKeeper
	}

	mu struct {
		sync.RWMutex
		restartTime        timestamp.Timestamp
		status             pb.Status
		lockAdmissions     uint64
		preDrainAdmissions uint64
		txnClosures        uint64
		drainSnapshotReady bool
		groupTables        [][]pb.LockTable
		lockTableRef       map[uint32]map[uint64]uint64
		// remoteBindRefs is a source-local index of exact remote binds that a
		// transaction may still depend on. A bind enters before its first remote
		// Lock RPC and leaves only after transaction cleanup succeeds. Route-cache
		// membership alone must not keep an owner-side lock lease alive.
		remoteBindRefs map[remoteBindKey]remoteBindRef
		allocating     map[uint32]map[uint64]chan struct{}
	}

	option struct {
		wait                      func(context.Context) error
		beforeRemoteLockBindCheck func()
		serverOpts                []ServerOption
	}
}

type txnClosureAdmission struct {
	token         chan struct{}
	refs          int
	txnID         []byte
	hash          uint64
	collisionNext *txnClosureAdmission
}

var txnClosureAdmissionPool = sync.Pool{
	New: func() any {
		return &txnClosureAdmission{token: make(chan struct{}, 1)}
	},
}

type txnClosureAdmissionGuard struct {
	service  *service
	single   *txnClosureAdmission
	refs     []*txnClosureAdmission
	acquired int
}

func (g *txnClosureAdmissionGuard) release() {
	if g.service == nil {
		return
	}
	if g.refs == nil {
		if g.acquired == 1 {
			<-g.single.token
		}
		g.service.unrefTxnClosureAdmission(g.single)
	} else {
		for idx := g.acquired - 1; idx >= 0; idx-- {
			<-g.refs[idx].token
		}
		for idx := len(g.refs) - 1; idx >= 0; idx-- {
			g.service.unrefTxnClosureAdmission(g.refs[idx])
		}
	}
	*g = txnClosureAdmissionGuard{}
}

type txnClosureAdmissionShard struct {
	sync.Mutex
	entries map[uint64]*txnClosureAdmission
}

// acquireTxnClosureAdmission serializes async, absent-generation and handoff
// closes before a source transaction mutex is acquired. Handoffs also acquire
// replacement transaction IDs; without that ordering, concurrent A -> B and
// B -> A transfers on different tables can each hold a source mutex while
// waiting for the other's replacement mutex. IDs are acquired in byte order,
// so overlapping closures serialize while unrelated transactions remain fully
// concurrent. A local synchronous close instead keeps its sole transaction
// mutex throughout. The fixed shards guard only short map operations; wait
// duration never occupies a shard-wide token.
const txnClosureAdmissionShards = 64

func txnClosureAdmissionShardIndex(txnID []byte) int {
	return int(txnClosureAdmissionHash(txnID) % txnClosureAdmissionShards)
}

func txnClosureAdmissionHash(txnID []byte) uint64 {
	hash := uint64(14695981039346656037)
	for _, value := range txnID {
		hash ^= uint64(value)
		hash *= 1099511628211
	}
	return hash
}

func (s *service) refTxnClosureAdmission(txnID []byte) *txnClosureAdmission {
	hash := txnClosureAdmissionHash(txnID)
	shard := &s.txnClosureAdmissions[hash%txnClosureAdmissionShards]
	shard.Lock()
	defer shard.Unlock()
	if shard.entries == nil {
		shard.entries = make(map[uint64]*txnClosureAdmission)
	}
	entry := shard.entries[hash]
	for entry != nil && !bytes.Equal(entry.txnID, txnID) {
		entry = entry.collisionNext
	}
	if entry == nil {
		entry = txnClosureAdmissionPool.Get().(*txnClosureAdmission)
		if entry.refs != 0 || len(entry.token) != 0 ||
			len(entry.txnID) != 0 || entry.hash != 0 ||
			entry.collisionNext != nil {
			panic("BUG: dirty transaction closure admission from pool")
		}
		entry.txnID = append(entry.txnID[:0], txnID...)
		entry.hash = hash
		entry.collisionNext = shard.entries[hash]
		shard.entries[hash] = entry
	}
	entry.refs++
	return entry
}

func (s *service) unrefTxnClosureAdmission(
	entry *txnClosureAdmission,
) {
	shard := &s.txnClosureAdmissions[entry.hash%txnClosureAdmissionShards]
	shard.Lock()
	defer shard.Unlock()
	current := shard.entries[entry.hash]
	var previous *txnClosureAdmission
	for current != nil && current != entry {
		previous = current
		current = current.collisionNext
	}
	if current == nil || entry.refs <= 0 {
		panic("BUG: invalid transaction closure admission reference")
	}
	entry.refs--
	if entry.refs == 0 {
		if previous == nil {
			if entry.collisionNext == nil {
				delete(shard.entries, entry.hash)
			} else {
				shard.entries[entry.hash] = entry.collisionNext
			}
		} else {
			previous.collisionNext = entry.collisionNext
		}
		entry.txnID = entry.txnID[:0]
		entry.hash = 0
		entry.collisionNext = nil
		txnClosureAdmissionPool.Put(entry)
	}
}

func (s *service) acquireTxnClosureAdmission(
	ctx context.Context,
	sourceTxnID []byte,
	mutations []pb.ExtraMutation,
) (txnClosureAdmissionGuard, error) {
	if err := ctx.Err(); err != nil {
		return txnClosureAdmissionGuard{}, err
	}
	if len(mutations) == 0 {
		// Ordinary commit/rollback closes are frequent, while cross-transaction
		// handoffs are exceptional. Keep the same cancellable pre-mutex admission
		// contract without building maps, slices or closure objects on that hot
		// path. The pooled entry owns a copy of the ID while any admission waiter
		// still references it, so caller buffer reuse cannot corrupt the map key.
		entry := s.refTxnClosureAdmission(sourceTxnID)
		guard := txnClosureAdmissionGuard{
			service: s,
			single:  entry,
		}
		// The ordinary close path is overwhelmingly uncontended. Avoid the
		// general select machinery unless another close already owns this exact
		// transaction admission; cancellation is checked immediately after the
		// fast acquisition and remains part of the contended wait below.
		select {
		case entry.token <- struct{}{}:
			guard.acquired = 1
			if err := ctx.Err(); err != nil {
				guard.release()
				return txnClosureAdmissionGuard{}, err
			}
			return guard, nil
		default:
		}
		select {
		case entry.token <- struct{}{}:
			guard.acquired = 1
			if err := ctx.Err(); err != nil {
				guard.release()
				return txnClosureAdmissionGuard{}, err
			}
			return guard, nil
		case <-ctx.Done():
			guard.release()
			return txnClosureAdmissionGuard{}, ctx.Err()
		}
	}
	selected := make(map[string]struct{}, len(mutations)+1)
	selected[string(sourceTxnID)] = struct{}{}
	for idx := range mutations {
		if len(mutations[idx].ReplaceTo) > 0 {
			selected[string(mutations[idx].ReplaceTo)] = struct{}{}
		}
	}
	ids := make([]string, 0, len(selected))
	for txnID := range selected {
		ids = append(ids, txnID)
	}
	sort.Strings(ids)
	refs := make([]*txnClosureAdmission, 0, len(ids))
	for _, txnID := range ids {
		refs = append(refs, s.refTxnClosureAdmission([]byte(txnID)))
	}
	guard := txnClosureAdmissionGuard{
		service: s,
		refs:    refs,
	}
	for idx := range refs {
		if err := ctx.Err(); err != nil {
			guard.release()
			return txnClosureAdmissionGuard{}, err
		}
		select {
		case refs[idx].token <- struct{}{}:
			guard.acquired++
			continue
		default:
		}
		select {
		case refs[idx].token <- struct{}{}:
			guard.acquired++
		case <-ctx.Done():
			guard.release()
			return txnClosureAdmissionGuard{}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		guard.release()
		return txnClosureAdmissionGuard{}, err
	}
	return guard, nil
}

// remoteBindKey identifies one exact allocator generation of a remote bind.
// Keep every routing field here: a transaction using an old generation must
// continue refreshing that generation until it is fenced and cleaned up.
type remoteBindKey struct {
	group       uint32
	table       uint64
	originTable uint64
	sharding    pb.Sharding
	serviceID   string
	version     uint64
	allocatorID string
}

type remoteBindRef struct {
	bind pb.LockTable
	refs uint64
}

func makeRemoteBindKey(bind pb.LockTable) remoteBindKey {
	return remoteBindKey{
		group:       bind.Group,
		table:       bind.Table,
		originTable: bind.OriginTable,
		sharding:    bind.Sharding,
		serviceID:   bind.ServiceID,
		version:     bind.Version,
		allocatorID: bind.AllocatorID,
	}
}

const maxSupersededAllocatorIDs = 64

var _ CommitSequenceProvider = (*service)(nil)

// NextCommitSequence returns a non-zero sequence scoped to this lockservice
// incarnation. serviceID also includes the process creation time, so a restart
// gets a distinct source identity at TN.
func (s *service) NextCommitSequence() uint64 {
	sequence := s.commitSequence.Add(1)
	if sequence != 0 {
		return sequence
	}
	return s.commitSequence.Add(1)
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
			stopper.WithLogger(getLogger(cfg.ServiceID).RawLogger())),
		fetchWhoWaitingListC: make(chan who, 10240),
		logger:               getLogger(cfg.ServiceID),
	}
	s.lifecycle.ctx, s.lifecycle.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(s)
	}

	s.tableGroups = &lockTableHolders{service: s.serviceID, logger: s.logger, holders: map[uint32]*lockTableHolder{}}
	s.mu.allocating = make(map[uint32]map[uint64]chan struct{})
	s.mu.lockTableRef = make(map[uint32]map[uint64]uint64)
	s.mu.remoteBindRefs = make(map[remoteBindKey]remoteBindRef)
	s.deadlockDetector = newDeadlockDetector(
		s.logger,
		s.fetchTxnWaitingList,
		s.abortDeadlockTxn,
	)
	s.clock = runtime.ServiceRuntime(cfg.ServiceID).Clock()

	s.initRemote()
	s.unknownCommitResolver = newUnknownCommitResolver(s)
	s.events = newWaiterEvents(eventsWorkers, s.deadlockDetector, s.activeTxnHolder, s.cfg.RemoteLockTimeout.Duration, s.Unlock, s.logger)
	s.events.start()
	for i := 0; i < fetchWhoWaitingListTaskCount; i++ {
		_ = s.stopper.RunTask(s.handleFetchWhoWaitingMe)
	}
	logLockServiceStartSucc(s.logger, s.serviceID)
	return s
}

func (s *service) Lock(
	ctx context.Context,
	tableID uint64,
	rows [][]byte,
	txnID []byte,
	options pb.LockOptions) (pb.Result, error) {
	if err := ctx.Err(); err != nil {
		return pb.Result{}, err
	}
	options = s.applyLockWaitTimeoutCeiling(options)
	if lockWaitDeadlineExpired(options, time.Now()) {
		return pb.Result{}, ErrLockTimeout
	}

	admission, admitted := s.beginLockAdmission(txnID, options, tableID, rows)
	if !admitted {
		return pb.Result{}, moerr.NewNewTxnInCNRollingRestart()
	}
	defer func() { s.endLockAdmission(admission) }()
	ctx, cancelServiceClose := contextWithServiceClose(ctx, admission.serviceCtx)
	defer cancelServiceClose()

	v2.TxnLockTotalCounter.Inc()
	options.Validate(rows)

	start := time.Now()
	defer func() {
		v2.TxnAcquireLockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if err := s.wait(ctx); err != nil {
		return pb.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return pb.Result{}, err
	}
	// Service admission/bind work may consume the remaining budget after the
	// entry check. Recheck before dispatch so a delayed hop cannot restart or
	// transmit an already exhausted absolute deadline.
	if lockWaitDeadlineExpired(options, time.Now()) {
		return pb.Result{}, ErrLockTimeout
	}

	// FIXME(fagongzi): too many mem alloc in trace
	ctx, span := trace.Debug(ctx, "lockservice.lock")
	defer span.End()

	if options.ForwardTo != "" {
		return s.forwardLock(ctx, tableID, rows, txnID, options)
	}

	physicalTableID := tableID
	if options.Sharding == pb.Sharding_ByRow {
		physicalTableID = ShardingByRow(rows[0])
	}
	var err error
	l := s.tableGroups.get(options.Group, physicalTableID)
	if l == nil {
		// Only bind allocation can block before the lock-table wait begins. Avoid
		// creating and canceling a long-lived deadline timer when the table is
		// already published, but keep the exact bounded context for every miss.
		bindCtx, cancel := newLockWaitContext(ctx, options)
		if cancel != nil {
			defer cancel()
		}
		l, err = s.getLockTableWithCreateContext(
			bindCtx,
			options.Group,
			tableID,
			rows,
			options.Sharding)
		if err != nil {
			return pb.Result{}, err
		}
		if err := bindCtx.Err(); err != nil {
			return pb.Result{}, lockWaitContextError(bindCtx, err)
		}
	} else if err := ctx.Err(); err != nil {
		return pb.Result{}, err
	}
	// Binding can finish concurrently with the deadline. Recheck after it
	// returns so an uncontended local table cannot admit an expired request.
	if lockWaitDeadlineExpired(options, time.Now()) {
		return pb.Result{}, ErrLockTimeout
	}
	txn, txnGeneration := s.activeTxnHolder.getActiveTxnWithGeneration(
		txnID, true, "")

	s.bindChangeMu.RLock()
	// All txn lock op must be serial. And avoid dead lock between doAcquireLock
	// and getLock. The doAcquireLock and getLock operations of the same transaction
	// will be concurrent (deadlock detection), which may lead to a deadlock in mutex.
	txn.Lock()
	if txn.generation != txnGeneration || !bytes.Equal(txn.txnID, txnID) {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, ErrTxnNotFound
	}
	if txn.deadlockFound {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, ErrDeadLockDetected
	}
	if txn.bindChanged {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, ErrLockTableBindChanged
	}
	if txn.closing.Load() {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, ErrTxnNotFound
	}
	if err := ctx.Err(); err != nil {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, err
	}

	// it needs to inc table bind ref when set restart cn
	bind := l.getBind()
	current := s.tableGroups.get(bind.Group, bind.Table)
	if current == nil || current.getBind().Changed(bind) {
		txn.Unlock()
		s.bindChangeMu.RUnlock()
		return pb.Result{}, ErrLockTableBindChanged
	}
	s.acquireTxnBindRef(txn, bind, &admission)
	s.bindChangeMu.RUnlock()
	defer txn.Unlock()
	if _, local := l.(*localLockTable); !local {
		// Local synchronous Lock has an existing cancellation contract: Unlock
		// may return while its waiter unwinds (and test hooks can deliberately
		// block that unwind on caller progress). Remote tables and Shared proxies
		// release txn around external work, so their generation must remain alive
		// until that work observes terminal cancellation.
		var finishLockOp func()
		ctx, finishLockOp = txn.beginLockOpLocked(ctx)
		defer finishLockOp()
	}
	originalRows := rows
	originalOptions := options
	rows, options, replaceTxnLocks := txn.coarsenLockRequest(
		bind.Group,
		bind.Table,
		rows,
		options,
		int(s.cfg.MaxLockRowCount),
	)

	var result pb.Result
	l.lock(
		ctx,
		txn,
		rows,
		LockOptions{
			LockOptions:     options,
			replaceTxnLocks: replaceTxnLocks,
			originalRows:    originalRows,
			originalOptions: originalOptions,
		},
		func(r pb.Result, e error) {
			result = r
			err = e
		})
	if terminalErr := txn.terminalLockErrorLocked(txnID); terminalErr != nil {
		result = pb.Result{}
		err = terminalErr
	} else if err == nil {
		if e := s.checkBindChangedBeforeLockSuccess(txn, txnID, bind); e != nil {
			result = pb.Result{}
			err = e
		}
	}
	return result, err
}

// applyLockWaitTimeoutCeiling bounds missing or oversized wait budgets and
// puts the effective absolute deadline in the returned options. Carrying that
// deadline keeps local-to-remote/forward hops on one budget. Lock receives
// options by value, so callers that retry by invoking Lock again must propagate
// their own deadline; this service-side safety net cannot update their copy.
func (s *service) applyLockWaitTimeoutCeiling(options pb.LockOptions) pb.LockOptions {
	ceiling := s.cfg.MaxLockWaitDuration.Duration
	if ceiling <= 0 {
		return options
	}
	// LockWaitTimeout is encoded as whole seconds. Round up so a positive
	// sub-second ceiling or remaining budget never becomes an unbounded zero.
	seconds := int64(ceiling / time.Second)
	if ceiling%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		seconds = 1
	}

	now := time.Now()
	requested := options.LockWaitTimeout
	effectiveSeconds := requested
	if effectiveSeconds <= 0 || effectiveSeconds > seconds {
		effectiveSeconds = seconds
	}
	effectiveDeadline := now.Add(time.Duration(effectiveSeconds) * time.Second)
	if options.LockWaitDeadline > 0 {
		callerDeadline := time.Unix(0, options.LockWaitDeadline)
		if callerDeadline.Before(effectiveDeadline) {
			effectiveDeadline = callerDeadline
			remaining := effectiveDeadline.Sub(now)
			if remaining <= 0 {
				// Keep an exhausted absolute budget exhausted. Consumers use the
				// deadline as the authority and service entry rejects it before a
				// waiter can enter the queue.
				effectiveSeconds = 0
			} else {
				effectiveSeconds = int64(remaining / time.Second)
				if remaining%time.Second != 0 {
					effectiveSeconds++
				}
			}
		}
	}
	options.LockWaitTimeout = effectiveSeconds
	options.LockWaitDeadline = effectiveDeadline.UnixNano()

	if requested > seconds {
		v2.TxnLockWaitTimeoutCeilingClampedCounter.Inc()
		if s.lockWaitCeilingWarned.CompareAndSwap(false, true) && s.logger != nil {
			s.logger.Warn("lock wait timeout exceeds lockservice safety ceiling; request was clamped",
				zap.Int64("requested-seconds", requested),
				zap.Duration("max-lock-wait-duration", ceiling),
				zap.Int64("effective-seconds", effectiveSeconds),
				zap.Time("effective-deadline", effectiveDeadline))
		}
	}
	return options
}

func lockWaitDeadlineExpired(options pb.LockOptions, now time.Time) bool {
	return options.LockWaitDeadline > 0 &&
		!now.Before(time.Unix(0, options.LockWaitDeadline))
}

// newLockWaitContext makes lock-table binding/allocation consume the same
// absolute budget as the subsequent lock wait. Unlike newLockRPCContext, it
// intentionally adds no transport slack: binding is part of the lock budget.
func newLockWaitContext(
	ctx context.Context,
	options pb.LockOptions,
) (context.Context, context.CancelFunc) {
	if options.LockWaitDeadline > 0 {
		return context.WithDeadlineCause(
			ctx,
			time.Unix(0, options.LockWaitDeadline),
			ErrLockTimeout)
	}
	if options.LockWaitTimeout > 0 {
		return context.WithTimeoutCause(
			ctx,
			time.Duration(options.LockWaitTimeout)*time.Second,
			ErrLockTimeout)
	}
	return ctx, nil
}

// lockWaitContextError preserves an earlier caller cancellation/deadline, but
// normalizes expiry of the lock budget to the public MySQL 1205 sentinel.
func lockWaitContextError(ctx context.Context, err error) error {
	if context.Cause(ctx) == ErrLockTimeout {
		return ErrLockTimeout
	}
	return err
}

func (s *service) Unlock(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	serviceCtx, admitted := s.beginTxnClosure()
	if !admitted {
		// Close owns every remaining local and remote lock once service
		// admission is sealed.
		return nil
	}
	defer s.endTxnClosure()

	// Keep ordinary unlock behavior unchanged: it retries remote cleanup until
	// completion even when the caller's request context has ended. Service
	// shutdown is the one terminal cancellation owner.
	unlockCtx := serviceCtx
	if unlockCtx == nil {
		unlockCtx = context.Background()
	}
	start := time.Now()
	defer func() {
		v2.TxnUnlockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if err := s.wait(unlockCtx); err != nil {
		return err
	}
	// Keep every source generation registered until all of its physical tables
	// acknowledge cleanup. Besides making conditional proxy handoff retryable,
	// this closes the ordinary Unlock generation gap: a concurrent/delayed Lock
	// with the same transaction ID observes closing instead of creating a second
	// activeTxn whose holders an old cleanup could remove.
	txn, txnGeneration, closureState := s.lockSynchronousTxnClosure(
		txnID, mutations)
	if closureState == synchronousTxnClosureStale {
		return nil
	}
	synchronousClosure := closureState == synchronousTxnClosureLocked
	if closureState == synchronousTxnClosureFallback {
		releaseClosure, err := s.acquireTxnClosureAdmission(unlockCtx, txnID, mutations)
		if err != nil {
			return err
		}
		defer releaseClosure.release()

		txn, txnGeneration, _ = s.lockActiveTxnGeneration(txnID)
		if txn == nil {
			return nil
		}
	}

	defer txn.Unlock()
	txn.beginClosingLocked(s.logger)
	if !txn.waitAsyncLockOpsLocked(txnID, txnGeneration) {
		return nil
	}

	defer logUnlockTxn(s.logger, txn)()
	binds := txn.lockTableBindsLocked()
	if len(mutations) == 0 {
		s.batchRemoteUnlockTables(unlockCtx, txn, commitTS)
	}
	lockTableFunc := func(bind pb.LockTable) (lockTable, error) {
		return s.getLockTableForTxnUnlock(bind), nil
	}
	var err error
	if synchronousClosure {
		err = txn.closeSynchronousWithoutFreeWithContext(
			unlockCtx, txnID, commitTS, lockTableFunc, s.logger, mutations...)
	} else {
		err = txn.closeWithoutFreeWithContext(
			unlockCtx, txnID, commitTS, lockTableFunc, s.logger, mutations...)
	}
	if err != nil {
		// The source remains registered and fenced. A retry resumes only the
		// tables that did not already acknowledge cleanup.
		return err
	}
	if s.activeTxnHolder.deleteActiveTxn(txnID) != txn {
		return moerr.NewInternalErrorNoCtx(
			"transaction changed while finalizing ordinary closure")
	}
	s.releaseTxnBindRefs(binds)
	s.tryCompleteDrain()
	s.deadlockDetector.txnClosed(txnID)
	// activeTxn pooling uses the still-held transaction mutex as its generation
	// barrier. Old pointers and a new allocation must observe the reset identity
	// only after this caller's deferred Unlock.
	s.activeTxnHolder.freeActiveTxn(txn)
	return nil
}

func (s *service) UnlockWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	return s.unlockWithContext(ctx, txnID, commitTS, mutations...)
}

// unlockRemoteLockTable releases one physical table from an owner-side remote
// transaction. remoteLockTable already sends one Unlock RPC per tracked table;
// keeping the owner operation table-scoped is required for proxy handoff. A
// transaction-level first RPC would otherwise apply one table's replacement
// mutations while silently releasing every other table on the same owner.
func (s *service) unlockRemoteLockTable(
	ctx context.Context,
	bind pb.LockTable,
	txnID []byte,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation,
) error {
	serviceCtx, admitted := s.beginTxnClosure()
	if !admitted {
		return nil
	}
	defer s.endTxnClosure()

	// Match ordinary Unlock durability: once an owner receives cleanup, caller
	// cancellation cannot leave a half-closed remote transaction. Service close
	// remains the terminal cancellation owner.
	unlockCtx := serviceCtx
	if unlockCtx == nil {
		unlockCtx = context.Background()
	}
	if err := s.wait(unlockCtx); err != nil {
		return err
	}
	releaseClosure, err := s.acquireTxnClosureAdmission(unlockCtx, txnID, mutations)
	if err != nil {
		return err
	}
	defer releaseClosure.release()

	txn, txnGeneration, _ := s.lockActiveTxnGeneration(txnID)
	if txn == nil {
		return nil
	}
	defer txn.Unlock()

	holder := txn.lockHolders[bind.Group]
	if holder == nil {
		return nil
	}
	recordedBind, touched := holder.tableBinds[bind.Table]
	if !touched {
		recordedBind, touched = holder.tableBindIntents[bind.Table]
	}
	if !touched {
		return nil
	}
	if !recordedBind.Equal(bind) {
		return moerr.NewInternalErrorNoCtx(
			"remote table unlock does not match transaction lock-table generation")
	}

	txn.beginClosingLocked(s.logger)
	if !txn.waitAsyncLockOpsLocked(txnID, txnGeneration) {
		return nil
	}
	holder = txn.lockHolders[bind.Group]
	if holder != nil {
		if locks := holder.tableKeys[bind.Table]; locks != nil {
			recordedBind, ok := holder.tableBinds[bind.Table]
			if !ok || !recordedBind.Equal(bind) {
				return moerr.NewInternalErrorNoCtx(
					"remote table unlock does not match transaction lock-table generation")
			}

			current := s.tableGroups.get(bind.Group, bind.Table)
			if current != nil && recordedBind.Equal(current.getBind()) {
				local, ok := current.(*localLockTable)
				if !ok {
					return moerr.NewInternalErrorNoCtx(
						"remote transaction generation resolves to a non-local lock table")
				}
				if err := local.unlockWithContext(
					unlockCtx,
					txn,
					locks,
					commitTS,
					mutations...,
				); err != nil {
					return err
				}
				s.adoptRemoteHandoffLockTableRefs(bind, mutations)
			}
			// A missing/rebound current table has already closed the old physical
			// generation. Forget only this transaction's matching ledger entry; never
			// apply its unlock to the replacement table.
			txn.removeClosedLockTable(bind.Group, bind.Table, locks)
		}
	}
	return s.finalizeRemoteTxnIfClosedLocked(txnID, txn)
}

// unlockRemoteLockTables closes a bounded set of ordinary tables with one
// owner-side admission and one transaction-generation lock. Proxy handoff
// mutations deliberately remain on unlockRemoteLockTable because each table
// has its own conditional ownership transition.
func (s *service) unlockRemoteLockTables(
	ctx context.Context,
	binds []pb.LockTable,
	txnID []byte,
	commitTS timestamp.Timestamp,
) error {
	if len(binds) < 2 || len(binds) > maxRemoteUnlockBatchSize {
		return moerr.NewInternalErrorNoCtx("invalid remote unlock batch size")
	}

	type tableKey struct {
		group uint32
		table uint64
	}
	seen := make(map[tableKey]struct{}, len(binds))
	for _, bind := range binds {
		if bind.ServiceID != s.serviceID {
			return moerr.NewInternalErrorNoCtx(
				"remote unlock batch contains a table owned by another service")
		}
		key := tableKey{group: bind.Group, table: bind.Table}
		if _, ok := seen[key]; ok {
			return moerr.NewInternalErrorNoCtx(
				"remote unlock batch contains a duplicate table")
		}
		seen[key] = struct{}{}
	}

	serviceCtx, admitted := s.beginTxnClosure()
	if !admitted {
		return nil
	}
	defer s.endTxnClosure()

	// Once the owner admits cleanup it is durable independently of the RPC
	// caller. Service shutdown is the only cancellation owner, matching the
	// table-scoped remote Unlock contract.
	unlockCtx := serviceCtx
	if unlockCtx == nil {
		unlockCtx = context.Background()
	}
	if err := s.wait(unlockCtx); err != nil {
		return err
	}
	releaseClosure, err := s.acquireTxnClosureAdmission(unlockCtx, txnID, nil)
	if err != nil {
		return err
	}
	defer releaseClosure.release()

	txn, txnGeneration, _ := s.lockActiveTxnGeneration(txnID)
	if txn == nil {
		return nil
	}
	defer txn.Unlock()

	validate := func() error {
		for _, bind := range binds {
			holder := txn.lockHolders[bind.Group]
			if holder == nil {
				continue
			}
			recorded, touched := holder.tableBinds[bind.Table]
			if !touched {
				recorded, touched = holder.tableBindIntents[bind.Table]
			}
			if touched && !recorded.Equal(bind) {
				return moerr.NewInternalErrorNoCtx(
					"remote batch unlock does not match transaction lock-table generation")
			}
		}
		return nil
	}
	// Reject a stale batch before sealing an otherwise live transaction.
	if err := validate(); err != nil {
		return err
	}
	txn.beginClosingLocked(s.logger)
	if !txn.waitAsyncLockOpsLocked(txnID, txnGeneration) {
		return nil
	}
	// In-flight Lock callbacks can publish ledgers while the drain yields the
	// transaction mutex, so the whole batch must be revalidated before its first
	// irreversible table release.
	if err := validate(); err != nil {
		return err
	}

	type releasePlan struct {
		bind  pb.LockTable
		locks *cowSlice
		local *localLockTable
	}
	plans := make([]releasePlan, 0, len(binds))
	for _, bind := range binds {
		holder := txn.lockHolders[bind.Group]
		if holder == nil {
			continue
		}
		locks := holder.tableKeys[bind.Table]
		if locks == nil {
			continue
		}
		recorded, ok := holder.tableBinds[bind.Table]
		if !ok || !recorded.Equal(bind) {
			return moerr.NewInternalErrorNoCtx(
				"remote batch unlock does not match transaction lock-table generation")
		}
		current := s.tableGroups.get(bind.Group, bind.Table)
		if current != nil && recorded.Equal(current.getBind()) {
			local, ok := current.(*localLockTable)
			if !ok {
				return moerr.NewInternalErrorNoCtx(
					"remote transaction generation resolves to a non-local lock table")
			}
			plans = append(plans, releasePlan{bind: bind, locks: locks, local: local})
			continue
		}
		plans = append(plans, releasePlan{bind: bind, locks: locks})
	}

	// Resolve the complete batch before its first irreversible release. This
	// keeps a corrupt or stale later table from partially unlocking an otherwise
	// valid prefix.
	for _, plan := range plans {
		if plan.local != nil {
			if err := plan.local.unlockWithContext(
				unlockCtx,
				txn,
				plan.locks,
				commitTS,
			); err != nil {
				return err
			}
		}
		// A missing/rebound table already closed the old physical generation.
		txn.removeClosedLockTable(plan.bind.Group, plan.bind.Table, plan.locks)
	}
	return s.finalizeRemoteTxnIfClosedLocked(txnID, txn)
}

func (s *service) finalizeRemoteTxnIfClosedLocked(
	txnID []byte,
	txn *activeTxn,
) error {
	if txn.hasHeldLockTablesLocked() {
		return nil
	}

	// Intents deliberately survive partial table release because they own the
	// service-drain references. The final table closes the transaction and drops
	// all of those references exactly once.
	binds := txn.lockTableBindsLocked()
	if s.activeTxnHolder.deleteActiveTxn(txnID) != txn {
		return moerr.NewInternalErrorNoCtx(
			"remote transaction changed while its final lock table was closing")
	}
	s.releaseTxnBindRefs(binds)
	s.tryCompleteDrain()
	s.deadlockDetector.txnClosed(txnID)
	s.activeTxnHolder.freeActiveTxn(txn)
	return nil
}

type remoteUnlockBatchEntry struct {
	group  uint32
	table  uint64
	bind   pb.LockTable
	locks  *cowSlice
	remote *remoteLockTable
}

// batchRemoteUnlockTables opportunistically removes negotiated ordinary
// remote tables from the origin ledger. Any protocol, transport or owner error
// leaves the affected entries attached so the existing table-scoped path can
// retry them without changing correctness semantics.
func (s *service) batchRemoteUnlockTables(
	ctx context.Context,
	txn *activeTxn,
	commitTS timestamp.Timestamp,
) {
	byOwner := make(map[string][]remoteUnlockBatchEntry)
	for group, holder := range txn.lockHolders {
		for table, locks := range holder.tableKeys {
			if !txn.isBatchUnlockSupportedLocked(group, table) {
				continue
			}
			bind, ok := holder.tableBinds[table]
			if !ok || bind.ServiceID == s.serviceID {
				continue
			}
			lockTable := s.getLockTableForTxnUnlock(bind)
			remote, ok := lockTable.(*remoteLockTable)
			if !ok {
				// Proxy tables may create per-table conditional mutations and must
				// retain their existing table-scoped transition.
				continue
			}
			byOwner[bind.ServiceID] = append(byOwner[bind.ServiceID], remoteUnlockBatchEntry{
				group:  group,
				table:  table,
				bind:   bind,
				locks:  locks,
				remote: remote,
			})
		}
	}

	owners := make([]string, 0, len(byOwner))
	for owner := range byOwner {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	for _, owner := range owners {
		entries := byOwner[owner]
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].group != entries[j].group {
				return entries[i].group < entries[j].group
			}
			return entries[i].table < entries[j].table
		})
		for len(entries) >= 2 {
			n := min(len(entries), maxRemoteUnlockBatchSize)
			binds := make([]pb.LockTable, n)
			for idx := range n {
				binds[idx] = entries[idx].bind
			}
			if err := entries[0].remote.doBatchUnlock(
				ctx,
				txn,
				binds,
				commitTS,
			); err != nil {
				break
			}
			for idx := range n {
				entry := entries[idx]
				txn.removeClosedLockTable(entry.group, entry.table, entry.locks)
			}
			entries = entries[n:]
		}
	}
}

// adoptRemoteHandoffLockTableRefs transfers service-drain ownership after a
// local owner atomically moves physical holders and transaction ledgers. A
// Shared proxy follower may not have existed at the physical owner before the
// handoff, so prepareLockUpdate creates its ledger without passing through the
// ordinary Lock admission that records tableBindIntents and increments the
// service reference. Source cleanup would then drop the last reference while
// the replacement still holds the table, allowing rolling restart to publish
// that table as movable.
//
// The caller still owns closure-admission tokens for the source and every
// ReplaceTo transaction. The source reference remains live until this method
// returns, so recording the replacement intent before incrementing its
// reference cannot create a zero-reference publication window.
func (s *service) adoptRemoteHandoffLockTableRefs(
	bind pb.LockTable,
	mutations []pb.ExtraMutation,
) {
	var firstReplacementID []byte
	var seen map[string]struct{}
	for idx := range mutations {
		replacementID := mutations[idx].ReplaceTo
		if mutations[idx].Skip || len(replacementID) == 0 {
			continue
		}
		if firstReplacementID == nil {
			firstReplacementID = replacementID
		} else {
			if seen == nil {
				if bytes.Equal(firstReplacementID, replacementID) {
					continue
				}
				seen = make(map[string]struct{}, len(mutations))
				seen[string(firstReplacementID)] = struct{}{}
			}
			replacementKey := string(replacementID)
			if _, ok := seen[replacementKey]; ok {
				continue
			}
			seen[replacementKey] = struct{}{}
		}

		replacement, replacementGeneration := s.activeTxnHolder.getActiveTxnWithGeneration(
			replacementID,
			false,
			"",
		)
		if replacement == nil {
			continue
		}

		adopted := false
		replacement.Lock()
		if replacement.generation != replacementGeneration ||
			!bytes.Equal(replacement.txnID, replacementID) {
			replacement.Unlock()
			continue
		}
		holder := replacement.lockHolders[bind.Group]
		if holder != nil {
			recordedBind, holdsTable := holder.tableBinds[bind.Table]
			_, ownsRef := holder.tableBindIntents[bind.Table]
			if holdsTable && recordedBind.Equal(bind) && !ownsRef {
				holder.tableBindIntents[bind.Table] = recordedBind
				adopted = true
			}
		}
		replacement.Unlock()

		if adopted {
			s.incRef(bind.Group, bind.Table)
		}
	}
}

// unlockUnknownCommit is used only after Commit returned an unknown outcome.
// The allocator fence has already made a later Commit impossible, so shutdown
// may cancel a remote cleanup and leave orphan recovery to release its lock.
func (s *service) unlockUnknownCommit(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	serviceCtx, admitted := s.beginTxnClosure()
	if !admitted {
		return nil
	}
	defer s.endTxnClosure()
	ctx, cancelServiceClose := contextWithServiceClose(ctx, serviceCtx)
	defer cancelServiceClose()

	start := time.Now()
	defer func() {
		v2.TxnUnlockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.wait(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	releaseClosure, err := s.acquireTxnClosureAdmission(ctx, txnID, mutations)
	if err != nil {
		return err
	}
	defer releaseClosure.release()

	// Keep the source transaction registered until every owner-side unlock has
	// acknowledged. In particular, a local proxy must not publish a replacement
	// holder before its ReplaceTo mutation reaches the remote owner. Retaining
	// the active transaction on a bounded-attempt failure makes orphan cleanup
	// fail closed; the resolver retries it later.
	txn, txnGeneration, _ := s.lockActiveTxnGeneration(txnID)
	if txn == nil {
		return nil
	}
	defer txn.Unlock()
	txn.beginClosingLocked(s.logger)
	if !txn.waitAsyncLockOpsLocked(txnID, txnGeneration) {
		return nil
	}

	defer logUnlockTxn(s.logger, txn)()
	binds := txn.lockTableBindsLocked()
	if len(mutations) == 0 {
		s.batchRemoteUnlockTables(ctx, txn, commitTS)
	}
	if err := txn.closeWithoutFreeWithContext(
		ctx,
		txnID,
		commitTS,
		func(bind pb.LockTable) (lockTable, error) {
			return s.getLockTableForTxnUnlock(bind), nil
		},
		s.logger,
		mutations...,
	); err != nil {
		return err
	}

	if s.activeTxnHolder.deleteActiveTxn(txnID) != txn {
		return moerr.NewInternalErrorNoCtx(
			"unknown-commit transaction changed while finalizing closure")
	}
	s.releaseTxnBindRefs(binds)
	s.tryCompleteDrain()
	s.deadlockDetector.txnClosed(txnID)
	s.activeTxnHolder.freeActiveTxn(txn)
	return nil
}

func (s *service) unlockWithContext(
	ctx context.Context,
	txnID []byte,
	commitTS timestamp.Timestamp,
	mutations ...pb.ExtraMutation) error {
	serviceCtx, admitted := s.beginTxnClosure()
	if !admitted {
		return nil
	}
	defer s.endTxnClosure()
	ctx, cancelServiceClose := contextWithServiceClose(ctx, serviceCtx)
	defer cancelServiceClose()

	start := time.Now()
	defer func() {
		v2.TxnUnlockDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.wait(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// This cleanup is cancellation-aware and therefore retryable. Keep the
	// transaction registered until every table acknowledges release, and fence
	// late Lock calls with closing instead of opening a same-ID generation gap.
	txn, txnGeneration, closureState := s.lockSynchronousTxnClosure(
		txnID, mutations)
	if closureState == synchronousTxnClosureStale {
		return nil
	}
	if closureState == synchronousTxnClosureFallback {
		releaseClosure, err := s.acquireTxnClosureAdmission(ctx, txnID, mutations)
		if err != nil {
			return err
		}
		defer releaseClosure.release()

		txn, txnGeneration, _ = s.lockActiveTxnGeneration(txnID)
		if txn == nil {
			return nil
		}
	}
	defer txn.Unlock()
	txn.beginClosingLocked(s.logger)
	if !txn.waitAsyncLockOpsLocked(txnID, txnGeneration) {
		return nil
	}

	defer logUnlockTxn(s.logger, txn)()
	binds := txn.lockTableBindsLocked()
	if len(mutations) == 0 {
		s.batchRemoteUnlockTables(ctx, txn, commitTS)
	}
	err := txn.closeWithoutFreeWithContext(ctx, txnID, commitTS, func(bind pb.LockTable) (lockTable, error) {
		return s.getLockTableForTxnUnlock(bind), nil
	}, s.logger, mutations...)
	if err != nil {
		return err
	}
	if s.activeTxnHolder.deleteActiveTxn(txnID) != txn {
		return moerr.NewInternalErrorNoCtx(
			"retryable transaction changed while finalizing closure")
	}
	s.releaseTxnBindRefs(binds)
	// The deadlock detector will hold the deadlocked transaction that is aborted
	// to avoid the situation where the deadlock detection is interfered with by
	// the abort transaction. When a transaction is unlocked, the deadlock detector
	// needs to be notified to release memory.
	s.deadlockDetector.txnClosed(txnID)
	s.activeTxnHolder.freeActiveTxn(txn)
	return nil
}

func (s *service) IsOrphanTxn(
	ctx context.Context,
	txn []byte,
) (bool, error) {
	req := acquireRequest()
	req.Method = pb.Method_CheckOrphan
	req.CheckOrphan.ServiceID = s.serviceID
	req.CheckOrphan.Txn = txn

	resp, err := s.remote.client.Send(ctx, req)
	if err != nil {
		return false, err
	}
	defer releaseResponse(resp)

	return resp.CheckOrphan.Orphan, nil
}

func (s *service) Resume() error {
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		defaultRPCTimeout,
		moerr.NewInfoNoCtx("lockservice.resume"),
	)
	defer cancel()

	req := acquireRequest()
	req.Method = pb.Method_ResumeInvalidCN
	req.ResumeInvalidCN.ServiceID = s.serviceID

	resp, err := s.remote.client.Send(ctx, req)
	if err != nil {
		return err
	}
	defer releaseResponse(resp)

	return err
}

func (s *service) releaseTxnBindRefs(binds []pb.LockTable) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, bind := range binds {
		if bind.ServiceID != s.serviceID {
			s.releaseRemoteBindRefLocked(bind)
			continue
		}
		s.releaseBindRefLocked(bind.Group, bind.Table, bind, s.mu.drainSnapshotReady)
	}
}

// acquireTxnBindRef records the first exact bind generation touched by a
// transaction. The caller holds txn's mutex. Local binds participate in CN
// drain accounting; remote binds participate in owner-side lease heartbeats.
func (s *service) acquireTxnBindRef(
	txn *activeTxn,
	bind pb.LockTable,
	admission *lockAdmission,
) {
	if !txn.lockTableBindTouched(bind) {
		return
	}
	if bind.ServiceID != s.serviceID {
		s.acquireRemoteBindRef(bind)
		return
	}
	if !admission.consume(bind) {
		s.incRef(bind.Group, bind.Table)
	}
}

func (s *service) acquireRemoteBindRef(bind pb.LockTable) {
	if bind.ServiceID == s.serviceID {
		return
	}
	key := makeRemoteBindKey(bind)
	s.mu.Lock()
	if s.mu.remoteBindRefs == nil {
		s.mu.remoteBindRefs = make(map[remoteBindKey]remoteBindRef)
	}
	s.acquireRemoteBindRefLocked(key, bind)
	s.mu.Unlock()
}

func (s *service) acquireRemoteBindRefLocked(key remoteBindKey, bind pb.LockTable) {
	ref := s.mu.remoteBindRefs[key]
	if ref.refs == 0 {
		ref.bind = bind
	}
	ref.refs++
	s.mu.remoteBindRefs[key] = ref
}

func (s *service) releaseRemoteBindRefLocked(bind pb.LockTable) {
	key := makeRemoteBindKey(bind)
	ref, ok := s.mu.remoteBindRefs[key]
	if !ok {
		return
	}
	if ref.refs > 1 {
		ref.refs--
		s.mu.remoteBindRefs[key] = ref
		return
	}
	delete(s.mu.remoteBindRefs, key)
}

func (s *service) collectRemoteLockBinds(scratch []pb.LockTable) []pb.LockTable {
	oldLen := len(scratch)
	binds := scratch[:0]
	s.mu.RLock()
	for _, ref := range s.mu.remoteBindRefs {
		binds = append(binds, ref.bind)
	}
	s.mu.RUnlock()
	if len(binds) < oldLen {
		clear(scratch[len(binds):oldLen])
	}
	return binds
}

func (s *service) releaseBindRefLocked(
	group uint32,
	table uint64,
	bind pb.LockTable,
	addMovable bool,
) {
	if _, ok := s.mu.lockTableRef[group][table]; !ok {
		return
	}
	s.mu.lockTableRef[group][table]--
	if s.mu.lockTableRef[group][table] != 0 {
		return
	}
	delete(s.mu.lockTableRef[group], table)
	if addMovable {
		s.mu.groupTables = append(s.mu.groupTables, []pb.LockTable{bind})
	}
}

func (s *service) checkCanMoveGroupTables() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.status != pb.Status_ServiceLockEnable {
		return
	}

	oldStatus := s.mu.status
	s.mu.restartTime, _ = s.clock.Now()
	s.mu.status = pb.Status_ServiceLockWaiting
	s.mu.preDrainAdmissions = s.mu.lockAdmissions
	s.mu.drainSnapshotReady = false
	logStatusChange(s.logger, oldStatus, s.mu.status)
	s.prepareDrainSnapshotLocked()
}

func (s *service) prepareDrainSnapshotLocked() {
	if s.mu.status != pb.Status_ServiceLockWaiting ||
		s.mu.drainSnapshotReady ||
		s.mu.preDrainAdmissions != 0 {
		return
	}

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
	s.mu.drainSnapshotReady = true
}

func (s *service) incRef(group uint32, table uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.mu.lockTableRef[group]; !ok {
		s.mu.lockTableRef[group] = make(map[uint64]uint64)
	}
	s.mu.lockTableRef[group][table]++
}

func (s *service) canLockOnServiceStatusLocked(
	txnID []byte,
	opts pb.LockOptions,
	tableID uint64,
	rows [][]byte,
) bool {
	if s.mu.status == pb.Status_ServiceLockEnable {
		return true
	}
	if opts.Sharding == pb.Sharding_ByRow {
		tableID = ShardingByRow(rows[0])
	}
	if _, ok := s.mu.lockTableRef[opts.Group][tableID]; !ok {
		logCanLockOnService(s.logger, s.serviceID)
		return false
	}
	if s.activeTxnHolder.hasActiveTxn(txnID) {
		return true
	}
	if s.activeTxnHolder.empty() {
		return false
	}
	if opts.SnapShotTs.LessEq(s.mu.restartTime) {
		return true
	}
	return false
}

type lockAdmission struct {
	preDrain     bool
	reservedBind pb.LockTable
	reserved     bool
	serviceCtx   context.Context
}

func (a *lockAdmission) consume(bind pb.LockTable) bool {
	if !a.reserved ||
		a.reservedBind.Group != bind.Group ||
		a.reservedBind.Table != bind.Table {
		return false
	}
	a.reserved = false
	return true
}

func (s *service) beginLockAdmission(
	txnID []byte,
	opts pb.LockOptions,
	tableID uint64,
	rows [][]byte,
) (lockAdmission, bool) {
	serviceCtx, admitted := s.beginServiceOperation()
	if !admitted {
		return lockAdmission{}, false
	}

	s.mu.Lock()
	if !s.canLockOnServiceStatusLocked(txnID, opts, tableID, rows) {
		s.mu.Unlock()
		s.endServiceOperation()
		return lockAdmission{}, false
	}
	admission := lockAdmission{
		preDrain:   s.mu.status == pb.Status_ServiceLockEnable,
		serviceCtx: serviceCtx,
	}
	if !admission.preDrain {
		if opts.Sharding == pb.Sharding_ByRow {
			tableID = ShardingByRow(rows[0])
		}
		l := s.tableGroups.get(opts.Group, tableID)
		if l == nil {
			s.mu.Unlock()
			s.endServiceOperation()
			return lockAdmission{}, false
		}
		admission.reservedBind = l.getBind()
		admission.reserved = true
		s.mu.lockTableRef[opts.Group][tableID]++
	}
	s.mu.lockAdmissions++
	s.mu.Unlock()
	return admission, true
}

func (s *service) endLockAdmission(admission lockAdmission) {
	s.mu.Lock()
	if s.mu.lockAdmissions == 0 {
		s.mu.Unlock()
		panic("lock admission underflow")
	}
	s.mu.lockAdmissions--
	if admission.preDrain && s.mu.status == pb.Status_ServiceLockWaiting {
		if s.mu.preDrainAdmissions == 0 {
			panic("pre-drain lock admission underflow")
		}
		s.mu.preDrainAdmissions--
	}
	if admission.reserved {
		bind := admission.reservedBind
		s.releaseBindRefLocked(
			bind.Group,
			bind.Table,
			bind,
			s.mu.drainSnapshotReady,
		)
	}
	s.prepareDrainSnapshotLocked()
	s.tryCompleteDrainLocked()
	s.mu.Unlock()
	s.endServiceOperation()
}

func (s *service) tryCompleteDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepareDrainSnapshotLocked()
	s.tryCompleteDrainLocked()
}

func (s *service) tryCompleteDrainLocked() {
	if s.mu.status != pb.Status_ServiceLockWaiting ||
		!s.mu.drainSnapshotReady ||
		s.mu.lockAdmissions != 0 ||
		s.mu.txnClosures != 0 ||
		s.hasLockTableRefsLocked() ||
		!s.activeTxnHolder.empty() {
		return
	}
	logStatusChange(s.logger, s.mu.status, pb.Status_ServiceUnLockSucc)
	s.mu.status = pb.Status_ServiceUnLockSucc
}

func (s *service) hasLockTableRefsLocked() bool {
	for _, refs := range s.mu.lockTableRef {
		if len(refs) != 0 {
			return true
		}
	}
	return false
}

func (s *service) beginTxnClosure() (context.Context, bool) {
	serviceCtx, admitted := s.beginServiceOperation()
	if !admitted {
		return nil, false
	}
	s.mu.Lock()
	s.mu.txnClosures++
	s.mu.Unlock()
	return serviceCtx, true
}

func (s *service) endTxnClosure() {
	s.mu.Lock()
	if s.mu.txnClosures == 0 {
		s.mu.Unlock()
		panic("transaction closure underflow")
	}
	s.mu.txnClosures--
	s.tryCompleteDrainLocked()
	s.mu.Unlock()
	s.endServiceOperation()
}

func (s *service) beginServiceOperation() (context.Context, bool) {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.lifecycle.closing {
		return nil, false
	}
	s.lifecycle.operations.Add(1)
	return s.lifecycle.ctx, true
}

func (s *service) endServiceOperation() {
	s.lifecycle.operations.Done()
}

func (s *service) beginResolverAdmission() bool {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.lifecycle.closing {
		return false
	}
	s.lifecycle.resolverAdmissions.Add(1)
	return true
}

func (s *service) endResolverAdmission() {
	s.lifecycle.resolverAdmissions.Done()
}

func contextWithServiceClose(
	parent context.Context,
	serviceCtx context.Context,
) (context.Context, context.CancelFunc) {
	if serviceCtx == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(serviceCtx, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (s *service) validGroupTable(group uint32, tableID uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.mu.lockTableRef[group][tableID]
	return ok
}

func (s *service) GetServiceID() string {
	return s.serviceID
}

func (s *service) GetConfig() Config {
	return s.cfg
}

func (s *service) Close() error {
	s.stopOnce.Do(func() {
		// Seal every public mutation and bind-publication path first. Add and
		// Wait are serialized by lifecycle: once closing is visible, no new
		// operation or resolver admission can join either wait group.
		s.lifecycle.Lock()
		s.lifecycle.closing = true
		if s.lifecycle.cancel != nil {
			s.lifecycle.cancel()
		}
		s.lifecycle.Unlock()
		if s.unknownCommitResolver != nil {
			s.unknownCommitResolver.callbacks.seal()
		}

		// Stop producers before their consumers and dependencies. Inbound RPC
		// handlers, service background tasks, and keeper tasks can all use lock
		// tables, waiter state, the detector, and the RPC client.
		serverErr := s.remote.server.Close()
		s.lifecycle.resolverAdmissions.Wait()
		s.stopper.Stop()
		keeperErr := s.remote.keeper.Close()
		releaseQueuedWhoWaitingList(s.fetchWhoWaitingListC)
		s.tableGroups.removeWithFilter(func(_ uint64, _ lockTable) bool { return true }, closeReasonServiceClose)
		// Closing tables wakes admitted local lock waiters. Service cancellation
		// bounds remote lock/unlock and bind waits. Join them while detector,
		// event workers, txn ownership, and the RPC client are still alive.
		s.lifecycle.operations.Wait()
		// Deadlock checks can abort a txn and notify one of its async waiters.
		// Seal that producer before waiterEvents closes its admission channel;
		// the event workers remain alive while detector.Close joins in-flight
		// checks, then drain every notification accepted before the seal.
		s.deadlockDetector.close()
		s.events.close()
		s.activeTxnHolder.close()
		if s.unknownCommitResolver != nil {
			// The resolver task is joined and callback admission is sealed. Drain
			// every remaining reservation by transferring invocation out of service
			// ownership before releasing the RPC transport. External callback bodies
			// are non-blocking by contract and are never a Close wait dependency.
			for _, txn := range s.unknownCommitResolver.takeResolvedTxns() {
				txn.complete()
			}
		}
		clientErr := s.remote.client.Close()
		close(s.fetchWhoWaitingListC)
		s.closeErr = errors.Join(serverErr, keeperErr, clientErr)
	})
	return s.closeErr
}

func releaseQueuedWhoWaitingList(values chan who) {
	for {
		select {
		case value, ok := <-values:
			if !ok {
				return
			}
			if value.cancel != nil {
				value.cancel()
			}
			if value.resp != nil {
				releaseResponse(value.resp)
			}
		default:
			return
		}
	}
}

func (s *service) setStatus(status pb.Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logStatusChange(s.logger, s.mu.status, status)
	s.mu.status = status
}

func (s *service) getStatus() pb.Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.status
}

func (s *service) topGroupTables() []pb.LockTable {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.groupTables) == 0 {
		return nil
	}
	g := s.mu.groupTables[0]
	return g
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

func (s *service) fetchTxnWaitingList(ctx context.Context, txn pb.WaitTxn, waiters *waiters) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
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
			ctx,
			s.serviceID,
			txnID,
			waiters.add,
			func(ctx context.Context, group uint32, table uint64) (lockTable, error) {
				return s.getLockTable(ctx, group, table)
			})
	}

	waitingList, err := s.getTxnWaitingListOnRemote(ctx, txn.TxnID, txn.CreatedOn)
	if err != nil {
		return false, err
	}
	for _, v := range waitingList {
		if !waiters.add(v, v.WaiterAddress) {
			return false, nil
		}
	}
	return true, nil
}

func (s *service) abortDeadlockTxn(wait pb.WaitTxn, err error) {
	if wait.WaiterAddress != "" && wait.WaiterAddress != s.serviceID {
		ok, err := s.abortRemoteDeadlockTxn(wait)
		if err != nil || !ok {
			logAbortRemoteDeadlockFailed(s.logger, wait, err)
		}
		return
	}

	activeTxn := s.activeTxnHolder.getActiveTxn(wait.TxnID, false, "")
	// the active txn closed
	if activeTxn == nil {
		return
	}
	activeTxn.abort(wait, err, s.logger)
}

func (s *service) getLockTable(
	ctx context.Context,
	group uint32,
	tableID uint64) (lockTable, error) {
	return s.getLockTableWithContext(ctx, group, tableID)
}

func (s *service) getLockTableWithContext(
	ctx context.Context,
	group uint32,
	tableID uint64) (lockTable, error) {
	if v := s.tableGroups.get(group, tableID); v != nil {
		return v, nil
	}
	return s.waitLockTableBindWithContext(
		ctx,
		group,
		tableID,
		false)
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

func (s *service) waitLockTableBindWithContext(
	ctx context.Context,
	group uint32,
	tableID uint64,
	locked bool) (lockTable, error) {
	c := s.getAllocatingC(group, tableID, locked)
	if c != nil {
		select {
		case <-c:
		case <-ctx.Done():
			return nil, lockWaitContextError(ctx, ctx.Err())
		}
	}
	return s.tableGroups.get(group, tableID), nil
}

func (s *service) getLockTableWithCreate(
	ctx context.Context,
	group uint32,
	tableID uint64,
	rows [][]byte,
	sharding pb.Sharding) (lockTable, error) {
	return s.getLockTableWithCreateContext(
		ctx,
		group,
		tableID,
		rows,
		sharding)
}

func (s *service) getLockTableWithCreateContext(
	ctx context.Context,
	group uint32,
	tableID uint64,
	rows [][]byte,
	sharding pb.Sharding) (lockTable, error) {
	originTableID := tableID
	if sharding == pb.Sharding_ByRow {
		tableID = ShardingByRow(rows[0])
	}

	if v := s.tableGroups.get(group, tableID); v != nil {
		if err := ctx.Err(); err != nil {
			return nil, lockWaitContextError(ctx, err)
		}
		return v, nil
	}

	var c chan struct{}
	fn := func() (lockTable, error) {
		s.mu.Lock()
		waitC := s.getAllocatingC(group, tableID, true)
		if waitC != nil {
			s.mu.Unlock()
			select {
			case <-waitC:
			case <-ctx.Done():
				return nil, lockWaitContextError(ctx, ctx.Err())
			}
			s.mu.Lock()
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return nil, lockWaitContextError(ctx, err)
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
		return v, nil
	}

	v, err := fn()
	if err != nil {
		return nil, err
	}
	if v != nil {
		return v, nil
	}

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.mu.allocating[group], tableID)
		close(c)
	}()

	requestAllocator := s.allocatorStateSnapshot()
	bind, allocator, err := getLockTableBindWithContext(
		ctx,
		s.remote.client,
		group,
		tableID,
		originTableID,
		s.serviceID,
		sharding)
	if err != nil {
		return nil, lockWaitContextError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, lockWaitContextError(ctx, err)
	}

	return s.publishLockTableBindFromAllocator(
		ctx,
		"get-bind",
		group,
		tableID,
		bind,
		allocator,
		requestAllocator)
}

func (s *service) publishLockTableBindFromAllocator(
	ctx context.Context,
	source string,
	group uint32,
	tableID uint64,
	bind pb.LockTable,
	allocator allocatorState,
	requestAllocator allocatorState,
) (lockTable, error) {
	if !s.beginLockTablePublication() {
		return nil, ErrLockTableBindChanged
	}
	defer s.lifecycle.RUnlock()

	s.allocatorVersionMu.Lock()
	defer s.allocatorVersionMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, lockWaitContextError(ctx, err)
	}

	// Allocator-state observation and bind publication form one non-cancellable
	// state transition. Once it starts, finish it and return its actual result.
	if _, accepted := s.observeAllocatorStateLocked(
		source,
		allocator,
		requestAllocator,
		true,
		s.tableGroups); !accepted {
		return nil, ErrLockTableBindChanged
	}

	s.bindChangeMu.Lock()
	defer s.bindChangeMu.Unlock()

	current := s.tableGroups.get(group, tableID)
	if current != nil {
		if current.getBind().Changed(bind) {
			return nil, ErrLockTableBindChanged
		}
		return current, nil
	}
	return s.tableGroups.set(group, tableID, s.createLockTableByBind(bind)), nil
}

func (s *service) handleBindChanged(newBind pb.LockTable) {
	if !s.beginLockTablePublication() {
		return
	}
	defer s.lifecycle.RUnlock()

	s.bindChangeMu.Lock()
	defer s.bindChangeMu.Unlock()

	current := s.tableGroups.get(newBind.Group, newBind.Table)
	if current != nil && !current.getBind().Changed(newBind) {
		return
	}

	new := s.createLockTableByBind(newBind)
	s.tableGroups.set(newBind.Group, newBind.Table, new)
	s.fenceByBindChanged(newBind)
}

func (s *service) handleBindChangedFromAllocator(
	source string,
	oldBind pb.LockTable,
	newBind pb.LockTable,
	allocator allocatorState,
	requestAllocator allocatorState,
) error {
	if !s.beginLockTablePublication() {
		return ErrLockTableBindChanged
	}
	defer s.lifecycle.RUnlock()

	s.allocatorVersionMu.Lock()
	defer s.allocatorVersionMu.Unlock()

	if _, accepted := s.observeAllocatorStateLocked(
		source,
		allocator,
		requestAllocator,
		true,
		s.tableGroups); !accepted {
		return ErrLockTableBindChanged
	}

	s.bindChangeMu.Lock()
	defer s.bindChangeMu.Unlock()

	current := s.tableGroups.get(newBind.Group, newBind.Table)
	if current != nil {
		currentBind := current.getBind()
		if !currentBind.Changed(newBind) {
			return nil
		}
		if currentBind.Changed(oldBind) {
			return ErrLockTableBindChanged
		}
	}

	new := s.createLockTableByBind(newBind)
	s.tableGroups.set(newBind.Group, newBind.Table, new)
	s.fenceByBindChanged(newBind)
	return nil
}

func (s *service) beginLockTablePublication() bool {
	s.lifecycle.RLock()
	if s.lifecycle.closing {
		s.lifecycle.RUnlock()
		return false
	}
	return true
}

func (s *service) fenceByBindChanged(bind pb.LockTable) {
	if s.activeTxnHolder == nil {
		return
	}
	s.activeTxnHolder.fenceByBindChanged(bind)
}

func (s *service) fenceByExactBind(bind pb.LockTable) {
	if s.activeTxnHolder == nil {
		return
	}
	s.activeTxnHolder.fenceByExactBind(bind)
}

func (s *service) checkBindChangedBeforeLockSuccess(
	txn *activeTxn,
	txnID []byte,
	bind pb.LockTable,
) error {
	// Let any pending bind-change fence complete before reporting lock success.
	// Keep the lock order consistent with Lock: bindChangeMu before txn.Lock.
	txn.Unlock()
	s.bindChangeMu.RLock()
	txn.Lock()
	defer s.bindChangeMu.RUnlock()

	if err := txn.terminalLockErrorLocked(txnID); err != nil {
		return err
	}
	l := s.tableGroups.get(bind.Group, bind.Table)
	if l == nil || l.getBind().Changed(bind) {
		return ErrLockTableBindChanged
	}
	return nil
}

func (s *service) observeAllocatorVersion(source string, observedVersion uint64) int {
	return s.observeAllocatorStateWithHolders(source, allocatorState{version: observedVersion}, s.tableGroups)
}

func (s *service) observeAllocatorStateWithHolders(
	source string,
	observed allocatorState,
	holders *lockTableHolders,
) int {
	removed, _ := s.observeAllocatorStateWithHoldersFromSnapshot(source, observed, allocatorState{}, false, holders)
	return removed
}

func (s *service) observeAllocatorStateWithHoldersFromSnapshot(
	source string,
	observed allocatorState,
	requestAllocator allocatorState,
	hasRequestAllocator bool,
	holders *lockTableHolders,
) (int, bool) {
	s.allocatorVersionMu.Lock()
	defer s.allocatorVersionMu.Unlock()
	return s.observeAllocatorStateLocked(source, observed, requestAllocator, hasRequestAllocator, holders)
}

func (s *service) observeAllocatorStateLocked(
	source string,
	observed allocatorState,
	requestAllocator allocatorState,
	hasRequestAllocator bool,
	holders *lockTableHolders,
) (int, bool) {
	if observed.version == 0 && observed.id == "" {
		return 0, true
	}

	oldVersion := s.lastAllocatorVersion
	oldID := s.lastAllocatorID
	allocatorChanged := observed.id != "" &&
		observed.id != oldID &&
		(oldID != "" || oldVersion != 0)
	if s.isAllocatorStateRejectedLocked(source, observed, requestAllocator, hasRequestAllocator, allocatorChanged, oldID, oldVersion) {
		return 0, false
	}
	if !allocatorChanged && oldVersion != 0 && observed.version < oldVersion {
		v2.GetLockServiceAllocatorEpochRegressionCounter(source).Inc()
		logAllocatorEpochRegression(s.logger, source, oldVersion, observed.version)
		return 0, false
	}
	if !allocatorChanged && observed.version == oldVersion {
		return 0, true
	}

	// Defensive path for minimal service instances used by keeper tests.
	// Normal lock services always pass a holder and can purge stale binds.
	if holders == nil {
		s.updateAllocatorStateLocked(observed)
		v2.LockServiceAllocatorEpochObservedGauge.Set(float64(observed.version))
		v2.GetLockServiceAllocatorEpochChangedCounter(source).Inc()
		logAllocatorEpochChanged(s.logger, source, oldVersion, observed.version, 0)
		return 0, true
	}

	s.bindChangeMu.Lock()
	removed := s.removeLockTablesWithFence(holders, func(bind pb.LockTable) bool {
		if allocatorChanged {
			if observed.id != "" && bind.AllocatorID != "" {
				return bind.AllocatorID != observed.id
			}
			return true
		}
		return bind.Version < observed.version
	}, observed)
	s.bindChangeMu.Unlock()

	s.updateAllocatorStateLocked(observed)
	v2.LockServiceAllocatorEpochObservedGauge.Set(float64(observed.version))
	v2.GetLockServiceAllocatorEpochChangedCounter(source).Inc()
	if removed > 0 {
		v2.GetLockServiceStaleBindPurgedCounter(source).Add(float64(removed))
	}
	logAllocatorEpochChanged(s.logger, source, oldVersion, observed.version, removed)
	return removed, true
}

func (s *service) updateAllocatorStateLocked(observed allocatorState) {
	if observed.id != "" {
		if s.lastAllocatorID != "" && s.lastAllocatorID != observed.id {
			s.addSupersededAllocatorIDLocked(s.lastAllocatorID)
		}
		s.lastAllocatorID = observed.id
	}
	if observed.version != 0 {
		s.lastAllocatorVersion = observed.version
	}
}

func (s *service) addSupersededAllocatorIDLocked(id string) {
	if id == "" {
		return
	}
	if s.supersededAllocatorIDs == nil {
		s.supersededAllocatorIDs = make(map[string]struct{})
	}
	if _, ok := s.supersededAllocatorIDs[id]; ok {
		return
	}
	s.supersededAllocatorIDs[id] = struct{}{}
	s.supersededAllocatorIDOrder = append(s.supersededAllocatorIDOrder, id)
	if len(s.supersededAllocatorIDOrder) <= maxSupersededAllocatorIDs {
		return
	}
	evicted := s.supersededAllocatorIDOrder[0]
	copy(s.supersededAllocatorIDOrder, s.supersededAllocatorIDOrder[1:])
	s.supersededAllocatorIDOrder[len(s.supersededAllocatorIDOrder)-1] = ""
	s.supersededAllocatorIDOrder = s.supersededAllocatorIDOrder[:len(s.supersededAllocatorIDOrder)-1]
	delete(s.supersededAllocatorIDs, evicted)
}

func (s *service) allocatorStateSnapshot() allocatorState {
	s.allocatorVersionMu.Lock()
	defer s.allocatorVersionMu.Unlock()
	return allocatorState{
		id:      s.lastAllocatorID,
		version: s.lastAllocatorVersion,
	}
}

func (s *service) isAllocatorStateRejectedLocked(
	source string,
	observed allocatorState,
	requestAllocator allocatorState,
	hasRequestAllocator bool,
	allocatorChanged bool,
	oldID string,
	oldVersion uint64,
) bool {
	if observed.id != "" {
		if _, ok := s.supersededAllocatorIDs[observed.id]; ok {
			v2.GetLockServiceAllocatorEpochRegressionCounter(source).Inc()
			logAllocatorEpochRegression(s.logger, source, oldVersion, observed.version)
			return true
		}
		if hasRequestAllocator &&
			(requestAllocator.id != oldID || requestAllocator.version != oldVersion) &&
			oldID != "" &&
			observed.id != oldID {
			v2.GetLockServiceAllocatorEpochRegressionCounter(source).Inc()
			logAllocatorEpochRegression(s.logger, source, oldVersion, observed.version)
			return true
		}
	}
	if !allocatorChanged && oldVersion != 0 && observed.version < oldVersion {
		v2.GetLockServiceAllocatorEpochRegressionCounter(source).Inc()
		logAllocatorEpochRegression(s.logger, source, oldVersion, observed.version)
		return true
	}
	return false
}

func (s *service) removeLockTablesWithFence(
	holders *lockTableHolders,
	filter func(pb.LockTable) bool,
	observed allocatorState,
) int {
	removedTables := holders.detachWithFilter(func(_ uint64, lt lockTable) bool {
		bind := lt.getBind()
		return filter(bind)
	})
	// Detach first so no new caller can enter a stale table, then fence every
	// transaction before table.close wakes existing waiters. Otherwise waiter
	// notification and transaction fencing race, making one stale-bind event
	// nondeterministically surface as ErrLockTableNotFound or
	// ErrLockTableBindChanged despite the terminal-state normalization in Lock.
	for _, table := range removedTables {
		s.fenceByBindChanged(fenceBindForAllocatorState(
			table.getBind(), observed))
	}
	closeLockTables(removedTables, closeReasonBindChanged)
	return len(removedTables)
}

func fenceBindForAllocatorState(
	bind pb.LockTable,
	observed allocatorState,
) pb.LockTable {
	bind.AllocatorID = observed.id
	if observed.version > bind.Version {
		bind.Version = observed.version
	} else {
		bind.Version++
	}
	return bind
}

func (s *service) handleKeepBindFailed(
	serviceID string,
	holders *lockTableHolders,
	oldTableVersion uint64,
	allocator allocatorState,
	requestAllocator allocatorState,
) int {
	s.allocatorVersionMu.Lock()
	defer s.allocatorVersionMu.Unlock()

	if s.isAllocatorStateRejectedLocked(
		"keepalive-ok-false-epoch",
		allocator,
		requestAllocator,
		true,
		allocator.id != "" && allocator.id != s.lastAllocatorID && (s.lastAllocatorID != "" || s.lastAllocatorVersion != 0),
		s.lastAllocatorID,
		s.lastAllocatorVersion) {
		return 0
	}

	if holders == nil {
		s.observeAllocatorStateLocked("keepalive-ok-false-epoch", allocator, requestAllocator, true, nil)
		return 0
	}

	// Keep the original OK=false snapshot guard: if another local purge already
	// changed holders.version, this service-level purge should skip this round.
	s.bindChangeMu.Lock()
	removed := s.removeLockTablesWithFence(holders, func(bind pb.LockTable) bool {
		if oldTableVersion != holders.getVersion() {
			return false
		}
		return bind.ServiceID == serviceID
	}, allocator)
	s.bindChangeMu.Unlock()
	if removed > 0 {
		v2.GetLockServiceStaleBindPurgedCounter("keepalive-ok-false-service").Add(float64(removed))
	}
	s.observeAllocatorStateLocked("keepalive-ok-false-epoch", allocator, requestAllocator, true, holders)
	return removed
}

func (s *service) createLockTableByBind(bind pb.LockTable) lockTable {
	defer logLockTableCreated(
		s.logger,
		s.serviceID,
		bind,
		bind.ServiceID != s.serviceID,
	)

	if bind.ServiceID == s.serviceID {
		return newLocalLockTable(
			bind,
			s.fsp,
			s.events,
			s.clock,
			s.activeTxnHolder,
			s.logger,
		)
	} else {
		remote := newRemoteLockTable(
			s.serviceID,
			s.cfg.RemoteLockTimeout.Duration,
			bind,
			s.remote.client,
			s.handleBindChanged,
			s.logger,
		)
		remote.allocatorStateProvider = s.allocatorStateSnapshot
		remote.allocatorBindChangedHandler = s.handleBindChangedFromAllocator
		if !s.cfg.EnableRemoteLocalProxy ||
			!supportsLockProtocolV28(s.cfg.ServiceID) {
			// Proxy holder handoff relies on one owner-side Unlock per physical
			// table. Before protocol v28 an old owner interprets the first request
			// as transaction-wide, so a multi-table proxy transaction would release
			// later tables without applying their replacement mutations.
			return remote
		}
		return newLockTableProxy(s.serviceID, s.cfg.ServiceID, remote, s.logger)
	}
}

// getLockTableForTxnUnlock resolves cleanup from the binding recorded when the
// transaction acquired the lock, rather than from the service's current table
// cache. A cache entry can be replaced while an old transaction is still
// closing. Sending that replacement generation in Unlock can neither release
// the old owner's transaction ledger nor be safely accepted by the new owner.
//
// A stale local generation has already released its physical locks when its
// localLockTable was closed, so it must never be applied to the current local
// table. A stale remote generation still needs one direct, uncached RPC to its
// recorded owner: that owner either removes the matching old ledger or proves
// through bind-change handling that the old generation is gone.
func (s *service) getLockTableForTxnUnlock(bind pb.LockTable) lockTable {
	current := s.tableGroups.get(bind.Group, bind.Table)
	if current != nil && current.getBind().Equal(bind) {
		return current
	}
	if bind.ServiceID == s.serviceID {
		return nil
	}

	remote := newRemoteLockTable(
		s.serviceID,
		s.cfg.RemoteLockTimeout.Duration,
		bind,
		s.remote.client,
		s.handleBindChanged,
		s.logger,
	)
	remote.allocatorStateProvider = s.allocatorStateSnapshot
	remote.allocatorBindChangedHandler = s.handleBindChangedFromAllocator
	return remote
}

func (s *service) wait(ctx context.Context) error {
	if s.option.wait == nil {
		return nil
	}
	return s.option.wait(ctx)
}

// lockSynchronousTxnClosure takes the common synchronous close path without a
// separate keyed admission. Such a generation has never published async work,
// so closing keeps its transaction mutex for the entire operation. An ordinary
// close never acquires a replacement transaction mutex and therefore cannot
// participate in the cross-handoff cycle that closure admission prevents.
func (s *service) lockSynchronousTxnClosure(
	txnID []byte,
	mutations []pb.ExtraMutation,
) (*activeTxn, uint64, synchronousTxnClosureState) {
	if len(mutations) != 0 {
		return nil, 0, synchronousTxnClosureFallback
	}
	txn, generation, stale := s.lockActiveTxnGeneration(txnID)
	if txn == nil {
		if stale {
			return nil, 0, synchronousTxnClosureStale
		}
		return nil, 0, synchronousTxnClosureFallback
	}
	if txn.lockOpsCtx == nil {
		return txn, generation, synchronousTxnClosureLocked
	}
	txn.Unlock()
	return nil, 0, synchronousTxnClosureFallback
}

type synchronousTxnClosureState uint8

const (
	synchronousTxnClosureFallback synchronousTxnClosureState = iota
	synchronousTxnClosureStale
	synchronousTxnClosureLocked
)

// lockActiveTxnGeneration pins the holder-published generation before waiting
// for the transaction mutex. The shard snapshot is the publication barrier:
// pooling can reuse the same pointer and txn ID only after that map entry is
// deleted, and newActiveTxn increments generation before republishing it.
func (s *service) lockActiveTxnGeneration(
	txnID []byte,
) (*activeTxn, uint64, bool) {
	txn, generation := s.activeTxnHolder.getActiveTxnWithGeneration(
		txnID, false, "")
	if txn == nil {
		return nil, 0, false
	}
	txn.Lock()
	if txn.generation != generation || !bytes.Equal(txn.txnID, txnID) {
		txn.Unlock()
		return nil, 0, true
	}
	return txn, generation, false
}

type activeTxnHolder interface {
	close()
	empty() bool
	getAllTxnID() [][]byte
	incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string)
	getActiveTxn(txnID []byte, create bool, remoteService string) *activeTxn
	getActiveTxnWithGeneration(
		txnID []byte, create bool, remoteService string) (*activeTxn, uint64)
	hasActiveTxn(txnID []byte) bool
	deleteActiveTxn(txnID []byte) *activeTxn
	restoreActiveTxn(txn *activeTxn) bool
	fenceByBindChanged(bind pb.LockTable) int
	fenceByExactBind(bind pb.LockTable) int
	keepRemoteActiveTxn(remoteService string)
	keepRemoteLockBindActive(remoteService string, bind pb.LockTable)
	hasRemoteLockBind(remoteService string, bind pb.LockTable, maxKeepInterval time.Duration) bool
	getActiveTxnWithCreated(
		txnID []byte, create bool, remoteService string) (*activeTxn, bool, uint64)
	freeActiveTxn(txn *activeTxn)
	deleteActiveTxnIf(txnID []byte, expected *activeTxn) bool
	canUnlockRemoteTxn(pb.WaitTxn) (bool, timestamp.Timestamp)
	getTimeoutRemoveTxn(
		timeoutServices map[string]struct{},
		timeoutTxns [][]byte,
		fenceTSByTxn map[string]timestamp.Timestamp,
		maxKeepInterval time.Duration) [][]byte
	isValidRemoteTxn(pb.WaitTxn) bool
}

const activeTxnHolderShards = 16

type activeTxnEntry struct {
	txn           *activeTxn
	remoteService string
}

type activeTxnShard struct {
	sync.RWMutex
	txns map[string]activeTxnEntry
}

type mapBasedTxnHolder struct {
	serviceID string
	logger    *log.MOLogger
	fsp       *fixedSlicePool
	// beforeFenceTxnLock is a test-only phase hook for holder/transaction lock
	// interleavings.
	beforeFenceTxnLock func(*activeTxn)
	// validTxn returns an authoritative liveness result only when err is nil.
	// Any error means the remote transaction state is unknown; transport
	// reachability is not evidence that the transaction is inactive.
	validTxn       func(txn pb.WaitTxn) (bool, error)
	valid          func(sid string) (bool, error)
	notify         func([]pb.OrphanTxn) (pb.CannotCommitResponse, error)
	activeTxnCount atomic.Int64
	activeTxns     [activeTxnHolderShards]activeTxnShard
	mu             struct {
		sync.RWMutex
		// remoteServices known remote service
		remoteServices map[string]*list.Element[remote]
		// remoteLockBinds records the last heartbeat seen for a specific remote service + bind.
		remoteLockBinds map[string]time.Time
		// head(oldest) -> tail (newest)
		dequeue list.Deque[remote]
	}
}

func newMapBasedTxnHandler(
	serviceID string,
	logger *log.MOLogger,
	fsp *fixedSlicePool,
	valid func(sid string) (bool, error),
	notify func([]pb.OrphanTxn) (pb.CannotCommitResponse, error),
	validTxn func(txn pb.WaitTxn) (bool, error),
) activeTxnHolder {
	h := &mapBasedTxnHolder{}
	h.logger = logger
	h.fsp = fsp
	h.valid = valid
	h.notify = notify
	h.validTxn = validTxn
	h.serviceID = serviceID
	for i := range h.activeTxns {
		h.activeTxns[i].txns = make(map[string]activeTxnEntry, 64)
	}
	h.mu.remoteServices = make(map[string]*list.Element[remote])
	h.mu.remoteLockBinds = make(map[string]time.Time)
	h.mu.dequeue = list.New[remote]()
	return h
}

func (h *mapBasedTxnHolder) getActiveTxnShard(txnKey string) *activeTxnShard {
	hash := uint32(2166136261)
	for i := 0; i < len(txnKey); i++ {
		hash ^= uint32(txnKey[i])
		hash *= 16777619
	}
	return &h.activeTxns[hash%activeTxnHolderShards]
}

func (h *mapBasedTxnHolder) getActiveTxn(
	txnID []byte,
	create bool,
	remoteService string,
) *activeTxn {
	txn, _, _ := h.getActiveTxnInternal(txnID, create, remoteService, false)
	return txn
}

func (h *mapBasedTxnHolder) getActiveTxnWithGeneration(
	txnID []byte,
	create bool,
	remoteService string,
) (*activeTxn, uint64) {
	txn, _, generation := h.getActiveTxnInternal(
		txnID, create, remoteService, false)
	return txn, generation
}

// getActiveTxnWithCreated returns a newly published transaction with its mutex
// held. Handoff preparation uses that publication barrier to prevent an
// ordinary Lock from populating the candidate before an aborted handoff can
// remove it. Existing transactions are returned unlocked.
func (h *mapBasedTxnHolder) getActiveTxnWithCreated(
	txnID []byte,
	create bool,
	remoteService string,
) (*activeTxn, bool, uint64) {
	return h.getActiveTxnInternal(txnID, create, remoteService, true)
}

func (h *mapBasedTxnHolder) freeActiveTxn(txn *activeTxn) {
	reuse.Free(txn, nil)
}

func (h *mapBasedTxnHolder) getActiveTxnInternal(
	txnID []byte,
	create bool,
	remoteService string,
	lockCreated bool,
) (*activeTxn, bool, uint64) {
	txnKey := util.UnsafeBytesToString(txnID)
	shard := h.getActiveTxnShard(txnKey)
	shard.RLock()
	entry, ok := shard.txns[txnKey]
	var generation uint64
	if ok {
		// generation changes only before publication. Holding the shard read
		// lock prevents deletion, reset and reuse until this snapshot is taken.
		generation = entry.txn.generation
	}
	shard.RUnlock()
	if ok {
		return entry.txn, false, generation
	}
	if !create {
		return nil, false, 0
	}

	shard.Lock()
	defer shard.Unlock()
	if entry, ok := shard.txns[txnKey]; ok {
		return entry.txn, false, entry.txn.generation
	}

	txn := newActiveTxn(txnID, txnKey, h.fsp, remoteService)
	if lockCreated {
		txn.Lock()
	}
	// Publish the transaction count before the map entry. This keeps empty()
	// conservative while a create is in flight: count == 0 always means that
	// every shard is empty, which is required by the service drain transition.
	h.activeTxnCount.Add(1)
	shard.txns[txnKey] = activeTxnEntry{txn: txn, remoteService: remoteService}
	if remoteService != "" {
		h.mu.Lock()
		if _, ok := h.mu.remoteServices[remoteService]; !ok {
			h.mu.remoteServices[remoteService] = h.mu.dequeue.PushBack(remote{
				id:   remoteService,
				time: time.Now(),
			})

		}
		h.mu.Unlock()
	}
	logTxnCreated(h.logger, txn)
	return txn, true, txn.generation
}

func (h *mapBasedTxnHolder) hasActiveTxn(txnID []byte) bool {
	txnKey := util.UnsafeBytesToString(txnID)
	shard := h.getActiveTxnShard(txnKey)
	shard.RLock()
	_, ok := shard.txns[txnKey]
	shard.RUnlock()
	return ok
}

func (h *mapBasedTxnHolder) empty() bool {
	return h.activeTxnCount.Load() == 0
}

func (h *mapBasedTxnHolder) getAllTxnID() [][]byte {
	txns := make([][]byte, 0, h.activeTxnCount.Load())
	for i := range h.activeTxns {
		shard := &h.activeTxns[i]
		shard.RLock()
		for txnKey := range shard.txns {
			txns = append(txns, []byte(txnKey))
		}
		shard.RUnlock()
	}
	return txns
}

func (h *mapBasedTxnHolder) deleteActiveTxn(txnID []byte) *activeTxn {
	txnKey := util.UnsafeBytesToString(txnID)
	shard := h.getActiveTxnShard(txnKey)
	shard.Lock()
	entry, ok := shard.txns[txnKey]
	if ok {
		delete(shard.txns, txnKey)
		h.activeTxnCount.Add(-1)
	}
	shard.Unlock()
	return entry.txn
}

func (h *mapBasedTxnHolder) deleteActiveTxnIf(
	txnID []byte,
	expected *activeTxn,
) bool {
	txnKey := util.UnsafeBytesToString(txnID)
	shard := h.getActiveTxnShard(txnKey)
	shard.Lock()
	entry, ok := shard.txns[txnKey]
	if ok && entry.txn == expected {
		delete(shard.txns, txnKey)
		h.activeTxnCount.Add(-1)
	}
	shard.Unlock()
	return ok && entry.txn == expected
}

func (h *mapBasedTxnHolder) restoreActiveTxn(txn *activeTxn) bool {
	if txn == nil {
		return false
	}
	shard := h.getActiveTxnShard(txn.txnKey)
	shard.Lock()
	if entry, ok := shard.txns[txn.txnKey]; ok {
		shard.Unlock()
		return entry.txn == txn
	}
	h.activeTxnCount.Add(1)
	shard.txns[txn.txnKey] = activeTxnEntry{txn: txn, remoteService: txn.remoteService}
	shard.Unlock()

	if txn.remoteService != "" {
		h.keepRemoteActiveTxn(txn.remoteService)
	}
	return true
}

func (h *mapBasedTxnHolder) fenceByBindChanged(bind pb.LockTable) int {
	return h.fenceByBind(bind, false)
}

func (h *mapBasedTxnHolder) fenceByExactBind(bind pb.LockTable) int {
	return h.fenceByBind(bind, true)
}

func (h *mapBasedTxnHolder) fenceByBind(bind pb.LockTable, exact bool) int {
	n := 0
	for i := range h.activeTxns {
		shard := &h.activeTxns[i]
		shard.RLock()
		txnKeys := make([]string, 0, len(shard.txns))
		for txnKey := range shard.txns {
			txnKeys = append(txnKeys, txnKey)
		}
		shard.RUnlock()

		for _, txnKey := range txnKeys {
			for {
				shard.RLock()
				entry, ok := shard.txns[txnKey]
				if !ok {
					shard.RUnlock()
					break
				}
				// Closing transactions reject all later Lock calls and are already
				// releasing their recorded generations. Waiting for their mutex while
				// bindChangeMu is held reverses the cleanup lock order and can deadlock
				// both the initiating refresh and a concurrent bind publisher.
				if entry.txn.closing.Load() {
					shard.RUnlock()
					break
				}
				if h.beforeFenceTxnLock != nil {
					h.beforeFenceTxnLock(entry.txn)
				}

				// Unknown-commit cleanup holds txn.Lock while releasing owner
				// locks, then removes the transaction from this shard. Never
				// wait for txn.Lock while retaining shard.RLock: doing so
				// reverses that order and deadlocks both cleanup paths. TryLock
				// keeps the pooled transaction alive under shard.RLock; on
				// contention, release the shard so cleanup can make progress.
				if !entry.txn.TryLock() {
					shard.RUnlock()
					time.Sleep(time.Millisecond)
					continue
				}
				var fenced bool
				if exact {
					fenced = entry.txn.fenceByExactBindLocked(bind, h.logger)
				} else {
					fenced = entry.txn.fenceByBindChangedLocked(bind, h.logger)
				}
				if fenced {
					n++
				}
				entry.txn.Unlock()
				shard.RUnlock()
				break
			}
		}
	}
	return n
}

func (h *mapBasedTxnHolder) keepRemoteActiveTxn(remoteService string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.mu.remoteServices[remoteService]; ok {
		e.Value.time = time.Now()
		h.mu.dequeue.MoveToBack(e)
	}
}

func (h *mapBasedTxnHolder) keepRemoteLockBindActive(remoteService string, bind pb.LockTable) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.remoteLockBinds[getRemoteLockBindKey(remoteService, bind)] = time.Now()
}

func (h *mapBasedTxnHolder) hasRemoteLockBind(remoteService string, bind pb.LockTable, maxKeepInterval time.Duration) bool {
	if remoteService == h.serviceID {
		return true
	}
	h.mu.RLock()
	lastSeen, ok := h.mu.remoteLockBinds[getRemoteLockBindKey(remoteService, bind)]
	h.mu.RUnlock()
	if !ok {
		return false
	}
	if maxKeepInterval <= 0 {
		return true
	}
	return time.Since(lastSeen) < maxKeepInterval
}

func (h *mapBasedTxnHolder) getTimeoutRemoveTxn(
	timeoutServices map[string]struct{},
	needRemoved [][]byte,
	fenceTSByTxn map[string]timestamp.Timestamp,
	maxKeepInterval time.Duration,
) [][]byte {
	needRemoved = needRemoved[:0]
	for k := range fenceTSByTxn {
		delete(fenceTSByTxn, k)
	}
	for k := range timeoutServices {
		delete(timeoutServices, k)
	}
	h.mu.Lock()
	now := time.Now()
	for key, lastSeen := range h.mu.remoteLockBinds {
		if now.Sub(lastSeen) >= maxKeepInterval {
			delete(h.mu.remoteLockBinds, key)
		}
	}
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

	// all txns in the timeout services need to be removed
	for i := range h.activeTxns {
		shard := &h.activeTxns[i]
		shard.RLock()
		for txnKey, entry := range shard.txns {
			if idx, ok := cannotCommitServices[entry.remoteService]; ok {
				cannotCommit[idx].Txn = append(cannotCommit[idx].Txn, []byte(txnKey))
				continue
			}

			if _, ok := timeoutServices[entry.remoteService]; ok {
				needRemoved = append(needRemoved, []byte(txnKey))
			}
		}
		shard.RUnlock()
	}

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
			if !committing.FenceTS.IsEmpty() {
				committingTxns := committing.CommittingTxn
				for sid, idx := range cannotCommitServices {
					if len(committingTxns) == 0 {
						needRemoved = append(needRemoved, cannotCommit[idx].Txn...)
						for _, txn := range cannotCommit[idx].Txn {
							fenceTSByTxn[util.UnsafeBytesToString(txn)] = committing.FenceTS
						}
						timeoutServices[sid] = struct{}{}
					} else {
						m := make(map[string]struct{}, len(committingTxns))
						for _, v := range committingTxns {
							m[util.UnsafeBytesToString(v)] = struct{}{}
						}
						for _, v := range cannotCommit[idx].Txn {
							if _, ok := m[util.UnsafeBytesToString(v)]; !ok {
								needRemoved = append(needRemoved, v)
								fenceTSByTxn[util.UnsafeBytesToString(v)] = committing.FenceTS
							}
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
	h.mu.Unlock()
	return needRemoved
}

func (h *mapBasedTxnHolder) isValidRemoteTxn(txn pb.WaitTxn) bool {
	if txn.CreatedOn == h.serviceID {
		return true
	}

	active, err := h.validTxn(txn)
	if err != nil {
		// A failed observation cannot establish transaction liveness. In
		// particular, BackendClosed also represents transient MORPC pool and
		// creation states; fencing on it can abort a healthy remote transaction.
		// Keep the holder for this cycle. The waiter checker retries periodically,
		// while a genuinely dead service is still reclaimed by the independent
		// remote-bind heartbeat timeout path.
		logValidTxnFailed(h.logger, txn, err)
		v2.TxnLockActiveTxnRecoveryCounter.WithLabelValues("indeterminate").Inc()
		return true
	}
	if active {
		return true
	}
	// A remote CN active-txn miss does not prove the txn is safe to unlock.
	// The commit response may be lost after TN has already committed it, so
	// require the allocator to confirm the txn cannot still be committing.
	canUnlock, _ := h.canUnlockRemoteTxn(txn)
	return !canUnlock
}

func (h *mapBasedTxnHolder) canUnlockRemoteTxn(txn pb.WaitTxn) (bool, timestamp.Timestamp) {
	if txn.CreatedOn == h.serviceID {
		return false, timestamp.Timestamp{}
	}
	cannotCommit := []pb.OrphanTxn{
		{
			Service: txn.CreatedOn,
			Txn:     [][]byte{txn.TxnID},
		},
	}

	committing, err := h.notify(cannotCommit)
	if err != nil {
		// any error, we cannot determine that the txn is safe to unlock.
		return false, timestamp.Timestamp{}
	}
	// The target txn is safe to unlock only when TN confirms it is not
	// committing and returns an allocator fence that dominates future commits.
	return len(committing.CommittingTxn) == 0 && !committing.FenceTS.IsEmpty(), committing.FenceTS
}

func (h *mapBasedTxnHolder) close() {
	for i := range h.activeTxns {
		h.activeTxns[i].Lock()
	}
	for i := range h.activeTxns {
		for txnKey, entry := range h.activeTxns[i].txns {
			h.freeActiveTxn(entry.txn)
			delete(h.activeTxns[i].txns, txnKey)
		}
	}
	h.activeTxnCount.Store(0)
	for i := len(h.activeTxns) - 1; i >= 0; i-- {
		h.activeTxns[i].Unlock()
	}
}

func (h *mapBasedTxnHolder) incLockTableRef(m map[uint32]map[uint64]uint64, serviceID string) {
	for i := range h.activeTxns {
		shard := &h.activeTxns[i]
		shard.RLock()
		for _, entry := range shard.txns {
			entry.txn.incLockTableRef(m, serviceID)
		}
		shard.RUnlock()
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

func getRemoteLockBindKey(remoteService string, bind pb.LockTable) string {
	var b strings.Builder
	b.Grow(len(remoteService) + len(bind.ServiceID) + 64)
	b.WriteString(remoteService)
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(uint64(bind.Group), 10))
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(bind.Table, 10))
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(bind.OriginTable, 10))
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(uint64(bind.Sharding), 10))
	b.WriteByte('/')
	b.WriteString(bind.ServiceID)
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(bind.Version, 10))
	b.WriteByte('/')
	b.WriteString(bind.AllocatorID)
	return b.String()
}

func ShardingByRow(row []byte) uint64 {
	return crc64.Checksum(row, crc64.MakeTable(crc64.ECMA))
}

type lockTableHolders struct {
	sync.RWMutex
	service string
	logger  *log.MOLogger
	holders map[uint32]*lockTableHolder
	version atomic.Uint64
}

func (m *lockTableHolders) get(group uint32, id uint64) lockTable {
	return m.mustGetHolder(group).get(id)
}

func (m *lockTableHolders) set(group uint32, id uint64, new lockTable) lockTable {
	result := m.mustGetHolder(group).set(id, new)
	if result.changed {
		m.version.Add(1)
	}
	// Closing can synchronously publish into the bounded waiter-event queue.
	// The holder mutation and version publication must therefore complete
	// before lifecycle callbacks run and potentially re-enter table lookup.
	if result.toClose != nil {
		result.toClose.close(closeReasonBindChanged)
	}
	if result.replaced {
		logRemoteBindChanged(
			m.logger,
			m.service,
			result.oldBind,
			result.newBind,
		)
	}
	return result.current
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
		tables: map[uint64]lockTable{},
	}
	m.holders[group] = h
	m.version.Add(1)
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

func (m *lockTableHolders) removeWithFilter(
	filter func(uint64, lockTable) bool,
	reason closeReason,
) int {
	removed := m.detachWithFilter(filter)
	closeLockTables(removed, reason)
	return len(removed)
}

// detachWithFilter removes matching tables from lookup without closing them.
// Callers that need to publish a terminal state before waiter notification can
// do so between detach and close, while ordinary removal keeps using
// removeWithFilter's combined operation.
func (m *lockTableHolders) detachWithFilter(
	filter func(uint64, lockTable) bool,
) []lockTable {
	m.RLock()
	var removed []lockTable
	for _, h := range m.holders {
		removed = append(removed, h.detachWithFilter(filter)...)
	}
	m.RUnlock()

	if len(removed) > 0 {
		m.version.Add(1)
	}
	return removed
}

func closeLockTables(removed []lockTable, reason closeReason) {
	// No holder lock is retained across table.close. An event consumer may
	// need the same holder to finish the callback that frees event-queue
	// capacity.
	for _, table := range removed {
		table.close(reason)
	}
}

// getVersion returns the current version of the lockTableHolders
func (m *lockTableHolders) getVersion() uint64 {
	return m.version.Load()
}

type lockTableHolder struct {
	sync.RWMutex
	tables map[uint64]lockTable
}

type lockTableHolderSetResult struct {
	current  lockTable
	toClose  lockTable
	oldBind  pb.LockTable
	newBind  pb.LockTable
	changed  bool
	replaced bool
}

func (m *lockTableHolder) get(id uint64) lockTable {
	m.RLock()
	defer m.RUnlock()
	return m.tables[id]
}

func (m *lockTableHolder) set(
	id uint64,
	new lockTable,
) lockTableHolderSetResult {
	m.Lock()
	old, ok := m.tables[id]

	if !ok {
		m.tables[id] = new
		m.Unlock()
		return lockTableHolderSetResult{
			current: new,
			changed: true,
		}
	}

	oldBind := old.getBind()
	newBind := new.getBind()
	if oldBind.Changed(newBind) {
		m.tables[id] = new
		m.Unlock()
		return lockTableHolderSetResult{
			current:  new,
			toClose:  old,
			oldBind:  oldBind,
			newBind:  newBind,
			changed:  true,
			replaced: true,
		}
	}
	m.Unlock()
	return lockTableHolderSetResult{
		current: old,
		toClose: new,
	}
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

func (m *lockTableHolder) detachWithFilter(
	filter func(uint64, lockTable) bool,
) []lockTable {
	m.Lock()
	var removed []lockTable
	for id, v := range m.tables {
		if filter(id, v) {
			delete(m.tables, id)
			removed = append(removed, v)
		}
	}
	m.Unlock()
	return removed
}
