// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	lazyDeleteInterval                    = time.Second * 10
	errCommittedTableCacheBuildSuperseded = errors.New("committed table cache build superseded")
)

type privateResetKey struct {
	txnID   string
	tableID uint64
}

type privateResetRegistration struct {
	ready chan struct{}
}

type privateResetCallback struct {
	key          privateResetKey
	registration *privateResetRegistration
}

type txnEpochCacheCallback struct {
	tableID      uint64
	cache        incrTableCache
	registration *privateResetRegistration
}

// committedTableCacheBuild is the single in-flight committed-cache build for
// one table generation. All fields are protected by service.mu; closing ready
// publishes the final err to waiters.
type committedTableCacheBuild struct {
	generation uint64
	epoch      uint32
	ready      chan struct{}
	err        error
	done       bool
}

// pendingTableCacheCommit fences the short interval in which a cache created
// by a transaction is already published in tables, but still carries that
// transaction while its commit transition runs outside service.mu. New users
// of the same table wait on ready; unrelated tables remain independent.
type pendingTableCacheCommit struct {
	ready chan struct{}
}

type tableCacheLifecycleAction struct {
	tableID       uint64
	cache         incrTableCache
	commit        bool
	pendingCommit *pendingTableCacheCommit
}

func (s *service) runTableCacheLifecycleAction(a tableCacheLifecycleAction) {
	defer s.builders.Done()
	if a.pendingCommit != nil {
		defer func() {
			s.mu.Lock()
			if s.mu.pendingCommits[a.tableID] == a.pendingCommit {
				delete(s.mu.pendingCommits, a.tableID)
			}
			close(a.pendingCommit.ready)
			s.mu.Unlock()
		}()
	}
	if a.commit {
		a.cache.commit()
	} else {
		a.cache.retire()
	}
}

type service struct {
	sid       string
	logger    *log.MOLogger
	cfg       Config
	store     IncrValueStore
	allocator valueAllocator
	stopper   *stopper.Stopper
	builders  sync.WaitGroup

	mu struct {
		sync.Mutex
		closed           bool
		destroyed        map[uint64]deleteCtx
		tables           map[uint64]incrTableCache
		generation       map[uint64]uint64
		generationBuilds map[uint64]uint64
		committedBuilds  map[uint64]*committedTableCacheBuild
		pendingCommits   map[uint64]*pendingTableCacheCommit
		private          map[privateResetKey]incrTableCache
		privateCallbacks map[privateResetKey]*privateResetRegistration
		createdResets    map[privateResetKey]incrTableCache
		creates          map[string][]uint64
		deletes          map[string][]deleteCtx
	}
}

func NewIncrService(
	sid string,
	store IncrValueStore,
	cfg Config,
) AutoIncrementService {
	logger := getLogger(sid)
	cfg.adjust()
	s := &service{
		sid:       sid,
		logger:    logger,
		cfg:       cfg,
		store:     store,
		allocator: newValueAllocator(sid, store),
		stopper:   stopper.NewStopper("incr-service", stopper.WithLogger(getLogger(sid).RawLogger())),
	}
	s.mu.destroyed = make(map[uint64]deleteCtx)
	s.mu.tables = make(map[uint64]incrTableCache, 1024)
	s.mu.generation = make(map[uint64]uint64, 1024)
	s.mu.generationBuilds = make(map[uint64]uint64)
	s.mu.committedBuilds = make(map[uint64]*committedTableCacheBuild)
	s.mu.pendingCommits = make(map[uint64]*pendingTableCacheCommit)
	s.mu.private = make(map[privateResetKey]incrTableCache)
	s.mu.privateCallbacks = make(map[privateResetKey]*privateResetRegistration)
	s.mu.createdResets = make(map[privateResetKey]incrTableCache)
	s.mu.creates = make(map[string][]uint64, 1024)
	s.mu.deletes = make(map[string][]deleteCtx, 1024)
	if err := s.stopper.RunTask(s.destroyTables); err != nil {
		panic(err)
	}
	return s
}

func (s *service) UUID() string {
	return s.sid
}

func (s *service) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator,
) error {
	s.logger.Info(
		"incrservice.create.table",
		zap.Uint64("table-id", tableID),
		zap.String("txn", txnOp.Txn().DebugString()),
	)

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		client.NewTxnEventCallback(s.txnClosed))
	if err := s.store.Create(ctx, tableID, cols, txnOp); err != nil {
		s.logger.Error(
			"incrservice.create.cache.failed",
			zap.Uint64("table-id", tableID),
			zap.String("txn", hex.EncodeToString(txnOp.Txn().ID)),
			zap.Error(err),
		)
		return err
	}
	c, err := newTableCache(
		ctx,
		s.sid,
		tableID,
		0,
		cols,
		s.cfg,
		s.allocator,
		txnOp,
		false,
	)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(txnOp.Txn().ID)
	s.mu.creates[key] = append(s.mu.creates[key], tableID)
	return s.doCreateLocked(
		tableID,
		c,
		txnOp.Txn().ID)
}

func (s *service) Reset(
	ctx context.Context,
	oldTableID,
	newTableID uint64,
	keep bool,
	txnOp client.TxnOperator) error {
	s.logger.Info(
		"incrservice.reset.table",
		zap.Uint64("table-id", oldTableID),
		zap.String("txn", txnOp.Txn().DebugString()),
		zap.Uint64("new-table-id", newTableID),
	)

	cols, err := s.store.GetColumns(ctx, oldTableID, txnOp)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		s.logger.Info(
			"incrservice.reset.table.no.columns.found",
			zap.Uint64("table-id", oldTableID),
			zap.String("txn", txnOp.Txn().DebugString()),
		)
	}

	if !keep {
		for idx := range cols {
			cols[idx].Offset = 0
		}
	} else {
		c, err := s.getTableCache(ctx, oldTableID)
		if err != nil {
			return err
		}
		if c != nil {
			// reuse ids in cache
			if err := c.adjust(ctx, cols); err != nil {
				return err
			}
		}
	}

	if err := s.Delete(ctx, oldTableID, txnOp); err != nil {
		return err
	}
	for idx := range cols {
		cols[idx].TableID = newTableID
	}
	return s.Create(ctx, newTableID, cols, txnOp)
}

func (s *service) Delete(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator) error {
	s.logger.Info("delete auto increment table",
		zap.Uint64("table-id", tableID),
		zap.String("txn", txnOp.Txn().DebugString()))

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		client.NewTxnEventCallback(s.txnClosed))

	s.mu.Lock()
	defer s.mu.Unlock()
	delCtx, err := newDeleteCtx(ctx, tableID)
	if err != nil {
		return err
	}
	key := string(txnOp.Txn().ID)
	s.mu.deletes[key] = append(s.mu.deletes[key], delCtx)
	if s.logger.Enabled(zap.InfoLevel) {
		s.logger.Info("ready to delete auto increment table cache",
			zap.Uint64("table-id", tableID),
			zap.String("txn", hex.EncodeToString(txnOp.Txn().ID)))
	}
	return nil
}

func (s *service) GetLastAllocateTS(
	ctx context.Context,
	tableID uint64,
	autoIncrEpoch uint32,
	txnOp client.TxnOperator,
	colName string,
) (timestamp.Timestamp, error) {
	tc, err := s.acquireTableCacheForEpoch(
		ctx,
		tableID,
		autoIncrEpoch,
		txnOp)
	if err != nil {
		return timestamp.Timestamp{}, err
	}
	defer tc.release()
	ts, err := tc.getLastAllocateTS(ctx, colName)
	if err != nil {
		return timestamp.Timestamp{}, err
	}

	return ts, nil
}

func (s *service) InsertValues(
	ctx context.Context,
	tableID uint64,
	autoIncrEpoch uint32,
	txnOp client.TxnOperator,
	vecs []*vector.Vector,
	rows int,
	estimate int64,
) (uint64, error) {
	ts, err := s.acquireTableCacheForEpoch(
		ctx,
		tableID,
		autoIncrEpoch,
		txnOp)
	if err != nil {
		return 0, err
	}
	defer ts.release()
	return ts.insertAutoValues(
		ctx,
		tableID,
		vecs,
		rows,
		estimate,
	)
}

func (s *service) CurrentValue(
	ctx context.Context,
	tableID uint64,
	col string) (uint64, error) {
	ts, err := s.acquireCommittedTableCache(
		ctx,
		tableID)
	if err != nil {
		return 0, err
	}
	defer ts.release()
	return ts.currentValue(ctx, tableID, col)
}

func (s *service) Reload(
	ctx context.Context,
	tableID uint64,
) error {
	for {
		s.mu.Lock()
		if s.mu.closed {
			s.mu.Unlock()
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if pending := s.mu.pendingCommits[tableID]; pending != nil {
			s.mu.Unlock()
			select {
			case <-pending.ready:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		s.bumpGenerationLocked(tableID)
		c, ok := s.mu.tables[tableID]
		if !ok {
			s.mu.Unlock()
			return nil
		}

		// drop cache, will be reloaded when next query
		delete(s.mu.tables, tableID)
		s.mu.Unlock()
		c.retire()
		return nil
	}
}

func (s *service) SetOffset(
	ctx context.Context,
	tableID uint64,
	colIndex int,
	colName string,
	offset uint64,
	txnOp client.TxnOperator,
) error {
	if txnOp != nil && !client.RequireAutoIncrEpochFenceCommit(txnOp) {
		return moerr.NewNotSupported(ctx, "transaction operator cannot enforce AUTO_INCREMENT epochs")
	}
	var (
		txnKey                string
		ownedCreate           bool
		createCache           incrTableCache
		createEpoch           uint32
		createGeneration      uint64
		createResetKey        privateResetKey
		originalCreateCache   incrTableCache
		supersededCreateCache incrTableCache
		trackGeneration       bool
	)

	for {
		s.mu.Lock()
		if s.mu.closed {
			s.mu.Unlock()
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if pending := s.mu.pendingCommits[tableID]; pending != nil {
			s.mu.Unlock()
			select {
			case <-pending.ready:
				continue
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		break
	}
	s.builders.Add(1)
	if txnOp != nil {
		txnKey = string(txnOp.Txn().ID)
		ownedCreate = s.ownsCreateLocked(txnKey, tableID)
		if ownedCreate {
			createResetKey = privateResetKey{txnID: txnKey, tableID: tableID}
			originalCreateCache = s.mu.createdResets[createResetKey]
			createCache = s.mu.tables[tableID]
			if createCache != nil {
				createEpoch = createCache.epoch()
				s.startGenerationBuildLocked(tableID)
				trackGeneration = true
				createGeneration = s.bumpGenerationLocked(tableID)
			}
		}
	}
	s.mu.Unlock()
	defer s.builders.Done()
	if trackGeneration {
		defer s.finishGenerationBuild(tableID)
	}

	if ownedCreate {
		if createCache == nil {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if createEpoch == math.MaxUint32 {
			return moerr.NewInternalErrorNoCtx("AUTO_INCREMENT epoch exhausted")
		}
	} else {
		if err := s.Reload(ctx, tableID); err != nil {
			return err
		}
	}

	// ALTER TABLE AUTO_INCREMENT explicitly resets the next value. The caller
	// has already checked table data and holds the DDL lock, so bypass the
	// store-level monotonic guard that protects normal pre-allocation updates.
	if err := s.allocator.forceSetOffset(ctx, tableID, colIndex, colName, offset, txnOp); err != nil {
		return err
	}
	if txnOp == nil {
		return nil
	}

	cols, err := s.store.GetColumns(ctx, tableID, txnOp)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}

	if ownedCreate {
		// CREATE TABLE (including clone/copy ALTER) is tracked by
		// handleCreatesLocked. Publish the post-reset cache through that path so
		// the committed table cannot retain its pre-reset range.
		replacement, err := newTableCache(
			ctx,
			s.sid,
			tableID,
			createEpoch+1,
			cols,
			s.cfg,
			s.allocator,
			txnOp,
			false,
		)
		if err != nil {
			return err
		}

		s.mu.Lock()
		if s.mu.closed ||
			!s.ownsCreateLocked(txnKey, tableID) ||
			s.mu.generation[tableID] != createGeneration ||
			s.mu.tables[tableID] != createCache {
			s.mu.Unlock()
			replacement.retire()
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		s.mu.tables[tableID] = replacement
		if originalCreateCache == nil {
			s.mu.createdResets[createResetKey] = createCache
		} else {
			supersededCreateCache = createCache
		}
		s.mu.Unlock()
		if supersededCreateCache != nil {
			supersededCreateCache.retire()
		}
		return nil
	}

	private := newLazyPrivateTableCache(
		tableID,
		cols,
		func(buildCtx context.Context) (incrTableCache, error) {
			return s.buildPrivateTableCache(
				buildCtx,
				func() (incrTableCache, error) {
					return newTableCache(
						buildCtx, s.sid, tableID, 0, cols, s.cfg, s.allocator, txnOp, false)
				})
		})
	if err := s.installPrivateReset(ctx, tableID, txnOp, private); err != nil {
		return err
	}
	return nil
}

func (s *service) ownsCreateLocked(txnKey string, tableID uint64) bool {
	for _, id := range s.mu.creates[txnKey] {
		if id == tableID {
			return true
		}
	}
	return false
}

func (s *service) startGenerationBuildLocked(tableID uint64) uint64 {
	s.mu.generationBuilds[tableID]++
	return s.mu.generation[tableID]
}

func (s *service) finishGenerationBuild(tableID uint64) {
	s.mu.Lock()
	if s.mu.generationBuilds[tableID] <= 1 {
		delete(s.mu.generationBuilds, tableID)
		delete(s.mu.generation, tableID)
	} else {
		s.mu.generationBuilds[tableID]--
	}
	s.mu.Unlock()
}

func (s *service) bumpGenerationLocked(tableID uint64) uint64 {
	return s.bumpGenerationWithBuildErrorLocked(
		tableID,
		moerr.NewTxnNeedRetryWithDefChanged(context.Background()),
	)
}

func (s *service) bumpGenerationWithBuildErrorLocked(
	tableID uint64,
	buildErr error,
) uint64 {
	if s.mu.generationBuilds[tableID] == 0 {
		delete(s.mu.generation, tableID)
		return 0
	}
	s.mu.generation[tableID]++
	s.finishCommittedTableCacheBuildLocked(
		tableID,
		s.mu.committedBuilds[tableID],
		buildErr,
	)
	return s.mu.generation[tableID]
}

func (s *service) finishCommittedTableCacheBuildLocked(
	tableID uint64,
	build *committedTableCacheBuild,
	err error,
) error {
	if build == nil {
		return err
	}
	if build.done {
		return build.err
	}
	build.err = err
	build.done = true
	if s.mu.committedBuilds[tableID] == build {
		delete(s.mu.committedBuilds, tableID)
	}
	close(build.ready)
	return err
}

func (s *service) finishCommittedTableCacheBuild(
	tableID uint64,
	build *committedTableCacheBuild,
	err error,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finishCommittedTableCacheBuildLocked(tableID, build, err)
}

func (s *service) buildPrivateTableCache(
	ctx context.Context,
	build func() (incrTableCache, error),
) (incrTableCache, error) {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	s.builders.Add(1)
	s.mu.Unlock()
	defer s.builders.Done()

	cache, err := build()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	closed := s.mu.closed
	s.mu.Unlock()
	if closed {
		cache.retire()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	return cache, nil
}

func (s *service) DiscardOffsetReset(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) error {
	if txnOp == nil {
		return nil
	}
	key := privateResetKey{txnID: string(txnOp.Txn().ID), tableID: tableID}
	s.mu.Lock()
	registration := s.mu.privateCallbacks[key]
	s.mu.Unlock()
	if registration != nil {
		<-registration.ready
	}
	s.mu.Lock()
	private := s.mu.private[key]
	delete(s.mu.private, key)
	previous := s.mu.createdResets[key]
	delete(s.mu.createdResets, key)
	var current incrTableCache
	if previous != nil && s.ownsCreateLocked(key.txnID, tableID) {
		current = s.mu.tables[tableID]
		s.bumpGenerationLocked(tableID)
		s.mu.tables[tableID] = previous
		previous = nil
	}
	s.mu.Unlock()
	if private != nil {
		private.retire()
	}
	if current != nil {
		current.retire()
	}
	if previous != nil {
		previous.retire()
	}
	return nil
}

func (s *service) Close() {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return
	}
	s.mu.closed = true
	for tableID, build := range s.mu.committedBuilds {
		s.finishCommittedTableCacheBuildLocked(
			tableID,
			build,
			moerr.NewTxnNeedRetryWithDefChanged(context.Background()),
		)
	}
	s.mu.Unlock()

	s.stopper.Stop()
	s.builders.Wait()

	s.mu.Lock()
	tables := make([]incrTableCache, 0, len(s.mu.tables)+len(s.mu.private)+len(s.mu.createdResets))
	for _, tc := range s.mu.tables {
		tables = append(tables, tc)
	}
	for _, tc := range s.mu.private {
		tables = append(tables, tc)
	}
	for _, tc := range s.mu.createdResets {
		tables = append(tables, tc)
	}
	s.mu.private = make(map[privateResetKey]incrTableCache)
	s.mu.privateCallbacks = make(map[privateResetKey]*privateResetRegistration)
	s.mu.createdResets = make(map[privateResetKey]incrTableCache)
	s.mu.generation = make(map[uint64]uint64)
	s.mu.generationBuilds = make(map[uint64]uint64)
	s.mu.committedBuilds = make(map[uint64]*committedTableCacheBuild)
	s.mu.pendingCommits = make(map[uint64]*pendingTableCacheCommit)
	s.mu.Unlock()
	for _, tc := range tables {
		tc.retire()
	}

	s.allocator.close()
	s.store.Close()
}

func (s *service) acquireTableCacheForEpoch(
	ctx context.Context,
	tableID uint64,
	autoIncrEpoch uint32,
	txnOp client.TxnOperator,
) (incrTableCache, error) {
	if txnOp != nil {
		key := privateResetKey{txnID: string(txnOp.Txn().ID), tableID: tableID}
		s.mu.Lock()
		if s.mu.closed {
			s.mu.Unlock()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if private, ok := s.mu.private[key]; ok {
			// A reset cache is transaction-private and authoritative while it
			// exists. Never mask a private-cache error by falling back to a
			// committed AUTO_INCREMENT epoch cache.
			private.acquire()
			s.mu.Unlock()
			return private, nil
		}
		s.mu.Unlock()
	}
	return s.getCommittedTableCacheForEpoch(ctx, tableID, autoIncrEpoch, txnOp, false)
}

func (s *service) installPrivateReset(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
	private incrTableCache,
) error {
	key := privateResetKey{txnID: string(txnOp.Txn().ID), tableID: tableID}
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		private.retire()
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	registration := s.mu.privateCallbacks[key]
	owner := registration == nil
	if owner {
		registration = &privateResetRegistration{ready: make(chan struct{})}
		s.mu.privateCallbacks[key] = registration
	}
	s.mu.Unlock()

	if owner {
		if err := s.appendPrivateResetCallback(txnOp, privateResetCallback{
			key:          key,
			registration: registration,
		}); err != nil {
			s.mu.Lock()
			if s.mu.privateCallbacks[key] == registration {
				delete(s.mu.privateCallbacks, key)
			}
			s.mu.Unlock()
			close(registration.ready)
			private.retire()
			return err
		}
	} else {
		<-registration.ready
	}

	s.mu.Lock()
	if s.mu.closed || s.mu.privateCallbacks[key] != registration {
		s.mu.Unlock()
		if owner {
			close(registration.ready)
		}
		private.retire()
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	old := s.mu.private[key]
	s.mu.private[key] = private
	s.mu.Unlock()
	if owner {
		close(registration.ready)
	}
	if old != nil {
		old.retire()
	}
	return nil
}

func (s *service) appendPrivateResetCallback(
	txnOp client.TxnOperator,
	callback privateResetCallback,
) (err error) {
	defer func() {
		if recover() != nil {
			err = moerr.NewTxnNeedRetryWithDefChanged(context.Background())
		}
	}()
	txnOp.AppendEventCallback(
		client.ClosedEvent,
		client.NewTxnEventCallbackWithValue(s.privateResetClosed, callback),
	)
	return nil
}

func (s *service) privateResetClosed(
	_ context.Context,
	_ client.TxnOperator,
	_ client.TxnEvent,
	v any,
) error {
	callback := v.(privateResetCallback)
	<-callback.registration.ready
	s.mu.Lock()
	private := s.mu.private[callback.key]
	delete(s.mu.private, callback.key)
	if s.mu.privateCallbacks[callback.key] == callback.registration {
		delete(s.mu.privateCallbacks, callback.key)
	}
	s.mu.Unlock()
	if private != nil {
		private.retire()
	}
	return nil
}

func (s *service) txnEpochCacheClosed(
	_ context.Context,
	_ client.TxnOperator,
	event client.TxnEvent,
	v any,
) error {
	callback := v.(txnEpochCacheCallback)
	<-callback.registration.ready
	if event.Txn.Status == txn.TxnStatus_Committed {
		return nil
	}

	s.mu.Lock()
	if s.mu.tables[callback.tableID] != callback.cache {
		s.mu.Unlock()
		return nil
	}
	s.bumpGenerationLocked(callback.tableID)
	delete(s.mu.tables, callback.tableID)
	s.mu.Unlock()
	callback.cache.retire()
	return nil
}

func (s *service) doCreateLocked(
	tableID uint64,
	c incrTableCache,
	txnID []byte) error {
	s.mu.tables[tableID] = c
	if s.logger.Enabled(zap.InfoLevel) {
		s.logger.Info(
			"incrservice.cache.created",
			zap.Uint64("table-id", tableID),
			zap.String("txn", hex.EncodeToString(txnID)),
		)
	}
	return nil
}

func (s *service) getCommittedTableCacheForEpoch(
	ctx context.Context,
	tableID uint64,
	autoIncrEpoch uint32,
	txnOp client.TxnOperator,
	anyEpoch bool,
) (incrTableCache, error) {
	for {
		s.mu.Lock()
		if s.mu.closed {
			s.mu.Unlock()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if pending := s.mu.pendingCommits[tableID]; pending != nil {
			s.mu.Unlock()
			select {
			case <-pending.ready:
				continue
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
		c, ok := s.mu.tables[tableID]
		if ok && (anyEpoch || c.epoch() == autoIncrEpoch) {
			c.acquire()
			s.mu.Unlock()
			return c, nil
		}
		if ok && c.epoch() > autoIncrEpoch {
			s.mu.Unlock()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if _, ok := s.mu.destroyed[tableID]; ok {
			s.mu.Unlock()
			return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
		}
		if build := s.mu.committedBuilds[tableID]; build != nil {
			// A higher explicit epoch must be able to supersede a blocked older
			// build. It starts a new generation; same-epoch callers and any-epoch
			// readers share the existing build instead.
			if !anyEpoch && autoIncrEpoch > build.epoch {
				s.bumpGenerationWithBuildErrorLocked(
					tableID, errCommittedTableCacheBuildSuperseded,
				)
			} else {
				s.mu.Unlock()
				select {
				case <-build.ready:
					if build.err == errCommittedTableCacheBuildSuperseded {
						if anyEpoch {
							continue
						}
						return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
					}
					if build.err != nil {
						return nil, build.err
					}
					continue
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
			}
		}
		generation := s.startGenerationBuildLocked(tableID)
		build := &committedTableCacheBuild{
			generation: generation,
			epoch:      autoIncrEpoch,
			ready:      make(chan struct{}),
		}
		s.mu.committedBuilds[tableID] = build
		s.builders.Add(1)
		s.mu.Unlock()
		cache, err := s.buildCommittedTableCacheForEpoch(
			ctx, tableID, autoIncrEpoch, txnOp, anyEpoch, build,
		)
		if err == errCommittedTableCacheBuildSuperseded {
			if anyEpoch {
				continue
			}
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		return cache, err
	}
}

func (s *service) buildCommittedTableCacheForEpoch(
	ctx context.Context,
	tableID uint64,
	autoIncrEpoch uint32,
	txnOp client.TxnOperator,
	anyEpoch bool,
	build *committedTableCacheBuild,
) (incrTableCache, error) {
	defer s.builders.Done()
	defer s.finishGenerationBuild(tableID)

	cols, err := s.store.GetColumns(ctx, tableID, nil)
	if err != nil {
		return nil, s.finishCommittedTableCacheBuild(tableID, build, err)
	}
	if len(cols) == 0 {
		err = moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
		return nil, s.finishCommittedTableCacheBuild(tableID, build, err)
	}

	s.mu.Lock()
	if build.done || s.mu.closed || s.mu.generation[tableID] != build.generation {
		err = s.finishCommittedTableCacheBuildLocked(
			tableID, build, moerr.NewTxnNeedRetryWithDefChanged(ctx),
		)
		s.mu.Unlock()
		return nil, err
	}
	var previous incrTableCache
	if current, ok := s.mu.tables[tableID]; ok {
		if anyEpoch || current.epoch() == autoIncrEpoch {
			current.acquire()
			s.finishCommittedTableCacheBuildLocked(tableID, build, nil)
			s.mu.Unlock()
			return current, nil
		}
		if current.epoch() > autoIncrEpoch {
			err = s.finishCommittedTableCacheBuildLocked(
				tableID, build, moerr.NewTxnNeedRetryWithDefChanged(ctx),
			)
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()

	replacement, err := newTableCache(
		ctx,
		s.sid,
		tableID,
		autoIncrEpoch,
		cols,
		s.cfg,
		s.allocator,
		nil,
		true,
	)
	if err != nil {
		return nil, s.finishCommittedTableCacheBuild(tableID, build, err)
	}
	var registration *privateResetRegistration
	if txnOp != nil {
		registration = &privateResetRegistration{ready: make(chan struct{})}
		callback := txnEpochCacheCallback{
			tableID:      tableID,
			cache:        replacement,
			registration: registration,
		}
		if err := s.appendTxnEpochCacheCallback(txnOp, callback); err != nil {
			close(registration.ready)
			_ = replacement.close()
			return nil, s.finishCommittedTableCacheBuild(tableID, build, err)
		}
		defer close(registration.ready)
	}

	s.mu.Lock()
	if build.done || s.mu.closed || s.mu.generation[tableID] != build.generation {
		err = s.finishCommittedTableCacheBuildLocked(
			tableID, build, moerr.NewTxnNeedRetryWithDefChanged(ctx),
		)
		s.mu.Unlock()
		_ = replacement.close()
		return nil, err
	}
	if _, ok := s.mu.destroyed[tableID]; ok {
		err = s.finishCommittedTableCacheBuildLocked(
			tableID, build, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID)),
		)
		s.mu.Unlock()
		_ = replacement.close()
		return nil, err
	}
	if current, ok := s.mu.tables[tableID]; ok {
		if anyEpoch || current.epoch() == autoIncrEpoch {
			current.acquire()
			s.finishCommittedTableCacheBuildLocked(tableID, build, nil)
			s.mu.Unlock()
			_ = replacement.close()
			return current, nil
		}
		if current.epoch() > autoIncrEpoch {
			err = s.finishCommittedTableCacheBuildLocked(
				tableID, build, moerr.NewTxnNeedRetryWithDefChanged(ctx),
			)
			s.mu.Unlock()
			_ = replacement.close()
			return nil, err
		}
		previous = current
	}
	s.mu.tables[tableID] = replacement
	replacement.acquire()
	s.finishCommittedTableCacheBuildLocked(tableID, build, nil)
	s.mu.Unlock()
	if previous != nil {
		previous.retire()
	}
	return replacement, nil
}

func (s *service) appendTxnEpochCacheCallback(
	txnOp client.TxnOperator,
	callback txnEpochCacheCallback,
) (err error) {
	defer func() {
		if recover() != nil {
			err = moerr.NewTxnNeedRetryWithDefChanged(context.Background())
		}
	}()
	txnOp.AppendEventCallback(
		client.ClosedEvent,
		client.NewTxnEventCallbackWithValue(s.txnEpochCacheClosed, callback),
	)
	return nil
}

func (s *service) acquireCommittedTableCache(
	ctx context.Context,
	tableID uint64,
) (incrTableCache, error) {
	// CurrentValue does not carry the table epoch. A published cache of any
	// epoch is valid. Epoch zero keeps the cold construction semantics while
	// anyEpoch makes every locked recheck accept a concurrently published cache.
	return s.getCommittedTableCacheForEpoch(ctx, tableID, 0, nil, true)
}

func (s *service) txnClosed(ctx context.Context, txnOp client.TxnOperator, event client.TxnEvent, v any) error {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return nil
	}
	actions := s.handleCreatesLocked(event.Txn)
	retired := s.handleDeletesLocked(event.Txn)
	for _, tc := range retired {
		actions = append(actions, tableCacheLifecycleAction{tableID: tc.table(), cache: tc})
	}
	// Register every action before releasing service.mu. Close sets closed under
	// the same lock before waiting, so no lifecycle work can outlive the cache,
	// allocator, or store objects it may still touch.
	s.builders.Add(len(actions))
	s.mu.Unlock()

	// Commit and retirement take table/column locks. A column may hold its lock
	// while waiting for allocator/store I/O, so lifecycle work must not extend
	// the service.mu critical path.
	for _, action := range actions {
		s.runTableCacheLifecycleAction(action)
	}
	return nil
}

func (s *service) handleCreatesLocked(txnMeta txn.TxnMeta) []tableCacheLifecycleAction {
	key := string(txnMeta.ID)
	tables, ok := s.mu.creates[key]
	if !ok {
		return nil
	}

	var actions []tableCacheLifecycleAction
	for _, id := range tables {
		resetKey := privateResetKey{txnID: key, tableID: id}
		if previous := s.mu.createdResets[resetKey]; previous != nil {
			actions = append(actions, tableCacheLifecycleAction{tableID: id, cache: previous})
			delete(s.mu.createdResets, resetKey)
		}
		if tc, ok := s.mu.tables[id]; ok {
			if txnMeta.Status == txn.TxnStatus_Committed {
				pending := &pendingTableCacheCommit{ready: make(chan struct{})}
				s.mu.pendingCommits[id] = pending
				actions = append(actions, tableCacheLifecycleAction{
					tableID: id, cache: tc, commit: true, pendingCommit: pending,
				})
			} else {
				actions = append(actions, tableCacheLifecycleAction{tableID: id, cache: tc})
				delete(s.mu.tables, id)
				s.logger.Info(
					"incrservice.cache.destroyed",
					zap.Uint64("table-id", id),
					zap.String("txn", hex.EncodeToString(txnMeta.ID)),
				)
			}
		}
	}

	delete(s.mu.creates, key)
	return actions
}

func (s *service) handleDeletesLocked(txnMeta txn.TxnMeta) []incrTableCache {
	key := string(txnMeta.ID)
	tables, ok := s.mu.deletes[key]
	if !ok {
		return nil
	}

	var retired []incrTableCache
	if txnMeta.Status == txn.TxnStatus_Committed {
		for _, ctx := range tables {
			// The cache may still be under construction and therefore absent from
			// tables. The committed delete must invalidate that builder and leave a
			// tombstone so it cannot publish a cache after the table was dropped.
			s.bumpGenerationLocked(ctx.tableID)
			if tc, ok := s.mu.tables[ctx.tableID]; ok {
				delete(s.mu.tables, ctx.tableID)
				retired = append(retired, tc)
			}
			s.mu.destroyed[ctx.tableID] = ctx
			s.logger.Info(
				"incrservice.cache.deleted",
				zap.Uint64("table-id", ctx.tableID),
				zap.String("txn", hex.EncodeToString(txnMeta.ID)),
			)
		}
	}
	delete(s.mu.deletes, key)
	return retired
}

func (s *service) getTableCache(ctx context.Context, tableID uint64) (incrTableCache, error) {
	for {
		s.mu.Lock()
		if s.mu.closed {
			s.mu.Unlock()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if pending := s.mu.pendingCommits[tableID]; pending != nil {
			s.mu.Unlock()
			select {
			case <-pending.ready:
				continue
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		}
		cache := s.mu.tables[tableID]
		s.mu.Unlock()
		return cache, nil
	}
}

func (s *service) destroyTables(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(lazyDeleteInterval):
			s.mu.Lock()
			deletes := make([]deleteCtx, 0, len(s.mu.destroyed))
			for _, ctx := range s.mu.destroyed {
				deletes = append(deletes, ctx)
			}
			s.mu.Unlock()

			for _, dc := range deletes {
				ctx, cancel := context.WithTimeoutCause(defines.AttachAccountId(ctx, dc.accountID), time.Second*30, moerr.CauseDestroyTables)
				if err := s.store.Delete(ctx, dc.tableID); err == nil {
					s.mu.Lock()
					delete(s.mu.destroyed, dc.tableID)
					s.mu.Unlock()
				}
				cancel()
			}
		}
	}
}

type deleteCtx struct {
	accountID uint32
	tableID   uint64
}

func newDeleteCtx(ctx context.Context, tableID uint64) (deleteCtx, error) {
	accountId, err := getAccountID(ctx)
	if err != nil {
		return deleteCtx{}, err
	}
	return deleteCtx{
		tableID:   tableID,
		accountID: accountId,
	}, nil
}
