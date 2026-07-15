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
	"fmt"
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
	lazyDeleteInterval = time.Second * 10
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
		private          map[privateResetKey]incrTableCache
		privateCallbacks map[privateResetKey]*privateResetRegistration
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
	s.mu.private = make(map[privateResetKey]incrTableCache)
	s.mu.privateCallbacks = make(map[privateResetKey]*privateResetRegistration)
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
	} else if c := s.getTableCache(oldTableID); c != nil {
		// reuse ids in cache
		if err := c.adjust(ctx, cols); err != nil {
			return err
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
	tableVersion uint32,
	txnOp client.TxnOperator,
	colName string,
) (timestamp.Timestamp, error) {
	tc, err := s.acquireTableCacheForVersion(
		ctx,
		tableID,
		tableVersion,
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
	tableVersion uint32,
	txnOp client.TxnOperator,
	vecs []*vector.Vector,
	rows int,
	estimate int64,
) (uint64, error) {
	ts, err := s.acquireTableCacheForVersion(
		ctx,
		tableID,
		tableVersion,
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
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	if s.mu.generation == nil {
		s.mu.generation = make(map[uint64]uint64)
	}
	s.mu.generation[tableID]++
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

func (s *service) SetOffset(
	ctx context.Context,
	tableID uint64,
	colName string,
	offset uint64,
	txnOp client.TxnOperator,
) error {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	s.builders.Add(1)
	s.mu.Unlock()
	defer s.builders.Done()

	if err := s.Reload(ctx, tableID); err != nil {
		return err
	}

	// ALTER TABLE AUTO_INCREMENT explicitly resets the next value. The caller
	// has already checked table data and holds the DDL lock, so bypass the
	// store-level monotonic guard that protects normal pre-allocation updates.
	if err := s.allocator.forceSetOffset(ctx, tableID, colName, offset, txnOp); err != nil {
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
	private := newLazyPrivateTableCache(
		tableID,
		cols,
		func(buildCtx context.Context) (incrTableCache, error) {
			s.mu.Lock()
			if s.mu.closed {
				s.mu.Unlock()
				return nil, moerr.NewTxnNeedRetryWithDefChanged(buildCtx)
			}
			s.builders.Add(1)
			s.mu.Unlock()
			defer s.builders.Done()
			return newTableCache(
				buildCtx, s.sid, tableID, 0, cols, s.cfg, s.allocator, txnOp, false)
		})
	if err := s.installPrivateReset(ctx, tableID, txnOp, private); err != nil {
		return err
	}
	return nil
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
	s.mu.Unlock()
	if private != nil {
		private.retire()
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
	s.mu.Unlock()

	s.stopper.Stop()
	s.builders.Wait()

	s.mu.Lock()
	tables := make([]incrTableCache, 0, len(s.mu.tables)+len(s.mu.private))
	for _, tc := range s.mu.tables {
		tables = append(tables, tc)
	}
	for _, tc := range s.mu.private {
		tables = append(tables, tc)
	}
	s.mu.private = make(map[privateResetKey]incrTableCache)
	s.mu.privateCallbacks = make(map[privateResetKey]*privateResetRegistration)
	s.mu.Unlock()
	for _, tc := range tables {
		tc.retire()
	}

	s.allocator.close()
	s.store.Close()
}

func (s *service) acquireTableCacheForVersion(
	ctx context.Context,
	tableID uint64,
	tableVersion uint32,
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
			// committed table-version cache.
			private.acquire()
			s.mu.Unlock()
			return private, nil
		}
		s.mu.Unlock()
	}
	return s.getCommittedTableCacheForVersion(ctx, tableID, tableVersion)
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

func (s *service) getCommittedTableCache(
	ctx context.Context,
	tableID uint64) (incrTableCache, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.mu.tables[tableID]
	if ok {
		return c, nil
	}

	if _, ok := s.mu.destroyed[tableID]; ok {
		return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}

	cols, err := s.store.GetColumns(ctx, tableID, nil)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}

	c, err = newTableCache(
		ctx,
		s.sid,
		tableID,
		0,
		cols,
		s.cfg,
		s.allocator,
		nil,
		true,
	)
	if err != nil {
		return nil, err
	}
	s.doCreateLocked(tableID, c, nil)
	return c, nil
}

func (s *service) getCommittedTableCacheForVersion(
	ctx context.Context,
	tableID uint64,
	tableVersion uint32,
) (incrTableCache, error) {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	c, ok := s.mu.tables[tableID]
	if ok && c.version() == tableVersion {
		c.acquire()
		s.mu.Unlock()
		return c, nil
	}
	if ok && c.version() > tableVersion {
		s.mu.Unlock()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	if _, ok := s.mu.destroyed[tableID]; ok {
		s.mu.Unlock()
		return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}
	generation := s.mu.generation[tableID]
	s.builders.Add(1)
	s.mu.Unlock()
	defer s.builders.Done()

	cols, err := s.store.GetColumns(ctx, tableID, nil)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}

	s.mu.Lock()
	if s.mu.closed || s.mu.generation[tableID] != generation {
		s.mu.Unlock()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	if current, ok := s.mu.tables[tableID]; ok {
		if current.version() == tableVersion {
			current.acquire()
			s.mu.Unlock()
			return current, nil
		}
		if current.version() > tableVersion {
			s.mu.Unlock()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
	}
	s.mu.Unlock()

	replacement, err := newTableCache(
		ctx,
		s.sid,
		tableID,
		tableVersion,
		cols,
		s.cfg,
		s.allocator,
		nil,
		true,
	)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.mu.closed || s.mu.generation[tableID] != generation {
		s.mu.Unlock()
		_ = replacement.close()
		return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	if _, ok := s.mu.destroyed[tableID]; ok {
		s.mu.Unlock()
		_ = replacement.close()
		return nil, moerr.NewNoSuchTableNoCtx("", fmt.Sprintf("%d", tableID))
	}
	if current, ok := s.mu.tables[tableID]; ok {
		if current.version() == tableVersion {
			current.acquire()
			s.mu.Unlock()
			_ = replacement.close()
			return current, nil
		}
		if current.version() > tableVersion {
			s.mu.Unlock()
			_ = replacement.close()
			return nil, moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		c = current
	} else {
		c = nil
	}
	s.mu.tables[tableID] = replacement
	replacement.acquire()
	s.mu.Unlock()
	if c != nil {
		c.retire()
	}
	return replacement, nil
}

func (s *service) acquireCommittedTableCache(
	ctx context.Context,
	tableID uint64,
) (incrTableCache, error) {
	for {
		c, err := s.getCommittedTableCache(ctx, tableID)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.mu.tables[tableID] == c {
			c.acquire()
			s.mu.Unlock()
			return c, nil
		}
		s.mu.Unlock()
	}
}

func (s *service) txnClosed(ctx context.Context, txnOp client.TxnOperator, event client.TxnEvent, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handleCreatesLocked(event.Txn)
	s.handleDeletesLocked(event.Txn)
	return nil
}

func (s *service) handleCreatesLocked(txnMeta txn.TxnMeta) {
	key := string(txnMeta.ID)
	tables, ok := s.mu.creates[key]
	if !ok {
		return
	}

	for _, id := range tables {
		if tc, ok := s.mu.tables[id]; ok {
			if txnMeta.Status == txn.TxnStatus_Committed {
				tc.commit()
			} else {
				tc.retire()
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
}

func (s *service) handleDeletesLocked(txnMeta txn.TxnMeta) {
	key := string(txnMeta.ID)
	tables, ok := s.mu.deletes[key]
	if !ok {
		return
	}

	if txnMeta.Status == txn.TxnStatus_Committed {
		for _, ctx := range tables {
			if tc, ok := s.mu.tables[ctx.tableID]; ok {
				tc.retire()
				delete(s.mu.tables, ctx.tableID)
				s.mu.destroyed[ctx.tableID] = ctx
				s.logger.Info(
					"incrservice.cache.deleted",
					zap.Uint64("table-id", ctx.tableID),
					zap.String("txn", hex.EncodeToString(txnMeta.ID)),
				)
			}
		}
	}
	delete(s.mu.deletes, key)
}

func (s *service) getTableCache(tableID uint64) incrTableCache {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.mu.tables[tableID]
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
