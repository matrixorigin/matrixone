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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

var (
	lazyDeleteInterval = time.Second * 10
)

type service struct {
	uuid      string
	logger    *log.MOLogger
	cfg       Config
	store     IncrValueStore
	allocator valueAllocator
	stopper   *stopper.Stopper

	mu struct {
		sync.Mutex
		closed    bool
		destroyed map[uint64]deleteCtx
		tables    map[uint64]incrTableCache
		creates   map[string][]uint64
		deletes   map[string][]deleteCtx
	}
}

func NewIncrService(
	uuid string,
	store IncrValueStore,
	cfg Config) AutoIncrementService {
	logger := getLogger()
	cfg.adjust()
	s := &service{
		uuid:      uuid,
		logger:    logger,
		cfg:       cfg,
		store:     store,
		allocator: newValueAllocator(store),
		stopper:   stopper.NewStopper("incr-service", stopper.WithLogger(getLogger().RawLogger())),
	}
	s.mu.destroyed = make(map[uint64]deleteCtx)
	s.mu.tables = make(map[uint64]incrTableCache, 1024)
	s.mu.creates = make(map[string][]uint64, 1024)
	s.mu.deletes = make(map[string][]deleteCtx, 1024)
	if err := s.stopper.RunTask(s.destroyTables); err != nil {
		panic(err)
	}
	return s
}

func (s *service) UUID() string {
	return s.uuid
}

func (s *service) Create(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	txnOp client.TxnOperator) error {
	s.logger.Info("create auto increment table",
		zap.Uint64("table-id", tableID),
		zap.String("txn", txnOp.Txn().DebugString()))

	txnOp.AppendEventCallback(
		client.ClosedEvent,
		s.txnClosed)
	if err := s.store.Create(ctx, tableID, cols, txnOp); err != nil {
		s.logger.Error("create auto increment cache failed",
			zap.Uint64("table-id", tableID),
			zap.String("txn", hex.EncodeToString(txnOp.Txn().ID)),
			zap.Error(err))
		return err
	}
	c, err := newTableCache(
		ctx,
		tableID,
		cols,
		s.cfg,
		s.allocator,
		txnOp,
		false)
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
	s.logger.Info("reset auto increment table",
		zap.Uint64("table-id", oldTableID),
		zap.String("txn", txnOp.Txn().DebugString()),
		zap.Uint64("new-table-id", newTableID))

	cols, err := s.store.GetColumns(ctx, oldTableID, txnOp)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		rows, err := s.store.SelectAll(ctx, oldTableID, txnOp)
		if err != nil {
			return err
		}
		s.logger.Info("no columns found",
			zap.Uint64("table-id", oldTableID),
			zap.String("txn", txnOp.Txn().DebugString()),
			zap.String("rows", rows))
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
		s.txnClosed)

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

func (s *service) InsertValues(
	ctx context.Context,
	tableID uint64,
	bat *batch.Batch,
	estimate int64,
) (uint64, error) {
	ts, err := s.getCommittedTableCache(
		ctx,
		tableID)
	if err != nil {
		return 0, err
	}
	return ts.insertAutoValues(
		ctx,
		tableID,
		bat,
		estimate,
	)
}

func (s *service) CurrentValue(
	ctx context.Context,
	tableID uint64,
	col string) (uint64, error) {
	ts, err := s.getCommittedTableCache(
		ctx,
		tableID)
	if err != nil {
		return 0, err
	}
	return ts.currentValue(ctx, tableID, col)
}

func (s *service) Close() {
	s.stopper.Stop()

	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return
	}
	s.mu.closed = true
	for _, tc := range s.mu.tables {
		if err := tc.close(); err != nil {
			panic(err)
		}
	}
	s.mu.Unlock()

	s.allocator.close()
	s.store.Close()
}

func (s *service) doCreateLocked(
	tableID uint64,
	c incrTableCache,
	txnID []byte) error {
	s.mu.tables[tableID] = c
	if s.logger.Enabled(zap.InfoLevel) {
		s.logger.Info("auto increment cache created",
			zap.Uint64("table-id", tableID),
			zap.String("txn", hex.EncodeToString(txnID)))
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

	txnOp := s.store.NewTxnOperator(ctx)
	if txnOp != nil {
		defer txnOp.Rollback(ctx)
	}
	s.logger.Info("try to get columns", zap.Uint64("tableId", tableID), zap.String("txn", txnOp.Txn().DebugString()))

	cols, err := s.store.GetColumns(ctx, tableID, txnOp)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		table, err := s.store.SelectAll(ctx, tableID, txnOp)
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewNoSuchTableNoCtx("", table)
	}

	c, err = newTableCache(
		ctx,
		tableID,
		cols,
		s.cfg,
		s.allocator,
		nil,
		true)
	if err != nil {
		return nil, err
	}
	s.doCreateLocked(tableID, c, nil)
	return c, nil
}

func (s *service) txnClosed(event client.TxnEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handleCreatesLocked(event.Txn)
	s.handleDeletesLocked(event.Txn)
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
				_ = tc.close()
				delete(s.mu.tables, id)
				s.logger.Info("auto increment cache destroyed with txn aborted",
					zap.Uint64("table-id", id),
					zap.String("txn", hex.EncodeToString(txnMeta.ID)))

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
				_ = tc.close()
				delete(s.mu.tables, ctx.tableID)
				s.mu.destroyed[ctx.tableID] = ctx
				s.logger.Info("auto increment cache delete",
					zap.Uint64("table-id", ctx.tableID),
					zap.String("txn", hex.EncodeToString(txnMeta.ID)))

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
				ctx, cancel := context.WithTimeout(defines.AttachAccountId(ctx, dc.accountID), time.Second*30)
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
