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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

type service struct {
	logger        *log.MOLogger
	cacheCapacity int
	store         IncrValueStore
	allocator     valueAllocator
	deleteC       chan []string
	stopper       *stopper.Stopper

	mu struct {
		sync.RWMutex
		closed  bool
		tables  map[uint64]incrTableCache
		creates map[string][]incrTableCache
		deletes map[string][]plan.TableDef
	}
}

func NewIncrService(
	store IncrValueStore,
	cacheCapacity int) AutoIncrementService {
	logger := getLogger()
	s := &service{
		logger:        logger,
		cacheCapacity: cacheCapacity,
		store:         store,
		allocator:     newValueAllocator(store),
		deleteC:       make(chan []string, 1024),
		stopper:       stopper.NewStopper("IncrService", stopper.WithLogger(logger.RawLogger())),
	}
	s.mu.tables = make(map[uint64]incrTableCache, 1024)
	s.mu.creates = make(map[string][]incrTableCache, 1024)
	s.mu.deletes = make(map[string][]plan.TableDef, 1024)
	s.stopper.RunTask(s.deleteTableCache)
	return s
}

func (s *service) Create(
	ctx context.Context,
	table *plan.TableDef,
	txnOp client.TxnOperator) error {
	if txnOp == nil {
		panic("txn operator is nil")
	}
	txnOp.(client.EventableTxnOperator).
		AppendEventCallback(
			client.ClosedEvent,
			s.txnClosed)

	return s.doCreate(
		ctx,
		table,
		txnOp.Txn().ID,
		true)
}

func (s *service) Delete(
	ctx context.Context,
	table *plan.TableDef,
	txnOp client.TxnOperator) error {
	if txnOp == nil {
		panic("txn operator is nil")
	}
	txnOp.(client.EventableTxnOperator).
		AppendEventCallback(
			client.ClosedEvent,
			s.txnClosed)

	s.mu.Lock()
	defer s.mu.Unlock()

	key := string(txnOp.Txn().ID)
	s.mu.deletes[key] = append(s.mu.deletes[key], *table)
	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("ready to delete auto increment table cache",
			zap.Uint64("table-id", table.TblId),
			zap.String("txn", hex.EncodeToString(txnOp.Txn().ID)))
	}
	return nil
}

func (s *service) InsertValues(
	ctx context.Context,
	tabelDef *plan.TableDef,
	bat *batch.Batch) error {
	ts := s.getCommittedTableCache(tabelDef.TblId)
	if ts == nil {
		if err := s.newCommittedTableCache(ctx, tabelDef); err != nil {
			return err
		}
		ts = s.getCommittedTableCache(tabelDef.TblId)
	}
	if ts == nil {
		return moerr.NewNoSuchTableNoCtx("", tabelDef.Name)
	}
	return ts.insertAutoValues(ctx, tabelDef, bat)
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
	close(s.deleteC)
	s.allocator.close()
	s.store.Close()
}

func (s *service) doCreate(
	ctx context.Context,
	table *plan.TableDef,
	txnID []byte,
	create bool) error {
	var autoCols []string
	var steps []int
	for _, col := range table.Cols {
		if col.Typ.AutoIncr {
			autoCols = append(autoCols, col.Name)
			// TODO: currently, step not supported in auto increment
			steps = append(steps, 1)
			if create {
				if err := s.store.Create(
					ctx,
					getStoreKey(table.TblId, col.Name),
					0,
					1); err != nil {
					s.logger.Error("create auto increment cache failed",
						zap.String("table", table.Name),
						zap.Uint64("table-id", table.TblId),
						zap.Bool("create", create),
						zap.String("txn", hex.EncodeToString(txnID)),
						zap.Error(err))
					return err
				}
			}
		}
	}

	c := newTableCache(
		ctx,
		table.TblId,
		autoCols,
		steps,
		s.cacheCapacity,
		s.allocator)

	s.mu.Lock()
	defer s.mu.Unlock()
	if create {
		key := string(txnID)
		s.mu.creates[key] = append(s.mu.creates[key], c)
	}
	s.mu.tables[table.TblId] = c
	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("auto increment cache created",
			zap.String("table", table.Name),
			zap.Uint64("table-id", table.TblId),
			zap.Bool("create", create),
			zap.String("txn", hex.EncodeToString(txnID)))
	}
	return nil
}

func (s *service) getCommittedTableCache(tableID uint64) incrTableCache {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.tables[tableID]
}

func (s *service) newCommittedTableCache(
	ctx context.Context,
	tabelDef *plan.TableDef) error {
	return s.doCreate(
		ctx,
		tabelDef,
		nil,
		false)
}

func (s *service) txnClosed(txnMeta txn.TxnMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handleCreatesLocked(txnMeta)
	s.handleDeletesLocked(txnMeta)
}

func (s *service) handleCreatesLocked(txnMeta txn.TxnMeta) {
	key := string(txnMeta.ID)
	tables, ok := s.mu.creates[key]
	if !ok {
		return
	}

	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("destroy creates table auto increment cache",
			zap.String("resean", "txn aborted"),
			zap.String("txn", hex.EncodeToString(txnMeta.ID)),
			zap.Any("tables", tables))
	}
	if txnMeta.Status != txn.TxnStatus_Committed {
		for _, c := range tables {
			s.destroyTableCacheAfterCreatedLocked(c.table())
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

	if s.logger.Enabled(zap.DebugLevel) {
		s.logger.Debug("destroy deletes table auto increment cache",
			zap.String("resean", "txn committed"),
			zap.String("txn", hex.EncodeToString(txnMeta.ID)),
			zap.Any("tables", tables))
	}
	if txnMeta.Status == txn.TxnStatus_Committed {
		for _, def := range tables {
			s.destroyTableCacheLocked(def)
		}
	}
	delete(s.mu.deletes, key)
}

func (s *service) destroyTableCacheAfterCreatedLocked(tableID uint64) {
	c, ok := s.mu.tables[tableID]
	if !ok {
		panic("missing created incr table cache")
	}

	delete(s.mu.tables, c.table())
	s.deleteC <- c.keys()
}

func (s *service) destroyTableCacheLocked(def plan.TableDef) {
	delete(s.mu.tables, def.TblId)
	keys := make([]string, 0, len(def.Cols))
	for _, col := range def.Cols {
		if col.Typ.AutoIncr {
			keys = append(keys, getStoreKey(def.TblId, col.Name))
		}
	}
	s.deleteC <- keys
}

func (s *service) deleteTableCache(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case keys := <-s.deleteC:
			for {
				if err := s.store.Delete(ctx, keys); err == nil {
					break
				}
			}
		}
	}
}
