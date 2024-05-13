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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type tableCache struct {
	logger  *log.MOLogger
	tableID uint64
	cols    []AutoColumn

	mu struct {
		sync.RWMutex
		committed bool
		txnOp     client.TxnOperator
		cols      map[string]*columnCache
	}
}

func newTableCache(
	ctx context.Context,
	tableID uint64,
	cols []AutoColumn,
	cfg Config,
	allocator valueAllocator,
	txnOp client.TxnOperator,
	committed bool) (incrTableCache, error) {
	c := &tableCache{
		logger:  getLogger(),
		tableID: tableID,
		cols:    cols,
	}
	c.mu.cols = make(map[string]*columnCache, 1)
	c.mu.txnOp = txnOp
	c.mu.committed = committed
	for _, col := range cols {
		cc, err := newColumnCache(
			ctx,
			tableID,
			col,
			cfg,
			committed,
			allocator,
			txnOp)
		if err != nil {
			return nil, err
		}
		c.mu.cols[col.ColName] = cc
	}
	return c, nil
}

func (c *tableCache) commit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.committed {
		panic("commit already committed cache")
	}
	c.mu.committed = true
	c.mu.txnOp = nil
	for _, col := range c.mu.cols {
		col.Lock()
		col.committed = true
		col.Unlock()
	}
}

func (c *tableCache) getTxn() client.TxnOperator {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.txnOp
}

func (c *tableCache) insertAutoValues(
	ctx context.Context,
	tableID uint64,
	bat *batch.Batch,
	estimate int64,
) (uint64, error) {
	lastInsert := uint64(0)
	txnOp := c.getTxn()
	for _, col := range c.cols {
		cc := c.getColumnCache(col.ColName)
		if cc == nil {
			panic("column cache should not be nil, " + col.ColName)
		}

		if estimate > int64(cc.cfg.CountPerAllocate) {
			cc.preAllocate(ctx, tableID, int(estimate), txnOp)
		}

		rows := bat.RowCount()
		vec := bat.GetVector(int32(col.ColIndex))
		if v, err := cc.insertAutoValues(ctx, tableID, vec, rows, txnOp); err != nil {
			return 0, err
		} else {
			lastInsert = v
		}
	}
	return lastInsert, nil
}

func (c *tableCache) currentValue(
	ctx context.Context,
	tableID uint64,
	targetCol string) (uint64, error) {
	for _, col := range c.cols {
		if col.ColName == targetCol {
			cc := c.getColumnCache(col.ColName)
			if cc == nil {
				panic("column cache should not be nil, " + col.ColName)
			}
			return cc.current(ctx)
		}
	}
	return 0, nil
}

func (c *tableCache) table() uint64 {
	return c.tableID
}

func (c *tableCache) columns() []AutoColumn {
	return c.cols
}

func (c *tableCache) getColumnCache(col string) *columnCache {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.cols[col]
}

func (c *tableCache) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.mu.cols {
		if err := cc.close(); err != nil {
			return err
		}
	}
	return nil
}

func (c *tableCache) adjust(
	ctx context.Context,
	cols []AutoColumn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx := range c.cols {
		v, err := c.mu.cols[c.cols[idx].ColName].current(ctx)
		if err != nil {
			return err
		}
		if v > 0 {
			cols[idx].Offset = v - 1
		}
	}
	return nil
}
