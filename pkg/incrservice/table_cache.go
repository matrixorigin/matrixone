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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type tableCache struct {
	logger  *log.MOLogger
	tableID uint64

	mu struct {
		sync.RWMutex
		cols map[string]*columnCache
	}
}

// compatible with the current design of hidden tables with
// auto-incrementing columns
func getStoreKey(tableID uint64, col string) string {
	return fmt.Sprintf("%d_%s", tableID, col)
}

func newTableCache(
	ctx context.Context,
	tableID uint64,
	autoColNames []string,
	steps []int,
	count int,
	allocator valueAllocator) incrTableCache {
	c := &tableCache{
		logger:  getLogger(),
		tableID: tableID,
	}
	c.mu.cols = make(map[string]*columnCache, 1)
	for i, col := range autoColNames {
		key := getStoreKey(tableID, col)
		c.mu.cols[col] = newColumnCache(ctx, key, count, steps[i], allocator)
	}
	return c
}

func (c *tableCache) insertAutoValues(
	ctx context.Context,
	tabelDef *plan.TableDef,
	bat *batch.Batch) error {
	for i, col := range tabelDef.Cols {
		if col.Typ.AutoIncr {
			cc := c.getColumnCache(col.Name)
			if cc == nil {
				panic("column cache should not be nil, " + col.Name)
			}
			rows := bat.Length()
			vec := bat.GetVector(int32(i))
			if err := cc.insertAutoValues(ctx, vec, rows); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *tableCache) table() uint64 {
	return c.tableID
}

func (c *tableCache) keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.mu.cols))
	for k := range c.mu.cols {
		keys = append(keys, getStoreKey(c.tableID, k))
	}
	return keys
}

func (c *tableCache) getColumnCache(col string) *columnCache {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.cols[col]
}
