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

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"go.uber.org/zap"
)

type alloc struct {
	ctx     context.Context
	tableID uint64
	key     string
	count   int
	apply   func(uint64, uint64, error)
}

type allocator struct {
	logger  *log.MOLogger
	store   IncrValueStore
	c       chan alloc
	stopper *stopper.Stopper
}

func newValueAllocator(store IncrValueStore) valueAllocator {
	a := &allocator{
		logger:  getLogger(),
		c:       make(chan alloc, 1024),
		stopper: stopper.NewStopper("valueAllocator"),
		store:   store,
	}
	a.adjust()
	a.stopper.RunTask(a.run)
	return a
}

func (a *allocator) adjust() {
	if a.store == nil {
		a.store = NewMemStore()
	}
}

func (a *allocator) alloc(
	ctx context.Context,
	tableID uint64,
	key string,
	count int) (uint64, uint64, error) {
	select {
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	default:
	}
	return a.store.Alloc(ctx, tableID, key, count)
}

func (a *allocator) asyncAlloc(
	ctx context.Context,
	tableID uint64,
	key string,
	count int,
	apply func(uint64, uint64, error)) {
	select {
	case <-ctx.Done():
		apply(0, 0, ctx.Err())
	case a.c <- alloc{
		ctx:     ctx,
		tableID: tableID,
		key:     key,
		count:   count,
		apply:   apply,
	}:
	}
}

func (a *allocator) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case alloc := <-a.c:
			v, next, err := a.alloc(
				alloc.ctx,
				alloc.tableID,
				alloc.key,
				alloc.count)
			if a.logger.Enabled(zap.DebugLevel) {
				a.logger.Debug(
					"allocate new range",
					zap.String("key", alloc.key),
					zap.Int("count", alloc.count),
					zap.Uint64("value", v),
					zap.Uint64("next", next),
					zap.Error(err))
			}
			alloc.apply(v, next, err)
		}
	}
}

func (a *allocator) close() {
	a.stopper.Stop()
	close(a.c)
}
