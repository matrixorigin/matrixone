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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"go.uber.org/zap"
)

type allocator struct {
	logger  *log.MOLogger
	store   IncrValueStore
	c       chan action
	stopper *stopper.Stopper
}

func newValueAllocator(store IncrValueStore) valueAllocator {
	a := &allocator{
		logger:  getLogger(),
		c:       make(chan action, 1024),
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
	c := make(chan struct{})
	var from, to uint64
	var err error
	a.asyncAlloc(
		ctx,
		tableID,
		key,
		count,
		func(
			v1, v2 uint64,
			e error) {
			from = v1
			to = v2
			err = e
			close(c)
		})
	<-c
	return from, to, err
}

func (a *allocator) asyncAlloc(
	ctx context.Context,
	tableID uint64,
	col string,
	count int,
	apply func(uint64, uint64, error)) {
	select {
	case <-ctx.Done():
		apply(0, 0, ctx.Err())
	case a.c <- action{
		actionType:    allocType,
		tableID:       tableID,
		col:           col,
		count:         count,
		applyAllocate: apply}:
	}
}

func (a *allocator) updateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64) error {
	var err error
	c := make(chan struct{})
	fn := func(e error) {
		err = e
		close(c)
	}
	select {
	case <-ctx.Done():
		fn(ctx.Err())
	case a.c <- action{
		actionType:  updateType,
		tableID:     tableID,
		col:         col,
		minValue:    minValue,
		applyUpdate: fn,
	}:
	}
	<-c
	return err
}

func (a *allocator) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case act := <-a.c:
			switch act.actionType {
			case allocType:
				a.doAllocate(act)
			case updateType:
				a.doUpdate(act)
			}
		}
	}
}

func (a *allocator) doAllocate(act action) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	from, to, err := a.store.Alloc(
		ctx,
		act.tableID,
		act.col,
		act.count)
	if a.logger.Enabled(zap.DebugLevel) {
		a.logger.Debug(
			"allocate new range",
			zap.String("key", act.col),
			zap.Int("count", act.count),
			zap.Uint64("value", from),
			zap.Uint64("next", to),
			zap.Error(err))
	}
	act.applyAllocate(from, to, err)
}

func (a *allocator) doUpdate(act action) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err := a.store.UpdateMinValue(
		ctx,
		act.tableID,
		act.col,
		act.minValue)
	if a.logger.Enabled(zap.DebugLevel) {
		a.logger.Debug(
			"update range min value",
			zap.String("key", act.col),
			zap.Int("count", act.count),
			zap.Uint64("min-value", act.minValue),
			zap.Error(err))
	}
	act.applyUpdate(err)
}

func (a *allocator) close() {
	a.stopper.Stop()
	close(a.c)
}

var (
	allocType  = 0
	updateType = 1
)

type action struct {
	actionType    int
	tableID       uint64
	col           string
	count         int
	minValue      uint64
	applyAllocate func(uint64, uint64, error)
	applyUpdate   func(error)
}
