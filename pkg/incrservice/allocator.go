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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

func (a *allocator) allocate(
	ctx context.Context,
	tableID uint64,
	key string,
	count int,
	txnOp client.TxnOperator) (uint64, uint64, error) {
	c := make(chan struct{})
	//UT test find race here
	var from, to atomic.Uint64
	var err error
	var err2 atomic.Value
	err = a.asyncAllocate(
		ctx,
		tableID,
		key,
		count,
		txnOp,
		func(
			v1, v2 uint64,
			e error) {
			from.Store(v1)
			to.Store(v2)
			if e != nil {
				err2.Store(e)
			}
			close(c)
		})
	if err2.Load() != nil && err2.Load().(error) != nil {
		err = err2.Load().(error)
	}
	if err != nil {
		return 0, 0, err
	}
	<-c
	return from.Load(), to.Load(), err
}

func (a *allocator) asyncAllocate(
	ctx context.Context,
	tableID uint64,
	col string,
	count int,
	txnOp client.TxnOperator,
	apply func(uint64, uint64, error)) error {
	accountId, err := getAccountID(ctx)
	if err != nil {
		return err
	}
	a.c <- action{
		txnOp:         txnOp,
		accountID:     accountId,
		actionType:    allocType,
		tableID:       tableID,
		col:           col,
		count:         count,
		applyAllocate: apply}
	return nil
}

func (a *allocator) updateMinValue(
	ctx context.Context,
	tableID uint64,
	col string,
	minValue uint64,
	txnOp client.TxnOperator) error {
	var err error
	var accountId uint32
	accountId, err = getAccountID(ctx)
	if err != nil {
		return err
	}
	c := make(chan struct{})
	fn := func(e error) {
		err = e
		close(c)
	}
	a.c <- action{
		txnOp:       txnOp,
		accountID:   accountId,
		actionType:  updateType,
		tableID:     tableID,
		col:         col,
		minValue:    minValue,
		applyUpdate: fn,
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
	ctx := defines.AttachAccountId(context.Background(), act.accountID)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	from, to, err := a.store.Allocate(
		ctx,
		act.tableID,
		act.col,
		act.count,
		act.txnOp)
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
	ctx := defines.AttachAccountId(context.Background(), act.accountID)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	err := a.store.UpdateMinValue(
		ctx,
		act.tableID,
		act.col,
		act.minValue,
		act.txnOp)
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
	txnOp         client.TxnOperator
	accountID     uint32
	actionType    int
	tableID       uint64
	col           string
	count         int
	minValue      uint64
	applyAllocate func(uint64, uint64, error)
	applyUpdate   func(error)
}

func getAccountID(ctx context.Context) (uint32, error) {
	return defines.GetAccountId(ctx)
}
