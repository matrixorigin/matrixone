// Copyright 2021 - 2022 Matrix Origin
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

package tnservice

import (
	"context"
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/service"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/util"
)

// replica tn shard replica.
type replica struct {
	rt           runtime.Runtime
	logger       *log.MOLogger
	shard        metadata.TNShard
	service      service.TxnService
	serviceC     chan struct{}
	startedC     chan struct{}
	createCtx    context.Context
	cancelCreate context.CancelFunc
	startedOnce  sync.Once

	mu struct {
		sync.RWMutex
		starting        bool
		cancelled       bool
		destroyOnCancel bool
		startErr        error
	}
}

func newReplica(shard metadata.TNShard, rt runtime.Runtime) *replica {
	ctx, cancel := context.WithCancel(context.Background())
	return &replica{
		rt:           rt,
		shard:        shard,
		logger:       rt.Logger().With(util.TxnTNShardField(shard)),
		serviceC:     make(chan struct{}),
		startedC:     make(chan struct{}),
		createCtx:    ctx,
		cancelCreate: cancel,
	}
}

func (r *replica) start(txnService service.TxnService) error {
	if !r.reserveStart() {
		return r.createCtx.Err()
	}
	return r.startReserved(txnService)
}

func (r *replica) reserveStart() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.starting || r.mu.cancelled {
		return false
	}
	r.mu.starting = true
	return true
}

func (r *replica) startReserved(txnService service.TxnService) error {
	r.mu.Lock()
	if !r.mu.starting {
		r.mu.Unlock()
		return context.Canceled
	}
	r.service = txnService
	r.mu.Unlock()
	close(r.serviceC)

	err := txnService.Start()
	r.finishStart(err)
	return err
}

func (r *replica) finishStart(err error) {
	r.startedOnce.Do(func() {
		r.mu.Lock()
		r.mu.startErr = err
		r.mu.Unlock()
		close(r.startedC)
	})
}

func (r *replica) cancelStart(destroy bool) {
	r.mu.Lock()
	r.mu.cancelled = true
	r.mu.destroyOnCancel = r.mu.destroyOnCancel || destroy
	r.mu.Unlock()
	r.cancelCreate()
}

func (r *replica) closeStorage(txnStorage storage.TxnStorage) error {
	r.mu.RLock()
	destroy := r.mu.destroyOnCancel
	r.mu.RUnlock()
	if destroy {
		return txnStorage.Destroy(context.Background())
	}
	return txnStorage.Close(context.Background())
}

func (r *replica) close(destroy bool) error {
	r.mu.RLock()
	starting := r.mu.starting
	r.mu.RUnlock()
	if !starting {
		return nil
	}
	select {
	case <-r.serviceC:
	case <-r.startedC:
	}
	r.mu.RLock()
	txnService := r.service
	r.mu.RUnlock()
	if txnService == nil {
		return r.waitStarted(context.Background())
	}
	txnService.CancelRecovery()
	startErr := r.waitStarted(context.Background())
	return errors.Join(startErr, txnService.Close(destroy))
}

func (r *replica) handleLocalRequest(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	if err := r.waitStarted(ctx); err != nil {
		return err
	}
	prepareResponse(request, response)

	switch request.Method {
	case txn.TxnMethod_GetStatus:
		return r.service.GetStatus(ctx, request, response)
	case txn.TxnMethod_Prepare:
		return r.service.Prepare(ctx, request, response)
	case txn.TxnMethod_CommitTNShard:
		return r.service.CommitTNShard(ctx, request, response)
	case txn.TxnMethod_RollbackTNShard:
		return r.service.RollbackTNShard(ctx, request, response)
	default:
		return moerr.NewNotSupportedf(ctx, "unknown txn request method: %s", request.Method.String())
	}
}

func (r *replica) waitStarted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.startedC:
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.mu.startErr
	}
}
