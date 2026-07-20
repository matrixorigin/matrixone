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
	closeOnce    sync.Once
	closeErr     error

	mu struct {
		sync.RWMutex
		cond            *sync.Cond
		starting        bool
		cancelled       bool
		destroyOnCancel bool
		startErr        error
		activeCalls     int
	}
}

type txnServiceLease struct {
	replica               *replica
	service               service.TxnService
	ctx                   context.Context
	cancel                context.CancelFunc
	stopCancelPropagation func() bool
	releaseOnce           sync.Once
}

func (l *txnServiceLease) release() {
	l.releaseOnce.Do(func() {
		l.stopCancelPropagation()
		l.cancel()
		l.replica.mu.Lock()
		l.replica.mu.activeCalls--
		if l.replica.mu.activeCalls == 0 {
			l.replica.mu.cond.Broadcast()
		}
		l.replica.mu.Unlock()
	})
}

func newReplica(shard metadata.TNShard, rt runtime.Runtime) *replica {
	ctx, cancel := context.WithCancel(context.Background())
	r := &replica{
		rt:           rt,
		shard:        shard,
		logger:       rt.Logger().With(util.TxnTNShardField(shard)),
		serviceC:     make(chan struct{}),
		startedC:     make(chan struct{}),
		createCtx:    ctx,
		cancelCreate: cancel,
	}
	r.mu.cond = sync.NewCond(&r.mu.RWMutex)
	return r
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
	r.cancelStart(destroy)
	r.closeOnce.Do(func() {
		r.closeErr = r.closeOnceFn()
	})
	return r.closeErr
}

func (r *replica) closeOnceFn() error {
	r.mu.RLock()
	starting := r.mu.starting
	r.mu.RUnlock()
	if !starting {
		return nil
	}
	// Recovery may block Start indefinitely while waiting for a participant.
	// Wait only until the service has been published, then cancel recovery
	// before waiting for Start to finish.
	select {
	case <-r.serviceC:
	case <-r.startedC:
	}
	r.mu.RLock()
	txnService := r.service
	r.mu.RUnlock()
	if txnService != nil {
		txnService.CancelRecovery()
	}

	r.waitStartCompleted()
	r.mu.Lock()
	for r.mu.activeCalls > 0 {
		r.mu.cond.Wait()
	}
	startErr := r.mu.startErr
	txnService = r.service
	destroy := r.mu.destroyOnCancel
	r.mu.Unlock()
	if txnService == nil {
		return startErr
	}
	return errors.Join(startErr, txnService.Close(destroy))
}

func (r *replica) started() bool {
	select {
	case <-r.startedC:
		r.mu.RLock()
		defer r.mu.RUnlock()
		return !r.mu.cancelled && r.mu.startErr == nil && r.service != nil
	default:
		return false
	}
}

func (r *replica) handleLocalRequest(ctx context.Context, request *txn.TxnRequest, response *txn.TxnResponse) error {
	lease, err := r.acquireService(ctx)
	if err != nil {
		return err
	}
	defer lease.release()
	prepareResponse(request, response)

	switch request.Method {
	case txn.TxnMethod_GetStatus:
		return lease.service.GetStatus(lease.ctx, request, response)
	case txn.TxnMethod_Prepare:
		return lease.service.Prepare(lease.ctx, request, response)
	case txn.TxnMethod_CommitTNShard:
		return lease.service.CommitTNShard(lease.ctx, request, response)
	case txn.TxnMethod_RollbackTNShard:
		return lease.service.RollbackTNShard(lease.ctx, request, response)
	default:
		return moerr.NewNotSupportedf(lease.ctx, "unknown txn request method: %s", request.Method.String())
	}
}

func (r *replica) waitStarted(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := context.Cause(r.createCtx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.createCtx.Done():
		return context.Cause(r.createCtx)
	case <-r.startedC:
		if err := context.Cause(ctx); err != nil {
			return err
		}
		r.mu.RLock()
		defer r.mu.RUnlock()
		if r.mu.cancelled {
			return context.Canceled
		}
		return r.mu.startErr
	}
}

// acquireService pins the transaction service until release is called. Closing
// a replica cancels the call context and waits for all acquired services to be
// released before closing the underlying storage.
func (r *replica) acquireService(ctx context.Context) (*txnServiceLease, error) {
	if err := r.waitStarted(ctx); err != nil {
		return nil, err
	}

	r.mu.Lock()
	if err := context.Cause(ctx); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if r.mu.cancelled {
		r.mu.Unlock()
		return nil, context.Canceled
	}
	if r.mu.startErr != nil {
		err := r.mu.startErr
		r.mu.Unlock()
		return nil, err
	}
	if r.service == nil {
		r.mu.Unlock()
		return nil, context.Canceled
	}
	txnService := r.service
	r.mu.activeCalls++
	r.mu.Unlock()

	callCtx, cancel := context.WithCancel(ctx)
	stopCancelPropagation := context.AfterFunc(r.createCtx, cancel)
	return &txnServiceLease{
		replica:               r,
		service:               txnService,
		ctx:                   callCtx,
		cancel:                cancel,
		stopCancelPropagation: stopCancelPropagation,
	}, nil
}
