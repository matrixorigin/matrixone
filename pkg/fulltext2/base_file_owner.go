// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package fulltext2

import (
	"context"
	"errors"
	"sync"
)

var (
	errBaseFileOwnerClosed  = errors.New("fulltext2 base file owner is closed")
	errBaseFileOwnerPending = errors.New("fulltext2 base file owner still owns resources")
)

// baseFileOwner is the lifetime owner for an experiment-scoped pool. Search
// handles borrow it; they never close it. One owner is intended to correspond
// to one CN service runtime, so deferred mappings and pinned files remain
// reachable even when a cache entry drops its Search handle.
type baseFileOwner struct {
	mu         sync.Mutex
	closeMu    sync.Mutex
	cond       *sync.Cond
	pool       *baseFilePool
	active     int
	closing    bool
	nextOp     uint64
	operations map[uint64]context.CancelFunc
}

func newBaseFileOwner(maxBytes int64, maxFiles int) *baseFileOwner {
	o := &baseFileOwner{pool: newBaseFilePool(maxBytes, maxFiles), operations: make(map[uint64]context.CancelFunc)}
	o.cond = sync.NewCond(&o.mu)
	return o
}

func (o *baseFileOwner) poolForSearch() (*baseFilePool, error) {
	if o == nil {
		return nil, errBaseFileOwnerClosed
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing || o.pool == nil {
		return nil, errBaseFileOwnerClosed
	}
	return o.pool, nil
}

func (o *baseFileOwner) isClosing() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.closing || o.pool == nil
}

// cancelOperations closes admission and cancels complete operations without
// waiting for cache entries, active work, or pool/OS cleanup. Global shutdown
// must cancel every owner before starting any of those destructive waits.
func (o *baseFileOwner) cancelOperations() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.closing = true
	cancels := make([]context.CancelFunc, 0, len(o.operations))
	for _, cancel := range o.operations {
		cancels = append(cancels, cancel)
	}
	o.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// beginClose preserves the dedicated service-close pool-fill cancellation
// contract, in addition to closing complete-operation admission before drain.
func (o *baseFileOwner) beginClose() {
	if o == nil {
		return
	}
	o.cancelOperations()
	o.mu.Lock()
	p := o.pool
	o.mu.Unlock()
	if p != nil {
		p.Close()
	}
}

// run admits one complete storage operation, including an ordinary-loader
// fallback. Close waits for this count to drain, so a fallback cannot begin
// after shutdown has passed its admission gate.
func (o *baseFileOwner) run(ctx context.Context, fn func(context.Context) (*Segment, error)) (*Segment, error) {
	var seg *Segment
	err := o.runOperation(ctx, func(ctx context.Context) error {
		var err error
		seg, err = fn(ctx)
		return err
	})
	return seg, err
}

// runOperation covers the entire search load, including reads before/after Base
// materialization. Close cancels and waits for these operations before draining
// cached search objects. Nested Base operations retain their standalone contract.
func (o *baseFileOwner) runOperation(ctx context.Context, fn func(context.Context) error) error {
	if o == nil {
		return errBaseFileOwnerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	if o.closing || o.pool == nil {
		o.mu.Unlock()
		return errBaseFileOwnerClosed
	}
	p := o.pool
	o.mu.Unlock()
	// Pool admission can wait for retained-file cleanup. Never carry the
	// owner lock through that wait: cancelOperations must issue cancellation
	// before any pool/OS cleanup wait. Recheck ownership after the pool gate.
	if err := p.mappingAdmissionError(); err != nil {
		return err
	}
	o.mu.Lock()
	if o.closing || o.pool != p {
		o.mu.Unlock()
		return errBaseFileOwnerClosed
	}
	opCtx, cancel := context.WithCancel(ctx)
	o.nextOp++
	opID := o.nextOp
	o.operations[opID] = cancel
	o.active++
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		delete(o.operations, opID)
		o.active--
		if o.active == 0 {
			o.cond.Broadcast()
		}
		o.mu.Unlock()
	}()
	defer cancel()
	select {
	case <-opCtx.Done():
		return context.Cause(opCtx)
	default:
	}
	return fn(opCtx)
}

func (o *baseFileOwner) hasResources() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	p := o.pool
	o.mu.Unlock()
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes != 0 || p.reserved != 0 || p.reservedFiles != 0 || p.deferredBytes != 0 ||
		p.openFiles != 0 || p.deferredSegmentBytes != 0 || len(p.entries) != 0 ||
		len(p.deferredSegments) != 0 || len(p.deferredMappings) != 0
}

// close is idempotent. A close that cannot finish returns a typed pending
// error; a later close after Search handles release their leases retries the
// same owner rather than losing the only cleanup reference.
func (o *baseFileOwner) close() error {
	if o == nil {
		return nil
	}
	o.closeMu.Lock()
	defer o.closeMu.Unlock()
	// close may be called directly by a service owner, without the cache
	// drain's separate beginClose step.  Publish the same admission barrier and
	// cancel every already-admitted operation before waiting for active work;
	// otherwise an ordinary-loader fallback can remain blocked in source I/O
	// and make owner shutdown wait forever.
	o.beginClose()
	o.mu.Lock()
	if o.pool == nil {
		o.mu.Unlock()
		return nil
	}
	p := o.pool
	o.mu.Unlock()

	o.mu.Lock()
	for o.active != 0 {
		o.cond.Wait()
	}
	o.mu.Unlock()
	p.retryDeferred()
	if o.hasResources() {
		return errBaseFileOwnerPending
	}
	o.mu.Lock()
	o.pool = nil
	o.mu.Unlock()
	return nil
}
