// Copyright 2026 Matrix Origin
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

package table_function

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type currentRolesCacheKey struct {
	service          string
	identity         process.CurrentRoleContext
	txnID            string
	snapshotPhysical int64
	snapshotLogical  uint32
	snapshotNode     uint32
	snapshotOffset   int
	writeOffset      uint64
}

type currentRolesCacheContextKey struct{}

// CurrentRolesExecutionCache is owned by one physical query execution.
type CurrentRolesExecutionCache struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	state  uint8 // empty, loading, ready, closed
	key    currentRolesCacheKey
	roles  []int64
	err    error
}

func NewCurrentRolesExecutionCache(ctx context.Context) *CurrentRolesExecutionCache {
	ctx, cancel := context.WithCancel(ctx)
	return &CurrentRolesExecutionCache{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// HasCurrentRolesExecutionCache reports whether an ancestor has an entry.
func HasCurrentRolesExecutionCache(ctx context.Context) bool {
	cache, _ := ctx.Value(currentRolesCacheContextKey{}).(*CurrentRolesExecutionCache)
	return cache != nil
}

// WithCurrentRolesExecutionCache also accepts nil, masking any parent entry.
func WithCurrentRolesExecutionCache(ctx context.Context, cache *CurrentRolesExecutionCache) context.Context {
	return context.WithValue(ctx, currentRolesCacheContextKey{}, cache)
}

// CloseCurrentRolesExecutionCache retires the cache associated with this execution.
// Pipeline cancellation alone must not call this: another consumer may still run.
func CloseCurrentRolesExecutionCache(ctx context.Context, cause error) {
	if ctx == nil {
		return
	}
	cache, _ := ctx.Value(currentRolesCacheContextKey{}).(*CurrentRolesExecutionCache)
	cache.close(cause)
}

func (c *CurrentRolesExecutionCache) Close() {
	c.close(nil)
}

func (c *CurrentRolesExecutionCache) close(cause error) {
	if c == nil {
		return
	}
	defer c.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == 3 {
		return
	}
	if cause == nil {
		cause = context.Canceled
	}
	if c.state != 2 {
		close(c.done)
	}
	c.state = 3
	c.roles = nil
	c.err = cause
	c.key = currentRolesCacheKey{}
}

func currentRolesKey(proc *process.Process) (currentRolesCacheKey, error) {
	var key currentRolesCacheKey
	if proc == nil || proc.Ctx == nil || proc.Base == nil {
		return key, moerr.NewInternalErrorNoCtx("current roles process unavailable")
	}
	if err := proc.Ctx.Err(); err != nil {
		return key, err
	}
	key.identity = proc.GetSessionInfo().CurrentRoleContext
	if err := validateCurrentRoleContext(proc.Ctx, key.identity); err != nil {
		return key, err
	}
	txn := proc.GetTxnOperator()
	if txn == nil || txn.GetWorkspace() == nil {
		return key, moerr.NewInternalError(proc.Ctx, "current roles requires an execution transaction and workspace")
	}
	meta := txn.Txn()
	key.service = proc.GetService()
	key.txnID = string(meta.ID)
	key.snapshotPhysical = meta.SnapshotTS.PhysicalTime
	key.snapshotLogical = meta.SnapshotTS.LogicalTime
	key.snapshotNode = meta.SnapshotTS.NodeID
	key.snapshotOffset = txn.GetWorkspace().GetSnapshotWriteOffset()
	key.writeOffset = txn.GetWorkspace().WriteOffset()
	return key, nil
}

func (c *CurrentRolesExecutionCache) get(ctx context.Context, key currentRolesCacheKey, load func(context.Context) ([]int64, error)) ([]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.state == 3 {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	if c.state != 0 && c.key != key {
		c.mu.Unlock()
		return nil, moerr.NewInternalError(ctx, "current roles execution cache identity or read view changed")
	}
	if c.state == 0 {
		c.key, c.state = key, 1
		c.mu.Unlock()
		roles, err := func() (roles []int64, err error) {
			defer func() {
				if v := recover(); v != nil {
					roles = nil
					err = moerr.ConvertPanicError(c.ctx, v)
				}
			}()
			return load(c.ctx)
		}()
		c.mu.Lock()
		if c.state != 3 {
			if ctxErr := c.ctx.Err(); ctxErr != nil {
				roles, err = nil, ctxErr
			}
			if err != nil {
				roles = nil
			}
			c.roles, c.err, c.state = roles, err, 2
			close(c.done)
		}
		c.mu.Unlock()
	} else {
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
	case <-c.done:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == 3 {
		return nil, c.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	return c.roles, c.err
}

func getCurrentRoles(proc *process.Process) ([]int64, error) {
	load := func(ctx context.Context) ([]int64, error) {
		copyProc := *proc
		copyProc.Ctx, copyProc.Cancel = ctx, nil
		roots, err := loadCurrentRoleRoots(&copyProc)
		if err != nil {
			return nil, err
		}
		return currentRoleClosure(&copyProc, roots, expandCurrentRoleFrontier)
	}
	cache, _ := proc.Ctx.Value(currentRolesCacheContextKey{}).(*CurrentRolesExecutionCache)
	if cache == nil {
		return load(proc.Ctx)
	}
	key, err := currentRolesKey(proc)
	if err != nil {
		return nil, err
	}
	return cache.get(proc.Ctx, key, load)
}
