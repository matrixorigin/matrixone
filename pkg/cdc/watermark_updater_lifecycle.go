// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

// Start borrows this CN generation. A stopped updater cannot be reopened.
func (u *CDCWatermarkUpdater) Start(ctx context.Context) error {
	u.lifecycle.mu.Lock()
	defer u.lifecycle.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if u.lifecycle.sealed {
		return sm.ErrClose
	}
	u.queue.Start()
	u.cronExecutor.Start()
	return nil
}

// CloseAndWait seals I/O before joining the existing workers. A timeout does
// not certify completion: the owner must retain the updater's dependencies.
func (u *CDCWatermarkUpdater) CloseAndWait(ctx context.Context) error {
	u.lifecycle.mu.Lock()
	if !u.lifecycle.sealed {
		u.lifecycle.sealed = true
		u.lifecycle.cancel()
		go func() {
			u.cronExecutor.Stop()
			u.lifecycle.io.Wait()
			u.queue.Stop()
			close(u.lifecycle.drained)
		}()
	}
	u.lifecycle.mu.Unlock()
	select {
	case <-u.lifecycle.drained:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (u *CDCWatermarkUpdater) Stop() {
	_ = u.CloseAndWait(context.Background())
}

// beginIO is shared by direct SQL, queued persistence, owner checks and queue
// publication. The gate never holds its mutex while executing downstream work.
func (u *CDCWatermarkUpdater) beginIO(ctx context.Context) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	u.lifecycle.mu.Lock()
	if u.lifecycle.sealed {
		u.lifecycle.mu.Unlock()
		return nil, nil, sm.ErrClose
	}
	if err := ctx.Err(); err != nil {
		u.lifecycle.mu.Unlock()
		return nil, nil, context.Cause(ctx)
	}
	u.lifecycle.io.Add(1)
	u.lifecycle.mu.Unlock()
	linked, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(u.lifecycle.ctx, cancel)
	return linked, func() {
		stop()
		cancel()
		u.lifecycle.io.Done()
	}, nil
}

func (u *CDCWatermarkUpdater) execSQL(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	ctx, done, err := u.beginIO(ctx)
	if err != nil {
		return err
	}
	defer done()
	return u.ie.Exec(ctx, sql, opts)
}

func (u *CDCWatermarkUpdater) querySQL(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (ie.InternalExecResult, error) {
	ctx, done, err := u.beginIO(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	result := u.ie.Query(ctx, sql, opts)
	return result, result.Error()
}

func (u *CDCWatermarkUpdater) checkOwnerFence(ctx context.Context, fence *OwnerFence) error {
	ctx, done, err := u.beginIO(ctx)
	if err != nil {
		return err
	}
	defer done()
	return fence.Check(ctx)
}

func (u *CDCWatermarkUpdater) enqueue(ctx context.Context, job *UpdaterJob) error {
	ctx, done, err := u.beginIO(ctx)
	if err != nil {
		return err
	}
	defer done()
	if queue, ok := u.queue.(sm.ContextQueue); ok {
		_, err = queue.EnqueueWithContext(ctx, job)
	} else {
		_, err = u.queue.Enqueue(job)
	}
	return err
}
