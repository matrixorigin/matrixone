// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type stubSQLExecutor struct {
	execFn func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error)
}

func (s *stubSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	if s.execFn != nil {
		return s.execFn(ctx, sql, opts)
	}
	return executor.Result{}, nil
}

func (s *stubSQLExecutor) ExecTxn(context.Context, func(executor.TxnExecutor) error, executor.Options) error {
	return errors.New("ExecTxn not implemented in test executor")
}

type fakeClock struct {
	times []time.Time
	index int
}

func (f *fakeClock) Now() time.Time {
	if len(f.times) == 0 {
		return time.Now()
	}
	if f.index >= len(f.times) {
		return f.times[len(f.times)-1]
	}
	now := f.times[f.index]
	f.index++
	return now
}

func newTestDetector(exec executor.SQLExecutor) *TableDetector {
	return &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		exec:                 exec,
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cleanupWarn:          10 * time.Millisecond,
	}
}

func TestCleanupOrphanWatermarksSkipWhenExecutorNil(t *testing.T) {
	td := newTestDetector(nil)
	require.NotPanics(t, func() {
		td.cleanupOrphanWatermarks(context.Background())
	})
}

func TestCleanupOrphanWatermarksExecError(t *testing.T) {
	var capturedCtx context.Context
	execCalled := false
	exec := &stubSQLExecutor{
		execFn: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			execCalled = true
			capturedCtx = ctx
			return executor.Result{}, errors.New("boom")
		},
	}
	td := newTestDetector(exec)
	td.nowFn = (&fakeClock{times: []time.Time{time.Unix(0, 0), time.Unix(0, 0)}}).Now

	td.cleanupOrphanWatermarks(context.Background())

	require.True(t, execCalled)
	require.NotNil(t, capturedCtx)
	_, hasDeadline := capturedCtx.Deadline()
	require.True(t, hasDeadline)
}

func TestCleanupOrphanWatermarksNoRows(t *testing.T) {
	execCalled := false
	exec := &stubSQLExecutor{
		execFn: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			execCalled = true
			require.Equal(t, CDCSQLBuilder.DeleteOrphanWatermarkSQL(), sql)
			require.Equal(t, catalog.System_Account, opts.StatementOption().AccountID())
			require.True(t, opts.StatementOption().DisableLog())
			return executor.Result{AffectedRows: 0}, nil
		},
	}
	td := newTestDetector(exec)
	fc := &fakeClock{times: []time.Time{time.Unix(0, 0), time.Unix(0, int64(5*time.Millisecond))}}
	td.nowFn = fc.Now

	td.cleanupOrphanWatermarks(context.Background())

	require.True(t, execCalled)
}

func TestCleanupOrphanWatermarksFastCompletion(t *testing.T) {
	exec := &stubSQLExecutor{
		execFn: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			return executor.Result{AffectedRows: 3}, nil
		},
	}
	td := newTestDetector(exec)
	td.cleanupWarn = 20 * time.Millisecond
	fc := &fakeClock{
		times: []time.Time{
			time.Unix(0, 0),
			time.Unix(0, int64(10*time.Millisecond)),
		},
	}
	td.nowFn = fc.Now

	td.cleanupOrphanWatermarks(context.Background())
}

func TestCleanupOrphanWatermarksSlowCompletion(t *testing.T) {
	exec := &stubSQLExecutor{
		execFn: func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			return executor.Result{AffectedRows: 2}, nil
		},
	}
	td := newTestDetector(exec)
	td.cleanupWarn = 2 * time.Millisecond
	fc := &fakeClock{
		times: []time.Time{
			time.Unix(0, 0),
			time.Unix(0, int64(5*time.Millisecond)),
		},
	}
	td.nowFn = fc.Now

	td.cleanupOrphanWatermarks(context.Background())
}
