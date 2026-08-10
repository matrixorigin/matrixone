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

package mometric

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type blockedStorageUsageResult struct {
	err error
}

func (r blockedStorageUsageResult) Error() error      { return r.err }
func (blockedStorageUsageResult) ColumnCount() uint64 { return 0 }
func (blockedStorageUsageResult) Column(context.Context, uint64) (string, uint8, bool, error) {
	return "", 0, false, nil
}
func (blockedStorageUsageResult) RowCount() uint64 { return 0 }
func (blockedStorageUsageResult) Row(context.Context, uint64) ([]interface{}, error) {
	return nil, nil
}
func (blockedStorageUsageResult) Value(context.Context, uint64, uint64) (interface{}, error) {
	return nil, nil
}
func (blockedStorageUsageResult) GetUint64(context.Context, uint64, uint64) (uint64, error) {
	return 0, nil
}
func (blockedStorageUsageResult) GetFloat64(context.Context, uint64, uint64) (float64, error) {
	return 0, nil
}
func (blockedStorageUsageResult) GetString(context.Context, uint64, uint64) (string, error) {
	return "", nil
}

type blockedStorageUsageExecutor struct {
	queryStarted      chan struct{}
	queryCanceled     chan struct{}
	releaseQuery      chan struct{}
	queryStartedOnce  sync.Once
	queryCanceledOnce sync.Once
}

func (e *blockedStorageUsageExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func (e *blockedStorageUsageExecutor) Exec(context.Context, string, ie.SessionOverrideOptions) error {
	return nil
}

func (e *blockedStorageUsageExecutor) Query(
	ctx context.Context,
	_ string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	e.queryStartedOnce.Do(func() {
		close(e.queryStarted)
	})
	<-ctx.Done()
	e.queryCanceledOnce.Do(func() {
		close(e.queryCanceled)
	})
	<-e.releaseQuery
	return blockedStorageUsageResult{err: ctx.Err()}
}

func TestCalculateStorageUsageWaitsForNewAccountQuery(t *testing.T) {
	withModifiedConfig(func() {
		previousEnable := enable
		previousInterval := GetStorageUsageCheckNewInterval()
		enable = true
		SetStorageUsageCheckNewInterval(time.Nanosecond)
		defer func() {
			enable = previousEnable
			SetStorageUsageCheckNewInterval(previousInterval)
		}()

		service := t.Name()
		moruntime.SetupServiceBasedRuntime(
			service,
			moruntime.NewRuntime(metadata.ServiceType_CN, service, logutil.GetGlobalLogger()),
		)
		executor := &blockedStorageUsageExecutor{
			queryStarted:  make(chan struct{}),
			queryCanceled: make(chan struct{}),
			releaseQuery:  make(chan struct{}),
		}
		ctx, cancel := context.WithCancel(context.Background())
		calculateDone := make(chan error, 1)
		go func() {
			calculateDone <- CalculateStorageUsage(ctx, service, func() ie.InternalExecutor {
				return executor
			})
		}()
		<-executor.queryStarted

		cancel()
		<-executor.queryCanceled
		select {
		case <-calculateDone:
			t.Fatal("CalculateStorageUsage returned before the child query exited")
		default:
		}

		close(executor.releaseQuery)
		require.ErrorIs(t, <-calculateDone, context.Canceled)
	})
}
