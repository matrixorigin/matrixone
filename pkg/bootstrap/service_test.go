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

package bootstrap

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapAlreadyBootstrapped(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())

	n := 0
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == "show databases" {
			n++
			memRes := executor.NewMemResult(
				[]types.Type{types.New(types.T_varchar, 2, 0)},
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{bootstrappedCheckerDB})
			return memRes.GetResult(), nil
		}
		return executor.Result{}, nil
	})

	b := NewService(
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	require.NoError(t, b.Bootstrap(ctx))
	assert.Equal(t, 1, n)
}

func TestBootstrapWithWait(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())

	var n atomic.Uint32
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == "show databases" && n.Load() == 1 {
			memRes := executor.NewMemResult(
				[]types.Type{types.New(types.T_varchar, 2, 0)},
				mpool.MustNewZero())
			memRes.NewBatch()
			executor.AppendStringRows(memRes, 0, []string{bootstrappedCheckerDB})
			return memRes.GetResult(), nil
		}
		n.Add(1)
		return executor.Result{}, nil
	})

	b := NewService(
		&memLocker{ids: map[string]uint64{
			bootstrapKey: 1,
		}},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	require.NoError(t, b.Bootstrap(ctx))
	assert.True(t, n.Load() > 0)
}

type memLocker struct {
	sync.Mutex
	ids map[string]uint64
}

func (l *memLocker) Get(
	ctx context.Context,
	key string) (bool, error) {
	l.Lock()
	defer l.Unlock()
	if l.ids == nil {
		l.ids = make(map[string]uint64)
	}

	l.ids[key]++
	return l.ids[key] == 1, nil
}
