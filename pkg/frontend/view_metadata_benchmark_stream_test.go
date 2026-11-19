//go:build integration && viewmetadatabench

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type viewBenchStreamExecutor struct {
	executor.SQLExecutor
	exec func(context.Context, executor.Options) (executor.Result, error)
}

func (e viewBenchStreamExecutor) Exec(ctx context.Context, _ string, options executor.Options) (executor.Result, error) {
	return e.exec(ctx, options)
}

type viewBenchStreamService struct {
	cnservice.Service
	exec executor.SQLExecutor
}

func (s viewBenchStreamService) GetSQLExecutor() executor.SQLExecutor { return s.exec }

func TestViewMetadataBenchStreamDrainsAfterConsumerError(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	wantRead := errors.New("consumer rejected metadata")
	wantExec := errors.New("executor terminated")
	for _, reject := range []bool{false, true} {
		calls, published := 0, 0
		svc := viewBenchStreamService{exec: viewBenchStreamExecutor{exec: func(ctx context.Context, options executor.Options) (executor.Result, error) {
			stream, errs, _ := options.Streaming()
			// 有限生产者有意继续发布，模拟取消时已产生的尾部 batch。
			// 若消费者错误后提前退出，容量 1 的通道会确定性阻塞。
			for range 3 {
				bat := batch.NewWithSize(1)
				t.Cleanup(func() { bat.Clean(mp) }) // 负控失败时也收尾；正常关闭的 batch 可重复清理。
				// 显式 off-heap，避免把 on-heap 分配误当作 CurrNB 的计费域。
				bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
				if err := vector.AppendFixed(bat.Vecs[0], int64(1), false, mp); err != nil {
					bat.Clean(mp)
					return executor.Result{}, err
				}
				if mp.CurrNB() == 0 {
					bat.Clean(mp)
					return executor.Result{}, errors.New("fixture must allocate charged vector memory")
				}
				bat.SetRowCount(1)
				stream <- executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
				published++
			}
			if reject {
				if !errors.Is(context.Cause(ctx), context.Canceled) {
					return executor.Result{}, errors.New("consumer did not cancel executor")
				}
				errs <- wantExec
				return executor.Result{}, wantExec
			}
			return executor.Result{}, nil
		}}}
		err := streamViewMetadataBench(t.Context(), svc, "unused", executor.Options{}, func(n int, _ []*vector.Vector) error {
			calls += n
			if reject {
				return wantRead
			}
			return nil
		})
		require.Equal(t, 3, published, "必须 join 发布者后才返回")
		if reject {
			require.ErrorIs(t, err, wantRead)
			require.ErrorIs(t, err, wantExec)
			require.Equal(t, 1, calls)
		} else {
			require.NoError(t, err)
			require.Equal(t, 3, calls)
		}
		require.Zero(t, mp.CurrNB())
	}
}
