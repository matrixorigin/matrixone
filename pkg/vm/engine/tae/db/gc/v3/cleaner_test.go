// Copyright 2021 Matrix Origin
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

package gc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// write to replay mode
func TestDiskCleaner_WriteToReplay(t *testing.T) {
	var (
		replayCnt  int
		executeCnt int
		tryGC      int
	)
	cleaner := NewMockCleaner(
		WithTryGCFunc(func(context.Context) (err error) {
			tryGC++
			return nil
		}),
		WithReplayFunc(func(context.Context) (err error) {
			replayCnt++
			return nil
		}),
		WithProcessFunc(func(context.Context) (err error) {
			time.Sleep(time.Millisecond * 2)
			executeCnt++
			return nil
		}),
	)

	diskCleaner := NewDiskCleaner(&cleaner, true)
	require.True(t, diskCleaner.IsWriteMode())
	require.Equal(t, tryGC, 0)
	require.Equal(t, replayCnt, 0)
	require.Equal(t, executeCnt, 0)

	diskCleaner.Start()
	err := diskCleaner.FlushQueue(context.Background())
	require.NoError(t, err)
	require.Equal(t, tryGC, 1)
	require.Equal(t, replayCnt, 1)
	require.Equal(t, executeCnt, 0)

	err = diskCleaner.GC(context.Background())
	require.NoError(t, err)
	err = diskCleaner.FlushQueue(context.Background())
	require.NoError(t, err)
	require.Equal(t, tryGC, 1)
	require.Equal(t, replayCnt, 1)
	require.Equal(t, executeCnt, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	WithProcessFunc(func(ctx context.Context) (err error) {
		defer func() {
			executeCnt++
		}()
		if rand.Intn(10) > 5 {
			wg.Wait()
		} else {
			select {
			case <-ctx.Done():
				err = context.Cause(ctx)
				t.Logf("%d: %v", executeCnt, err)
				return
			default:
			}
			time.Sleep(2 * time.Millisecond)
		}
		return nil
	})(&cleaner)

	for i := 0; i < 10; i++ {
		err = diskCleaner.GC(context.Background())
		require.NoError(t, err)
	}
	go func() {
		time.Sleep(time.Millisecond * 4)
		wg.Done()
	}()

	err = diskCleaner.SwitchToReplayMode(context.Background())
	require.NoError(t, err)
	require.Equal(t, executeCnt, 11)
	require.True(t, diskCleaner.IsReplayMode())

	WithProcessFunc(func(context.Context) (err error) {
		executeCnt++
		return nil
	})(&cleaner)

	err = diskCleaner.GC(context.Background())
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnControl))
}
