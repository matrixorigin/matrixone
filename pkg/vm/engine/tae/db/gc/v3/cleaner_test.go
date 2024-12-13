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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// write to replay mode
func TestDiskCleaner_WriteToReplay(t *testing.T) {
	var (
		replayCnt  int
		executeCnt int
		tryGC      int
	)
	cleaner := NewMockCleaner(
		WithTryGCFunc(func() (err error) {
			tryGC++
			return nil
		}),
		WithReplayFunc(func() (err error) {
			replayCnt++
			return nil
		}),
		WithProcessFunc(func() (err error) {
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
	err := diskCleaner.WaitFlushAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, tryGC, 1)
	require.Equal(t, replayCnt, 1)
	require.Equal(t, executeCnt, 0)

	err = diskCleaner.GC(context.Background())
	require.NoError(t, err)
	err = diskCleaner.WaitFlushAll(context.Background())
	require.NoError(t, err)
	require.Equal(t, tryGC, 1)
	require.Equal(t, replayCnt, 1)
	require.Equal(t, executeCnt, 1)

}
