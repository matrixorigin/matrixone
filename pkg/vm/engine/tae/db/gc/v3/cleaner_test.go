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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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
	jobErr := moerr.NewInternalErrorNoCtx("test")
	noopJob, err := diskCleaner.addJob(
		context.Background(),
		JT_GCNoop,
		func(ctx context.Context) *tasks.JobResult {
			time.Sleep(time.Millisecond)
			result := new(tasks.JobResult)
			result.Err = jobErr
			return result
		},
	)
	require.NoError(t, err)
	result := noopJob.WaitDone()
	require.Equal(t, jobErr, result.Err)
}
