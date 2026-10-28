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

package txnentries

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/require"
)

type rejectingMergeCleanupScheduler struct{ tasks.TaskScheduler }

func (rejectingMergeCleanupScheduler) ScheduleScopedFn(*tasks.Context, tasks.TaskType, *common.ID, func() error) (tasks.Task, error) {
	return nil, tasks.ErrSchedule
}

func TestMergeRollbackDeletesOutputWhenCleanupScheduleFails(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("merge-cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	objID := objectio.NewObjectidWithSegmentIDAndNum(objectio.NewSegmentid(), 0)
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsLocation(stats, objectio.MockLocation(objectio.BuildObjectNameWithObjectID(objID)))
	name := stats.ObjectName().String()
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}},
	}))
	_, err = fs.StatFile(ctx, name)
	require.NoError(t, err)

	rt := &dbutils.Runtime{Fs: fs, Scheduler: rejectingMergeCleanupScheduler{}}
	entry := &mergeObjectsEntry{
		createdObjs: []*catalog.ObjectEntry{catalog.MockObjectEntry(nil, stats, false, nil, types.TS{})},
		rt:          rt,
	}
	require.NoError(t, entry.PrepareRollback())
	_, err = fs.StatFile(ctx, name)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "stat error: %v", err)
}
