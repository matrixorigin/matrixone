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

package taskservice

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestDeleteStaleSQLTaskBatchPreservesClaimsAndNewGeneration(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := factory(t)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			definition := newTestSQLTask("owned", 1)
			definition.TriggerCount = 300
			mustAddTestSQLTask(t, store, 1, definition)
			definition = mustGetTestSQLTask(t, store, 1)[0]
			parent := fmt.Sprintf("sql-task:%d", definition.TaskID)
			metadataID := func(sequence int) string { return fmt.Sprintf("sqltask2:%d:%020d", definition.TaskID, sequence) }
			makeFiring := func(id string) task.AsyncTask {
				return task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1, ParentTaskID: parent,
					Metadata: task.TaskMetadata{ID: id, Executor: task.TaskCode_SQLTask}}
			}
			owned := makeFiring(metadataID(1))
			mustAddTestAsyncTask(t, store, 1, owned)
			owned = mustGetTestAsyncTask(t, store, 1)[0]
			claim := &SQLTaskClaim{ID: owned.ID, Runner: owned.TaskRunner, Epoch: owned.Epoch, ParentTaskID: parent, MetadataID: owned.Metadata.ID}
			run := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
			run.TriggerType = SQLTaskTriggerScheduled
			_, err := store.AcquireSQLTaskRun(ctx, definition, run, testSQLTaskAdmission(claim, 1))
			require.NoError(t, err)
			_, err = store.AlterSQLTask(ctx, definition, definition, SQLTaskSuspend)
			require.NoError(t, err)
			current := mustGetTestSQLTask(t, store, 1)[0]
			require.Equal(t, uint64(301), current.ScheduleGeneration)

			for sequence := 2; sequence <= 131; sequence++ {
				mustAddTestAsyncTask(t, store, 1, makeFiring(metadataID(sequence)))
			}
			legacy := makeFiring(parent + ":legacy")
			currentFiring := makeFiring(metadataID(301))
			future := makeFiring(metadataID(302))
			malformed := makeFiring(fmt.Sprintf("sqltask2:%d:%019da", definition.TaskID, 2))
			otherExecutor := makeFiring(parent + ":other-executor")
			otherExecutor.Metadata.Executor = task.TaskCode_TestOnly
			completed := makeFiring(metadataID(299))
			completed.Status = task.TaskStatus_Completed
			mustAddTestAsyncTask(t, store, 6, legacy, currentFiring, future, malformed, otherExecutor, completed)

			deleted, err := deleteStaleSQLTaskBatch(ctx, store, definition.TaskID)
			require.NoError(t, err)
			require.Equal(t, sqlTaskStaleBatchSize, deleted)
			deleted, err = deleteStaleSQLTaskBatch(ctx, store, definition.TaskID)
			require.NoError(t, err)
			require.Equal(t, 3, deleted)
			for _, id := range []string{owned.Metadata.ID, currentFiring.Metadata.ID, future.Metadata.ID, malformed.Metadata.ID, otherExecutor.Metadata.ID, completed.Metadata.ID} {
				mustGetTestAsyncTask(t, store, 1, WithTaskMetadataId(EQ, id))
			}
			mustGetTestSQLTaskRun(t, store, 1)
		})
	}
}
