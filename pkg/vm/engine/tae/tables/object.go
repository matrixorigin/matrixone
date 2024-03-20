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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func BuildObjectCompactionTaskFactory(meta *catalog.ObjectEntry, rt *dbutils.Runtime) (
	factory tasks.TxnTaskFactory, taskType tasks.TaskType, scopes []common.ID, err error,
) {
	if !meta.IsAppendable() {
		return
	}
	meta.RLock()
	dropped := meta.HasDropCommittedLocked()
	inTxn := meta.IsCreatingOrAborted()
	meta.RUnlock()
	if dropped || inTxn {
		return
	}
	filter := catalog.NewComposedFilter()
	filter.AddBlockFilter(catalog.NonAppendableBlkFilter)
	filter.AddCommitFilter(catalog.ActiveWithNoTxnFilter)
	blks := meta.CollectBlockEntries(filter.FilteCommit, filter.FilteBlock)
	if len(blks) < int(meta.GetTable().GetLastestSchemaLocked().ObjectMaxBlocks) {
		return
	}
	for _, blk := range blks {
		scopes = append(scopes, *blk.AsCommonID())
	}
	factory = func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeObjectsTask(ctx, txn, []*catalog.ObjectEntry{meta}, rt)
	}
	taskType = tasks.DataCompactionTask
	return
}
