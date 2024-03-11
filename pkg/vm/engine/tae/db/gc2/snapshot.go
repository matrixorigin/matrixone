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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"sync"
)

type Snapshot struct {
	ts types.TS
	dbs       []uint64
}

type SnapshotList struct {
	sync.RWMutex
	snapshots []*Snapshot
}

func NewSnapshot(ts types.TS, dbs []uint64) *Snapshot {
	return &Snapshot{
		ts: ts,
		dbs:       dbs,
	}
}

func NewSnapshotList() *SnapshotList {
	return &SnapshotList{
		snapshots: make([]*Snapshot, 0),
	}
}

func (sl *SnapshotList) Add(snapshot *Snapshot)  {
	sl.Lock()
	defer sl.Unlock()
	sl.snapshots = append(sl.snapshots, snapshot)
}

func mergeCheckpoint(fs fileservice.FileService,ckpClient checkpoint.RunnerReader, snapshotList *SnapshotList) error {
	gckp := ckpClient.MaxGlobalCheckpoint()
	_, data, err := logtail.LoadCheckpointEntriesFromKey(context.Background(), fs,
		gckp.GetLocation(), gckp.GetVersion(), nil)
	if err != nil {
		return err
	}
	ins := data.GetObjectBatchs()
	insCommitTSVec := ins.GetVectorByName(txnbase.SnapshotAttr_CommitTS).GetDownstreamVector()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tid := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()
	table := NewGCTable()
	for i := 0; i < ins.Length(); i++ {
		var objectStats objectio.ObjectStats
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		objectStats.UnMarshal(buf)
		commitTS := vector.GetFixedAt[types.TS](insCommitTSVec, i)
		deleteTS := vector.GetFixedAt[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAt[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			commitTS: commitTS,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAt[uint64](dbid, i),
			table:    vector.GetFixedAt[uint64](tid, i),
		}
		table.addObject(objectStats.ObjectName().String(), object, commitTS)
	}

	return nil
}