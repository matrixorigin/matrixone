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

package logtail

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
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
	_, data, err := LoadCheckpointEntriesFromKey(context.Background(), fs,
		gckp.GetLocation(), gckp.GetVersion(), nil)
	if err != nil {
		return err
	}

	return nil
}