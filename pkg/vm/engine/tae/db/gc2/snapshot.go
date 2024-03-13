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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
)

type Snapshot struct {
	ts types.TS
	dbs       []uint64
}

type SnapshotList struct {
	sync.RWMutex
	snapshots []types.TS
}

func NewSnapshot(ts types.TS, dbs []uint64) *Snapshot {
	return &Snapshot{
		ts: ts,
		dbs:       dbs,
	}
}

func NewSnapshotList() *SnapshotList {
	return &SnapshotList{
		snapshots: make([]types.TS, 0),
	}
}

func (sl *SnapshotList) Add(snapshot types.TS)  {
	sl.Lock()
	defer sl.Unlock()
	sl.snapshots = append(sl.snapshots, snapshot)
}

func mergeCheckpoint(fs fileservice.FileService,ckpClient checkpoint.RunnerReader, snapshotList *SnapshotList) error {
	gckp := ckpClient.MaxGlobalCheckpoint()
	datas := make([]*logtail.CheckpointData,0)
	_, data, err := logtail.LoadCheckpointEntriesFromKey(context.Background(), fs,
		gckp.GetLocation(), gckp.GetVersion(), nil)
	if err != nil {
		return err
	}
	datas = append(datas, data)

	table := NewGCTable()
	table.UpdateTable(data)
	checkpoints := ckpClient.ICKPSeekLT(gckp.GetEnd(), 10)
	for i, ckp := range checkpoints {
		_, data, err = logtail.LoadCheckpointEntriesFromKey(context.Background(), fs,
			ckp.GetLocation(), ckp.GetVersion(), nil)
		if err != nil {
			return err
		}
		datas = append(datas, data)
		table.UpdateTableForSnapshot(data, i)
	}

	gcTable := NewGCTable()
	gcTable.UpdateTable(data)
	table.SoftGC(gcTable, gckp.GetEnd(), snapshotList.snapshots)
	objectInfoMeta := makeRespBatchFromSchema(logtail.ObjectInfoSchema, common.CheckpointAllocator)
	for i, data := range datas {
		for _, object:= range table.objects{
			if object.fileIterm[i] == nil {
				continue
			}
			for _, row := range object.fileIterm[i] {
				appendValToBatch(data.GetObjectBatchs(), objectInfoMeta, int(row))
			}
		}

	}

	return nil
}

func appendValToBatch(src, dst *containers.Batch, row int) {
	for v, vec := range src.Vecs {
		val := vec.Get(row)
		if val == nil {
			dst.Vecs[v].Append(val, true)
		} else {
			dst.Vecs[v].Append(val, false)
		}
	}
}

func makeRespBatchFromSchema(schema *catalog.Schema, mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(
		catalog.AttrRowID,
		containers.MakeVector(types.T_Rowid.ToType(), mp),
	)
	bat.AddVector(
		catalog.AttrCommitTs,
		containers.MakeVector(types.T_TS.ToType(), mp),
	)
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(
			attr,
			containers.MakeVector(typs[i], mp),
		)
	}
	return bat
}