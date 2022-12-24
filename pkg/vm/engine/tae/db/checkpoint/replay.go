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

package checkpoint

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type metaFile struct {
	index int
	start types.TS
	end   types.TS
}

func (r *runner) Replay(dataFactory catalog.DataFactory) (maxTs types.TS, err error) {
	ctx := context.Background()
	dirs, err := r.fs.ListDir(CheckpointDir)
	if err != nil {
		return
	}
	if len(dirs) == 0 {
		return
	}
	metaFiles := make([]*metaFile, 0)
	var readDuration, applyDuration time.Duration
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &metaFile{
			start: start,
			end:   end,
			index: i,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.Less(metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	dir := dirs[targetIdx]
	reader, err := objectio.NewObjectReader(CheckpointDir+dir.Name, r.fs.Service)
	if err != nil {
		return
	}
	bs, err := reader.ReadAllMeta(ctx, dir.Size, common.DefaultAllocator)
	if err != nil {
		return
	}
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	nullables := CheckpointSchema.Nullables()
	t0 := time.Now()
	for i := range colNames {
		if bs[0].GetExtent().End() == 0 {
			continue
		}
		col, err2 := bs[0].GetColumn(uint16(i))
		if err2 != nil {
			return types.TS{}, err2
		}
		data, err2 := col.GetData(ctx, nil)
		if err2 != nil {
			return types.TS{}, err2
		}
		pkgVec := vector.New(colTypes[i])
		v := make([]byte, len(data.Entries[0].Object.([]byte)))
		copy(v, data.Entries[0].Object.([]byte))
		if err = pkgVec.Read(v); err != nil {
			return
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i], nullables[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[i])
		}
		bat.AddVector(colNames[i], vec)
	}
	readDuration += time.Since(t0)
	datas := make([]*logtail.CheckpointData, bat.Length())
	defer func() {
		for _, data := range datas {
			if data != nil {
				data.Close()
			}
		}
	}()

	jobScheduler := tasks.NewParallelJobScheduler(200)
	defer jobScheduler.Stop()
	entries := make([]*CheckpointEntry, bat.Length())
	var errMu sync.RWMutex
	var wg sync.WaitGroup
	readfn := func(i int) {
		defer wg.Done()
		start := bat.GetVectorByName(CheckpointAttr_StartTS).Get(i).(types.TS)
		end := bat.GetVectorByName(CheckpointAttr_EndTS).Get(i).(types.TS)
		metaloc := string(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
		isIncremental := bat.GetVectorByName(CheckpointAttr_EntryType).Get(i).(bool)
		typ := ET_Global
		if isIncremental {
			typ = ET_Incremental
		}
		checkpointEntry := &CheckpointEntry{
			start:     start,
			end:       end,
			location:  metaloc,
			state:     ST_Finished,
			entryType: typ,
		}
		var err2 error
		if datas[i], err2 = checkpointEntry.Read(ctx, jobScheduler, r.fs); err2 != nil {
			errMu.Lock()
			err = err2
			errMu.Unlock()
		}
		entries[i] = checkpointEntry
	}
	wg.Add(bat.Length())
	t0 = time.Now()
	for i := 0; i < bat.Length(); i++ {
		go readfn(i)
	}
	wg.Wait()
	readDuration += time.Since(t0)
	if err != nil {
		return
	}
	t0 = time.Now()
	globalIdx := 0
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if !checkpointEntry.IsIncremental() {
			globalIdx = i
			r.tryAddNewGlobalCheckpointEntry(checkpointEntry)
		} else {
			r.tryAddNewIncrementalCheckpointEntry(checkpointEntry)
		}
	}
	maxGlobal := r.MaxGlobalCheckpoint()
	if maxGlobal != nil {
		logutil.Infof("replay checkpoint %v", maxGlobal)
		err = datas[globalIdx].ApplyReplayTo(r.catalog, dataFactory)
		if err != nil {
			return
		}
		if maxTs.Less(maxGlobal.end) {
			maxTs = maxGlobal.end
		}
	}
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry.end.LessEq(maxTs) {
			continue
		}
		logutil.Infof("replay checkpoint %v", checkpointEntry)
		err = datas[i].ApplyReplayTo(r.catalog, dataFactory)
		if err != nil {
			return
		}
		if maxTs.Less(checkpointEntry.end) {
			maxTs = checkpointEntry.end
		}
	}
	applyDuration = time.Since(t0)
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("checkpoint"),
		common.AnyField("apply cost", applyDuration),
		common.AnyField("read cost", readDuration))
	r.source.Init(maxTs)
	return
}
