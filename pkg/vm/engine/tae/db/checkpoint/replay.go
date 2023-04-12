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

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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
	reader, err := blockio.NewFileReader(r.fs.Service, CheckpointDir+dir.Name)
	if err != nil {
		return
	}
	bats, err := reader.LoadAllColumns(ctx, nil, dir.Size, common.DefaultAllocator)
	if err != nil {
		return
	}
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	t0 := time.Now()
	for i := range colNames {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(bats[0].Vecs[i])
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

	entries := make([]*CheckpointEntry, bat.Length())
	emptyFile := make([]*CheckpointEntry, 0)
	var emptyFileMu sync.RWMutex
	readfn := func(i int, prefetch bool) {
		start := bat.GetVectorByName(CheckpointAttr_StartTS).Get(i).(types.TS)
		end := bat.GetVectorByName(CheckpointAttr_EndTS).Get(i).(types.TS)
		metaloc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
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
		if prefetch {
			if datas[i], err2 = checkpointEntry.Prefetch(ctx, r.fs); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else {
			if datas[i], err2 = checkpointEntry.Read(ctx, r.fs); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
				emptyFileMu.Lock()
				emptyFile = append(emptyFile, checkpointEntry)
				emptyFileMu.Unlock()
			} else {
				entries[i] = checkpointEntry
			}
		}
	}
	t0 = time.Now()
	for i := 0; i < bat.Length(); i++ {
		metaLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))

		err = blockio.PrefetchMeta(r.fs.Service, metaLoc)
		if err != nil {
			return
		}
	}

	for i := 0; i < bat.Length(); i++ {
		readfn(i, true)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, false)
	}
	readDuration += time.Since(t0)
	if err != nil {
		return
	}
	t0 = time.Now()
	globalIdx := 0
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
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
	for _, e := range emptyFile {
		if e.end.GreaterEq(maxTs) {
			return types.TS{},
				moerr.NewInternalError(ctx,
					"read checkpoint %v failed",
					e.String())
		}
	}
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
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
