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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

type metaFile struct {
	index int
	start types.TS
	end   types.TS
}

func (r *runner) Replay() {
	dirs, err := r.fs.ListDir(CheckpointDir)
	if err != nil {
		panic(err)
	}
	metaFiles := make([]*metaFile, 0)
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
		panic(err)
	}
	bs, err := reader.ReadAllMeta(context.Background(), dir.Size, common.DefaultAllocator)
	if err != nil {
		panic(err)
	}
	bat := containers.NewBatch()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	nullables := CheckpointSchema.Nullables()
	for i := range colNames {
		if bs[0].GetExtent().End() == 0 {
			continue
		}
		col, err := bs[0].GetColumn(uint16(i))
		if err != nil {
			panic(err)
		}
		data, err := col.GetData(context.Background(), nil)
		if err != nil {
			panic(err)
		}
		pkgVec := vector.New(colTypes[i])
		if err = pkgVec.Read(data.Entries[0].Data); err != nil {
			panic(err)
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i], nullables[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[i])
		}
		bat.AddVector(colNames[i], vec)
	}
	for i := 0; i < bat.Length(); i++ {
		start := bat.GetVectorByName(CheckpointAttr_StartTS).Get(i).(types.TS)
		end := bat.GetVectorByName(CheckpointAttr_EndTS).Get(i).(types.TS)
		metaloc := string(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
		checkpointEntry := &CheckpointEntry{
			start:    start,
			end:      end,
			location: metaloc,
			state:    ST_Finished,
		}
		r.storage.entries.Set(checkpointEntry)
		checkpointEntry.Replay(r.catalog, r.fs)
	}
}
