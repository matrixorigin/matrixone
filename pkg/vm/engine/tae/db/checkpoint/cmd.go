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
	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
)

func GetCheckpointStat(ctx context.Context, rt *dbutils.Runtime, name string) (res string, err error) {
	reader, err := blockio.NewFileReader(rt.SID(), rt.Fs.Service, CheckpointDir+name)
	if err != nil {
		return
	}
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.CheckpointAllocator)
	if err != nil {
		return
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	var checkpointVersion int
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bats[0].Vecs)
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.CheckpointAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	datas := make([]*logtail.CheckpointData, bat.Length())

	entries, maxGlobalEnd := replayCheckpointEntries(bat, checkpointVersion)
	emptyFile := make([]*CheckpointEntry, 0)
	var emptyFileMu sync.RWMutex
	closecbs := make([]func(), 0)
	var readCount, _, _ int
	_ = len(entries)
	readfn := func(i int, readType uint16) (obj *logtail.ObjectInfoJson, err error) {
		checkpointEntry := entries[i]
		checkpointEntry.sid = rt.SID()
		if checkpointEntry.end.Less(&maxGlobalEnd) {
			return
		}
		var err2 error
		if readType == PrefetchData {
			if err2 = checkpointEntry.Prefetch(ctx, rt.Fs, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else if readType == PrefetchMetaIdx {
			readCount++
			datas[i], err = checkpointEntry.PrefetchMetaIdx(ctx, rt.Fs)
			if err != nil {
				return
			}
		} else if readType == ReadMetaIdx {
			err = checkpointEntry.ReadMetaIdx(ctx, rt.Fs, datas[i])
			if err != nil {
				return
			}
		} else {
			if err2 = checkpointEntry.Read(ctx, rt.Fs, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
				emptyFileMu.Lock()
				emptyFile = append(emptyFile, checkpointEntry)
				emptyFileMu.Unlock()
			} else {
				entries[i] = checkpointEntry
				closecbs = append(closecbs, func() { datas[i].CloseWhenLoadFromCache(checkpointEntry.version) })
			}
			obj, err = datas[i].PrintMetaBatch()
		}
		return
	}
	defer func() {
		for _, cb := range closecbs {
			cb()
		}
	}()
	for i := 0; i < bat.Length(); i++ {
		metaLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))

		err = blockio.PrefetchMeta(rt.SID(), rt.Fs.Service, metaLoc)
		if err != nil {
			return
		}
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchData)
	}
	objs := make([]logtail.ObjectInfoJson, 0, bat.Length())
	for i := 0; i < bat.Length(); i++ {
		obj, _ := readfn(i, ReadData)
		if len(obj.Tables) != 0 {
			obj.Index = len(objs)
			objs = append(objs, *obj)
		}
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	checkpointJsoon := logtail.CheckpointInfoJson{
		CheckpointDataCount: len(objs),
		Data:                objs,
	}
	jsonData, err := json.MarshalIndent(checkpointJsoon, "", "  ")
	if err != nil {
		logutil.Infof("[checkpointStat] error: %v", err)
		return
	}

	res = string(jsonData)

	return
}
