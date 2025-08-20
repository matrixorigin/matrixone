// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"go.uber.org/zap"
	"time"
)

const NotFoundLimit = 10

type gcChecker struct {
	cleaner *checkpointCleaner
}

func (c *gcChecker) getObjects(ctx context.Context) (map[string]struct{}, error) {
	entries := c.cleaner.fs.List(ctx, "")
	objects := make(map[string]struct{})
	for entry, err := range entries {
		if err != nil {
			return nil, err
		}
		if entry.IsDir {
			continue
		}
		objects[entry.Name] = struct{}{}
	}
	return objects, nil
}

func (c *gcChecker) Verify(ctx context.Context, mp *mpool.MPool) (returnStr string) {
	returnStr = "[{"
	logutil.Info("[Verify GC] Starting...")
	now := time.Now()
	defer func() {
		returnStr += "}]"
		logutil.Info("[Verify GC] End!",
			zap.Duration("duration", time.Since(now)))
	}()
	if c.cleaner.fs.Cost().List != fileservice.CostLow {
		logutil.Info("[Verify GC]skip gc check, cost is high")
		returnStr += "{'verify': 'skip gc check, cost is high'}"
		return
	}
	buffer := MakeGCWindowBuffer(mpool.MB)
	defer buffer.Close(mp)
	bat := buffer.Fetch()
	defer buffer.Putback(bat, mp)
	objects := make(map[string]*ObjectEntry)

	buildObjects := func(table *GCWindow,
		objects map[string]*ObjectEntry,
		loadfn func(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error),
	) error {
		for {
			bat.CleanOnlyData()
			done, err := loadfn(context.Background(), nil, nil, mp, bat)
			if err != nil {
				return err
			}
			if done {
				break
			}

			createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
			deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().String()
				tableID := tableIDs[i]
				createTS := createTSs[i]
				dropTS := deleteTSs[i]
				object := &ObjectEntry{
					createTS: createTS,
					dropTS:   dropTS,
					db:       dbs[i],
					table:    tableID,
				}
				objects[name] = object
			}
		}
		return nil
	}
	sancWindow := c.cleaner.GetScannedWindowLocked()
	if sancWindow == nil {
		returnStr += fmt.Sprintf("{'verify': 'OK', 'msg': 'Not-GC'}")
		return
	}
	window := sancWindow.Clone()
	windowCount := len(window.files)
	for _, stats := range window.files {
		objects[stats.ObjectName().UnsafeString()] = &ObjectEntry{}
	}
	err := buildObjects(&window, objects, window.LoadBatchData)
	if err != nil {
		returnStr += fmt.Sprintf("{'verify': '%v'}", err.Error())
		return
	}

	allObjects, err := c.getObjects(ctx)
	if err != nil {
		returnStr += fmt.Sprintf("{'verify': '%v'}", err.Error())
		return
	}
	allCount := len(allObjects)
	for name := range allObjects {
		isfound := false
		if _, ok := objects[name]; ok {
			isfound = true
			delete(objects, name)
		}
		if isfound {
			delete(allObjects, name)
		}
	}

	// Collect all objects in memory
	catalog := c.cleaner.checkpointCli.GetCatalog()
	it := catalog.MakeDBIt(true)
	for ; it.Valid(); it.Next() {
		db := it.Get().GetPayload()
		itTable := db.MakeTableIt(true)
		for itTable.Valid() {
			table := itTable.Get().GetPayload()
			itObject := table.MakeDataObjectIt()
			defer itObject.Release()
			for ok := itObject.Last(); ok; ok = itObject.Prev() {
				obj := itObject.Item()
				delete(allObjects, obj.ObjectName().UnsafeString())
			}
			itTombstone := table.MakeTombstoneObjectIt()
			defer itTombstone.Release()
			for ok := itTombstone.Last(); ok; ok = itTombstone.Prev() {
				obj := itTombstone.Item()
				delete(allObjects, obj.ObjectName().UnsafeString())
			}

			itTable.Next()
		}
	}

	lostCount := len(objects)
	if lostCount != 0 {
		returnStr += "{'lost object':"
		for name := range objects {
			returnStr += fmt.Sprintf("{ 'object': %v}", name)
			logutil.Errorf("[Verify GC]lost object %s,", name)
		}
		returnStr += "}"
	}

	// Collect all checkpoint files
	var ckpObjectCount int
	ckps := c.cleaner.checkpointCli.GetAllGlobalCheckpoints()
	ckps = append(ckps, c.cleaner.checkpointCli.GetAllIncrementalCheckpoints()...)

	compacted := c.cleaner.checkpointCli.GetCompacted()
	if compacted != nil {
		ckps = append(ckps, compacted)
	}
	for _, ckp := range ckps {
		var files []string
		files, err = getCheckpointLocation(ctx, ckp, c.cleaner.fs)
		if err != nil {
			returnStr += fmt.Sprintf("{'verify': '%v'}", err.Error())
			return returnStr
		}
		for _, file := range files {
			delete(allObjects, file)
		}
		count := len(files)
		ckpObjectCount += count
	}
	if len(allObjects) > NotFoundLimit {
		returnStr += "{'verify': 'abnormal',"
		returnStr += fmt.Sprintf("'const': %v,", time.Since(now))
		returnStr += fmt.Sprintf("'objects-count': %d,", allCount)
		returnStr += fmt.Sprintf("'not-found': %d,", len(allObjects))
		returnStr += fmt.Sprintf("'lost-count': %d,", lostCount)
		returnStr += fmt.Sprintf("'checkpoints': %d,", ckpObjectCount)
		returnStr += fmt.Sprintf("'windows': %d},", windowCount)
		returnStr += "{'not-found-object': { "
		for name := range allObjects {
			returnStr += fmt.Sprintf("'object': %v,", name)
			logutil.Infof("[Verify GC]not found object %s,", name)
		}
		returnStr += "}}"
	} else {
		returnStr += "{'verify': 'OK',"
		returnStr += fmt.Sprintf("'const': %v,", time.Since(now))
		returnStr += fmt.Sprintf("'objects-count': %d,", allCount)
		returnStr += fmt.Sprintf("'not-found': %d,", len(allObjects))
		returnStr += fmt.Sprintf("'lost-count': %d,", lostCount)
		returnStr += fmt.Sprintf("'checkpoints': %d,", ckpObjectCount)
		returnStr += fmt.Sprintf("'windows': %d}", windowCount)
	}
	return
}
