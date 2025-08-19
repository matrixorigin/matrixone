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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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

func (c *gcChecker) Check(ctx context.Context, mp *mpool.MPool) error {
	logutil.Info("[Check GC] Starting...")
	if c.cleaner.fs.Cost().List != fileservice.CostLow {
		logutil.Info("[Check GC]skip gc check, cost is high")
		return nil
	}
	now := time.Now()
	defer func() {
		logutil.Infof("GC Check end! time: %v", time.Since(now))
	}()
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
				logutil.Error(
					"GCWindow-Compre-Err",
					zap.Error(err),
				)
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
		return nil
	}
	window := sancWindow.Clone()
	windowCount := len(window.files)
	for _, stats := range window.files {
		objects[stats.ObjectName().UnsafeString()] = &ObjectEntry{}
	}
	buildObjects(&window, objects, window.LoadBatchData)

	scanWM := c.cleaner.GetScanWaterMark()
	maxTS := types.TS{}
	if scanWM != nil {
		maxTS = scanWM.GetEnd()
	}
	// Collect all objects
	allObjects, err := c.getObjects(ctx)
	if err != nil {
		return err
	}

	candidates := c.cleaner.checkpointCli.ICKPSeekLT(maxTS, 40)

	unconsumedWindow := NewGCWindow(mp, c.cleaner.fs)
	if _, err := unconsumedWindow.ScanCheckpoints(
		ctx,
		candidates,
		c.cleaner.getCkpReader,
		nil,
		nil,
		buffer,
	); err != nil {
		unconsumedWindow.Close()
		unconsumedWindow = nil
		return err
	}
	unconsumedWindowCount := len(unconsumedWindow.files)
	objects2 := make(map[string]*ObjectEntry)
	for _, stats := range unconsumedWindow.files {
		objects2[stats.ObjectName().UnsafeString()] = &ObjectEntry{}
	}
	buildObjects(unconsumedWindow, objects2, unconsumedWindow.LoadBatchDataAndDelete)
	logutil.Infof("object1: %d, object2: %d, maxTS is %v, num %v", len(objects), len(objects2), maxTS.ToString(), len(candidates))

	allCount := len(allObjects)
	for name := range allObjects {
		isfound := false
		if _, ok := objects[name]; ok {
			isfound = true
			delete(objects, name)
		}
		if _, ok := objects2[name]; ok {
			isfound = true
			delete(objects2, name)
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

	if len(objects) != 0 || len(objects2) != 0 {
		for name := range objects {
			logutil.Errorf("[Check GC]lost object %s,", name)
		}

		for name := range objects2 {
			logutil.Errorf("[Check GC]lost unconsumed object %s,", name)
		}
	}

	// Collect all checkpoint files
	var ckpObjectCount int
	ckps := c.cleaner.checkpointCli.GetAllCheckpoints()
	compacted := c.cleaner.checkpointCli.GetCompacted()
	if compacted != nil {
		ckps = append(ckps, compacted)
	}
	for i, ckp := range ckps {
		reader := logtail.NewCKPReader(
			ckps[i].GetVersion(),
			ckps[i].GetLocation(),
			common.CheckpointAllocator,
			c.cleaner.fs,
		)
		if err = reader.ReadMeta(ctx); err != nil {
			return err
		}
		rows := uint32(0)
		delete(allObjects, ckps[i].GetLocation().Name().UnsafeString())
		tableIDLocations := ckp.GetTableIDLocation()
		for y := 0; y < tableIDLocations.Len(); y++ {
			location := tableIDLocations.Get(i)
			delete(allObjects, location.Name().UnsafeString())
			logutil.Infof("GetTableIDLocation .Name().String() is %v", ckp.String(), location.Name().UnsafeString())
		}
		logutil.Infof("checkpoint1 %v, file: %v", ckp.String(), ckps[i].GetLocation().Name().UnsafeString())
		for _, loc := range reader.GetLocations() {
			delete(allObjects, loc.Name().UnsafeString())
			rows += loc.Rows()
			logutil.Infof("checkpoint %v, file: %v", ckp.String(), loc.Name().UnsafeString())
		}
		count := len(reader.GetLocations()) + 1
		ckpObjectCount += count
		logutil.Infof("checkpoint %v, file count: %v, rows: %d", ckp.String(), len(reader.GetLocations())+1, rows)
	}
	for name := range allObjects {
		logutil.Infof("not GC name: %v", name)
	}
	if len(allObjects) > NotFoundLimit {
		for name := range allObjects {
			logutil.Infof("[Check GC]not found object %s,", name)
		}
		logutil.Warnf("[Check GC]GC abnormal!!! const: %v, all objects: %d, not found: %d, checkpoint file: %d, window file: %d, unconsumedWindow file: %d",
			time.Since(now), allCount, len(allObjects), ckpObjectCount, windowCount, unconsumedWindowCount)
	} else {
		logutil.Infof("[Check GC]Check end!!! const: %v, all objects: %d, not found: %d, checkpoint: %d",
			time.Since(now), allCount, len(allObjects), ckpObjectCount)
	}
	return nil
}
