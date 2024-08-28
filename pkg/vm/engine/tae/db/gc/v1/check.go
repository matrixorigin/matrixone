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

package v1

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

const NotFoundLimit = 10

type checker struct {
	cleaner *checkpointCleaner
}

func (c *checker) getObjects() (map[string]struct{}, error) {
	dirs, err := c.cleaner.fs.ListDir("")
	if err != nil {
		return nil, err
	}
	objects := make(map[string]struct{})
	for _, entry := range dirs {
		if entry.IsDir {
			continue
		}
		objects[entry.Name] = struct{}{}
	}
	return objects, nil
}

func (c *checker) Check() error {
	if c.cleaner.fs.Service.Cost().List != fileservice.CostLow {
		logutil.Info("[Check GC]skip gc check, cost is high")
		return nil
	}
	now := time.Now()
	c.cleaner.inputs.RLock()
	defer c.cleaner.inputs.RUnlock()
	gcTables := c.cleaner.GetGCTables()

	// Collect the objects and tombstones that the disk cleaner has already consumed
	gcTable := NewGCTable()
	for _, table := range gcTables {
		gcTable.Merge(table)
	}
	objects := gcTable.objects
	tombstones := gcTable.tombstones
	entry := c.cleaner.GetMaxConsumed()
	maxTs := entry.GetEnd()

	// Collect the objects and tombstones that the disk cleaner has not consumed
	checkpoints := c.cleaner.ckpClient.ICKPSeekLT(entry.GetEnd(), 40)
	unconsumedTable := NewGCTable()
	for _, ckp := range checkpoints {
		_, data, err := logtail.LoadCheckpointEntriesFromKey(c.cleaner.ctx, c.cleaner.sid, c.cleaner.fs.Service,
			ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
		if err != nil {
			logutil.Errorf("load checkpoint failed: %v", err)
			continue
		}
		defer data.Close()
		unconsumedTable.UpdateTable(data)
		end := ckp.GetEnd()
		if end.Greater(&maxTs) {
			maxTs = ckp.GetEnd()
		}
	}
	unconsumedObjects := unconsumedTable.objects
	unconsumedTombstones := unconsumedTable.tombstones

	// Collect all objects
	allObjects, err := c.getObjects()
	if err != nil {
		return err
	}

	// Collect all checkpoint files
	ckpfiles := c.cleaner.GetCheckpoints()
	checkFiles, _, err := checkpoint.ListSnapshotMeta(c.cleaner.ctx, c.cleaner.fs.Service, entry.GetStart(), nil)
	if err != nil {
		return err
	}
	// The number of checkpoint files is ckpObjectCount
	ckpObjectCount := len(checkFiles) * 2
	allCount := len(allObjects)
	for name := range allObjects {
		isfound := false
		if _, ok := objects[name]; ok {
			isfound = true
			delete(objects, name)
		}
		if _, ok := tombstones[name]; ok {
			isfound = true
			delete(tombstones, name)
		}
		if _, ok := unconsumedObjects[name]; ok {
			isfound = true
			delete(unconsumedObjects, name)
		}
		if _, ok := unconsumedTombstones[name]; ok {
			isfound = true
			delete(unconsumedTombstones, name)
		}
		if isfound {
			delete(allObjects, name)
		}
	}

	for _, ckp := range checkFiles {
		if _, ok := ckpfiles[ckp.GetName()]; !ok {
			logutil.Errorf("[Check GC]lost checkpoint file %s", ckp.GetName())
			continue
		}
		delete(ckpfiles, ckp.GetName())
	}
	if len(ckpfiles) != 0 {
		for name := range ckpfiles {
			logutil.Errorf("[Check GC]not deleted checkpoint file %s", name)
		}
	}

	// Collect all objects in memory
	catalog := c.cleaner.ckpClient.GetCatalog()
	it := catalog.MakeDBIt(true)
	// end := types.BuildTS(time.Now().UnixNano(), 0)
	for ; it.Valid(); it.Next() {
		db := it.Get().GetPayload()
		itTable := db.MakeTableIt(true)
		for itTable.Valid() {
			table := itTable.Get().GetPayload()
			itObject := table.MakeObjectIt(true)
			for itObject.Next() {
				objectEntry := itObject.Item()
				stats := objectEntry.GetObjectStats()
				delete(allObjects, stats.ObjectName().String())
			}
		}
	}

	if len(objects) != 0 || len(tombstones) != 0 || len(unconsumedObjects) != 0 || len(unconsumedTombstones) != 0 {
		for name := range objects {
			logutil.Errorf("[Check GC]lost object %s,", name)
		}

		for name := range tombstones {
			logutil.Errorf("[Check GC]lost tombstone %s,", name)
		}

		for name := range unconsumedObjects {
			logutil.Errorf("[Check GC]lost unconsumed object %s,", name)
		}

		for name := range unconsumedTombstones {
			logutil.Errorf("[Check GC]lost unconsumed tombstone %s,", name)
		}
	}

	if len(allObjects) > ckpObjectCount+NotFoundLimit {
		for name := range allObjects {
			logutil.Infof("[Check GC]not found object %s,", name)
		}
		logutil.Warnf("[Check GC]GC abnormal!!! const: %v, all objects: %d, not found: %d, checkpoint file: %d",
			time.Since(now), allCount, len(allObjects)-ckpObjectCount, ckpObjectCount)
	} else {
		logutil.Infof("[Check GC]Check end!!! const: %v, all objects: %d, not found: %d",
			time.Since(now), allCount, len(allObjects)-ckpObjectCount)
	}

	return nil
}
