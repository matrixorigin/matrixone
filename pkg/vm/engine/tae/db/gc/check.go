package gc

import (
	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"time"
)

const NotFoundLimit = 30

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
	now := time.Now()
	c.cleaner.inputs.RLock()
	defer c.cleaner.inputs.RUnlock()
	gcTables := c.cleaner.GetGCTables()
	gcTable := NewGCTable()
	for _, table := range gcTables {
		gcTable.Merge(table)
	}
	gcTable.Lock()
	objects := gcTable.objects
	tombstones := gcTable.tombstones
	gcTable.Unlock()
	entry := c.cleaner.GetMaxConsumed()
	maxTs := entry.GetEnd()
	checkpoints := c.cleaner.ckpClient.ICKPSeekLT(entry.GetEnd(), 40)
	unconsumedTable := NewGCTable()
	for _, ckp := range checkpoints {
		_, data, err := logtail.LoadCheckpointEntriesFromKey(c.cleaner.ctx, c.cleaner.fs.Service,
			ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
		if err != nil {
			logutil.Errorf("load checkpoint failed: %v", err)
			continue
		}
		unconsumedTable.UpdateTable(data)
		end := ckp.GetEnd()
		if end.Greater(&maxTs) {
			maxTs = ckp.GetEnd()
		}
	}
	unconsumedObjects := unconsumedTable.objects
	unconsumedTombstones := unconsumedTable.tombstones
	allObjects, err := c.getObjects()
	if err != nil {
		return err
	}
	ckpfiles, _, err := checkpoint.ListSnapshotMeta(c.cleaner.ctx, c.cleaner.fs.Service, entry.GetStart(), nil)
	if err != nil {
		return err
	}
	ckpObjectCount := len(ckpfiles) * 2
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
	catalog := c.cleaner.ckpClient.GetCatalog()
	it := catalog.MakeDBIt(true)
	bat := makeRespBatchFromSchema(logtail.BlkMetaSchema, common.DebugAllocator)
	defer bat.Close()
	end := types.BuildTS(time.Now().UnixNano(), 0)
	for ; it.Valid(); it.Next() {
		db := it.Get().GetPayload()
		itTable := db.MakeTableIt(true)
		for itTable.Valid() {
			table := itTable.Get().GetPayload()
			itObject := table.MakeObjectIt(true)
			for itObject.Valid() {
				objectEntry := itObject.Get().GetPayload()
				stats := objectEntry.GetObjectStats()
				if _, ok := allObjects[stats.ObjectName().String()]; ok {
					delete(allObjects, stats.ObjectName().String())
				}
				itObject.Next()
			}
			it2 := table.GetDeleteList().Items()
			for _, itt := range it2 {
				_, _, _, err = itt.VisitDeletes(c.cleaner.ctx, maxTs, end, bat, nil, true)
				if err != nil {
					logutil.Errorf("visit deletes failed: %v", err)
					continue
				}
			}
			itTable.Next()
		}
	}
	for i := 0; i < bat.Length(); i++ {
		deltaLoc := objectio.Location(bat.GetVectorByName(catalog2.BlockMeta_DeltaLoc).Get(i).([]byte))
		if _, ok := allObjects[deltaLoc.Name().String()]; ok {
			delete(allObjects, deltaLoc.Name().String())
		}
	}

	if len(objects) != 0 || len(tombstones) != 0 || len(unconsumedObjects) != 0 || len(unconsumedTombstones) != 0 {
		for _, name := range objects {
			logutil.Errorf("[Check GC]lost object %s,", name)
		}

		for _, name := range tombstones {
			logutil.Errorf("[Check GC]lost tombstone %s,", name)
		}

		for _, name := range unconsumedObjects {
			logutil.Errorf("[Check GC]lost unconsumed object %s,", name)
		}

		for _, name := range unconsumedTombstones {
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
