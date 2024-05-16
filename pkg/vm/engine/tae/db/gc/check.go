package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

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
	c.cleaner.inputs.RLock()
	defer c.cleaner.inputs.RUnlock()
	gcTable := c.cleaner.GetInputs()
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
				it2 := table.GetDeleteList().Iter()
				for it2.Next() {
					objID := it2.Item().ObjectID
					if _, ok := allObjects[objID.String()]; ok {
						delete(allObjects, objID.String())
					}
				}
				itObject.Next()
			}
			itTable.Next()
		}
		it.Next()
	}
	for name := range allObjects {
		logutil.Infof("not found object %s,", name)
	}

	logutil.Infof("all objects: %d, objects: %d, tombstones: %d, unconsumed objects: %d, unconsumed tombstones: %d, allObjects: %d",
		allCount, len(objects), len(tombstones), len(unconsumedObjects), len(unconsumedTombstones), len(allObjects))
	return nil
}
