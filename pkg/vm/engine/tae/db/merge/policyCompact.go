package merge

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type objCompactPolicy struct {
	tblEntry *catalog.TableEntry
	objects  []*catalog.ObjectEntry
	fs       fileservice.FileService
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{
		objects: make([]*catalog.ObjectEntry, 0),
		fs:      fs,
	}
}

func (o *objCompactPolicy) onObject(entry *catalog.ObjectEntry, config *BasicPolicyConfig) bool {
	if entry.IsTombstone {
		return false
	}

	if entry.OriginSize() < config.ObjectMinOsize {
		return false
	}

	o.objects = append(o.objects, entry)
	return true
}

func (o *objCompactPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	if o.tblEntry == nil {
		return nil
	}

	tIter := o.tblEntry.MakeTombstoneObjectIt()
	tombstoneStats := make([]objectio.ObjectStats, 0)
	for tIter.Next() {
		tEntry := tIter.Item()

		if !tEntry.IsActive() {
			continue
		}

		if !objectValid(tEntry) {
			continue
		}

		tombstoneStats = append(tombstoneStats, tIter.Item().GetObjectStats())
	}
	results := make([]reviseResult, 0)

	for _, obj := range o.objects {
		deletedRows := uint32(0)
		sels, err := blockio.FindTombstonesOfObject(context.TODO(), *obj.ID(), tombstoneStats, o.fs)
		if err != nil {
			return nil
		}
		iter := sels.Iterator()
		for iter.HasNext() {
			statsOffset := iter.Next()
			tombstoneStat := tombstoneStats[statsOffset]
			deletedRows += tombstoneStat.Rows()
		}
		rows := obj.Rows()
		fmt.Println(rows, deletedRows)
		if deletedRows > rows {
			results = append(results, reviseResult{[]*catalog.ObjectEntry{obj}, TaskHostDN})
		}
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry) {
	o.tblEntry = entry
	o.objects = o.objects[:0]
}
