package merge

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"go.uber.org/zap"
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
	if o.tblEntry == nil {
		return false
	}
	if entry.IsTombstone {
		return false
	}

	if entry.OriginSize() < config.ObjectMinOsize {
		return false
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
	if len(tombstoneStats) == 0 {
		return false
	}

	sels, err := blockio.FindTombstonesOfObject(context.TODO(), *entry.ID(), tombstoneStats, o.fs)
	if err != nil {
		return false
	}
	iter := sels.Iterator()
	deletedRows := uint32(0)
	for iter.HasNext() {
		deletedRows += tombstoneStats[iter.Next()].Rows()
	}
	rows := entry.Rows()
	logutil.Info("[MERGE-POLICY-REVISE]",
		zap.String("policy", "compact"),
		zap.String("data object", entry.String()),
		zap.Uint32("data rows", rows),
		zap.Uint32("tombstone rows", deletedRows),
	)
	if deletedRows > rows {
		o.objects = append(o.objects, entry)
		return true
	}
	return false
}

func (o *objCompactPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	if o.tblEntry == nil {
		return nil
	}
	results := make([]reviseResult, 0, len(o.objects))
	for _, obj := range o.objects {
		results = append(results, reviseResult{[]*catalog.ObjectEntry{obj}, TaskHostDN})
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry) {
	o.tblEntry = entry
	o.objects = o.objects[:0]
}
