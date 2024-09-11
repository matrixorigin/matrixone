package merge

import (
	"context"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	tombstoneRows := uint32(0)
	for iter.HasNext() {
		i := iter.Next()
		deletedRows += o.countRowsInOneTombstoneForOneObject(entry, tombstoneStats[i])
		tombstoneRows += tombstoneStats[i].Rows()
	}
	rows := entry.Rows()
	if deletedRows > rows/2 || tombstoneRows > rows*5 {
		logutil.Info("[MERGE-POLICY-REVISE]",
			zap.String("policy", "compact"),
			zap.String("data object", entry.String()),
			zap.Uint32("data rows", rows),
			zap.Uint32("deleted rows", deletedRows),
			zap.Uint32("tombstone rows", tombstoneRows),
		)
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

func (o *objCompactPolicy) countRowsInOneTombstoneForOneObject(entry *catalog.ObjectEntry, stats objectio.ObjectStats) uint32 {
	count := uint32(0)
	for i := 0; i < int(stats.BlkCnt()); i++ {
		loc := stats.BlockLocation(uint16(i), o.tblEntry.GetLastestSchema(true).BlockMaxRows)
		c, err := o.countRowsInOneTombstoneBlockForOneObject(context.TODO(), entry, loc)
		if err != nil {
			return 0
		}
		count += c
	}
	return count
}

func (o *objCompactPolicy) countRowsInOneTombstoneBlockForOneObject(ctx context.Context, entry *catalog.ObjectEntry, loc objectio.Location) (uint32, error) {
	count := uint32(0)
	vectors, closeFunc, err := blockio.LoadColumns2(
		ctx, []uint16{0, 1}, nil, o.fs, loc,
		fileservice.Policy(0), false, nil,
	)
	defer closeFunc()
	if err != nil {
		logutil.Warn("[MERGE-POLICY-REVISE] failed to load tombstone columns",
			zap.String("policy", "compact"),
			zap.Error(err),
		)
		return 0, err
	}
	for j := range vectors[0].Length() {
		rowID := vectors[0].Get(j).(types.Rowid)
		blkID2, _ := rowID.Decode()
		if *blkID2.Object() != *entry.ID() {
			continue
		}
		count++
	}
	return count, nil
}
