// Copyright 2024 Matrix Origin
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

package merge

import (
	"context"
	"go.uber.org/zap"
	"time"

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

	tombstoneEntries []*catalog.ObjectEntry
	tombstoneStats   []objectio.ObjectStats

	validTombstones map[*catalog.ObjectEntry]struct{}
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{
		objects: make([]*catalog.ObjectEntry, 0),
		fs:      fs,

		tombstoneEntries: make([]*catalog.ObjectEntry, 0),
		tombstoneStats:   make([]objectio.ObjectStats, 0),
		validTombstones:  make(map[*catalog.ObjectEntry]struct{}),
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
	if len(o.tombstoneStats) == 0 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	sels, err := blockio.FindTombstonesOfObject(ctx, *entry.ID(), o.tombstoneStats, o.fs)
	cancel()
	if err != nil {
		return false
	}
	iter := sels.Iterator()
	tombstoneRows := uint32(0)
	mergeForOutdatedTombstone := false
	for iter.HasNext() {
		i := iter.Next()

		o.validTombstones[o.tombstoneEntries[i]] = struct{}{}

		if entryOutdated(o.tombstoneEntries[i], config.TombstoneLifetime) {
			mergeForOutdatedTombstone = true
			break
		}

		tombstoneRows += o.tombstoneStats[i].Rows()
	}
	rows := entry.Rows()
	if mergeForOutdatedTombstone || tombstoneRows > rows*5 {
		logutil.Info("[MERGE-POLICY-REVISE]",
			zap.String("policy", "compact"),
			zap.String("table", o.tblEntry.GetFullName()),
			zap.String("data object", entry.ID().ShortStringEx()),
			zap.Uint32("data rows", rows),
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
	o.filterValidTombstones()
	if len(o.tombstoneStats) > 0 {
		results = append(results, reviseResult{o.tombstoneEntries, TaskHostDN})
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry) {
	o.tblEntry = entry
	o.objects = o.objects[:0]
	o.tombstoneEntries = o.tombstoneEntries[:0]
	o.tombstoneStats = o.tombstoneStats[:0]
	clear(o.validTombstones)

	tIter := entry.MakeTombstoneObjectIt()
	for tIter.Next() {
		tEntry := tIter.Item()

		if !objectValid(tEntry) {
			continue
		}

		o.tombstoneEntries = append(o.tombstoneEntries, tEntry)
		o.tombstoneStats = append(o.tombstoneStats, tEntry.GetObjectStats())
	}
}

func (o *objCompactPolicy) filterValidTombstones() {
	i := 0
	for _, x := range o.tombstoneEntries {
		if _, ok := o.validTombstones[x]; !ok {
			o.tombstoneEntries[i] = x
			i++
		}
	}
	for j := i; j < len(o.tombstoneEntries); j++ {
		o.tombstoneEntries[j] = nil
	}
	o.tombstoneEntries = o.tombstoneEntries[:i]
}
