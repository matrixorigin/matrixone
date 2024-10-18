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
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type objCompactPolicy struct {
	tblEntry *catalog.TableEntry
	objects  []*catalog.ObjectEntry
	fs       fileservice.FileService

	tombstones     []*catalog.ObjectEntry
	tombstoneStats []objectio.ObjectStats

	validTombstones map[*catalog.ObjectEntry]struct{}
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{
		objects: make([]*catalog.ObjectEntry, 0),
		fs:      fs,

		tombstones:      make([]*catalog.ObjectEntry, 0),
		tombstoneStats:  make([]objectio.ObjectStats, 0),
		validTombstones: make(map[*catalog.ObjectEntry]struct{}),
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
	sels, err := blockio.FindTombstonesOfObject(ctx, entry.ID(), o.tombstoneStats, o.fs)
	cancel()
	if err != nil {
		return false
	}
	iter := sels.Iterator()
	for iter.HasNext() {
		i := iter.Next()
		tombstone := o.tombstones[i]

		o.validTombstones[tombstone] = struct{}{}

		if (entryOutdated(tombstone, config.TombstoneLifetime) && tombstone.OriginSize() > 10*common.Const1MBytes) ||
			tombstone.OriginSize() > common.DefaultMinOsizeQualifiedMB*common.Const1MBytes {
			logutil.Info("[MERGE-POLICY]",
				zap.String("policy", "compact"),
				zap.String("table", o.tblEntry.GetFullName()),
				zap.String("data object", entry.ID().ShortStringEx()),
				zap.Uint32("data rows", entry.Rows()),
				zap.Uint32("tombstone size", tombstone.OriginSize()),
			)
			o.objects = append(o.objects, entry)
			return true
		}
	}
	return false
}

func (o *objCompactPolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	if o.tblEntry == nil {
		return nil
	}
	o.filterValidTombstones()
	results := make([]reviseResult, 0, len(o.objects)+len(o.tombstones))
	for _, obj := range o.objects {
		results = append(results, reviseResult{[]*catalog.ObjectEntry{obj}, TaskHostDN})
	}
	if len(o.tombstoneStats) > 0 {
		results = append(results, reviseResult{o.tombstones, TaskHostDN})
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry) {
	o.tblEntry = entry
	o.objects = o.objects[:0]
	o.tombstones = o.tombstones[:0]
	o.tombstoneStats = o.tombstoneStats[:0]
	clear(o.validTombstones)

	tIter := entry.MakeTombstoneObjectIt()
	for tIter.Next() {
		tEntry := tIter.Item()

		if !objectValid(tEntry) {
			continue
		}

		o.tombstones = append(o.tombstones, tEntry)
		o.tombstoneStats = append(o.tombstoneStats, *tEntry.GetObjectStats())
	}
}

func (o *objCompactPolicy) filterValidTombstones() {
	i := 0
	for _, x := range o.tombstones {
		if _, ok := o.validTombstones[x]; !ok {
			o.tombstones[i] = x
			i++
		}
	}
	for j := i; j < len(o.tombstones); j++ {
		o.tombstones[j] = nil
	}
	o.tombstones = o.tombstones[:i]
}
