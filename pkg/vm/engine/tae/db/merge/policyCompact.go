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
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type objCompactPolicy struct {
	tblEntry *catalog.TableEntry
	fs       fileservice.FileService

	segObjects map[objectio.Segmentid][]*catalog.ObjectEntry

	tombstones     []*catalog.ObjectEntry
	tombstoneMetas []objectio.ObjectDataMeta

	validTombstones map[*catalog.ObjectEntry]struct{}
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{
		fs: fs,

		segObjects: make(map[objectio.Segmentid][]*catalog.ObjectEntry),

		tombstones:      make([]*catalog.ObjectEntry, 0),
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
	if len(o.tombstones) == 0 {
		return false
	}

	for i, meta := range o.tombstoneMetas {
		if !checkTombstoneMeta(meta, entry.ID()) {
			continue
		}
		o.validTombstones[o.tombstones[i]] = struct{}{}
		o.segObjects[entry.ObjectName().SegmentId()] = append(o.segObjects[entry.ObjectName().SegmentId()], entry)
	}
	return false
}

func (o *objCompactPolicy) revise(rc *resourceController, config *BasicPolicyConfig) []reviseResult {
	if o.tblEntry == nil {
		return nil
	}
	o.filterValidTombstones()
	results := make([]reviseResult, 0, len(o.segObjects)+1)
	for _, objs := range o.segObjects {
		if rc.resourceAvailable(objs) {
			rc.reserveResources(objs)
			for _, obj := range objs {
				results = append(results, reviseResult{[]*catalog.ObjectEntry{obj}, taskHostDN})
			}
		}
	}
	if len(o.tombstones) > 0 {
		results = append(results, reviseResult{o.tombstones, taskHostDN})
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	o.tblEntry = entry
	o.tombstones = o.tombstones[:0]
	o.tombstoneMetas = o.tombstoneMetas[:0]
	clear(o.segObjects)
	clear(o.validTombstones)

	tIter := entry.MakeTombstoneObjectIt()
	for tIter.Next() {
		tEntry := tIter.Item()

		if !objectValid(tEntry) {
			continue
		}

		if (entryOutdated(tEntry, config.TombstoneLifetime) && tEntry.OriginSize() > 10*common.Const1MBytes) ||
			tEntry.OriginSize() > common.DefaultMaxOsizeObjMB*common.Const1MBytes {

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			meta, err := loadTombstoneMeta(ctx, tEntry.GetObjectStats(), o.fs)
			cancel()
			if err != nil {
				continue
			}

			o.tombstoneMetas = append(o.tombstoneMetas, meta)
			o.tombstones = append(o.tombstones, tEntry)
		}
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

func loadTombstoneMeta(ctx context.Context, tombstoneObject *objectio.ObjectStats, fs fileservice.FileService) (objectio.ObjectDataMeta, error) {
	location := tombstoneObject.ObjectLocation()
	objMeta, err := objectio.FastLoadObjectMeta(
		ctx, &location, false, fs,
	)
	if err != nil {
		return nil, err
	}
	return objMeta.MustDataMeta(), nil
}

func checkTombstoneMeta(tombstoneMeta objectio.ObjectDataMeta, objectId *objectio.ObjectId) bool {
	prefixPattern := objectId[:]
	blkCnt := int(tombstoneMeta.BlockCount())

	startIdx := sort.Search(blkCnt, func(i int) bool {
		return tombstoneMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(prefixPattern)
	})

	for pos := startIdx; pos < blkCnt; pos++ {
		blkMeta := tombstoneMeta.GetBlockMeta(uint32(pos))
		columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
		// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
		// !PrefixEq means there is no rowid of this block in this zonemap, so skip
		if columnZonemap.RowidPrefixEq(prefixPattern) {
			return true
		}
		if columnZonemap.RowidPrefixGT(prefixPattern) {
			// all zone maps are sorted by the rowid
			// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
			break
		}
	}
	return false
}
