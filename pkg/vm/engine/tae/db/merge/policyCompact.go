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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var _ policy = (*objCompactPolicy)(nil)

type objCompactPolicy struct {
	tblEntry *catalog.TableEntry
	fs       fileservice.FileService

	objects []*catalog.ObjectEntry

	tombstoneMetas []objectio.ObjectDataMeta
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{fs: fs}
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
	if len(o.tombstoneMetas) == 0 {
		return false
	}

	for _, meta := range o.tombstoneMetas {
		if !checkTombstoneMeta(meta, entry.ID()) {
			continue
		}
		o.objects = append(o.objects, entry)
	}
	return false
}

func (o *objCompactPolicy) revise(rc *resourceController) []reviseResult {
	if o.tblEntry == nil {
		return nil
	}
	results := make([]reviseResult, 0, len(o.objects))
	for _, obj := range o.objects {
		if rc.resourceAvailable([]*catalog.ObjectEntry{obj}) {
			rc.reserveResources([]*catalog.ObjectEntry{obj})
			results = append(results, reviseResult{[]*catalog.ObjectEntry{obj}, taskHostDN})
		}
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	o.tblEntry = entry
	o.tombstoneMetas = o.tombstoneMetas[:0]
	o.objects = o.objects[:0]

	tIter := entry.MakeTombstoneVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	for tIter.Next() {
		tEntry := tIter.Item()

		if !objectValid(tEntry) {
			continue
		}

		if tEntry.OriginSize() > common.DefaultMaxOsizeObjMB*common.Const1MBytes {
			meta, err := loadTombstoneMeta(tEntry.GetObjectStats(), o.fs)
			if err != nil {
				continue
			}
			o.tombstoneMetas = append(o.tombstoneMetas, meta)
		}
	}
}

func loadTombstoneMeta(tombstoneObject *objectio.ObjectStats, fs fileservice.FileService) (objectio.ObjectDataMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
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
