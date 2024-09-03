// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// func GetTombstonesByBlockId(
// 	ctx context.Context,
// 	blockId objectio.BlockId,
// 	getTombstoneFile func() (*objectio.ObjectStats, error),
// 	ts types.TS,
// 	deletedMask *nulls.Nulls,
// 	fs fileservice.FileService,
// ) (err error) {
// 	onBlockSelectedFn := func(tombstoneObject *objectio.ObjectStats, pos int) (bool, error) {
// 		location := catalog.BlockLocation(
// 			*tombstoneObject,
// 			uint16(pos),
// 			options.DefaultBlockMaxRows,
// 		)
// 		var mask *nulls.Nulls
// 	}
// 	return
// }

func CheckTombstoneFile(
	ctx context.Context,
	prefixPattern []byte,
	getTombstoneFile func() (*objectio.ObjectStats, error),
	onBlockSelectedFn func(*objectio.ObjectStats, int) (bool, error),
	fs fileservice.FileService,
) (err error) {
	if getTombstoneFile == nil {
		return
	}
	var tombstoneObject *objectio.ObjectStats
	for tombstoneObject, err = getTombstoneFile(); err == nil && tombstoneObject != nil; tombstoneObject, err = getTombstoneFile() {
		tombstoneZM := tombstoneObject.SortKeyZoneMap()
		if !tombstoneZM.PrefixEq(prefixPattern) {
			continue
		}
		var objMeta objectio.ObjectMeta
		location := tombstoneObject.ObjectLocation()

		if objMeta, err = objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		); err != nil {
			return
		}
		dataMeta := objMeta.MustDataMeta()

		blkCnt := int(dataMeta.BlockCount())

		startIdx := sort.Search(blkCnt, func(i int) bool {
			return dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(prefixPattern)
		})

		for pos := startIdx; pos < blkCnt; pos++ {
			blkMeta := dataMeta.GetBlockMeta(uint32(pos))
			columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
			// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
			// !PrefixEq means there is no rowid of this block in this zonemap, so skip
			if !columnZonemap.PrefixEq(prefixPattern) {
				if columnZonemap.PrefixGT(prefixPattern) {
					// all zone maps are sorted by the rowid
					// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
					break
				}
				continue
			}
			var goOn bool
			if goOn, err = onBlockSelectedFn(tombstoneObject, pos); err != nil || !goOn {
				return
			}
		}
	}
	return
}
