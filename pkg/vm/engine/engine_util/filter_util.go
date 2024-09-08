// Copyright 2022 Matrix Origin
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

package engine_util

/* DONT remove me, will be used later

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// Either len(val) == 0 or vec == nil
// inVec should be sorted
func CompilePrimaryKeyEqualFilter(
	ctx context.Context,
	val []byte,
	inVec *vector.Vector,
	colSeqnum uint16,
	isFakePK bool,
	skipBloomFilter bool,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	err error,
) {
	if skipBloomFilter {
		loadOp = loadMetadataOnlyOpFactory(fs)
	} else {
		loadOp = loadMetadataAndBFOpFactory(fs)
	}

	// Here process the pk in filter
	if inVec != nil {
		if !isFakePK {
			fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
				if obj.ZMIsEmpty() {
					return true, nil
				}
				return obj.SortKeyZoneMap().AnyIn(inVec), nil
			}
		}

		objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
			if !isFakePK {
				return true, nil
			}
			dataMeta := meta.MustDataMeta()
			return dataMeta.MustGetColumn(colSeqnum).ZoneMap().AnyIn(inVec), nil
		}

		blockFilterOp = func(
			blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
		) (bool, bool, error) {
			// TODO: support skipFollowing
			zm := blkMeta.MustGetColumn(colSeqnum).ZoneMap()
			if !zm.AnyIn(inVec) {
				return false, false, nil
			}

			if skipBloomFilter || bf.Size() == 0 {
				return false, true, nil
			}

			buf := bf.GetBloomFilter(uint32(blkIdx))
			var blkBF index.BloomFilter
			if err := blkBF.Unmarshal(buf); err != nil {
				return false, false, err
			}
			lb, ub := zm.SubVecIn(inVec)
			if exist := blkBF.MayContainsAny(
				inVec, lb, ub,
			); !exist {
				return false, false, nil
			}
			return false, true, nil
		}

		return
	}

	// Here process the pk equal filter

	// for non-fake PK, we can use the object stats sort key zone map
	// to filter as the fastFilterOp
	if !isFakePK {
		fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
			if obj.ZMIsEmpty() {
				return true, nil
			}
			return obj.SortKeyZoneMap().ContainsKey(val), nil
		}
	}

	objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
		if !isFakePK {
			return true, nil
		}
		dataMeta := meta.MustDataMeta()
		return dataMeta.MustGetColumn(colSeqnum).ZoneMap().ContainsKey(val), nil
	}

	blockFilterOp = func(
		blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, bool, error) {
		var (
			skipFollowing, ok bool
		)
		zm := blkMeta.MustGetColumn(colSeqnum).ZoneMap()
		if isFakePK {
			skipFollowing = false
			ok = zm.ContainsKey(val)
		} else {
			skipFollowing = !zm.AnyLEByValue(val)
			if skipFollowing {
				ok = false
			} else {
				ok = zm.ContainsKey(val)
			}
		}
		if !ok || skipBloomFilter || bf.Size() == 0 {
			return skipFollowing, ok, nil
		}

		// check bloom filter here
		blkBFBuf := bf.GetBloomFilter(uint32(blkIdx))
		var blkBF index.BloomFilter
		if err := blkBF.Unmarshal(blkBFBuf); err != nil {
			return false, false, err
		}
		exist, err := blkBF.MayContainsKey(val)
		if err != nil || !exist {
			return false, false, err
		}

		return false, true, nil
	}
	if !isFakePK {
		seekOp = func(meta objectio.ObjectDataMeta) int {
			blockCnt := int(meta.BlockCount())
			blkIdx := sort.Search(blockCnt, func(i int) bool {
				return meta.GetBlockMeta(uint32(i)).MustGetColumn(colSeqnum).ZoneMap().AnyGEByValue(val)
			})
			return blkIdx
		}
	}
	return
}
*/
