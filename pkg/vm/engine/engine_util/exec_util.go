// Copyright 2022 Matrix Origin
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

package engine_util

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var ErrNoMore = moerr.NewInternalErrorNoCtx("no more")

func ForeachObjectsExecute(
	onObject func(objectio.ObjectStats) error,
	nextObjectFn func() (objectio.ObjectStats, error),
	latestObjects []objectio.ObjectStats,
	extraObjects []objectio.ObjectStats,
) (err error) {
	var obj objectio.ObjectStats
	for _, obj = range latestObjects {
		if err = onObject(obj); err != nil {
			return
		}
	}
	for _, obj := range extraObjects {
		if err = onObject(obj); err != nil {
			return
		}
	}
	if nextObjectFn != nil {
		for obj, err = nextObjectFn(); err == nil; obj, err = nextObjectFn() {
			if err = onObject(obj); err != nil {
				return
			}
		}
		if err == ErrNoMore {
			err = nil
		}
	}
	return
}

func ExecuteFilterOnObjects(
	ctx context.Context,
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	nextObjectFn func() (objectio.ObjectStats, error),
	latestObjects []objectio.ObjectStats,
	extraObjects []objectio.ObjectStats,
	outBlocks *objectio.BlockInfoSlice,
	highSelectivityHint bool,
	fs fileservice.FileService,
) (
	totalBlocks int,
	loadHit int,
	objectFilterTotal int,
	objectFilterHit int,
	blockFilterTotal int,
	blockFilterHit int,
	fastFilterTotal int,
	fastFilterHit int,
	err error,
) {
	onObject := func(objStats objectio.ObjectStats) (err error) {
		var ok bool
		totalBlocks += int(objStats.BlkCnt())
		if fastFilterOp != nil {
			fastFilterTotal++
			if ok, err = fastFilterOp(objStats); err != nil || !ok {
				fastFilterHit++
				return
			}
		}

		var (
			meta objectio.ObjectMeta
			bf   objectio.BloomFilter
		)
		if loadOp != nil {
			loadHit++
			if meta, bf, err = loadOp(
				ctx, objStats, meta, bf,
			); err != nil {
				return
			}
		}
		if objectFilterOp != nil {
			objectFilterTotal++
			if ok, err = objectFilterOp(
				meta, bf,
			); err != nil || !ok {
				objectFilterHit++
				return
			}
		}

		var dataMeta objectio.ObjectDataMeta
		if meta != nil {
			dataMeta = meta.MustDataMeta()
		}
		var blockCnt int
		if dataMeta != nil {
			blockCnt = int(dataMeta.BlockCount())
		} else {
			blockCnt = int(objStats.BlkCnt())
		}

		var pos int
		if seekOp != nil {
			pos = seekOp(dataMeta)
		}

		if objStats.Rows() == 0 {
			logutil.Errorf("object stats has zero rows: %s", objStats.String())
			util.EnableCoreDump()
			util.CoreDump()
		}

		for ; pos < blockCnt; pos++ {
			var blkMeta objectio.BlockObject
			if dataMeta != nil && blockFilterOp != nil {
				blockFilterTotal++
				var (
					quickBreak, ok2 bool
				)
				blkMeta = dataMeta.GetBlockMeta(uint32(pos))
				if quickBreak, ok2, err = blockFilterOp(pos, blkMeta, bf); err != nil {
					return
				}
				// skip the following block checks
				if quickBreak {
					blockFilterHit++
					break
				}
				// skip this block
				if !ok2 {
					blockFilterHit++
					continue
				}
			}
			loc := objStats.BlockLocation(uint16(pos), objectio.BlockMaxRows)
			blk := objectio.BlockInfo{
				BlockID: *objectio.BuildObjectBlockid(objStats.ObjectName(), uint16(pos)),
				MetaLoc: objectio.ObjectLocation(loc),
			}

			blk.Sorted = objStats.GetSorted()
			blk.Appendable = objStats.GetAppendable()
			outBlocks.AppendBlockInfo(blk)
		}

		return
	}
	err = ForeachObjectsExecute(
		onObject,
		nextObjectFn,
		latestObjects,
		extraObjects,
	)
	return
}
