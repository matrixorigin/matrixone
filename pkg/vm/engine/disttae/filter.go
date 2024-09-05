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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TryFastFilterBlocks(
	ctx context.Context,
	tbl *txnTable,
	snapshotTS timestamp.Timestamp,
	tableDef *plan.TableDef,
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	extraCommittedObjects []objectio.ObjectStats,
	uncommittedObjects []objectio.ObjectStats,
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (ok bool, err error) {
	fastFilterOp, loadOp, objectFilterOp, blockFilterOp, seekOp, ok, highSelectivityHint := engine_util.CompileFilterExprs(exprs, proc, tableDef, fs)
	if !ok {
		return false, nil
	}

	err = ExecuteBlockFilter(
		ctx,
		tbl,
		snapshotTS,
		fastFilterOp,
		loadOp,
		objectFilterOp,
		blockFilterOp,
		seekOp,
		snapshot,
		extraCommittedObjects,
		uncommittedObjects,
		outBlocks,
		fs,
		proc,
		highSelectivityHint,
	)
	return true, err
}

func ExecuteBlockFilter(
	ctx context.Context,
	tbl *txnTable,
	snapshotTS timestamp.Timestamp,
	fastFilterOp engine_util.FastFilterOp,
	loadOp engine_util.LoadOp,
	objectFilterOp engine_util.ObjectFilterOp,
	blockFilterOp engine_util.BlockFilterOp,
	seekOp engine_util.SeekFirstBlockOp,
	snapshot *logtailreplay.PartitionState,
	extraCommittedObjects []objectio.ObjectStats,
	uncommittedObjects []objectio.ObjectStats,
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
	highSelectivityHint bool,
) (err error) {
	var (
		totalBlocks                    float64
		loadHit                        float64
		objFilterTotal, objFilterHit   float64
		blkFilterTotal, blkFilterHit   float64
		fastFilterTotal, fastFilterHit float64
	)

	defer func() {
		v2.TxnRangesFastPathLoadObjCntHistogram.Observe(loadHit)
		v2.TxnRangesFastPathSelectedBlockCntHistogram.Observe(float64(outBlocks.Len() - 1))
		if fastFilterTotal > 0 {
			v2.TxnRangesFastPathObjSortKeyZMapSelectivityHistogram.Observe(fastFilterHit / fastFilterTotal)
		}
		if objFilterTotal > 0 {
			v2.TxnRangesFastPathObjColumnZMapSelectivityHistogram.Observe(objFilterHit / objFilterTotal)
		}
		if blkFilterTotal > 0 {
			v2.TxnRangesFastPathBlkColumnZMapSelectivityHistogram.Observe(blkFilterHit / blkFilterTotal)
		}
		if totalBlocks > 0 {
			v2.TxnRangesFastPathBlkTotalSelectivityHistogram.Observe(float64(outBlocks.Len()-1) / totalBlocks)
		}
	}()

	err = ForeachSnapshotObjects(
		snapshotTS,
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var ok bool
			objStats := obj.ObjectStats
			totalBlocks += float64(objStats.BlkCnt())
			if fastFilterOp != nil {
				fastFilterTotal++
				if ok, err2 = fastFilterOp(objStats); err2 != nil || !ok {
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
				if meta, bf, err2 = loadOp(
					ctx, objStats, meta, bf,
				); err2 != nil {
					return
				}
			}
			if objectFilterOp != nil {
				objFilterTotal++
				if ok, err2 = objectFilterOp(meta, bf); err2 != nil || !ok {
					objFilterHit++
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

			name := objStats.ObjectName()
			extent := objStats.Extent()

			var pos int
			if seekOp != nil {
				pos = seekOp(dataMeta)
			}

			if objStats.Rows() == 0 {
				logutil.Errorf("object stats has zero rows, isCommitted: %v, detail: %s",
					isCommitted, obj.String())
				util.EnableCoreDump()
				util.CoreDump()
			}

			for ; pos < blockCnt; pos++ {
				var blkMeta objectio.BlockObject
				if dataMeta != nil && blockFilterOp != nil {
					blkFilterTotal++
					var (
						quickBreak, ok2 bool
					)
					blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					if quickBreak, ok2, err2 = blockFilterOp(pos, blkMeta, bf); err2 != nil {
						return

					}
					// skip the following block checks
					if quickBreak {
						blkFilterHit++
						break
					}
					// skip this block
					if !ok2 {
						blkFilterHit++
						continue
					}
				}
				var rows uint32
				if objRows := objStats.Rows(); objRows != 0 {
					if pos < blockCnt-1 {
						rows = objectio.BlockMaxRows
					} else {
						rows = objRows - objectio.BlockMaxRows*uint32(pos)
					}
				} else {
					if blkMeta == nil {
						blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					}
					rows = blkMeta.GetRows()
				}
				loc := objectio.BuildLocation(name, extent, rows, uint16(pos))
				blk := objectio.BlockInfo{
					BlockID: *objectio.BuildObjectBlockid(name, uint16(pos)),
					MetaLoc: objectio.ObjectLocation(loc),
				}

				blk.Sorted = obj.Sorted
				blk.Appendable = obj.Appendable
				outBlocks.AppendBlockInfo(blk)
			}

			return
		},
		snapshot,
		extraCommittedObjects,
		uncommittedObjects...,
	)
	return
}
