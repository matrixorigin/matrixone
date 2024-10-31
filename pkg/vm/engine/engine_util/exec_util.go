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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var ErrNoMore = moerr.NewInternalErrorNoCtx("no more")

func StreamBatchProcess(
	ctx context.Context,
	// sourcer loads data into the input batch. The input batch is always empty
	sourcer func(context.Context, *batch.Batch, *mpool.MPool) (bool, error),
	// processor always do some in-place operations on the input batch
	processor func(context.Context, *batch.Batch, *mpool.MPool) error,
	// sinker always copy the input batch
	sinker func(context.Context, *batch.Batch) error,
	buffer containers.IBatchBuffer,
	mp *mpool.MPool,
) error {
	bat := buffer.Fetch()
	defer buffer.Putback(bat, mp)
	for {
		bat.CleanOnlyData()
		done, err := sourcer(ctx, bat, mp)
		if err != nil {
			return err
		}
		if done {
			break
		}
		if err := processor(ctx, bat, mp); err != nil {
			return err
		}
		if err := sinker(ctx, bat); err != nil {
			return err
		}
	}
	return nil
}

func ForeachObjectsExecute(
	onObject func(*objectio.ObjectStats) error,
	nextObjectFn func() (objectio.ObjectStats, error),
	latestObjects []objectio.ObjectStats,
	extraObjects []objectio.ObjectStats,
) (err error) {
	for _, obj := range latestObjects {
		if err = onObject(&obj); err != nil {
			return
		}
	}
	for _, obj := range extraObjects {
		if err = onObject(&obj); err != nil {
			return
		}
	}
	if nextObjectFn != nil {
		var obj objectio.ObjectStats
		for obj, err = nextObjectFn(); err == nil; obj, err = nextObjectFn() {
			if err = onObject(&obj); err != nil {
				return
			}
		}
		if err == ErrNoMore {
			err = nil
		}
	}
	return
}

func FilterObjects(
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
	onObject := func(objStats *objectio.ObjectStats) (err error) {
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
			logutil.Errorf("object stats has zero rows: %s", objStats.ObjectName().String())
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
			var blk objectio.BlockInfo
			objStats.ConstructBlockInfoTo(uint16(pos), &blk)
			outBlocks.AppendBlockInfo(&blk)
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

func TryFastFilterBlocks(
	ctx context.Context,
	snapshotTS timestamp.Timestamp,
	tableDef *plan.TableDef,
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	extraCommittedObjects []objectio.ObjectStats,
	uncommittedObjects []objectio.ObjectStats,
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
) (ok bool, err error) {
	fastFilterOp, loadOp, objectFilterOp, blockFilterOp, seekOp, ok, highSelectivityHint := CompileFilterExprs(exprs, tableDef, fs)
	if !ok {
		return false, nil
	}

	err = FilterTxnObjects(
		ctx,
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
		highSelectivityHint,
	)
	return true, err
}

func FilterTxnObjects(
	ctx context.Context,
	snapshotTS timestamp.Timestamp,
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	snapshot *logtailreplay.PartitionState,
	extraCommittedObjects []objectio.ObjectStats,
	uncommittedObjects []objectio.ObjectStats,
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	highSelectivityHint bool,
) (err error) {

	var iter logtailreplay.ObjectsIter
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	var getNextStats func() (objectio.ObjectStats, error)

	if snapshot != nil {
		getNextStats = func() (objectio.ObjectStats, error) {
			if iter == nil {
				iter, err = snapshot.NewObjectsIter(
					types.TimestampToTS(snapshotTS),
					true,
					false,
				)
				if err != nil {
					return objectio.ZeroObjectStats, err
				}
			}
			if !iter.Next() {
				return objectio.ZeroObjectStats, ErrNoMore
			}
			return iter.Entry().ObjectStats, nil
		}
	}

	totalBlocks, loadHit,
		objFilterTotal, objFilterHit,
		blkFilterTotal, blkFilterHit,
		fastFilterTotal, fastFilterHit,
		err := FilterObjects(
		ctx,
		fastFilterOp,
		loadOp,
		objectFilterOp,
		blockFilterOp,
		seekOp,
		getNextStats,
		uncommittedObjects,
		extraCommittedObjects,
		outBlocks,
		highSelectivityHint,
		fs,
	)

	if err != nil {
		return err
	}

	v2.TxnRangesFastPathLoadObjCntHistogram.Observe(float64(loadHit))
	v2.TxnRangesFastPathSelectedBlockCntHistogram.Observe(float64(outBlocks.Len() - 1))
	if fastFilterTotal > 0 {
		v2.TxnRangesFastPathObjSortKeyZMapSelectivityHistogram.Observe(float64(fastFilterHit) / float64(fastFilterTotal))
	}
	if objFilterTotal > 0 {
		v2.TxnRangesFastPathObjColumnZMapSelectivityHistogram.Observe(float64(objFilterHit) / float64(objFilterTotal))
	}
	if blkFilterTotal > 0 {
		v2.TxnRangesFastPathBlkColumnZMapSelectivityHistogram.Observe(float64(blkFilterHit) / float64(blkFilterTotal))
	}
	if totalBlocks > 0 {
		v2.TxnRangesFastPathBlkTotalSelectivityHistogram.Observe(float64(outBlocks.Len()-1) / float64(totalBlocks))
	}

	return nil
}
