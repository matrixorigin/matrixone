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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

	var iter logtailreplay.ObjectsIter
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	getNextStats := func() (objectio.ObjectStats, error) {
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
			return objectio.ZeroObjectStats, engine_util.ErrNoMore
		}
		return iter.Entry().ObjectStats, nil
	}

	totalBlocks, loadHit,
		objFilterTotal, objFilterHit,
		blkFilterTotal, blkFilterHit,
		fastFilterTotal, fastFilterHit,
		err := engine_util.ExecuteFilterOnObjects(
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
