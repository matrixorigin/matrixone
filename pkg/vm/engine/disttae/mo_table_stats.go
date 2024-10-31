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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func GetMOTableStatsExecutor(
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, task task.Task) error {
		return tableStatsExecutor(ctx, service, eng, sqlExecutor)
	}
}

func tableStatsExecutor(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	executeTicker := time.NewTicker(time.Second * 10)

	for {
		select {
		case <-ctx.Done():
			logutil.Info("table stats executor exit by ctx.Done")
			return ctx.Err()

		case <-executeTicker.C:
			if err = updateTableStats(ctx, service, eng, sqlExecutor); err != nil {
				logutil.Infof("table stats executor exit by err: %v", err)
				return err
			}

			executeTicker.Reset(time.Second * 20)
		}
	}
}

// issue:
//  1. truncate not work
func updateTableStats(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {
	de := eng.(*Engine)

	de.Lock()
	defer de.Unlock()

	var (
		totalSize   float64
		totalRows   float64
		deletedSize float64
		deletedRows float64

		objIds []types.Objectid
	)

	snapshot := types.BuildTS(time.Now().UnixNano(), 0)
	for id, partition := range de.partitions {
		start := time.Now()

		tblItem := de.catalog.GetTableById(0, id[0], id[1])
		if !strings.Contains(tblItem.Name, "hhhh") {
			continue
		}

		pState := partition.Snapshot().Copy()
		totalRows, totalSize, err = collectVisibleData(snapshot, &objIds, pState)
		if deletedRows, err = applyTombstones(ctx, de.fs, de.mp, snapshot, objIds, pState); err != nil {
			return err
		}

		deletedSize = totalSize / totalRows * deletedRows

		fmt.Printf("%s(%d)-%s(%d), %f, %f, %v\n",
			tblItem.DatabaseName, id[0], tblItem.Name, id[1],
			(totalSize-deletedSize)/1024.0/1024.0, totalRows-deletedRows,
			time.Since(start))

		totalRows = 0
		deletedSize = 0
		objIds = objIds[:0]
	}

	return nil
}

// O(m+n)
func getDeletedRows(
	objIds []types.Objectid,
	rowIds []types.Rowid,
) (deletedCnt int) {

	var (
		i int
		j int
	)

	for i < len(objIds) && j < len(rowIds) {
		if j > 0 && rowIds[j-1].EQ(&rowIds[j]) {
			j++
			continue
		}

		cmp := rowIds[j].BorrowObjectID().Compare(&objIds[i])

		if cmp == 0 {
			deletedCnt++
			j++
		} else if cmp > 0 {
			i++
		} else {
			// cmp < 0
			j++
		}
	}

	return deletedCnt
}

func collectVisibleData(
	snapshot types.TS,
	outObjIds *[]types.Objectid,
	pState *logtailreplay.PartitionState,
) (visibleRows, visibleSize float64, err error) {

	*outObjIds = make([]types.Objectid, 0, pState.ApproxDataObjectsNum())

	var (
		rowIter logtailreplay.RowsIter
		objIter logtailreplay.ObjectsIter

		estimatedOneRowSize float64
	)
	if objIter, err = pState.NewObjectsIter(
		snapshot, true, false); err != nil {
		return
	}

	for objIter.Next() {
		obj := objIter.Entry()
		visibleSize += float64(obj.Size())
		visibleRows += float64(obj.Rows())
		*outObjIds = append(*outObjIds, *obj.ObjectStats.ObjectName().ObjectId())
	}

	if err = objIter.Close(); err != nil {
		return
	}

	if visibleRows != 0 {
		estimatedOneRowSize = visibleSize / visibleRows
	}

	rowIter = pState.NewRowsIter(snapshot, nil, false)
	for rowIter.Next() {
		row := rowIter.Entry()
		if !row.BlockID.Object().EQ(&((*outObjIds)[len(*outObjIds)-1])) {
			*outObjIds = append(*outObjIds, *row.BlockID.Object())
		}

		visibleRows += float64(1)
		visibleSize += estimatedOneRowSize
	}

	err = rowIter.Close()
	return
}

// 1. deletes in tombstone obj can be a deletes on in-mem rows
// 2. deletes in in-mem rows can be a deletes on rows in data objects

func applyTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	snapshot types.TS,
	dataObjIds []types.Objectid,
	pState *logtailreplay.PartitionState,
) (deletedRows float64, err error) {
	var (
		hidden  objectio.HiddenColumnSelection
		release func()
		objIter logtailreplay.ObjectsIter
	)

	if objIter, err = pState.NewObjectsIter(
		snapshot, true, true); err != nil {
		return
	}

	for objIter.Next() {
		tombstone := objIter.Entry()

		attrs := objectio.GetTombstoneAttrs(hidden)
		persistedDeletes := containers.NewVectors(len(attrs))

		ForeachBlkInObjStatsList(true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {

				if _, release, err = blockio.ReadDeletes(
					ctx, blk.MetaLoc[:], fs, tombstone.GetCNCreated(), persistedDeletes,
				); err != nil {
					return false
				}
				defer release()

				rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](&persistedDeletes[0])
				cnt := getDeletedRows(dataObjIds, rowIds)

				deletedRows += float64(cnt)

				return true
			}, tombstone.ObjectStats)

		if err != nil {
			return
		}
	}

	if err = objIter.Close(); err != nil {
		return
	}

	vec := vector.NewVec(types.T_Rowid.ToType())
	if err = vec.PreExtend(options.DefaultBlockMaxRows, mp); err != nil {
		return
	}

	if err = pState.CollectInMemDeletesOnNAObjs(mp, snapshot, vec); err != nil {
		return
	}

	rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
	deletedRows += float64(getDeletedRows(dataObjIds, rowIds))

	return
}
