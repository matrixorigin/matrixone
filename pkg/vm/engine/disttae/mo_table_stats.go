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
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"math"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

const (
	TableStatsTableSize = iota
	TableStatsTableRows

	TableStatsTObjectCnt
	TableStatsDObjectCnt

	TableStatsTBlockCnt
	TableStatsDBlockCnt

	TableStatsCnt
)

const (
	insertOrUpdate = "" +
		"insert into " +
		"	`mo_catalog`.`mo_table_stats` " +
		"	(`account_id`,`database_id`,`table_id`,`database_name`,`table_name`,`table_stats`,`update_time`,`took`) " +
		"	values(%d,%d,%d,'%s','%s','%s',CURRENT_TIMESTAMP, %d) " +
		"on duplicate key update " +
		"	`table_stats` = '%s', `update_time` = CURRENT_TIMESTAMP, `took` = %d;"
)

var TableStatsName = [TableStatsCnt]string{
	"table_size",
	"table_rows",
	"tobject_cnt",
	"dobject_cnt",
	"tblock_cnt",
	"dblock_cnt",
}

type tablePair struct {
	acc, db, tbl    uint64
	dbName, tblName string
}

type statsList struct {
	took  time.Duration
	stats map[string]any
}

type MoTableStats struct {
	stats map[tablePair]*statsList
	stash struct {
		took time.Duration

		dataObjIds []types.Objectid

		totalSize   float64
		totalRows   float64
		deletedRows float64

		tblockCnt, dblockCnt   int
		tobjectCnt, dobjectCnt int
	}
}

func (mts *MoTableStats) stashToStats(pair tablePair) {
	var (
		ok bool
		sl *statsList
	)

	if sl, ok = moTableStats.stats[pair]; !ok {
		sl = new(statsList)
		sl.stats = make(map[string]any)
		moTableStats.stats[pair] = sl
	}

	// table rows
	{
		leftRows := mts.stash.totalRows - mts.stash.deletedRows
		sl.stats[TableStatsName[TableStatsTableRows]] = leftRows
	}

	// table size
	{
		deletedSize := float64(0)
		if mts.stash.totalRows > 0 && mts.stash.deletedRows > 0 {
			deletedSize = mts.stash.totalSize / mts.stash.totalRows * mts.stash.deletedRows
		}

		leftSize := math.Round((mts.stash.totalSize-deletedSize)*1000) / 1000
		sl.stats[TableStatsName[TableStatsTableSize]] = leftSize
	}

	// data object, block count
	// tombstone object, block count
	{
		sl.stats[TableStatsName[TableStatsTObjectCnt]] = mts.stash.tobjectCnt
		sl.stats[TableStatsName[TableStatsTBlockCnt]] = mts.stash.tblockCnt
		sl.stats[TableStatsName[TableStatsDObjectCnt]] = mts.stash.dobjectCnt
		sl.stats[TableStatsName[TableStatsDBlockCnt]] = mts.stash.dblockCnt
	}

	sl.took = mts.stash.took
}
func (mts *MoTableStats) cleanStash() {
	mts.stash.took = 0

	mts.stash.tblockCnt = 0
	mts.stash.tobjectCnt = 0

	mts.stash.dblockCnt = 0
	mts.stash.dobjectCnt = 0

	mts.stash.totalRows = 0
	mts.stash.totalSize = 0
	mts.stash.deletedRows = 0

	mts.stash.dataObjIds = mts.stash.dataObjIds[:0]
}

var moTableStats MoTableStats

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

	if moTableStats.stats == nil {
		moTableStats.stats = make(map[tablePair]*statsList)
	}

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

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

	var (
		accs, dbs, tbls []uint64
	)

	accs, dbs, tbls, err = collectAndSubscribeTables(ctx, eng, sqlExecutor)
	if err != nil {
		return err
	}

	snapshot := types.BuildTS(time.Now().UnixNano(), 0)
	for i := range tbls {
		start := time.Now()

		tblItem := de.catalog.GetTableById(uint32(accs[i]), dbs[i], tbls[i])
		if !strings.Contains(tblItem.Name, "bmsql") &&
			!strings.Contains(tblItem.Name, "hhh") {
			continue
		}

		pState := de.GetOrCreateLatestPart(dbs[i], tbls[i]).Snapshot()
		moTableStats.stash.totalRows, moTableStats.stash.totalSize, err = collectVisibleData(snapshot, pState)
		if moTableStats.stash.deletedRows, err = applyTombstones(ctx, de.fs, de.mp, snapshot, pState); err != nil {
			return err
		}

		tbl := tablePair{
			acc:     accs[i],
			db:      dbs[i],
			tbl:     tbls[i],
			dbName:  tblItem.DatabaseName,
			tblName: tblItem.Name,
		}

		moTableStats.stash.took = time.Since(start)

		moTableStats.stashToStats(tbl)
		moTableStats.cleanStash()
	}

	err = updateStatsTable(ctx, service, sqlExecutor)

	return err
}

func updateStatsTable(
	ctx context.Context,
	service string,
	executor func() ie.InternalExecutor,
) (err error) {

	var (
		val []byte
		ret ie.InternalExecResult
	)

	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	for tbl, sl := range moTableStats.stats {
		if val, err = json.Marshal(sl.stats); err != nil {
			return err
		}

		sql := fmt.Sprintf(insertOrUpdate,
			tbl.acc, tbl.db, tbl.tbl,
			tbl.dbName, tbl.tblName,
			string(val), sl.took.Microseconds(), string(val), sl.took.Microseconds())

		ret = executor().Query(ctx, sql, opts)

		if err = ret.Error(); err != nil {
			fmt.Println(err, sql)
			return err
		}
	}

	return nil
}

func collectAndSubscribeTables(
	ctx context.Context,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (accs, dbs, tbls []uint64, err error) {

	cc := eng.(*Engine).GetLatestCatalogCache()
	cc.TableItemScan(func(item *cache.TableItem) bool {
		if strings.ToUpper(item.Kind) != "R" {
			return true
		}

		tbl := txnTable{}
		tbl.tableId = item.Id
		tbl.tableName = item.Name
		tbl.accountId = uint32(item.AccountId)
		tbl.db = &txnDatabase{
			databaseId:   item.DatabaseId,
			databaseName: item.DatabaseName,
		}

		tbl.primarySeqnum = item.PrimarySeqnum
		if _, err = eng.(*Engine).PushClient().toSubscribeTable(ctx, &tbl); err != nil {
			return false
		}

		tbls = append(tbls, item.Id)
		dbs = append(dbs, item.DatabaseId)
		accs = append(accs, uint64(item.AccountId))

		return true
	})

	return
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
	pState *logtailreplay.PartitionState,
) (visibleRows, visibleSize float64, err error) {

	moTableStats.stash.dataObjIds = make([]types.Objectid, 0, pState.ApproxDataObjectsNum())

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

		moTableStats.stash.dobjectCnt++
		moTableStats.stash.dblockCnt += int(obj.BlkCnt())

		visibleSize += float64(obj.Size())
		visibleRows += float64(obj.Rows())
		moTableStats.stash.dataObjIds = append(
			moTableStats.stash.dataObjIds, *obj.ObjectStats.ObjectName().ObjectId())
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
		if len(moTableStats.stash.dataObjIds) == 0 ||
			!row.BlockID.Object().EQ(
				&(moTableStats.stash.dataObjIds[len(moTableStats.stash.dataObjIds)-1])) {
			moTableStats.stash.dataObjIds = append(
				moTableStats.stash.dataObjIds, *row.BlockID.Object())
		}

		visibleRows += float64(1)
		visibleSize += estimatedOneRowSize
	}

	err = rowIter.Close()
	return
}

func applyTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	snapshot types.TS,
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

		moTableStats.stash.tobjectCnt++
		moTableStats.stash.tblockCnt += int(tombstone.BlkCnt())

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
				cnt := getDeletedRows(moTableStats.stash.dataObjIds, rowIds)

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
	deletedRows += float64(getDeletedRows(moTableStats.stash.dataObjIds, rowIds))

	return
}
