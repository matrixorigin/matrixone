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
	"go.uber.org/zap"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// considerations:
// 1. truncate table
// 2. truncate database
// 3. index table size, rows
// 4. negative rows, size
// 5. waiting too long to get correct rows
// 6. why size remain 0 after load into table? (waiting a longer???)

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
	insertOrUpdateSQL = `
				insert into 
				    %s.%s (account_id, database_id, table_id, database_name, table_name, table_stats, update_time, takes)
					values(%d, %d, %d, '%s', '%s', '%s', '%s', %d)
				on duplicate key update
					table_stats = '%s', update_time = '%s', takes = %d;`

	lastUpdateTSSQL = `select min(update_time) from %s.%s;`

	getTableStatsSQL = `
				select 
    				table_id, COALESCE(table_stats, CAST('{}' AS JSON)) from  %s.%s
              	where 
                	account_id in (%v) and 
                	database_id in (%v) and 
                  	table_id in (%v) 
			  	order by table_id asc;`
)

const (
	alphaCycleDur = time.Minute
	betaCycleDur  = time.Second * 5
)

var TableStatsName = [TableStatsCnt]string{
	"table_size",
	"table_rows",
	"tobject_cnt",
	"dobject_cnt",
	"tblock_cnt",
	"dblock_cnt",
}

////////////////// MoTableStats Interface //////////////////

func intsJoin(items []uint64, delimiter string) string {
	str := ""
	for i := range items {
		str += fmt.Sprintf("%d", items[i])
		if i < len(items)-1 {
			str += delimiter
		}
	}
	return str
}

func queryStats(
	ctx context.Context,
	statsIdx int,
	defaultVal any,
	accs, dbs, tbls []uint64,
) (statsVals []any, err error) {

	executor := moTableStats.sqlExecutor()
	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	sql := fmt.Sprintf(getTableStatsSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		intsJoin(accs, ","),
		intsJoin(dbs, ","),
		intsJoin(tbls, ","))

	// tricky here, only sys can visit this table,
	// but there should have not any privilege leak, since the [acc, dbs, tbls] already checked.
	sysCtx := context.WithValue(context.Background(), defines.TenantIDKey{}, catalog.System_Account)

	ret := executor.Query(sysCtx, sql, opts)
	if err = ret.Error(); err != nil {
		return
	}

	var (
		idxes   []uint64
		gotTIds []uint64
		stats   map[string]any
		val     interface{}
	)

	for i := range ret.RowCount() {
		if val, err = ret.Value(ctx, i, 0); err != nil {
			return
		}

		idxes = append(idxes, i)
		gotTIds = append(gotTIds, val.(uint64))
	}

	for i := range tbls {
		idx, found := sort.Find(len(gotTIds), func(j int) int { return int(tbls[i]) - int(gotTIds[j]) })

		if !found {
			statsVals = append(statsVals, defaultVal)
			continue
		}

		if val, err = ret.Value(ctx, idxes[idx], 1); err != nil {
			return
		}

		if err = json.Unmarshal([]byte(val.(bytejson.ByteJson).String()), &stats); err != nil {
			return
		}

		statsVals = append(statsVals, stats[TableStatsName[statsIdx]])
	}

	return
}

func MTSTableSize(
	ctx context.Context, server string,
	accs, dbs, tbls []uint64,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(ctx, TableStatsTableSize, float64(0), accs, dbs, tbls)
	if err != nil {
		return nil, err
	}

	for i := range statsVals {
		sizes = append(sizes, uint64(statsVals[i].(float64)))
	}

	return
}

func MTSTableRows(
	ctx context.Context, server string,
	accs, dbs, tbls []uint64,
) (sizes []uint64, err error) {

	statsVals, err := queryStats(ctx, TableStatsTableRows, float64(0), accs, dbs, tbls)
	if err != nil {
		return nil, err
	}

	for i := range statsVals {
		sizes = append(sizes, uint64(statsVals[i].(float64)))
	}

	return
}

/////////////// MoTableStats Implementation ///////////////

type tablePair struct {
	acc, db, tbl    uint64
	dbName, tblName string
}

func (tp tablePair) String() string {
	return fmt.Sprintf("%d-%s(%d)-%s(%d)",
		tp.acc, tp.dbName, tp.db, tp.tblName, tp.tbl)
}

type statsList struct {
	took  time.Duration
	stats map[string]any
}

type MoTableStats struct {
	stats map[tablePair]*statsList

	sqlExecutor  func() ie.InternalExecutor
	lastUpdateTS types.TS

	// alphaCycleStash
	acs struct {
		snapshot types.TS

		dataBlkNeedToLoad      int
		tombstoneBlkNeedToLoad int
		blkNeedToLoadDelta     int

		pStateStack []*logtailreplay.PartitionState

		dObjIterStack []logtailreplay.ObjectsIter
		tObjIterStack []logtailreplay.ObjectsIter
		dRowIterStack []logtailreplay.RowsIter
		tRowIterStack []logtailreplay.RowsIter

		tblInfoStack []tablePair
	}

	// betaCycleStash
	bcs struct {
		took time.Duration

		//vec        *vector.Vector
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
		leftRows := mts.bcs.totalRows - mts.bcs.deletedRows
		sl.stats[TableStatsName[TableStatsTableRows]] = leftRows
	}

	// table size
	{
		deletedSize := float64(0)
		if mts.bcs.totalRows > 0 && mts.bcs.deletedRows > 0 {
			deletedSize = mts.bcs.totalSize / mts.bcs.totalRows * mts.bcs.deletedRows
		}

		leftSize := math.Round((mts.bcs.totalSize-deletedSize)*1000) / 1000
		sl.stats[TableStatsName[TableStatsTableSize]] = leftSize
	}

	// data object, block count
	// tombstone object, block count
	{
		sl.stats[TableStatsName[TableStatsTObjectCnt]] = mts.bcs.tobjectCnt
		sl.stats[TableStatsName[TableStatsTBlockCnt]] = mts.bcs.tblockCnt
		sl.stats[TableStatsName[TableStatsDObjectCnt]] = mts.bcs.dobjectCnt
		sl.stats[TableStatsName[TableStatsDBlockCnt]] = mts.bcs.dblockCnt
	}

	sl.took = mts.bcs.took
}

func (mts *MoTableStats) cleanStash() {
	mts.bcs.took = 0

	mts.bcs.tblockCnt = 0
	mts.bcs.tobjectCnt = 0

	mts.bcs.dblockCnt = 0
	mts.bcs.dobjectCnt = 0

	mts.bcs.totalRows = 0
	mts.bcs.totalSize = 0
	mts.bcs.deletedRows = 0

	//mts.bcs.vec.CleanOnlyData()
	mts.bcs.dataObjIds = mts.bcs.dataObjIds[:0]
}

func (mts *MoTableStats) freeStash(mp *mpool.MPool) {
	mts.acs.dRowIterStack = nil
	mts.acs.dObjIterStack = nil
	mts.acs.tObjIterStack = nil
	mts.acs.tRowIterStack = nil
	mts.acs.tblInfoStack = nil
	mts.acs.pStateStack = nil

	mts.bcs.dataObjIds = nil
	//if mts.bcs.vec != nil {
	//	mts.bcs.vec.Free(mp)
	//}
	mts.stats = nil
}

func (mts *MoTableStats) init(
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {
	if mts.stats != nil {
		return
	}

	moTableStats.stats = make(map[tablePair]*statsList)
	//moTableStats.bcs.vec = vector.NewVec(types.T_Rowid.ToType())
	//err = moTableStats.bcs.vec.PreExtend(options.DefaultBlockMaxRows, eng.(*Engine).mp)
	//if err != nil {
	//	return err
	//}

	moTableStats.sqlExecutor = sqlExecutor

	{
		ff1 := func() func(
			context.Context, string, []uint64, []uint64, []uint64) ([]uint64, error) {
			return MTSTableSize
		}
		function.GetMoTableSizeFunc.Store(&ff1)

		ff2 := func() func(
			context.Context, string, []uint64, []uint64, []uint64) ([]uint64, error) {
			return MTSTableRows
		}
		function.GetMoTableRowsFunc.Store(&ff2)
	}

	return nil
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

	if err = moTableStats.init(eng, sqlExecutor); err != nil {
		return
	}

	defer func() {
		moTableStats.freeStash(eng.(*Engine).mp)
	}()

	if val := ctx.Value(defines.TenantIDKey{}); val == nil {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}

	tickerDur := time.Second
	executeTicker := time.NewTicker(tickerDur)

	for {
		select {
		case <-ctx.Done():
			logutil.Info("table stats executor exit by ctx.Done")
			return ctx.Err()

		case <-executeTicker.C:
			if err = statsCalculator(ctx, service, eng, sqlExecutor); err != nil {
				logutil.Infof("table stats executor exit by err: %v", err)
				return err
			}

			executeTicker.Reset(alphaCycleDur)
		}
	}
}

func statsCalculator(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	var (
		newCtx context.Context
		cancel context.CancelFunc
	)

	if _, ok := ctx.Deadline(); !ok {
		newCtx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
	}

	if err = prepare(newCtx, service, eng, sqlExecutor); err != nil {
		return err
	}

	var done bool

	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		if done, err =
			betaCycleScheduler(newCtx, service, eng, sqlExecutor); err != nil || done {
			break
		}
		ticker.Reset(betaCycleDur)
	}

	if err != nil {
		return err
	}
	return nil
}

func getChangedTableList(
	ctx context.Context,
	service string,
	eng engine.Engine,
	from types.TS,
) (tbls []tablePair, pkSequms []int, newest types.TS, err error) {

	defer func() {
		logutil.Info("get changed table list",
			zap.Time("from", from.ToTimestamp().ToStdTime()),
			zap.Int("cnt", len(tbls)))
	}()

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := cmd_util.GetChangedTableListReq{}
		ts := from.ToTimestamp()
		req.From = &ts
		return req.Marshal()
	}

	responseUnmarshaler := func(payload []byte) (any, error) {
		list := &cmd_util.GetChangedTableListResp{}
		if err = list.Unmarshal(payload); err != nil {
			return nil, err
		}
		return list, nil
	}

	de := eng.(*Engine)
	txnOperator, err := de.cli.New(ctx, timestamp.Timestamp{})
	if err != nil {
		return nil, nil, types.TS{}, err
	}

	proc := process.NewTopProcess(ctx,
		de.mp, de.cli, txnOperator,
		de.fs, de.ls,
		de.qc, de.hakeeper,
		de.us, nil,
	)

	handler := ctl.GetTNHandlerFunc(api.OpCode_OpGetChangedTableList, whichTN, payload, responseUnmarshaler)
	ret, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if err != nil {
		return nil, nil, types.TS{}, err
	}

	cc := de.GetLatestCatalogCache()

	resp := ret.Data.([]any)[0].(*cmd_util.GetChangedTableListResp)

	if len(resp.AccIds) == 0 {
		return nil, nil, types.TS{}, nil
	}

	for i := range resp.AccIds {
		item := cc.GetTableById(uint32(resp.AccIds[i]), resp.DatabaseIds[i], resp.TableIds[i])
		if item == nil {
			continue
		}

		tbls = append(tbls, tablePair{
			acc:     uint64(item.AccountId),
			db:      item.DatabaseId,
			tbl:     item.Id,
			tblName: item.Name,
			dbName:  item.DatabaseName,
		})

		pkSequms = append(pkSequms, int(item.PrimarySeqnum))
	}

	return tbls, pkSequms, types.TimestampToTS(*resp.Newest), nil
}

func getLastUpdateTS(
	ctx context.Context,
	sqlExecutor func() ie.InternalExecutor,
) (types.TS, error) {
	if !moTableStats.lastUpdateTS.IsEmpty() {
		return moTableStats.lastUpdateTS, nil
	}

	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()
	ret := sqlExecutor().Query(ctx, fmt.Sprintf(lastUpdateTSSQL, catalog.MO_CATALOG, catalog.MO_TABLE_STATS), opts)
	if ret.Error() != nil {
		return types.TS{}, ret.Error()
	}

	val, err := ret.Value(ctx, 0, 0)
	if err != nil {
		return types.TS{}, err
	}

	if val == nil {
		return types.TS{}, nil
	}

	tt, err := time.Parse("2006-01-02 15:04:05.000000", val.(string))
	if err != nil {
		return types.TS{}, err
	}

	moTableStats.lastUpdateTS = types.BuildTS(tt.UnixNano(), 0)
	return moTableStats.lastUpdateTS, nil
}

func prepare(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (err error) {

	var (
		tbls     []tablePair
		pkSequms []int

		now       types.TS
		lastMinTS types.TS
		iter      logtailreplay.ObjectsIter
		pState    *logtailreplay.PartitionState
	)

	lastMinTS, err = getLastUpdateTS(ctx, sqlExecutor)
	if err != nil {
		return err
	}

	tbls, pkSequms, now, err = getChangedTableList(ctx, service, eng, lastMinTS)
	for i, item := range tbls {
		tbl := txnTable{}
		tbl.tableId = item.tbl
		tbl.tableName = item.tblName
		tbl.accountId = uint32(item.acc)
		tbl.db = &txnDatabase{
			databaseId:   item.db,
			databaseName: item.dbName,
		}

		tbl.primarySeqnum = pkSequms[i]
		if pState, err = eng.(*Engine).PushClient().toSubscribeTable(ctx, &tbl); err != nil {
			return err
		}

		if pState == nil {
			continue
		}

		pState = pState.Copy()

		dBlkCnt, tBlkCnt, _, _ := pState.GetDTBlockCnt()

		moTableStats.acs.dataBlkNeedToLoad = dBlkCnt
		moTableStats.acs.tombstoneBlkNeedToLoad = tBlkCnt

		if iter, err = pState.NewObjectsIter(now, true, false); err != nil {
			return err
		}
		moTableStats.acs.dObjIterStack = append(moTableStats.acs.dObjIterStack, iter)

		if iter, err = pState.NewObjectsIter(now, true, true); err != nil {
			return err
		}
		moTableStats.acs.tObjIterStack = append(moTableStats.acs.tObjIterStack, iter)
		moTableStats.acs.pStateStack = append(moTableStats.acs.pStateStack, pState)

		moTableStats.acs.dRowIterStack = append(moTableStats.acs.dRowIterStack,
			pState.NewRowsIter(now, nil, false))

		moTableStats.acs.tRowIterStack = append(moTableStats.acs.tRowIterStack,
			pState.NewRowsIter(now, nil, true))

		moTableStats.acs.tblInfoStack =
			append(moTableStats.acs.tblInfoStack, tablePair{
				db:      item.db,
				tbl:     item.tbl,
				acc:     uint64(item.acc),
				tblName: item.tblName,
				dbName:  item.dbName,
			})
	}

	moTableStats.lastUpdateTS = now
	moTableStats.acs.blkNeedToLoadDelta =
		int(math.Ceil(float64(moTableStats.acs.tombstoneBlkNeedToLoad+
			moTableStats.acs.dataBlkNeedToLoad) / float64(alphaCycleDur/betaCycleDur)))

	return nil
}

func betaCycleScheduler(
	ctx context.Context,
	service string,
	eng engine.Engine,
	sqlExecutor func() ie.InternalExecutor,
) (done bool, err error) {

	de := eng.(*Engine)
	var start time.Time = time.Now()

	for {
		if len(moTableStats.acs.tblInfoStack) == 0 {
			return true, nil
		}

		var (
			loaded     int
			loadedDBlk int
			loadedTBlk int

			totalSize   float64
			totalRows   float64
			deletedRows float64

			tblInfo       = moTableStats.acs.tblInfoStack[0]
			blkNeedToLoad = moTableStats.acs.blkNeedToLoadDelta

			pState   = moTableStats.acs.pStateStack[0]
			dObjIter = moTableStats.acs.dObjIterStack[0]
			tObjIter = moTableStats.acs.tObjIterStack[0]
			dRowIter = moTableStats.acs.dRowIterStack[0]
			tRowIter = moTableStats.acs.tRowIterStack[0]
		)

		if totalRows, totalSize, loaded, err = collectVisibleData(dObjIter, dRowIter); err != nil {
			return false, err
		}

		loadedDBlk += loaded
		moTableStats.bcs.totalSize += totalSize
		moTableStats.bcs.totalRows += totalRows

		if deletedRows, loaded, err = applyTombstones(
			ctx, de.fs, de.mp, tObjIter, tRowIter, pState, blkNeedToLoad); err != nil {
			return false, err
		}

		loadedTBlk += loaded
		moTableStats.bcs.deletedRows += deletedRows

		if err = dRowIter.Close(); err != nil {
			return false, err
		}

		if err = tRowIter.Close(); err != nil {
			return false, err
		}

		singleTableDone := true
		if dObjIter.Done() {
			if err = dObjIter.Close(); err != nil {
				return false, err
			}
		} else {
			singleTableDone = false
		}

		if tObjIter.Done() {
			if err = tObjIter.Close(); err != nil {
				return false, err
			}
		} else {
			singleTableDone = false
		}

		if singleTableDone {
			moTableStats.bcs.took = time.Since(start)

			moTableStats.stashToStats(tblInfo)
			moTableStats.cleanStash()

			err = updateStatsTable(ctx, service, tblInfo, *moTableStats.stats[tblInfo], sqlExecutor)
			if err != nil {
				return false, err
			}

			moTableStats.acs.tblInfoStack = moTableStats.acs.tblInfoStack[1:]
			moTableStats.acs.dObjIterStack = moTableStats.acs.dObjIterStack[1:]
			moTableStats.acs.tObjIterStack = moTableStats.acs.tObjIterStack[1:]
			moTableStats.acs.dRowIterStack = moTableStats.acs.dRowIterStack[1:]
			moTableStats.acs.tRowIterStack = moTableStats.acs.tRowIterStack[1:]

			start = time.Now()
		}

		if loadedDBlk+loadedTBlk >= blkNeedToLoad {
			break
		}
	}

	return len(moTableStats.acs.tblInfoStack) == 0, err
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
	//pState *logtailreplay.PartitionState,
	dObjIter logtailreplay.ObjectsIter,
	dRowIter logtailreplay.RowsIter,
) (visibleRows, visibleSize float64, loaded int, err error) {

	var (
		estimatedOneRowSize float64
	)

	// there won't exist visible appendable obj
	for dObjIter.Next() {
		obj := dObjIter.Entry()

		moTableStats.bcs.dobjectCnt++
		moTableStats.bcs.dblockCnt += int(obj.BlkCnt())

		visibleSize += float64(obj.Size())
		visibleRows += float64(obj.Rows())
		moTableStats.bcs.dataObjIds = append(
			moTableStats.bcs.dataObjIds, *obj.ObjectStats.ObjectName().ObjectId())
	}

	if visibleRows != 0 {
		estimatedOneRowSize = visibleSize / visibleRows
	}

	// 1. inserts on appendable object
	for dRowIter.Next() {
		entry := dRowIter.Entry()

		idx := slices.IndexFunc(moTableStats.bcs.dataObjIds, func(objId types.Objectid) bool {
			return objId.EQ(entry.BlockID.Object())
		})

		if idx == -1 {
			moTableStats.bcs.dataObjIds = append(
				moTableStats.bcs.dataObjIds, *entry.BlockID.Object())
		}

		visibleRows += float64(1)
		if estimatedOneRowSize > 0 {
			visibleSize += estimatedOneRowSize
		} else {
			visibleSize += float64(entry.Batch.Size()) / float64(entry.Batch.RowCount())
		}
	}

	return
}

func applyTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	mp *mpool.MPool,
	tObjIter logtailreplay.ObjectsIter,
	tRowIter logtailreplay.RowsIter,
	pState *logtailreplay.PartitionState,
	blkNeedToLoadDelta int,
) (deletedRows float64, loaded int, err error) {
	var (
		hidden  objectio.HiddenColumnSelection
		release func()
	)

	// 1. non-appendable tombstone obj
	// 2. appendable tombstone obj
	for tObjIter.Next() && loaded <= blkNeedToLoadDelta {
		tombstone := tObjIter.Entry()

		moTableStats.bcs.tobjectCnt++
		moTableStats.bcs.tblockCnt += int(tombstone.BlkCnt())

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
				cnt := getDeletedRows(moTableStats.bcs.dataObjIds, rowIds)

				deletedRows += float64(cnt)
				return true
			}, tombstone.ObjectStats)

		loaded += int(tombstone.BlkCnt())

		if err != nil {
			return
		}
	}

	// if appendable tombstone persisted, deletes on it already eliminated
	// 1. deletes on appendable object
	// 2. deletes on non-appendable objects
	//
	// here, we only collect the deletes that have not paired inserts
	// according to its LESS function, the deletes come first
	var lastInsert logtailreplay.RowEntry
	err = pState.ScanRows(true, func(entry logtailreplay.RowEntry) (bool, error) {
		if !entry.Deleted {
			lastInsert = entry
			return true, nil
		}

		if entry.RowID.EQ(&lastInsert.RowID) {
			return true, nil
		}

		deletedRows += float64(getDeletedRows(moTableStats.bcs.dataObjIds, []types.Rowid{entry.RowID}))

		return true, nil
	})

	return deletedRows, loaded, err
}

func updateStatsTable(
	ctx context.Context,
	service string,
	tbl tablePair,
	sl statsList,
	executor func() ie.InternalExecutor,
) (err error) {

	var (
		val []byte
		ret ie.InternalExecResult
	)

	opts := ie.NewOptsBuilder().Database(catalog.MO_CATALOG).Internal(true).Finish()

	if val, err = json.Marshal(sl.stats); err != nil {
		return err
	}

	utcTime := moTableStats.lastUpdateTS.
		ToTimestamp().
		ToStdTime().
		Format("2006-01-02 15:04:05.000000")

	sql := fmt.Sprintf(insertOrUpdateSQL,
		catalog.MO_CATALOG,
		catalog.MO_TABLE_STATS,
		tbl.acc, tbl.db, tbl.tbl,
		tbl.dbName, tbl.tblName,
		string(val), utcTime, sl.took.Microseconds(),
		string(val), utcTime, sl.took.Microseconds())

	ret = executor().Query(ctx, sql, opts)

	if err = ret.Error(); err != nil {
		return err
	}

	return nil
}
