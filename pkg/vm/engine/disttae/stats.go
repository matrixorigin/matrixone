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
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const (
	// MinUpdateInterval is the minimal interval to update stats info as it
	// is necessary to update stats every time.
	MinUpdateInterval = time.Second * 10
)

type updateStatsRequest struct {
	// statsInfo is the field which is to update.
	statsInfo *pb.StatsInfo

	// The following fields are needed to update the stats.

	// tableDef is the main table definition.
	tableDef *plan2.TableDef
	// partitionsTableDef is the partitions table definition.
	partitionsTableDef []*plan2.TableDef

	partitionState  *logtailreplay.PartitionState
	fs              fileservice.FileService
	ts              types.TS
	approxObjectNum int64
}

func newUpdateStatsRequest(
	tableDef *plan2.TableDef,
	partitionsTableDef []*plan2.TableDef,
	partitionState *logtailreplay.PartitionState,
	fs fileservice.FileService,
	ts types.TS,
	approxObjectNum int64,
	stats *pb.StatsInfo,
) *updateStatsRequest {
	return &updateStatsRequest{
		statsInfo:          stats,
		tableDef:           tableDef,
		partitionsTableDef: partitionsTableDef,
		partitionState:     partitionState,
		fs:                 fs,
		ts:                 ts,
		approxObjectNum:    approxObjectNum,
	}
}

type logtailUpdate struct {
	c  chan uint64
	mu struct {
		sync.Mutex
		updated map[uint64]struct{}
	}
}

func newLogtailUpdate() *logtailUpdate {
	u := &logtailUpdate{
		c: make(chan uint64, 1000),
	}
	u.mu.updated = make(map[uint64]struct{})
	return u
}

type GlobalStatsConfig struct {
	LogtailUpdateStatsThreshold int
}

type GlobalStatsOption func(s *GlobalStats)

// WithUpdateWorkerFactor set the update worker factor.
func WithUpdateWorkerFactor(f int) GlobalStatsOption {
	return func(s *GlobalStats) {
		s.updateWorkerFactor = f
	}
}

type GlobalStats struct {
	ctx context.Context

	// engine is the global Engine instance.
	engine *Engine

	// tailC is the chan to receive entries from logtail
	// and then update the stats info map.
	// TODO(volgariver6): add metrics of the chan length.
	tailC chan *logtail.TableLogtail

	updateC chan pb.StatsInfoKey

	// statsUpdated is used to control the frequency of updating stats info.
	// It is not necessary to update stats info too frequently.
	// It records the update time of the stats info key.
	statsUpdated sync.Map

	logtailUpdate *logtailUpdate

	// tableLogtailCounter is the counter of the logtail entry of stats info key.
	tableLogtailCounter map[pb.StatsInfoKey]int64

	// statsInfoMap is the global stats info in engine which
	// contains all subscribed tables stats info.
	mu struct {
		sync.Mutex

		// cond is used to wait for stats updated for the first time.
		// If sync parameter is false, it is unuseful.
		cond *sync.Cond

		// statsInfoMap is the real stats info data.
		statsInfoMap map[pb.StatsInfoKey]*pb.StatsInfo
	}

	// updateWorkerFactor is the times of CPU number of this node
	// to start update worker. Default is 8.
	updateWorkerFactor int

	// KeyRouter is the router to decides which node should send to.
	KeyRouter client.KeyRouter[pb.StatsInfoKey]
}

func NewGlobalStats(
	ctx context.Context, e *Engine, keyRouter client.KeyRouter[pb.StatsInfoKey], opts ...GlobalStatsOption,
) *GlobalStats {
	s := &GlobalStats{
		ctx:                 ctx,
		engine:              e,
		tailC:               make(chan *logtail.TableLogtail, 10000),
		updateC:             make(chan pb.StatsInfoKey, 3000),
		logtailUpdate:       newLogtailUpdate(),
		tableLogtailCounter: make(map[pb.StatsInfoKey]int64),
		KeyRouter:           keyRouter,
	}
	s.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	s.mu.cond = sync.NewCond(&s.mu)
	for _, opt := range opts {
		opt(s)
	}
	go s.consumeWorker(ctx)
	go s.updateWorker(ctx)
	return s
}

func (gs *GlobalStats) ShouldUpdate(key pb.StatsInfoKey, entryNum int64) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	info, ok := gs.mu.statsInfoMap[key]
	if ok && info != nil && info.BlockNumber-entryNum > 64 {
		return false
	}
	return true
}

func (gs *GlobalStats) Get(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	info, ok := gs.mu.statsInfoMap[key]
	if ok && info != nil {
		return info
	}

	// Get stats info from remote node.
	if gs.KeyRouter != nil {
		client := gs.engine.qc
		target := gs.KeyRouter.Target(key)
		if len(target) != 0 && client != nil {
			resp, err := client.SendMessage(ctx, target, client.NewRequest(query.CmdMethod_GetStatsInfo))
			if err != nil || resp == nil {
				logutil.Errorf("failed to send request to %s, err: %v, resp: %v", "", err, resp)
			} else if resp.GetStatsInfoResponse != nil {
				defer client.Release(resp)

				info := resp.GetStatsInfoResponse.StatsInfo
				// If we get stats info from remote node, update local stats info.
				gs.mu.statsInfoMap[key] = info
				return info
			}
		}
	}

	ok = false
	if sync {
		for !ok {
			if ctx.Err() != nil {
				return nil
			}

			func() {
				// We force to trigger the update, which will hang when the channel
				// is full. Another goroutine will fetch items from the channel
				// which hold the lock, so we need to unlock it first.
				gs.mu.Unlock()
				defer gs.mu.Lock()
				// If the trigger condition is not satisfied, the stats will not be updated
				// for long time. So we trigger the update here to get the stats info as soon
				// as possible.
				gs.triggerUpdate(key, true)
			}()

			// Wait until stats info of the key is updated.
			gs.mu.cond.Wait()

			info, ok = gs.mu.statsInfoMap[key]
		}
	}
	return info
}

func (gs *GlobalStats) RemoveTid(tid uint64) {
	gs.logtailUpdate.mu.Lock()
	defer gs.logtailUpdate.mu.Unlock()
	delete(gs.logtailUpdate.mu.updated, tid)
}

func (gs *GlobalStats) enqueue(tail *logtail.TableLogtail) {
	select {
	case gs.tailC <- tail:
	default:
		logutil.Errorf("the channel of logtails is full")
	}
}

func (gs *GlobalStats) consumeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case tail := <-gs.tailC:
			gs.consumeLogtail(tail)
		}
	}
}

func (gs *GlobalStats) updateWorker(ctx context.Context) {
	for i := 0; i < runtime.GOMAXPROCS(0)*gs.updateWorkerFactor; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case key := <-gs.updateC:
					gs.updateTableStats(key)
				}
			}
		}()
	}
}

func (gs *GlobalStats) triggerUpdate(key pb.StatsInfoKey, force bool) {
	if force {
		gs.updateC <- key
		return
	}

	select {
	case gs.updateC <- key:
	default:
	}
}

func (gs *GlobalStats) consumeLogtail(tail *logtail.TableLogtail) {
	key := pb.StatsInfoKey{
		DatabaseID: tail.Table.DbId,
		TableID:    tail.Table.TbId,
	}
	if len(tail.CkpLocation) > 0 {
		gs.triggerUpdate(key, false)
	} else if tail.Table != nil {
		var triggered bool
		for _, cmd := range tail.Commands {
			if logtailreplay.IsBlkTable(cmd.TableName) ||
				logtailreplay.IsObjTable(cmd.TableName) ||
				logtailreplay.IsMetaTable(cmd.TableName) {
				triggered = true
				gs.triggerUpdate(key, false)
				break
			}
		}
		if _, ok := gs.tableLogtailCounter[key]; !ok {
			gs.tableLogtailCounter[key] = 1
		} else {
			gs.tableLogtailCounter[key]++
		}
		if !triggered && gs.ShouldUpdate(key, gs.tableLogtailCounter[key]) {
			gs.tableLogtailCounter[key] = 0
			gs.triggerUpdate(key, false)
		}
	}
}

func (gs *GlobalStats) notifyLogtailUpdate(tid uint64) {
	gs.logtailUpdate.mu.Lock()
	defer gs.logtailUpdate.mu.Unlock()
	_, ok := gs.logtailUpdate.mu.updated[tid]
	if ok {
		return
	}
	gs.logtailUpdate.mu.updated[tid] = struct{}{}

	select {
	case gs.logtailUpdate.c <- tid:
	default:
	}
}

func (gs *GlobalStats) waitLogtailUpdated(tid uint64) {
	// If the tid is less than reserved, return immediately.
	if tid < catalog.MO_RESERVED_MAX {
		return
	}

	// checkUpdated is a function used to check if the table's
	// first logtail has been received. Return true means that
	// the first logtail has already been received by the CN server.
	checkUpdated := func() bool {
		gs.logtailUpdate.mu.Lock()
		defer gs.logtailUpdate.mu.Unlock()
		_, ok := gs.logtailUpdate.mu.updated[tid]
		return ok
	}

	// just return if the logtail of the table already received.
	if checkUpdated() {
		return
	}

	// There are three ways to break out of the select:
	//   1. context done
	//   2. interval checking, whose init interval is 10ms and max interval is 5s
	//   3. logtail update notify, to check if it is the required table.
	initCheckInterval := time.Millisecond * 10
	maxCheckInterval := time.Second * 5
	checkInterval := initCheckInterval
	timer := time.NewTimer(checkInterval)
	defer timer.Stop()

	var done bool
	for {
		if done {
			return
		}
		if checkUpdated() {
			return
		}
		select {
		case <-gs.ctx.Done():
			return

		case <-timer.C:
			if checkUpdated() {
				return
			}
			// Increase the check interval to reduce the CPU usage.
			// The max interval is 5s, means we check the logtail of
			// the table every 5s at last.
			checkInterval = checkInterval * 2
			if checkInterval > maxCheckInterval {
				checkInterval = maxCheckInterval
			}
			timer.Reset(checkInterval)

		case i := <-gs.logtailUpdate.c:
			if i == tid {
				done = true
			}
		}
	}
}

func (gs *GlobalStats) updateTableStats(key pb.StatsInfoKey) {
	// wait until the table's logtail has been updated.
	gs.waitLogtailUpdated(key.TableID)

	// Protect the update progress.
	ts, ok := gs.statsUpdated.Load(key)
	if ok && time.Since(ts.(time.Time)) < MinUpdateInterval {
		return
	}

	// updated is used to mark that the stats info is updated.
	var updated bool

	stats := plan2.NewStatsInfo()
	defer func() {
		gs.mu.Lock()
		defer gs.mu.Unlock()

		// If it is the first time that the stats info is updated,
		// send it to key router.
		if _, ok := gs.statsUpdated.Load(key); !ok && gs.KeyRouter != nil && updated {
			gs.KeyRouter.AddItem(gossip.CommonItem{
				Operation: gossip.Operation_Set,
				Key: &gossip.CommonItem_StatsInfoKey{
					StatsInfoKey: &pb.StatsInfoKey{
						DatabaseID: key.DatabaseID,
						TableID:    key.TableID,
					},
				},
			})
		}

		if updated {
			gs.mu.statsInfoMap[key] = stats
			// The update time of the table key should be updated just before
			// trying to update stats.
			gs.statsUpdated.Store(key, time.Now())
		} else if _, ok := gs.mu.statsInfoMap[key]; !ok {
			gs.mu.statsInfoMap[key] = nil
		}

		// Notify all the waiters to read the new stats info.
		gs.mu.cond.Broadcast()
	}()

	table := gs.engine.getLatestCatalogCache().GetTableById(key.DatabaseID, key.TableID)
	// table or its definition is nil, means that the table is created but not committed yet.
	if table == nil || table.TableDef == nil {
		logutil.Errorf("cannot get table by ID %v", key)
		return
	}

	partitionState := gs.engine.getOrCreateLatestPart(key.DatabaseID, key.TableID).Snapshot()
	var partitionsTableDef []*plan2.TableDef
	var approxObjectNum int64
	if table.Partitioned > 0 {
		partitionInfo := &plan2.PartitionByDef{}
		if err := partitionInfo.UnMarshalPartitionInfo([]byte(table.Partition)); err != nil {
			logutil.Errorf("failed to unmarshal partition table: %v", err)
			return
		}
		for _, partitionTableName := range partitionInfo.PartitionTableNames {
			partitionTable := gs.engine.getLatestCatalogCache().GetTableByName(key.DatabaseID, partitionTableName)
			partitionsTableDef = append(partitionsTableDef, partitionTable.TableDef)
			ps := gs.engine.getOrCreateLatestPart(key.DatabaseID, partitionTable.Id).Snapshot()
			approxObjectNum += int64(ps.ApproxObjectsNum())
		}
	} else {
		approxObjectNum = int64(partitionState.ApproxObjectsNum())
	}

	if approxObjectNum == 0 {
		// There are no objects flushed yet.
		return
	}

	// the time used to init stats info is not need to be too precise.
	now := timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
	req := newUpdateStatsRequest(
		table.TableDef,
		partitionsTableDef,
		partitionState,
		gs.engine.fs,
		types.TimestampToTS(now),
		approxObjectNum,
		stats,
	)
	if err := UpdateStats(gs.ctx, req); err != nil {
		logutil.Errorf("failed to init stats info for table %v, err: %v", key, err)
		return
	}
	updated = true
}

func calcNdvUsingZonemap(zm objectio.ZoneMap, t *types.Type) float64 {
	if !zm.IsInited() {
		return -1 /*for new added column, its zonemap will be empty and not initialized*/
	}
	switch t.Oid {
	case types.T_bool:
		return 2
	case types.T_bit:
		return float64(types.DecodeFixed[uint64](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint64](zm.GetMinBuf())) + 1
	case types.T_int8:
		return float64(types.DecodeFixed[int8](zm.GetMaxBuf())) - float64(types.DecodeFixed[int8](zm.GetMinBuf())) + 1
	case types.T_int16:
		return float64(types.DecodeFixed[int16](zm.GetMaxBuf())) - float64(types.DecodeFixed[int16](zm.GetMinBuf())) + 1
	case types.T_int32:
		return float64(types.DecodeFixed[int32](zm.GetMaxBuf())) - float64(types.DecodeFixed[int32](zm.GetMinBuf())) + 1
	case types.T_int64:
		return float64(types.DecodeFixed[int64](zm.GetMaxBuf())) - float64(types.DecodeFixed[int64](zm.GetMinBuf())) + 1
	case types.T_uint8:
		return float64(types.DecodeFixed[uint8](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint8](zm.GetMinBuf())) + 1
	case types.T_uint16:
		return float64(types.DecodeFixed[uint16](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint16](zm.GetMinBuf())) + 1
	case types.T_uint32:
		return float64(types.DecodeFixed[uint32](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint32](zm.GetMinBuf())) + 1
	case types.T_uint64:
		return float64(types.DecodeFixed[uint64](zm.GetMaxBuf())) - float64(types.DecodeFixed[uint64](zm.GetMinBuf())) + 1
	case types.T_decimal64:
		return types.Decimal64ToFloat64(types.DecodeFixed[types.Decimal64](zm.GetMaxBuf()), t.Scale) -
			types.Decimal64ToFloat64(types.DecodeFixed[types.Decimal64](zm.GetMinBuf()), t.Scale) + 1
	case types.T_decimal128:
		return types.Decimal128ToFloat64(types.DecodeFixed[types.Decimal128](zm.GetMaxBuf()), t.Scale) -
			types.Decimal128ToFloat64(types.DecodeFixed[types.Decimal128](zm.GetMinBuf()), t.Scale) + 1
	case types.T_float32:
		return float64(types.DecodeFixed[float32](zm.GetMaxBuf())) - float64(types.DecodeFixed[float32](zm.GetMinBuf())) + 1
	case types.T_float64:
		return types.DecodeFixed[float64](zm.GetMaxBuf()) - types.DecodeFixed[float64](zm.GetMinBuf()) + 1
	case types.T_timestamp:
		return float64(types.DecodeFixed[types.Timestamp](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Timestamp](zm.GetMinBuf())) + 1
	case types.T_date:
		return float64(types.DecodeFixed[types.Date](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Date](zm.GetMinBuf())) + 1
	case types.T_time:
		return float64(types.DecodeFixed[types.Time](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Time](zm.GetMinBuf())) + 1
	case types.T_datetime:
		return float64(types.DecodeFixed[types.Datetime](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Datetime](zm.GetMinBuf())) + 1
	case types.T_uuid, types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text,
		types.T_array_float32, types.T_array_float64:
		//NDV Function
		// An aggregate function that returns an approximate value similar to the result of COUNT(DISTINCT col),
		// the "number of distinct values".
		return -1
	case types.T_enum:
		return float64(types.DecodeFixed[types.Enum](zm.GetMaxBuf())) - float64(types.DecodeFixed[types.Enum](zm.GetMinBuf())) + 1
	default:
		return -1
	}
}

func getMinMaxValueByFloat64(typ types.Type, buf []byte) float64 {
	switch typ.Oid {
	case types.T_bit:
		return float64(types.DecodeUint64(buf))
	case types.T_int8:
		return float64(types.DecodeInt8(buf))
	case types.T_int16:
		return float64(types.DecodeInt16(buf))
	case types.T_int32:
		return float64(types.DecodeInt32(buf))
	case types.T_int64:
		return float64(types.DecodeInt64(buf))
	case types.T_uint8:
		return float64(types.DecodeUint8(buf))
	case types.T_uint16:
		return float64(types.DecodeUint16(buf))
	case types.T_uint32:
		return float64(types.DecodeUint32(buf))
	case types.T_uint64:
		return float64(types.DecodeUint64(buf))
	case types.T_date:
		return float64(types.DecodeDate(buf))
	case types.T_time:
		return float64(types.DecodeTime(buf))
	case types.T_timestamp:
		return float64(types.DecodeTimestamp(buf))
	case types.T_datetime:
		return float64(types.DecodeDatetime(buf))
	//case types.T_char, types.T_varchar, types.T_text:
	//return float64(plan2.ByteSliceToUint64(buf)), true
	default:
		panic("unsupported type")
	}
}

// get ndv, minval , maxval, datatype from zonemap. Retrieve all columns except for rowid, return accurate number of objects
func updateInfoFromZoneMap(ctx context.Context, req *updateStatsRequest, info *plan2.InfoFromZoneMap) error {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateInfoFromZonemapHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(req.tableDef.Cols) - 1 /* row-id */
	var (
		init    bool
		err     error
		meta    objectio.ObjectDataMeta
		objMeta objectio.ObjectMeta
	)
	fs, err := fileservice.Get[fileservice.FileService](req.fs, defines.SharedFileServiceName)
	if err != nil {
		return err
	}

	onObjFn := func(obj logtailreplay.ObjectEntry) error {
		location := obj.Location()
		if objMeta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
			return err
		}
		meta = objMeta.MustDataMeta()
		info.AccurateObjectNumber++
		info.BlockNumber += int64(obj.BlkCnt())
		info.TableCnt += float64(meta.BlockHeader().Rows())
		if !init {
			init = true
			for idx, col := range req.tableDef.Cols[:lenCols] {
				objColMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] = int64(objColMeta.NullCnt())
				info.ColumnZMs[idx] = objColMeta.ZoneMap().Clone()
				info.DataTypes[idx] = types.T(col.Typ.Id).ToType()
				info.ColumnNDVs[idx] = float64(objColMeta.Ndv())
				info.ColumnSize[idx] = int64(meta.BlockHeader().ZoneMapArea().Length() +
					meta.BlockHeader().BFExtent().Length() + objColMeta.Location().Length())
				if info.ColumnNDVs[idx] > 100 || info.ColumnNDVs[idx] > 0.1*float64(meta.BlockHeader().Rows()) {
					switch info.DataTypes[idx].Oid {
					case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_time, types.T_timestamp, types.T_date, types.T_datetime:
						info.ShuffleRanges[idx] = plan2.NewShuffleRange(false)
						if info.ColumnZMs[idx].IsInited() {
							minvalue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMinBuf())
							maxvalue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMaxBuf())
							info.ShuffleRanges[idx].Update(minvalue, maxvalue, int64(meta.BlockHeader().Rows()), int64(objColMeta.NullCnt()))
						}
					case types.T_varchar, types.T_char, types.T_text:
						info.ShuffleRanges[idx] = plan2.NewShuffleRange(true)
						if info.ColumnZMs[idx].IsInited() {
							info.ShuffleRanges[idx].UpdateString(info.ColumnZMs[idx].GetMinBuf(), info.ColumnZMs[idx].GetMaxBuf(), int64(meta.BlockHeader().Rows()), int64(objColMeta.NullCnt()))
						}
					}
				}
			}
		} else {
			for idx, col := range req.tableDef.Cols[:lenCols] {
				objColMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] += int64(objColMeta.NullCnt())
				zm := objColMeta.ZoneMap().Clone()
				if !zm.IsInited() {
					continue
				}
				index.UpdateZM(info.ColumnZMs[idx], zm.GetMaxBuf())
				index.UpdateZM(info.ColumnZMs[idx], zm.GetMinBuf())
				info.ColumnNDVs[idx] += float64(objColMeta.Ndv())
				info.ColumnSize[idx] += int64(objColMeta.Location().Length())
				if info.ShuffleRanges[idx] != nil {
					switch info.DataTypes[idx].Oid {
					case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_time, types.T_timestamp, types.T_date, types.T_datetime:
						minvalue := getMinMaxValueByFloat64(info.DataTypes[idx], zm.GetMinBuf())
						maxvalue := getMinMaxValueByFloat64(info.DataTypes[idx], zm.GetMaxBuf())
						info.ShuffleRanges[idx].Update(minvalue, maxvalue, int64(meta.BlockHeader().Rows()), int64(objColMeta.NullCnt()))
					case types.T_varchar, types.T_char, types.T_text:
						info.ShuffleRanges[idx].UpdateString(zm.GetMinBuf(), zm.GetMaxBuf(), int64(meta.BlockHeader().Rows()), int64(objColMeta.NullCnt()))
					}
				}
			}
		}
		return nil
	}
	if err = ForeachVisibleDataObject(req.partitionState, req.ts, onObjFn); err != nil {
		return err
	}

	return nil
}

func adjustNDV(info *plan2.InfoFromZoneMap, tableDef *plan2.TableDef) {
	lenCols := len(tableDef.Cols) - 1 /* row-id */

	if info.AccurateObjectNumber > 1 {
		for idx := range tableDef.Cols[:lenCols] {
			rate := info.ColumnNDVs[idx] / info.TableCnt
			if rate > 1 {
				rate = 1
			}
			if rate < 0.1 {
				info.ColumnNDVs[idx] /= math.Pow(float64(info.AccurateObjectNumber), (1 - rate))
			}
			ndvUsingZonemap := calcNdvUsingZonemap(info.ColumnZMs[idx], &info.DataTypes[idx])
			if ndvUsingZonemap != -1 && info.ColumnNDVs[idx] > ndvUsingZonemap {
				info.ColumnNDVs[idx] = ndvUsingZonemap
			}

			if info.ColumnNDVs[idx] > info.TableCnt {
				info.ColumnNDVs[idx] = info.TableCnt
			}
		}
	}
}

// UpdateStats is the main function to calculate and update the stats for scan node.
func UpdateStats(ctx context.Context, req *updateStatsRequest) error {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateStatsDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(req.tableDef.Cols) - 1 /* row-id */
	info := plan2.NewInfoFromZoneMap(lenCols)
	if req.approxObjectNum == 0 {
		return nil
	}

	info.ApproxObjectNumber = req.approxObjectNum
	baseTableDef := req.tableDef
	if len(req.partitionsTableDef) > 0 {
		for _, def := range req.partitionsTableDef {
			req.tableDef = def
			if err := updateInfoFromZoneMap(ctx, req, info); err != nil {
				return err
			}
		}
	} else if err := updateInfoFromZoneMap(ctx, req, info); err != nil {
		return err
	}
	adjustNDV(info, baseTableDef)
	plan2.UpdateStatsInfo(info, baseTableDef, req.statsInfo)
	return nil
}
