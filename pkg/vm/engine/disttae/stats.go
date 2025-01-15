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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var (
	// MinUpdateInterval is the minimal interval to update stats info as it
	// is necessary to update stats every time.
	MinUpdateInterval = time.Second * 15

	initCheckInterval = time.Millisecond * 10
	maxCheckInterval  = time.Second * 5
	checkTimeout      = time.Minute
)

// waitKeeper is used to mark the table has finished waited,
// only after which, the table can be unsubscribed.
type waitKeeper struct {
	sync.Mutex
	records map[uint64]struct{}
}

func newWaitKeeper() *waitKeeper {
	return &waitKeeper{
		records: make(map[uint64]struct{}),
	}
}

func (w *waitKeeper) reset() {
	w.Lock()
	defer w.Unlock()
	w.records = make(map[uint64]struct{})
}

func (w *waitKeeper) add(tid uint64) {
	w.Lock()
	defer w.Unlock()
	w.records[tid] = struct{}{}
}

func (w *waitKeeper) del(tid uint64) {
	w.Lock()
	defer w.Unlock()
	delete(w.records, tid)
}

type updateStatsRequest struct {
	// statsInfo is the field which is to update.
	statsInfo *pb.StatsInfo

	// The following fields are needed to update the stats.

	// tableDef is the main table definition.
	tableDef *plan2.TableDef

	partitionState  *logtailreplay.PartitionState
	fs              fileservice.FileService
	ts              types.TS
	approxObjectNum int64
}

func newUpdateStatsRequest(
	tableDef *plan2.TableDef,
	partitionState *logtailreplay.PartitionState,
	fs fileservice.FileService,
	ts types.TS,
	approxObjectNum int64,
	stats *pb.StatsInfo,
) *updateStatsRequest {
	return &updateStatsRequest{
		statsInfo:       stats,
		tableDef:        tableDef,
		partitionState:  partitionState,
		fs:              fs,
		ts:              ts,
		approxObjectNum: approxObjectNum,
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

// WithStatsUpdater set the update function to update stats info.
func WithStatsUpdater(f func(context.Context, pb.StatsInfoKey, *pb.StatsInfo) bool) GlobalStatsOption {
	return func(s *GlobalStats) {
		s.statsUpdater = f
	}
}

// WithApproxObjectNumUpdater set the update function to update approx object num.
func WithApproxObjectNumUpdater(f func() int64) GlobalStatsOption {
	return func(s *GlobalStats) {
		s.approxObjectNumUpdater = f
	}
}

// updateRecord records the update status of a key.
type updateRecord struct {
	// inProgress indicates if the stats of a table is being updated.
	inProgress bool
	// lastUpdate is the time of the stats last updated.
	lastUpdate time.Time
}

type GlobalStats struct {
	ctx context.Context

	// engine is the global Engine instance.
	engine *Engine

	// tailC is the chan to receive entries from logtail
	// and then update the stats info map.
	// TODO(volgariver6): add metrics of the chan length.
	tailC chan *logtail.TableLogtail

	updateC chan pb.StatsInfoKeyWithContext

	// queueWatcher keeps the table id and its enqueue time.
	// and watch the queue item in the queue.
	queueWatcher *queueWatcher

	updatingMu struct {
		sync.Mutex
		updating map[pb.StatsInfoKey]*updateRecord
	}

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

	// waitKeeper is used to make sure the table is safe to unsubscribe.
	// Only when the table is finished waited, it can be unsubscribed safely.
	waitKeeper *waitKeeper

	// updateWorkerFactor is the times of CPU number of this node
	// to start update worker. Default is 8.
	updateWorkerFactor int

	// KeyRouter is the router to decides which node should send to.
	KeyRouter client.KeyRouter[pb.StatsInfoKey]

	concurrentExecutor ConcurrentExecutor

	// statsUpdate is the function which updates the stats info.
	// If it is nil, set it to doUpdate.
	statsUpdater func(context.Context, pb.StatsInfoKey, *pb.StatsInfo) bool
	// for test only currently.
	approxObjectNumUpdater func() int64
}

func NewGlobalStats(
	ctx context.Context, e *Engine, keyRouter client.KeyRouter[pb.StatsInfoKey], opts ...GlobalStatsOption,
) *GlobalStats {
	s := &GlobalStats{
		ctx:                 ctx,
		engine:              e,
		tailC:               make(chan *logtail.TableLogtail, 10000),
		updateC:             make(chan pb.StatsInfoKeyWithContext, 3000),
		logtailUpdate:       newLogtailUpdate(),
		tableLogtailCounter: make(map[pb.StatsInfoKey]int64),
		KeyRouter:           keyRouter,
		waitKeeper:          newWaitKeeper(),
		queueWatcher:        newQueueWatcher(),
	}
	s.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	s.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	s.mu.cond = sync.NewCond(&s.mu)
	for _, opt := range opts {
		opt(s)
	}
	if s.statsUpdater == nil {
		s.statsUpdater = s.doUpdate
	}
	s.concurrentExecutor = newConcurrentExecutor(runtime.GOMAXPROCS(0) * s.updateWorkerFactor * 4)
	s.concurrentExecutor.Run(ctx)
	go s.consumeWorker(ctx)
	go s.updateWorker(ctx)
	go s.queueWatcher.run(ctx)
	return s
}

// shouldTrigger returns true only if key already exists in the map.
func (gs *GlobalStats) shouldTrigger(key pb.StatsInfoKey) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	_, ok := gs.mu.statsInfoMap[key]
	return ok
}

// checkTriggerCond checks the condition that if we should trigger the stats update.
func (gs *GlobalStats) checkTriggerCond(key pb.StatsInfoKey, entryNum int64) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	info, ok := gs.mu.statsInfoMap[key]
	if ok && info != nil && info.BlockNumber*16-entryNum > 64 {
		return false
	}
	return true
}

func (gs *GlobalStats) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}
	return gs.triggerUpdate(wrapkey, false)
}

func (gs *GlobalStats) Get(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}

	info, ok := gs.mu.statsInfoMap[key]
	if ok && info != nil {
		return info
	}

	if _, ok = ctx.Value(perfcounter.CalcTableStatsKey{}).(bool); ok {
		stats := statistic.StatsInfoFromContext(ctx)
		start := time.Now()
		defer func() {
			stats.AddBuildPlanStatsIOConsumption(time.Since(start))
		}()
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
				gs.triggerUpdate(wrapkey, true)
			}()

			info, ok = gs.mu.statsInfoMap[key]
			if ok {
				break
			}

			// Wait until stats info of the key is updated.
			gs.mu.cond.Wait()

			info, ok = gs.mu.statsInfoMap[key]
		}
	}
	return info
}

func (gs *GlobalStats) RemoveTid(tid uint64) {
	gs.waitKeeper.del(tid)

	gs.logtailUpdate.mu.Lock()
	defer gs.logtailUpdate.mu.Unlock()
	delete(gs.logtailUpdate.mu.updated, tid)
}

// clearTables clears the tables in the map if there are any tables in it.
func (gs *GlobalStats) clearTables() {
	// clear all the waiters in the keeper.
	gs.waitKeeper.reset()

	gs.logtailUpdate.mu.Lock()
	defer gs.logtailUpdate.mu.Unlock()
	if len(gs.logtailUpdate.mu.updated) > 0 {
		gs.logtailUpdate.mu.updated = make(map[uint64]struct{})
	}
}

func (gs *GlobalStats) safeToUnsubscribe(tid uint64) bool {
	gs.waitKeeper.Lock()
	defer gs.waitKeeper.Unlock()
	if _, ok := gs.waitKeeper.records[tid]; ok {
		return true
	}
	return false
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
			gs.consumeLogtail(ctx, tail)
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
					// after dequeue from the chan, remove the table ID from the queue watcher.
					gs.queueWatcher.del(key.Key.TableID)

					v2.StatsTriggerConsumeCounter.Add(1)
					gs.updateTableStats(key)
				}
			}
		}()
	}
}

func (gs *GlobalStats) triggerUpdate(key pb.StatsInfoKeyWithContext, force bool) bool {
	defer func() {
		v2.StatsTriggerQueueSizeGauge.Set(float64(len(gs.updateC)))
	}()
	if force {
		gs.updateC <- key
		gs.queueWatcher.add(key.Key.TableID)
		v2.StatsTriggerForcedCounter.Add(1)
		return true
	}

	select {
	case gs.updateC <- key:
		gs.queueWatcher.add(key.Key.TableID)
		v2.StatsTriggerUnforcedCounter.Add(1)
		return true
	default:
		return false
	}
}

func (gs *GlobalStats) consumeLogtail(ctx context.Context, tail *logtail.TableLogtail) {
	key := pb.StatsInfoKey{
		AccId:      tail.Table.AccId,
		DatabaseID: tail.Table.DbId,
		TableID:    tail.Table.TbId,
	}

	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}

	if len(tail.CkpLocation) > 0 {
		if gs.shouldTrigger(key) {
			gs.triggerUpdate(wrapkey, false)
		}
	} else if tail.Table != nil {
		var triggered bool
		for _, cmd := range tail.Commands {
			if logtailreplay.IsMetaEntry(cmd.TableName) {
				triggered = true
				if gs.shouldTrigger(key) {
					gs.triggerUpdate(wrapkey, false)
				}
				break
			}
		}
		if _, ok := gs.tableLogtailCounter[key]; !ok {
			gs.tableLogtailCounter[key] = 1
		} else {
			gs.tableLogtailCounter[key]++
		}
		if !triggered && gs.checkTriggerCond(key, gs.tableLogtailCounter[key]) {
			gs.tableLogtailCounter[key] = 0
			if gs.shouldTrigger(key) {
				gs.triggerUpdate(wrapkey, false)
			}
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
	defer gs.waitKeeper.add(tid)

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
	checkInterval := initCheckInterval
	timer := time.NewTimer(checkInterval)
	defer timer.Stop()
	timeout := time.NewTimer(checkTimeout)
	defer timeout.Stop()

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

		case <-timeout.C:
			logutil.Warnf("wait logtail updated timeout, table ID: %d", tid)
			timeout.Reset(checkTimeout)

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

// shouldUpdate returns true only the stats of the key should be updated.
func (gs *GlobalStats) shouldUpdate(key pb.StatsInfoKey) bool {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		gs.updatingMu.updating[key] = &updateRecord{
			inProgress: true,
		}
		return true
	}
	if rec.inProgress {
		return false
	}
	if time.Since(rec.lastUpdate) > MinUpdateInterval {
		rec.inProgress = true
		return true
	}
	return false
}

func (gs *GlobalStats) doneUpdate(key pb.StatsInfoKey, updated bool) {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		return
	}
	rec.inProgress = false
	// only if the stats is updated, set the update time.
	if updated {
		rec.lastUpdate = time.Now()
	}
}

// broadcastStats send the table stats key to gossip manager.
// when other cns needs the stats, they will send query to this
// node to get the table stats.
func (gs *GlobalStats) broadcastStats(key pb.StatsInfoKey) {
	if gs.KeyRouter == nil {
		return
	}
	var broadcast bool
	func() {
		gs.updatingMu.Lock()
		defer gs.updatingMu.Unlock()
		rec, ok := gs.updatingMu.updating[key]
		if !ok {
			return
		}
		broadcast = rec.lastUpdate.IsZero()
	}()
	if !broadcast {
		return
	}
	// If it is the first time that the stats info is updated,
	// send it to key router.
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

func (gs *GlobalStats) updateTableStats(warpKey pb.StatsInfoKeyWithContext) {
	statser := statistic.StatsInfoFromContext(warpKey.Ctx)
	crs := new(perfcounter.CounterSet)

	if !gs.shouldUpdate(warpKey.Key) {
		return
	}

	// wait until the table's logtail has been updated.
	gs.waitLogtailUpdated(warpKey.Key.TableID)

	// updated is used to mark that the stats info is updated.
	var updated bool

	stats := plan2.NewStatsInfo()

	newCtx := perfcounter.AttachS3RequestKey(warpKey.Ctx, crs)
	if gs.statsUpdater != nil {
		updated = gs.statsUpdater(newCtx, warpKey.Key, stats)
	}
	statser.AddBuildPlanStatsS3Request(statistic.S3Request{
		List:      crs.FileService.S3.List.Load(),
		Head:      crs.FileService.S3.Head.Load(),
		Put:       crs.FileService.S3.Put.Load(),
		Get:       crs.FileService.S3.Get.Load(),
		Delete:    crs.FileService.S3.Delete.Load(),
		DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
	})

	gs.mu.Lock()
	defer gs.mu.Unlock()
	if updated {
		gs.mu.statsInfoMap[warpKey.Key] = stats
		gs.broadcastStats(warpKey.Key)
	} else if _, ok := gs.mu.statsInfoMap[warpKey.Key]; !ok {
		gs.mu.statsInfoMap[warpKey.Key] = nil
	}

	// Notify all the waiters to read the new stats info.
	gs.mu.cond.Broadcast()

	gs.doneUpdate(warpKey.Key, updated)
}

func (gs *GlobalStats) doUpdate(ctx context.Context, key pb.StatsInfoKey, stats *pb.StatsInfo) bool {
	table := gs.engine.GetLatestCatalogCache().GetTableById(key.AccId, key.DatabaseID, key.TableID)
	// table or its definition is nil, means that the table is created but not committed yet.
	if table == nil || table.TableDef == nil {
		logutil.Errorf("cannot get table by ID %v", key)
		return false
	}

	partitionState := gs.engine.GetOrCreateLatestPart(key.DatabaseID, key.TableID).Snapshot()
	approxObjectNum := int64(partitionState.ApproxDataObjectsNum())
	if gs.approxObjectNumUpdater == nil && approxObjectNum == 0 {
		// There are no objects flushed yet.
		return false
	}

	// the time used to init stats info is not need to be too precise.
	now := timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
	req := newUpdateStatsRequest(
		table.TableDef,
		partitionState,
		gs.engine.fs,
		types.TimestampToTS(now),
		approxObjectNum,
		stats,
	)
	start := time.Now()
	if err := UpdateStats(ctx, req, gs.concurrentExecutor); err != nil {
		logutil.Errorf("failed to init stats info for table %v, err: %v", key, err)
		return false
	}
	v2.StatsUpdateDurationHistogram.Observe(time.Since(start).Seconds())
	v2.StatsUpdateBlockCounter.Add(float64(stats.BlockNumber))
	return true
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
func updateInfoFromZoneMap(
	ctx context.Context, req *updateStatsRequest, info *plan2.InfoFromZoneMap, executor ConcurrentExecutor,
) error {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateInfoFromZonemapHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(req.tableDef.Cols) - 1 /* row-id */
	fs, fsErr := fileservice.Get[fileservice.FileService](req.fs, defines.SharedFileServiceName)
	if fsErr != nil {
		return fsErr
	}

	var updateMu sync.Mutex
	var init bool
	onObjFn := func(obj objectio.ObjectEntry) error {
		location := obj.Location()
		objMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}
		updateMu.Lock()
		defer updateMu.Unlock()
		meta := objMeta.MustDataMeta()
		info.AccurateObjectNumber++
		info.BlockNumber += int64(obj.BlkCnt())
		objSize := meta.BlockHeader().Rows()
		info.TableCnt += float64(objSize)
		if !init {
			init = true
			for idx, col := range req.tableDef.Cols[:lenCols] {
				objColMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] = int64(objColMeta.NullCnt())
				info.ColumnZMs[idx] = objColMeta.ZoneMap().Clone()
				info.DataTypes[idx] = plan2.ExprType2Type(&col.Typ)
				ndv := float64(objColMeta.Ndv())
				info.ColumnNDVs[idx] = ndv
				info.MaxNDVs[idx] = ndv
				info.NDVinMinOBJ[idx] = ndv
				info.NDVinMaxOBJ[idx] = ndv
				info.MaxOBJSize = objSize
				info.MinOBJSize = objSize
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
				ndv := float64(objColMeta.Ndv())

				info.ColumnNDVs[idx] += ndv
				if ndv > info.MaxNDVs[idx] {
					info.MaxNDVs[idx] = ndv
				}
				if objSize > info.MaxOBJSize {
					info.MaxOBJSize = objSize
					info.NDVinMaxOBJ[idx] = ndv
				} else if objSize == info.MaxOBJSize && ndv > info.NDVinMaxOBJ[idx] {
					info.NDVinMaxOBJ[idx] = ndv
				}
				if objSize < info.MinOBJSize {
					info.MinOBJSize = objSize
					info.NDVinMinOBJ[idx] = ndv
				} else if objSize == info.MinOBJSize && ndv < info.NDVinMinOBJ[idx] {
					info.NDVinMinOBJ[idx] = ndv
				}
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
	if err := ForeachVisibleDataObject(
		req.partitionState,
		req.ts,
		onObjFn,
		executor,
	); err != nil {
		return err
	}

	return nil
}

// UpdateStats is the main function to calculate and update the stats for scan node.
func UpdateStats(ctx context.Context, req *updateStatsRequest, executor ConcurrentExecutor) error {
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
	if err := updateInfoFromZoneMap(ctx, req, info, executor); err != nil {
		return err
	}
	plan2.UpdateStatsInfo(info, baseTableDef, req.statsInfo)
	plan2.AdjustNDV(info, baseTableDef, req.statsInfo)

	for i, coldef := range baseTableDef.Cols[:len(baseTableDef.Cols)-1] {
		colName := coldef.Name
		overlap := 1.0
		if req.statsInfo.ShuffleRangeMap[colName] != nil {
			overlap = req.statsInfo.ShuffleRangeMap[colName].Overlap
		}
		if req.statsInfo.MaxValMap[colName] < req.statsInfo.MinValMap[colName] {
			logutil.Errorf("error happended in stats!")
		}
		logutil.Infof("debug: table %v tablecnt %v  col %v max %v min %v ndv %v overlap %v maxndv %v maxobj %v ndvinmaxobj %v minobj %v ndvinminobj %v",
			baseTableDef.Name, info.TableCnt, colName, req.statsInfo.MaxValMap[colName], req.statsInfo.MinValMap[colName],
			req.statsInfo.NdvMap[colName], overlap, info.MaxNDVs[i], info.MaxOBJSize, info.NDVinMaxOBJ[i], info.MinOBJSize, info.NDVinMinOBJ[i])
	}
	return nil
}

type enqueueItem struct {
	tableID     uint64
	enqueueTime time.Time
}

type queueWatcher struct {
	sync.Mutex
	value         map[uint64]time.Time
	threshold     time.Duration
	checkInterval time.Duration
}

func newQueueWatcher() *queueWatcher {
	return &queueWatcher{
		value:         make(map[uint64]time.Time),
		threshold:     time.Second * 30,
		checkInterval: time.Minute,
	}
}

func (qw *queueWatcher) add(tid uint64) {
	qw.Lock()
	defer qw.Unlock()
	qw.value[tid] = time.Now()
}

func (qw *queueWatcher) del(tid uint64) {
	qw.Lock()
	defer qw.Unlock()
	delete(qw.value, tid)
}

func (qw *queueWatcher) check() []enqueueItem {
	var timeoutList []enqueueItem
	qw.Lock()
	defer qw.Unlock()
	for tid, et := range qw.value {
		if time.Since(et) > qw.threshold {
			timeoutList = append(timeoutList, enqueueItem{tid, et})
		}
	}
	return timeoutList
}

func (qw *queueWatcher) run(ctx context.Context) {
	ticker := time.NewTicker(qw.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.Infof("stats trigger queue watcher stopped")
			return

		case <-ticker.C:
			list := qw.check()
			if len(list) > 0 {
				logutil.Warnf("there are some timeout items in the queue: %v", list)
			}
		}
	}
}
