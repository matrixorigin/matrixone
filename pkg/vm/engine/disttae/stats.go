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
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	"go.uber.org/zap"
)

// ```
// Logtail 事件
//     │
//     ▼
// tailC (chan, cap=10000) logtail 消费专用，最小化阻塞 logtail 消费
//     │
//     ▼
// logtailConsumer (1个 goroutine)
//     │
//     │ 判断入队条件（第一层）：
//     │ - keyExists(): key 必须已存在
//     │ - CkpLocation: checkpoint 时触发
//     │ - MetaEntry: object 元数据变更时触发
//     │
//     ▼
// updateC (chan, cap=3000)
//     │
//     ▼
// spawnUpdateWorkers (16-27个 goroutine)
//     │
//     │ 判断执行条件（第二层）： 便于统一 debounce force/normal update request
//     │ - shouldExecuteUpdate(): 检查 inProgress 和 MinUpdateInterval (15s)
//     │
//     ▼
// coordinateStatsUpdate()
//     │
//     ├─→ 订阅表获取 PartitionState
//     ├─→ 从 CatalogCache 获取 TableDef
//     └─→ CollectAndCalculateStats()
//             │
//             ▼
//         ForeachVisibleObjects()
//             │ 并发遍历所有**已落盘的 Object** (concurrentExecutor)
//             │ 注意：内存中的 dirty blocks 不参与统计
//             ▼
//         FastLoadObjectMeta() (S3 IO)
//             │
//             ▼
//         累加统计信息 (ZoneMap, NDV, RowCount 等)
// ```

const (
	// MinExecutorConcurrency is the minimum concurrency for concurrentExecutor
	// which handles IO-intensive tasks (reading S3 objects).
	MinExecutorConcurrency = 32

	// MaxExecutorConcurrency is the maximum concurrency for concurrentExecutor
	// to avoid extreme cases in high-core systems.
	MaxExecutorConcurrency = 108

	// MinWorkerConcurrency is the minimum concurrency for updateWorker
	// which handles table-level update requests (coordinator role).
	MinWorkerConcurrency = 16

	// WorkerConcurrencyRatio is the ratio of updateWorker concurrency to executor concurrency.
	// updateWorker concurrency = executorConcurrency / WorkerConcurrencyRatio
	WorkerConcurrencyRatio = 4

	// SamplingThreshold is the minimum number of objects to enable sampling.
	// Below this threshold, full scan is used for accuracy.
	SamplingThreshold = 100

	// MinSampleObjects is the minimum number of objects to sample.
	// Set equal to SamplingThreshold to ensure sampling count never drops below full scan count.
	MinSampleObjects = 100

	// MaxSampleObjects is the maximum number of objects to sample.
	// Raise to allow large tables (>5w objects) to reach ~1-2% sampling.
	MaxSampleObjects = 2000

	// objectIDRandomOffset is the offset of random bytes in ObjectNameShort.
	// UUIDv7's bytes 8-15 are random, providing uniform distribution for sampling.
	objectIDRandomOffset = 8
)

var (
	// MinUpdateInterval is the minimal interval to update stats info as it
	// is necessary to update stats every time.
	MinUpdateInterval = time.Second * 15
)

const (
	// LargeTableThreshold is the object count threshold to classify a table as large.
	// Tables with fewer objects are considered small tables.
	LargeTableThreshold = 500

	// LargeTableChangeRateThreshold is the minimum change rate to trigger stats update for large tables.
	// Change rate = pendingChanges / baseObjectCount
	LargeTableChangeRateThreshold = 0.05 // 5%

	// LargeTableMaxUpdateInterval is the maximum interval between stats updates for large tables.
	// Even if change rate is below threshold, update will be triggered after this interval.
	LargeTableMaxUpdateInterval = 30 * time.Minute
)

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

	// samplingMode controls the sampling behavior
	// "auto": use default sampling logic (default)
	// "full": force full scan (no sampling)
	samplingMode string
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
	// baseObjectCount is the object count at last update completion (baseline for change rate calculation).
	baseObjectCount int64
	// pendingChanges is the accumulated object changes since last update (reset after update completion).
	pendingChanges int
	// samplingRatio is the sampling ratio used in the last stats update.
	samplingRatio float64
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

	concurrentExecutor ConcurrentExecutor

	// approxObjectNumUpdater is for test only currently.
	approxObjectNumUpdater func() int64
}

func NewGlobalStats(
	ctx context.Context, e *Engine, keyRouter client.KeyRouter[pb.StatsInfoKey], opts ...GlobalStatsOption,
) *GlobalStats {
	s := &GlobalStats{
		ctx:          ctx,
		engine:       e,
		tailC:        make(chan *logtail.TableLogtail, 10000),
		updateC:      make(chan pb.StatsInfoKeyWithContext, 3000),
		KeyRouter:    keyRouter,
		queueWatcher: newQueueWatcher(),
	}
	s.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	s.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	s.mu.cond = sync.NewCond(&s.mu)
	for _, opt := range opts {
		opt(s)
	}
	// Optimize goroutine concurrency:
	// 1. concurrentExecutor handles IO-intensive tasks (reading S3 objects), needs high concurrency
	//    - Set limits [MinExecutorConcurrency, MaxExecutorConcurrency] to avoid extreme cases
	// 2. updateWorker handles table-level update requests (coordinator role), needs lower concurrency
	//    - Set to executorConcurrency / WorkerConcurrencyRatio, but minimum MinWorkerConcurrency
	// This optimization reduces goroutine count significantly (e.g., 192 -> 120 in typical environments)
	// while maintaining performance since updateWorker's actual concurrency is much lower.
	executorConcurrency := runtime.GOMAXPROCS(0)
	if s.updateWorkerFactor > 0 {
		executorConcurrency = executorConcurrency * s.updateWorkerFactor
	}
	// Apply limits: min MinExecutorConcurrency, max MaxExecutorConcurrency
	if executorConcurrency < MinExecutorConcurrency {
		executorConcurrency = MinExecutorConcurrency
	}
	if executorConcurrency > MaxExecutorConcurrency {
		executorConcurrency = MaxExecutorConcurrency
	}
	// Calculate updateWorker concurrency: executorConcurrency / WorkerConcurrencyRatio, but minimum MinWorkerConcurrency
	updateWorkerConcurrency := max(executorConcurrency/WorkerConcurrencyRatio, MinWorkerConcurrency)
	s.concurrentExecutor = newConcurrentExecutor(executorConcurrency)
	s.concurrentExecutor.Run(ctx)
	go s.logtailConsumer(ctx)
	s.spawnUpdateWorkers(ctx, updateWorkerConcurrency) // updateWorker内部已启动goroutines，不需要再用go
	go s.queueWatcher.run(ctx)
	logutil.Info(
		"GlobalStats-Started",
		zap.Int("exector-num", executorConcurrency),
		zap.Int("worker-num", updateWorkerConcurrency),
		zap.Int("worker-factor", s.updateWorkerFactor),
	)
	return s
}

// keyExists returns true only if key already exists in the map.
func (gs *GlobalStats) keyExists(key pb.StatsInfoKey) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	_, ok := gs.mu.statsInfoMap[key]
	return ok
}

func (gs *GlobalStats) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}
	return gs.enqueueStatsUpdate(wrapkey, false)
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

	// after checking first potential patched cache
	// we check the approx to avoid taking a place in statInfo map
	ps, err := gs.engine.pClient.toSubscribeTable(
		ctx,
		uint64(key.AccId),
		key.TableID,
		key.TableName,
		key.DatabaseID,
		key.DbName)

	if err == nil && ps.ApproxDataObjectsNum() == 0 {
		return nil
	}

	if _, ok = ctx.Value(perfcounter.CalcTableStatsKey{}).(bool); ok {
		stats := statistic.StatsInfoFromContext(ctx)
		start := time.Now()
		defer func() {
			stats.AddBuildPlanStatsIOConsumption(time.Since(start))
		}()
	}

	// Get stats info from remote node.
	if gs.KeyRouter != nil && gs.engine.qc != nil {
		client := gs.engine.qc
		target := gs.KeyRouter.Target(key)
		if len(target) != 0 {
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
				gs.enqueueStatsUpdate(wrapkey, true)
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

func (gs *GlobalStats) enqueue(tail *logtail.TableLogtail) {
	select {
	case gs.tailC <- tail:
	default:
		logutil.Errorf("the channel of logtails is full")
	}
}

func (gs *GlobalStats) logtailConsumer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case tail := <-gs.tailC:
			gs.processLogtail(ctx, tail)
		}
	}
}

func (gs *GlobalStats) spawnUpdateWorkers(ctx context.Context, num int) {
	for range num {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case key := <-gs.updateC:
					// after dequeue from the chan, remove the table ID from the queue watcher.
					gs.queueWatcher.del(key.Key.TableID)

					v2.StatsTriggerConsumeCounter.Add(1)
					gs.coordinateStatsUpdate(key)
				}
			}
		}()
	}
}

func (gs *GlobalStats) enqueueStatsUpdate(key pb.StatsInfoKeyWithContext, force bool) bool {
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

func (gs *GlobalStats) processLogtail(ctx context.Context, tail *logtail.TableLogtail) {
	key := pb.StatsInfoKey{
		AccId:      tail.Table.AccId,
		DatabaseID: tail.Table.DbId,
		TableID:    tail.Table.TbId,
		TableName:  tail.Table.GetTbName(),
		DbName:     tail.Table.GetDbName(),
	}

	// Count meta changes from logtail by checking batch length
	metaChanges := 0
	for i := range tail.Commands {
		if logtailreplay.IsMetaEntry(tail.Commands[i].TableName) {
			if tail.Commands[i].Bat != nil && len(tail.Commands[i].Bat.Vecs) > 0 {
				metaChanges += int(tail.Commands[i].Bat.Vecs[0].Len)
			}
		}
	}

	if len(tail.CkpLocation) > 0 || metaChanges > 0 {
		if gs.keyExists(key) && gs.shouldEnqueueUpdate(key, metaChanges, len(tail.CkpLocation) > 0) {
			gs.enqueueStatsUpdate(pb.StatsInfoKeyWithContext{
				Ctx: ctx,
				Key: key,
			}, false)
		}
	}
}

// shouldEnqueueUpdate determines if a stats update request should be enqueued.
// This is where change rate checking happens for large tables.
//
// Rules:
// - Checkpoint: always enqueue (full refresh needed)
// - Small table (baseObjectCount < 1000): enqueue on any meta change
// - Large table: enqueue if:
//   - Accumulated change rate >= 5%, OR
//   - Time since last update > 30min
func (gs *GlobalStats) shouldEnqueueUpdate(key pb.StatsInfoKey, metaChanges int, hasCheckpoint bool) bool {

	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()

	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		// First time: create record and enqueue
		gs.updatingMu.updating[key] = &updateRecord{
			pendingChanges: metaChanges,
		}
		return true
	}

	// Accumulate pending changes
	rec.pendingChanges += metaChanges

	// Small table: enqueue on any change
	if rec.baseObjectCount < LargeTableThreshold {
		return metaChanges > 0 || hasCheckpoint
	}

	// Large table: check two conditions (enqueue if either is true)
	// Condition 1: Change rate >= 5%
	if rec.baseObjectCount > 0 {
		changeRate := float64(rec.pendingChanges) / float64(rec.baseObjectCount)
		if changeRate >= LargeTableChangeRateThreshold {
			return true
		}
	}

	// Condition 2: Time since last update > 30min
	if time.Since(rec.lastUpdate) > LargeTableMaxUpdateInterval {
		return true
	}

	return false
}

// shouldUpdate implements a debounce mechanism to prevent excessive stats updates.

// shouldExecuteUpdate implements a debounce mechanism to prevent excessive stats updates.
// Only checks inProgress and MinUpdateInterval.
// Change rate is NOT checked here (already checked in shouldEnqueueUpdate).
func (gs *GlobalStats) shouldExecuteUpdate(key pb.StatsInfoKey) bool {
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

func (gs *GlobalStats) markUpdateComplete(key pb.StatsInfoKey, updated bool, actualObjectCount int64, samplingRatio float64) {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		// set new record for RefreshWithMode
		rec = &updateRecord{}
		gs.updatingMu.updating[key] = rec
	}
	rec.inProgress = false
	// only if the stats is updated, set the update time and reset baseline.
	if updated {
		rec.lastUpdate = time.Now()
		rec.baseObjectCount = actualObjectCount
		rec.pendingChanges = 0
		rec.samplingRatio = samplingRatio
	}
}

// GetSamplingRatio returns the sampling ratio used in the last stats update for the given key.
// Returns 0 if no update record exists.
func (gs *GlobalStats) GetSamplingRatio(key pb.StatsInfoKey) float64 {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		return 0
	}
	return rec.samplingRatio
}

func (gs *GlobalStats) GetBaseObjectCnt(key pb.StatsInfoKey) int64 {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		return 0
	}
	return rec.baseObjectCount
}

// ShuffleRangePartialUpdate contains fields that can be independently updated in ShuffleRange
type ShuffleRangePartialUpdate struct {
	Overlap *float64  `json:"overlap,omitempty"`
	Uniform *float64  `json:"uniform,omitempty"`
	Result  []float64 `json:"result,omitempty"`
}

// PatchArgs defines arguments for patch command
type PatchArgs struct {
	// Table-level stats
	TableCnt             *float64 `json:"table_cnt,omitempty"`
	BlockNumber          *int64   `json:"block_number,omitempty"`
	AccurateObjectNumber *int64   `json:"accurate_object_number,omitempty"`

	// Column-level stats (merge mode)
	NdvMap     map[string]float64 `json:"ndv_map,omitempty"`
	MinValMap  map[string]float64 `json:"min_val_map,omitempty"`
	MaxValMap  map[string]float64 `json:"max_val_map,omitempty"`
	NullCntMap map[string]uint64  `json:"null_cnt_map,omitempty"`
	SizeMap    map[string]uint64  `json:"size_map,omitempty"`

	// ShuffleRange partial updates (fine-grained control per column)
	// Each column can have its Overlap/Uniform/Result fields updated independently
	ShuffleRangeMap map[string]*ShuffleRangePartialUpdate `json:"shuffle_range_map,omitempty"`
}

// PatchStats partially updates the stats for the given key
func (gs *GlobalStats) PatchStats(key pb.StatsInfoKey, patch *PatchArgs) error {
	if patch == nil {
		return nil
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	stats := gs.mu.statsInfoMap[key]
	if stats == nil {
		// Create new stats if not exists
		stats = plan2.NewStatsInfo()
		gs.mu.statsInfoMap[key] = stats
	}

	// Apply table-level stats
	if patch.TableCnt != nil {
		stats.TableCnt = *patch.TableCnt
	}
	if patch.BlockNumber != nil {
		stats.BlockNumber = *patch.BlockNumber
	}
	if patch.AccurateObjectNumber != nil {
		stats.AccurateObjectNumber = *patch.AccurateObjectNumber
	}

	// Apply column-level stats (merge mode)
	for col, v := range patch.NdvMap {
		if stats.NdvMap == nil {
			stats.NdvMap = make(map[string]float64)
		}
		stats.NdvMap[col] = v
	}
	for col, v := range patch.MinValMap {
		if stats.MinValMap == nil {
			stats.MinValMap = make(map[string]float64)
		}
		stats.MinValMap[col] = v
	}
	for col, v := range patch.MaxValMap {
		if stats.MaxValMap == nil {
			stats.MaxValMap = make(map[string]float64)
		}
		stats.MaxValMap[col] = v
	}
	for col, v := range patch.NullCntMap {
		if stats.NullCntMap == nil {
			stats.NullCntMap = make(map[string]uint64)
		}
		stats.NullCntMap[col] = v
	}
	for col, v := range patch.SizeMap {
		if stats.SizeMap == nil {
			stats.SizeMap = make(map[string]uint64)
		}
		stats.SizeMap[col] = v
	}

	// Apply ShuffleRange partial updates (fine-grained)
	for col, update := range patch.ShuffleRangeMap {
		if stats.ShuffleRangeMap == nil {
			stats.ShuffleRangeMap = make(map[string]*pb.ShuffleRange)
		}

		// Get or create ShuffleRange for this column
		sr := stats.ShuffleRangeMap[col]
		if sr == nil {
			sr = &pb.ShuffleRange{}
			stats.ShuffleRangeMap[col] = sr
		}

		// Apply individual field updates (only if not nil)
		if update.Overlap != nil {
			sr.Overlap = *update.Overlap
		}
		if update.Uniform != nil {
			sr.Uniform = *update.Uniform
		}
		if update.Result != nil {
			sr.Result = update.Result
		}
	}

	gs.broadcastStats(key)

	// Broadcast update
	gs.mu.cond.Broadcast()

	return nil
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

func (gs *GlobalStats) coordinateStatsUpdate(wrapKey pb.StatsInfoKeyWithContext) {
	statser := statistic.StatsInfoFromContext(wrapKey.Ctx)
	crs := new(perfcounter.CounterSet)
	if !gs.shouldExecuteUpdate(wrapKey.Key) {
		return
	}

	// updated is used to mark that the stats info is updated.
	var updated bool
	var actualObjectCount int64
	var samplingRatio float64
	defer func() {
		gs.markUpdateComplete(wrapKey.Key, updated, actualObjectCount, samplingRatio)
	}()

	broadcastWithoutUpdate := func() {
		gs.mu.Lock()
		defer gs.mu.Unlock()
		gs.mu.statsInfoMap[wrapKey.Key] = nil
		gs.mu.cond.Broadcast()
	}

	// Get the latest partition state of the table.
	//Notice that for snapshot read, subscribing the table maybe failed since the invalid table id,
	//We should handle this case in next PR if needed.
	ps, err := gs.engine.pClient.toSubscribeTable(
		wrapKey.Ctx,
		uint64(wrapKey.Key.AccId),
		wrapKey.Key.TableID,
		wrapKey.Key.TableName,
		wrapKey.Key.DatabaseID,
		wrapKey.Key.DbName)
	if err != nil {
		logutil.Warnf(
			"updateTableStats:failed to subsrcribe table[%d-%s], error:%s",
			wrapKey.Key.TableID,
			wrapKey.Key.TableName,
			err)
		broadcastWithoutUpdate()
		return
	}
	stats := plan2.NewStatsInfo()

	newCtx := perfcounter.AttachS3RequestKey(wrapKey.Ctx, crs)
	updated, samplingRatio = gs.executeStatsUpdate(newCtx, ps, wrapKey.Key, stats)

	// Get actual object count for baseline update
	if updated {
		actualObjectCount = stats.AccurateObjectNumber
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
		gs.mu.statsInfoMap[wrapKey.Key] = stats
		gs.broadcastStats(wrapKey.Key)
	} else if _, ok := gs.mu.statsInfoMap[wrapKey.Key]; !ok {
		gs.mu.statsInfoMap[wrapKey.Key] = nil
	}

	// Notify all the waiters to read the new stats info.
	gs.mu.cond.Broadcast()
}

// RefreshWithMode triggers a stats refresh with the specified sampling mode
func (gs *GlobalStats) RefreshWithMode(ctx context.Context, key pb.StatsInfoKey, samplingMode string) error {
	// Get partition state
	ps, err := gs.engine.pClient.toSubscribeTable(
		ctx,
		uint64(key.AccId),
		key.TableID,
		key.TableName,
		key.DatabaseID,
		key.DbName)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to subscribe table: %v", err)
	}

	// Get table definition
	table := gs.engine.GetLatestCatalogCache().GetTableById(key.AccId, key.DatabaseID, key.TableID)
	if table == nil || table.TableDef == nil {
		return moerr.NewInternalErrorNoCtx("table not found")
	}

	// Create stats info
	stats := plan2.NewStatsInfo()
	approxObjectNum := int64(ps.ApproxDataObjectsNum())

	lastActualObjectCnt := gs.GetBaseObjectCnt(key)
	if lastActualObjectCnt > 0 {
		approxObjectNum = lastActualObjectCnt
	}

	// Create update request with custom sampling mode
	now := timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
	req := &updateStatsRequest{
		statsInfo:       stats,
		tableDef:        table.TableDef,
		partitionState:  ps,
		fs:              gs.engine.fs,
		ts:              types.TimestampToTS(now),
		approxObjectNum: approxObjectNum,
		samplingMode:    samplingMode,
	}

	// Execute stats update
	samplingRatio, err := CollectAndCalculateStats(ctx, req, gs.concurrentExecutor)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to update stats: %v", err)
	}

	// Update cache
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.mu.statsInfoMap[key] = stats
	gs.broadcastStats(key)
	gs.mu.cond.Broadcast()
	// Record sampling ratio in updateRecord
	gs.markUpdateComplete(key, true, stats.AccurateObjectNumber, samplingRatio)

	return nil
}

func (gs *GlobalStats) executeStatsUpdate(ctx context.Context, ps *logtailreplay.PartitionState, key pb.StatsInfoKey, stats *pb.StatsInfo) (bool, float64) {
	table := gs.engine.GetLatestCatalogCache().GetTableById(key.AccId, key.DatabaseID, key.TableID)
	// table or its definition is nil, means that the table is created but not committed yet.
	if table == nil || table.TableDef == nil {
		logutil.Errorf("cannot get table by ID %v", key)
		return false, 0
	}

	//partitionState := gs.engine.GetOrCreateLatestPart(key.DatabaseID, key.TableID).Snapshot()
	approxObjectNum := int64(ps.ApproxDataObjectsNum())
	if gs.approxObjectNumUpdater == nil && approxObjectNum == 0 {
		// There are no objects flushed yet.
		return false, 0
	}

	lastActualObjectCnt := gs.GetBaseObjectCnt(key)
	if lastActualObjectCnt > 0 {
		approxObjectNum = lastActualObjectCnt
	}

	// the time used to init stats info is not need to be too precise.
	now := timestamp.Timestamp{PhysicalTime: time.Now().UnixNano()}
	req := newUpdateStatsRequest(
		table.TableDef,
		ps,
		gs.engine.fs,
		types.TimestampToTS(now),
		approxObjectNum,
		stats,
	)
	start := time.Now()
	samplingRatio, err := CollectAndCalculateStats(ctx, req, gs.concurrentExecutor)
	if err != nil {
		logutil.Errorf("failed to init stats info for table %v, err: %v", key, err)
		return false, 0
	}
	v2.StatsUpdateDurationHistogram.Observe(time.Since(start).Seconds())
	v2.StatsUpdateBlockCounter.Add(float64(stats.BlockNumber))
	return true, samplingRatio
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
	case types.T_year:
		return float64(types.DecodeMoYear(buf))
	case types.T_decimal64:
		// Fix: Use Decimal64ToFloat64 to handle negative values correctly
		dec := types.DecodeDecimal64(buf)
		return types.Decimal64ToFloat64(dec, typ.Scale)
	case types.T_decimal128:
		// Fix: Use Decimal128ToFloat64 to handle negative values correctly
		dec := types.DecodeDecimal128(buf)
		return types.Decimal128ToFloat64(dec, typ.Scale)
	//case types.T_char, types.T_varchar, types.T_text:
	//return float64(plan2.ByteSliceToUint64(buf)), true
	default:
		panic("unsupported type")
	}
}

// shouldSampleObject determines if an object should be sampled.
// It combines the random part of Segmentid (UUIDv7 bytes 8-15) with the object Num
// using XOR and golden ratio prime to ensure each object has independent sampling probability.
// This fixes the issue where objects sharing the same Segmentid would all be sampled or rejected together.
func shouldSampleObject(objName *objectio.ObjectNameShort, threshold uint64) bool {
	randomPart := binary.LittleEndian.Uint64(objName[objectIDRandomOffset : objectIDRandomOffset+8])
	num := uint64(objName.Num())
	// XOR with golden ratio prime provides good bit mixing
	combined := randomPart ^ (num * 0x9E3779B97F4A7C15)
	return combined < threshold
}

// calcSamplingThreshold converts sampling ratio to uint64 threshold.
func calcSamplingThreshold(ratio float64) uint64 {
	if ratio >= 1.0 {
		return math.MaxUint64
	}
	return uint64(ratio * float64(math.MaxUint64))
}

// calcSamplingRatio calculates the sampling ratio based on object count.
// Sample count = clamp(sqrt(objectCount), MinSampleObjects, MaxSampleObjects)
func calcSamplingRatio(approxObjectNum int64) float64 {
	if approxObjectNum <= SamplingThreshold {
		return 1.0
	}

	// targetCount = clamp(max(sqrt(N), 0.02·N), 100, 2000)//
	// Candidate1: sqrt(N)
	targetCount := int(math.Sqrt(float64(approxObjectNum)))
	// Candidate2: 2% of objects
	targetCount = max(targetCount, int(float64(approxObjectNum)*0.02))
	// Lower/upper bounds
	targetCount = max(targetCount, MinSampleObjects)
	targetCount = min(targetCount, MaxSampleObjects)

	ratio := float64(targetCount) / float64(approxObjectNum)
	if ratio > 1.0 {
		ratio = 1.0
	}
	return ratio
}

// get ndv, minval , maxval, datatype from zonemap. Retrieve all columns except for rowid, return accurate number of objects
// Returns the actual sampling ratio (sampledObjects / totalObjects).
func collectTableStats(
	ctx context.Context, req *updateStatsRequest, info *plan2.TableStatsInfo, executor ConcurrentExecutor,
) (float64, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateInfoFromZonemapHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(req.tableDef.Cols) - 1 /* row-id */
	fs, fsErr := fileservice.Get[fileservice.FileService](req.fs, defines.SharedFileServiceName)
	if fsErr != nil {
		return 0, fsErr
	}

	// Calculate sampling parameters based on mode
	var samplingRatio float64
	switch req.samplingMode {
	case "full":
		// Force full scan
		samplingRatio = 1.0
	default:
		// "auto" or empty: use default logic
		samplingRatio = calcSamplingRatio(req.approxObjectNum)
	}

	isSampling := samplingRatio < 1.0
	samplingThreshold := calcSamplingThreshold(samplingRatio)

	var updateMu sync.Mutex
	var init bool

	// Phase 1: Exact stats from ObjectStats (no IO)
	var exactRowCount float64
	var exactBlockNumber int64
	var exactObjectNumber int64

	// Phase 2: Sampled stats from ObjectMeta (requires IO)
	var sampledRowCount float64
	var sampledObjectCount int64

	onObjFn := func(obj objectio.ObjectEntry) error {
		objName := obj.ObjectShortName()

		// ===== Phase 1: Get exact values from ObjectStats (no IO) =====
		objRows := obj.Rows()
		objBlkCnt := obj.BlkCnt()

		updateMu.Lock()
		exactRowCount += float64(objRows)
		exactBlockNumber += int64(objBlkCnt)
		exactObjectNumber++
		updateMu.Unlock()

		// ===== Phase 2: Sampling decision =====
		if isSampling && !shouldSampleObject(objName, samplingThreshold) {
			return nil // Skip non-sampled objects, no IO
		}

		// Sampled object: read ObjectMeta (requires IO)
		location := obj.Location()
		objMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}

		updateMu.Lock()
		defer updateMu.Unlock()

		meta := objMeta.MustDataMeta()
		sampledObjectCount++
		info.BlockNumber += int64(objBlkCnt)
		objectRowCount := meta.BlockHeader().Rows()
		info.TableRowCount += float64(objectRowCount)
		sampledRowCount += float64(objectRowCount)

		if !init {
			init = true
			// Initialize table-level MaxObjectRowCount and MinObjectRowCount before column loop
			info.MaxObjectRowCount = objectRowCount
			info.MinObjectRowCount = objectRowCount
			for idx, col := range req.tableDef.Cols[:lenCols] {
				columnMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] = int64(columnMeta.NullCnt())
				info.ColumnZMs[idx] = columnMeta.ZoneMap().Clone()
				info.DataTypes[idx] = plan2.ExprType2Type(&col.Typ)
				columnNDV := float64(columnMeta.Ndv())
				info.ColumnNDVs[idx] = columnNDV
				info.MaxNDVs[idx] = columnNDV
				info.NDVinMinObject[idx] = columnNDV
				info.NDVinMaxObject[idx] = columnNDV
				// Use OriginSize() instead of Length() for accurate data size estimation
				// ZoneMapArea and BFExtent are block-level metadata, not column-level, so they are excluded
				info.ColumnSize[idx] = int64(columnMeta.Location().OriginSize())
				if info.ColumnNDVs[idx] > 100 || info.ColumnNDVs[idx] > 0.1*float64(meta.BlockHeader().Rows()) {
					switch info.DataTypes[idx].Oid {
					case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_time, types.T_timestamp, types.T_date, types.T_datetime, types.T_year, types.T_decimal64, types.T_decimal128:
						info.ShuffleRanges[idx] = plan2.NewShuffleRange(false)
						if info.ColumnZMs[idx].IsInited() {
							minValue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMinBuf())
							maxValue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMaxBuf())
							info.ShuffleRanges[idx].Update(minValue, maxValue, int64(meta.BlockHeader().Rows()), int64(columnMeta.NullCnt()))
						}
					case types.T_varchar, types.T_char, types.T_text:
						info.ShuffleRanges[idx] = plan2.NewShuffleRange(true)
						if info.ColumnZMs[idx].IsInited() {
							info.ShuffleRanges[idx].UpdateString(info.ColumnZMs[idx].GetMinBuf(), info.ColumnZMs[idx].GetMaxBuf(), int64(meta.BlockHeader().Rows()), int64(columnMeta.NullCnt()))
						}
					}
				}
			}
		} else {
			// Update sampled Max/MinObjectRowCount (for NDVinMaxObject etc.)
			isMaxObject := objectRowCount > info.MaxObjectRowCount
			isMinObject := objectRowCount < info.MinObjectRowCount
			if isMaxObject {
				info.MaxObjectRowCount = objectRowCount
			}
			if isMinObject {
				info.MinObjectRowCount = objectRowCount
			}

			for idx, col := range req.tableDef.Cols[:lenCols] {
				columnMeta := meta.MustGetColumn(uint16(col.Seqnum))
				info.NullCnts[idx] += int64(columnMeta.NullCnt())
				// CRITICAL FIX: Always accumulate ColumnSize, even if ZoneMap is not initialized
				// ZoneMap initialization status should not affect size calculation
				// Use OriginSize() instead of Length() for accurate data size estimation
				info.ColumnSize[idx] += int64(columnMeta.Location().OriginSize())

				// CRITICAL FIX: Always accumulate NDV, even if ZoneMap is not initialized
				// NDV is calculated independently using HyperLogLog sketch, not dependent on ZoneMap
				columnNDV := float64(columnMeta.Ndv())
				info.ColumnNDVs[idx] += columnNDV
				if columnNDV > info.MaxNDVs[idx] {
					info.MaxNDVs[idx] = columnNDV
				}
				// Update NDVinMaxObject and NDVinMinObject based on table-level MaxObjectRowCount/MinObjectRowCount
				if isMaxObject {
					// This is the new maximum object, update NDVinMaxObject for this column
					info.NDVinMaxObject[idx] = columnNDV
				} else if objectRowCount == info.MaxObjectRowCount && columnNDV > info.NDVinMaxObject[idx] {
					// Same row count as current max, but this column has higher NDV
					info.NDVinMaxObject[idx] = columnNDV
				}
				if isMinObject {
					// This is the new minimum object, update NDVinMinObject for this column
					info.NDVinMinObject[idx] = columnNDV
				} else if objectRowCount == info.MinObjectRowCount && columnNDV < info.NDVinMinObject[idx] {
					// Same row count as current min, but this column has lower NDV
					info.NDVinMinObject[idx] = columnNDV
				}

				// CRITICAL FIX: Check if ShuffleRanges should be created based on accumulated stats
				// This allows ShuffleRanges to be created even if the first object didn't meet the condition
				// This check is done before ZoneMap check, so we can create ShuffleRanges even if current object's ZoneMap is not initialized
				if info.ShuffleRanges[idx] == nil {
					// Use accumulated NDV and total row count to decide if ShuffleRanges should be created
					if info.ColumnNDVs[idx] > 100 || info.ColumnNDVs[idx] > 0.1*float64(info.TableRowCount) {
						switch info.DataTypes[idx].Oid {
						case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_time, types.T_timestamp, types.T_date, types.T_datetime, types.T_year, types.T_decimal64, types.T_decimal128:
							info.ShuffleRanges[idx] = plan2.NewShuffleRange(false)
							// Initialize with accumulated ZoneMap if available
							if info.ColumnZMs[idx].IsInited() {
								minValue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMinBuf())
								maxValue := getMinMaxValueByFloat64(info.DataTypes[idx], info.ColumnZMs[idx].GetMaxBuf())
								// Use accumulated row count and null count
								info.ShuffleRanges[idx].Update(minValue, maxValue, int64(info.TableRowCount), info.NullCnts[idx])
							}
						case types.T_varchar, types.T_char, types.T_text:
							info.ShuffleRanges[idx] = plan2.NewShuffleRange(true)
							if info.ColumnZMs[idx].IsInited() {
								info.ShuffleRanges[idx].UpdateString(info.ColumnZMs[idx].GetMinBuf(), info.ColumnZMs[idx].GetMaxBuf(), int64(info.TableRowCount), info.NullCnts[idx])
							}
						}
					}
				}

				zoneMap := columnMeta.ZoneMap().Clone()
				if !zoneMap.IsInited() {
					continue
				}
				index.UpdateZM(info.ColumnZMs[idx], zoneMap.GetMaxBuf())
				index.UpdateZM(info.ColumnZMs[idx], zoneMap.GetMinBuf())

				// Update existing ShuffleRanges with current object's data
				if info.ShuffleRanges[idx] != nil {
					switch info.DataTypes[idx].Oid {
					case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_time, types.T_timestamp, types.T_date, types.T_datetime, types.T_year, types.T_decimal64, types.T_decimal128:
						minValue := getMinMaxValueByFloat64(info.DataTypes[idx], zoneMap.GetMinBuf())
						maxValue := getMinMaxValueByFloat64(info.DataTypes[idx], zoneMap.GetMaxBuf())
						info.ShuffleRanges[idx].Update(minValue, maxValue, int64(meta.BlockHeader().Rows()), int64(columnMeta.NullCnt()))
					case types.T_varchar, types.T_char, types.T_text:
						info.ShuffleRanges[idx].UpdateString(zoneMap.GetMinBuf(), zoneMap.GetMaxBuf(), int64(meta.BlockHeader().Rows()), int64(columnMeta.NullCnt()))
					}
				}
			}
		}
		return nil
	}

	if err := ForeachVisibleObjects(
		req.partitionState,
		req.ts,
		onObjFn,
		executor,
		false,
	); err != nil {
		return 0, err
	}

	// ===== Apply exact values from Phase 1 =====
	// Note: Only apply table-level exact stats (row count, block count, object count).
	// MaxObjectRowCount/MinObjectRowCount are NOT overwritten with exact values because:
	// - NDVinMaxObject/NDVinMinObject are from sampled objects
	// - AdjustNDV uses rateMax = NDVinMaxObject / MaxObjectRowCount
	// - If we use exact MaxObjectRowCount but sampled NDVinMaxObject, the rate calculation
	//   would be semantically incorrect (different objects)
	// - Keeping sampled Max/MinObjectRowCount maintains consistency with NDV extremes
	info.TableRowCount = exactRowCount
	info.BlockNumber = exactBlockNumber
	info.AccurateObjectNumber = exactObjectNumber

	// Calculate actual sampling ratio
	var actualSamplingRatio = float64(1.0)
	if exactObjectNumber > 0 {
		actualSamplingRatio = float64(sampledObjectCount) / float64(exactObjectNumber)
	}

	// ===== Scale column-level stats if sampling =====
	if isSampling && sampledRowCount > 0 {
		rowScaleFactor := exactRowCount / sampledRowCount

		for i := range info.ColumnSize {
			info.ColumnSize[i] = int64(float64(info.ColumnSize[i]) * rowScaleFactor)
			info.NullCnts[i] = int64(float64(info.NullCnts[i]) * rowScaleFactor)
			// NDV: scale up by inverse of sampling ratio, cap at row count
			upper := info.TableRowCount * 0.99
			info.ColumnNDVs[i] = math.Min(info.ColumnNDVs[i]*rowScaleFactor, upper)
			info.MaxNDVs[i] = math.Min(info.MaxNDVs[i]*rowScaleFactor, upper)
			// Keep NDVinMax/MinObject as sampled (do not scale), so the rate
			// calculations in AdjustNDV stay consistent with sampled
			// MaxObjectRowCount/MinObjectRowCount.
		}
	}

	return actualSamplingRatio, nil
}

// CollectAndCalculateStats is the main function to calculate and update the stats for scan node.
// Returns the actual sampling ratio used (sampledObjects / totalObjects).
func CollectAndCalculateStats(ctx context.Context, req *updateStatsRequest, executor ConcurrentExecutor) (float64, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateStatsDurationHistogram.Observe(time.Since(start).Seconds())
	}()
	lenCols := len(req.tableDef.Cols) - 1 /* row-id */
	info := plan2.NewTableStatsInfo(lenCols)
	if req.approxObjectNum == 0 {
		return 1.0, nil
	}
	info.ApproxObjectNumber = req.approxObjectNum
	baseTableDef := req.tableDef

	actualSamplingRatio, err := collectTableStats(ctx, req, info, executor)
	if err != nil {
		return 0, err
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
			logutil.Error(
				"UpdateStats-Error",
				zap.String("table", baseTableDef.Name),
				zap.String("col", colName),
				zap.Float64("max", req.statsInfo.MaxValMap[colName]),
				zap.Float64("min", req.statsInfo.MinValMap[colName]),
			)
		}
		logutil.Debugf("debug: table %v tablecnt %v  col %v max %v min %v ndv %v overlap %v maxndv %v maxobj %v ndvinmaxobj %v minobj %v ndvinminobj %v",
			baseTableDef.Name, info.TableRowCount, colName, req.statsInfo.MaxValMap[colName], req.statsInfo.MinValMap[colName],
			req.statsInfo.NdvMap[colName], overlap, info.MaxNDVs[i], info.MaxObjectRowCount, info.NDVinMaxObject[i], info.MinObjectRowCount, info.NDVinMinObject[i])
	}
	return actualSamplingRatio, nil
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
