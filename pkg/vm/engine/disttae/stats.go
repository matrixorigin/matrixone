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
	"errors"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
//     │ - cache/generation 原子检查：key 必须已存在
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
//     │ - startAutomaticUpdate(): 检查 generation、inProgress 和 MinUpdateInterval (15s)
//     │
//     ▼
// coordinateStatsUpdateJob()
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
	MaxSampleObjects = 5000

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

const optimizerStatsRefreshStripes = 64

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
	// queued is the number of registered enqueue attempts that have not reached
	// worker admission or rolled back. It includes a forced sender blocked on a
	// full queue. Together with inProgress it is the durable predicate that
	// proves a synchronous waiter still has a producer.
	queued int
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

// statsUpdateJob carries the scheduling generation observed by the producer.
// Pointer identity prevents a queued worker from publishing into a newer table
// generation after RemoveTid deleted the old record.
type statsUpdateJob struct {
	wrapKey        pb.StatsInfoKeyWithContext
	expectedRecord *updateRecord
	// registered means enqueueStatsUpdateForRecord accounted this job in
	// expectedRecord. Direct test helpers leave it false.
	registered bool
}

type GlobalStats struct {
	ctx context.Context

	// engine is the global Engine instance.
	engine *Engine

	// tailC is the chan to receive entries from logtail
	// and then update the stats info map.
	// TODO(volgariver6): add metrics of the chan length.
	tailC chan *logtail.TableLogtail

	updateC chan statsUpdateJob

	// queueWatcher keeps the table id and its enqueue time.
	// and watch the queue item in the queue.
	queueWatcher *queueWatcher

	updatingMu struct {
		sync.Mutex
		updating map[pb.StatsInfoKey]*updateRecord
	}

	// Explicit ANALYZE refreshes and automatic logtail refreshes share this
	// bounded admission layer. The same table is calculated and published in
	// order; unrelated tables normally remain parallel.
	refreshAdmission [optimizerStatsRefreshStripes]chan struct{}

	// statsInfoMap is the global stats info in engine which
	// contains all subscribed tables stats info.
	mu struct {
		sync.Mutex

		// cond is used to wait for stats updated for the first time.
		// If sync parameter is false, it is unuseful.
		cond *sync.Cond

		// statsInfoMap is the real stats info data.
		statsInfoMap map[pb.StatsInfoKey]*pb.StatsInfo

		// tableDefVersions is present only for statistics that contain a
		// table-wide observation bound to one schema definition. It has the
		// same owner and lifetime as statsInfoMap. Metadata-only and remote
		// statistics remain unbound for wire compatibility.
		tableDefVersions map[pb.StatsInfoKey]uint32
	}

	// updateWorkerFactor is the times of CPU number of this node
	// to start update worker. Default is 8.
	updateWorkerFactor int

	// KeyRouter is the router to decides which node should send to.
	KeyRouter client.KeyRouter[pb.StatsInfoKey]

	concurrentExecutor ConcurrentExecutor

	// approxObjectNumUpdater is for test only currently.
	approxObjectNumUpdater func() int64

	// beforeCacheRemoteInfo is for test only.
	beforeCacheRemoteInfo func(pb.StatsInfoKey)

	// beforeSubscribeTable is for test only.
	beforeSubscribeTable func(pb.StatsInfoKey)

	// beforeStatsWait is for deterministic wait-protocol tests only. It runs
	// with gs.mu held immediately before cond.Wait atomically releases it.
	beforeStatsWait func(pb.StatsInfoKey, *updateRecord)

	// beforeVersionedStatsPublish is for deterministic schema-publication
	// tests only. It runs after schema validation while the catalog table-change
	// read lock is still held and before the statistics cache swap.
	beforeVersionedStatsPublish func(pb.StatsInfoKey, uint32)

	// afterAutomaticUpdateStarted is for deterministic producer-cancellation
	// tests only. It runs after worker admission and before refresh admission.
	afterAutomaticUpdateStarted func(pb.StatsInfoKey, *updateRecord)
}

func NewGlobalStats(
	ctx context.Context, e *Engine, keyRouter client.KeyRouter[pb.StatsInfoKey], opts ...GlobalStatsOption,
) *GlobalStats {
	s := &GlobalStats{
		ctx:          ctx,
		engine:       e,
		tailC:        make(chan *logtail.TableLogtail, 10000),
		updateC:      make(chan statsUpdateJob, 3000),
		KeyRouter:    keyRouter,
		queueWatcher: newQueueWatcher(),
	}
	s.updatingMu.updating = make(map[pb.StatsInfoKey]*updateRecord)
	s.mu.statsInfoMap = make(map[pb.StatsInfoKey]*pb.StatsInfo)
	s.mu.tableDefVersions = make(map[pb.StatsInfoKey]uint32)
	s.mu.cond = sync.NewCond(&s.mu)
	// One lifecycle callback wakes every current waiter when update workers
	// stop. Register it once per GlobalStats rather than once per cache miss.
	context.AfterFunc(ctx, s.notifyStatsWaiters)
	s.initStatsRefreshAdmission()
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

func (gs *GlobalStats) initStatsRefreshAdmission() {
	for i := range gs.refreshAdmission {
		gs.refreshAdmission[i] = make(chan struct{}, 1)
	}
}

func optimizerStatsRefreshStripe(key pb.StatsInfoKey) int {
	mixed := key.TableID ^ uint64(key.AccId)*0x9e3779b97f4a7c15
	return int(mixed % optimizerStatsRefreshStripes)
}

func (gs *GlobalStats) acquireStatsRefresh(
	ctx context.Context,
	key pb.StatsInfoKey,
) (func(), error) {
	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		return nil, cause
	}
	admission := gs.refreshAdmission[optimizerStatsRefreshStripe(key)]
	select {
	case admission <- struct{}{}:
		// Cancellation can race the select and make both cases ready. Recheck
		// the authoritative caller and owner contexts before transferring the
		// admission token to the caller.
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			<-admission
			return nil, cause
		}
		return func() { <-admission }, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-gs.lifecycleDone():
		return nil, gs.statsRefreshCancellationCause(ctx)
	}
}

// statsRefreshCancellationCause treats the request context and the GlobalStats
// owner lifecycle as durable predicates. Async callbacks may wake blocked work,
// but they are never the source of truth for admission or publication.
func (gs *GlobalStats) statsRefreshCancellationCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if gs.ctx != nil {
		return context.Cause(gs.ctx)
	}
	return nil
}

// newStatsRefreshContext preserves request values and deadlines while linking
// downstream subscription and object I/O to the GlobalStats owner lifecycle.
// The returned context delivers cancellation; admission and publication still
// re-read statsRefreshCancellationCause as their durable predicate.
func (gs *GlobalStats) newStatsRefreshContext(
	ctx context.Context,
) (context.Context, func(), error) {
	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		return nil, nil, cause
	}
	if gs.ctx == nil {
		return ctx, func() {}, nil
	}
	refreshCtx, cancelRefresh := context.WithCancelCause(ctx)
	stopOwnerWatch := context.AfterFunc(gs.ctx, func() {
		cause := context.Cause(gs.ctx)
		if cause == nil {
			cause = context.Canceled
		}
		cancelRefresh(cause)
	})
	stop := func() {
		stopOwnerWatch()
		cancelRefresh(nil)
	}
	// Close the owner check/register race without depending on callback
	// dispatch. No downstream operation is admitted on the error path.
	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		stop()
		return nil, nil, cause
	}
	return refreshCtx, stop, nil
}

// RemoveTid removes every GlobalStats entry owned by the given table ID.
// Called from cleanMemoryTableWithTable (1+ hour after unsubscribe/drop)
// to prevent both published statistics and refresh-scheduling metadata from
// growing for the process lifetime. Safe because no queries target a table
// that has been unsubscribed for over an hour.
func (gs *GlobalStats) RemoveTid(tableID uint64) {
	// Keep the established gs.mu -> updatingMu lock order used by
	// broadcastStats. Table cleanup is the common lifetime boundary for both
	// maps, so a removed table cannot retain one owner after losing the other.
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	for key := range gs.mu.statsInfoMap {
		if key.TableID == tableID {
			delete(gs.mu.statsInfoMap, key)
			delete(gs.mu.tableDefVersions, key)
		}
	}
	for key := range gs.updatingMu.updating {
		if key.TableID == tableID {
			delete(gs.updatingMu.updating, key)
		}
	}
	if gs.mu.cond != nil {
		gs.mu.cond.Broadcast()
	}
}

func (gs *GlobalStats) PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool {
	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}
	generation, ok := gs.currentOrCreateSubscribedUpdateRecord(key)
	if !ok {
		return false
	}
	return gs.enqueueStatsUpdateForRecord(wrapkey, false, generation)
}

// currentOrCreateUpdateRecord returns the table-lifetime token that every
// queued or explicit refresh must carry. In particular, the first refresh must
// not use nil as an "expected absence" token: RemoveTid can make absence true
// again, allowing old queued work to cross the cleanup boundary and recreate
// table-owned state. Production callers must already hold or have proved a
// cleanup owner; use currentOrCreateSubscribedUpdateRecord for request-driven
// work and shouldEnqueueExistingStatsUpdateGeneration for logtail work.
func (gs *GlobalStats) currentOrCreateUpdateRecord(key pb.StatsInfoKey) *updateRecord {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec := gs.updatingMu.updating[key]
	if rec == nil {
		rec = &updateRecord{}
		gs.updatingMu.updating[key] = rec
	}
	return rec
}

// currentOrCreateSubscribedUpdateRecord captures the scheduling generation
// only while the subscription that owns its cleanup is still current. Explicit
// refreshes call this after toSubscribeTable succeeds, so a failed subscription
// cannot leave metadata that no unsubscribe path can reclaim. Holding the
// subscription read lock across record creation also prevents cleanup from
// falling between subscription validation and token capture.
func (gs *GlobalStats) currentOrCreateSubscribedUpdateRecord(
	key pb.StatsInfoKey,
) (*updateRecord, bool) {
	gs.engine.pClient.subscribed.rw.RLock()
	defer gs.engine.pClient.subscribed.rw.RUnlock()

	ent, ok := gs.engine.pClient.subscribed.m[key.TableID]
	if !ok || ent == nil || ent.dbID != key.DatabaseID || ent.state != Subscribed {
		return nil, false
	}
	return gs.currentOrCreateUpdateRecord(key), true
}

// currentOrCreateExactSubscribedUpdateRecord is the first-read variant. It
// validates the exact subscription generation before mutating scheduling
// state, so an old read cannot create even idle metadata for a replacement
// lifetime.
func (gs *GlobalStats) currentOrCreateExactSubscribedUpdateRecord(
	key pb.StatsInfoKey,
	expectedEnt *subEntry,
) (*updateRecord, bool) {
	if expectedEnt == nil {
		return nil, false
	}
	gs.engine.pClient.subscribed.rw.RLock()
	defer gs.engine.pClient.subscribed.rw.RUnlock()

	ent, ok := gs.engine.pClient.subscribed.m[key.TableID]
	if !ok || ent != expectedEnt || ent.dbID != key.DatabaseID || ent.state != Subscribed {
		return nil, false
	}
	return gs.currentOrCreateUpdateRecord(key), true
}

func (gs *GlobalStats) subscribedEntry(key pb.StatsInfoKey) *subEntry {
	gs.engine.pClient.subscribed.rw.RLock()
	defer gs.engine.pClient.subscribed.rw.RUnlock()

	ent, ok := gs.engine.pClient.subscribed.m[key.TableID]
	if !ok || ent == nil {
		return nil
	}
	if ent.dbID != key.DatabaseID || ent.state != Subscribed {
		return nil
	}
	return ent
}

func (gs *GlobalStats) cacheRemoteInfoIfSubscribed(
	key pb.StatsInfoKey,
	subscribedEnt *subEntry,
	remoteInfo *pb.StatsInfo,
	tableDefVersion *uint32,
	rejectBoundWithoutVersion bool,
) *pb.StatsInfo {
	if subscribedEnt == nil || remoteInfo == nil {
		return nil
	}

	gs.engine.pClient.subscribed.rw.RLock()
	defer gs.engine.pClient.subscribed.rw.RUnlock()

	currentEnt, ok := gs.engine.pClient.subscribed.m[key.TableID]
	if !ok || currentEnt != subscribedEnt || currentEnt.dbID != key.DatabaseID || currentEnt.state != Subscribed {
		return nil
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	info, complete, incompatible := gs.statsInfoForTableVersionLocked(
		key, tableDefVersion, rejectBoundWithoutVersion)
	if complete && info != nil {
		return info
	}
	if incompatible {
		return nil
	}

	gs.mu.statsInfoMap[key] = remoteInfo
	delete(gs.mu.tableDefVersions, key)
	if gs.mu.cond != nil {
		gs.mu.cond.Broadcast()
	}
	return remoteInfo
}

func (gs *GlobalStats) Get(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo {
	return gs.get(ctx, key, sync, nil, false)
}

// GetForRemote returns only statistics that are safe to serialize to a CN
// whose table-definition version is unknown. Local unversioned readers use
// Get and may inspect the process-local published value.
func (gs *GlobalStats) GetForRemote(ctx context.Context, key pb.StatsInfoKey) *pb.StatsInfo {
	return gs.get(ctx, key, false, nil, true)
}

// GetAtTableVersion returns schema-bound statistics only when they were
// observed from the table definition used by the caller's plan. Unbound
// metadata statistics retain the legacy behavior.
func (gs *GlobalStats) GetAtTableVersion(
	ctx context.Context,
	key pb.StatsInfoKey,
	sync bool,
	tableDefVersion uint32,
) *pb.StatsInfo {
	return gs.get(ctx, key, sync, &tableDefVersion, false)
}

// statsInfoForTableVersionLocked distinguishes a true cache miss from a
// schema-bound entry that this reader must not consume. A mismatched entry is
// retained because an older snapshot reader may still match it.
func (gs *GlobalStats) statsInfoForTableVersionLocked(
	key pb.StatsInfoKey,
	tableDefVersion *uint32,
	rejectBoundWithoutVersion bool,
) (info *pb.StatsInfo, complete bool, incompatible bool) {
	info, complete = gs.mu.statsInfoMap[key]
	if !complete {
		return nil, false, false
	}
	if version, bound := gs.mu.tableDefVersions[key]; bound {
		if tableDefVersion != nil && version != *tableDefVersion {
			return nil, false, true
		}
		if tableDefVersion == nil && rejectBoundWithoutVersion {
			return nil, false, true
		}
	}
	return info, true, false
}

func (gs *GlobalStats) get(
	ctx context.Context,
	key pb.StatsInfoKey,
	sync bool,
	tableDefVersion *uint32,
	rejectBoundWithoutVersion bool,
) *pb.StatsInfo {
	wrapkey := pb.StatsInfoKeyWithContext{
		Ctx: ctx,
		Key: key,
	}

	gs.mu.Lock()
	info, _, incompatible := gs.statsInfoForTableVersionLocked(
		key, tableDefVersion, rejectBoundWithoutVersion)
	if info != nil {
		gs.mu.Unlock()
		return info
	}
	gs.mu.Unlock()
	if incompatible && !sync {
		// Non-blocking callers (notably remote CN exports) cannot establish a
		// replacement observation. Fail closed without subscribing or exporting
		// statistics whose schema version they cannot prove.
		return nil
	}

	// after checking first potential patched cache
	// we check the approx to avoid taking a place in statInfo map
	if gs.beforeSubscribeTable != nil {
		gs.beforeSubscribeTable(key)
	}
	ps, err := gs.engine.pClient.toSubscribeTable(
		ctx,
		uint64(key.AccId),
		key.TableID,
		key.TableName,
		key.DatabaseID,
		key.DbName)

	if err != nil {
		// A failed initial subscription has no table-lifetime cleanup owner.
		// Retrying through updateC would create a scheduling generation (and
		// potentially a nil cache sentinel) that RemoveTid can never reclaim.
		return nil
	}
	if ps.ApproxDataObjectsNum() == 0 {
		return nil
	}

	subscribedEnt := gs.subscribedEntry(key)
	if subscribedEnt == nil {
		// Cleanup crossed the successful subscribe return before this read could
		// capture the exact lifetime. Do not let nil mean "accept any later
		// subscription" when the synchronous producer is created below.
		return nil
	}
	var remoteInfo *pb.StatsInfo
	if _, ok := ctx.Value(perfcounter.CalcTableStatsKey{}).(bool); ok {
		stats := statistic.StatsInfoFromContext(ctx)
		start := time.Now()
		defer func() {
			stats.AddBuildPlanStatsIOConsumption(time.Since(start))
		}()
	}

	// Get stats info from remote node.
	if !incompatible && gs.KeyRouter != nil && gs.engine.qc != nil {
		client := gs.engine.qc
		// Gossip advertises statistics ownership by the stable physical table
		// identity only. Names and account context are needed by the stats
		// producer, but including them in the exact router-map lookup would not
		// match the advertised key and would silently disable remote reuse.
		routingKey := pb.StatsInfoKey{
			DatabaseID: key.DatabaseID,
			TableID:    key.TableID,
		}
		target := gs.KeyRouter.Target(routingKey)
		if len(target) != 0 {
			req := client.NewRequest(query.CmdMethod_GetStatsInfo)
			req.GetStatsInfoRequest = &query.GetStatsInfoRequest{
				StatsInfoKey: &key,
			}
			resp, err := client.SendMessage(ctx, target, req)
			if err != nil || resp == nil {
				logutil.Errorf("failed to send request to %s, err: %v, resp: %v", target, err, resp)
			} else if resp.GetStatsInfoResponse == nil || resp.GetStatsInfoResponse.StatsInfo == nil {
				// A remote miss may fall back to a synchronous local build. Return
				// the empty pooled response before that potentially long wait.
				client.Release(resp)
			} else {
				// Keep a response that owns usable stats alive until the stats have
				// been copied into the local cache (or rejected by the lifetime gate).
				defer client.Release(resp)
				remoteInfo = resp.GetStatsInfoResponse.StatsInfo
			}
		}
	}

	if remoteInfo != nil {
		if gs.beforeCacheRemoteInfo != nil {
			gs.beforeCacheRemoteInfo(key)
		}
		if info = gs.cacheRemoteInfoIfSubscribed(
			key, subscribedEnt, remoteInfo, tableDefVersion,
			rejectBoundWithoutVersion); info != nil {
			return info
		}
	}

	// Another producer may have published while subscription or remote lookup
	// was in progress. Preserve this recheck for both synchronous and non-blocking
	// callers. For a synchronous caller, an existing nil sentinel still admits a
	// background retry, as before.
	gs.mu.Lock()
	info, _, _ = gs.statsInfoForTableVersionLocked(
		key, tableDefVersion, rejectBoundWithoutVersion)
	gs.mu.Unlock()
	if info != nil {
		return info
	}
	if !sync {
		return nil
	}

	// Capture the producer generation only while the exact subscription
	// observed above still owns cleanup. A replacement subscription must not
	// silently retarget this read to a new table lifetime.
	generation, generationOwned :=
		gs.currentOrCreateExactSubscribedUpdateRecord(key, subscribedEnt)
	if !generationOwned {
		return nil
	}

	// A forced enqueue is the ownership transfer to a producer. If admission
	// is canceled, no waiter may depend on work that was never accepted.
	if !gs.enqueueStatsUpdateForRecord(wrapkey, true, generation) {
		return nil
	}
	return gs.waitForStatsUpdate(
		ctx, key, generation, tableDefVersion, rejectBoundWithoutVersion)
}

// waitForStatsUpdate waits on durable state, not on a Broadcast edge. The
// cache predicate and exact queued/running producer predicate are both checked
// while gs.mu is held; RemoveTid uses the same gs.mu -> updatingMu order.
// Cleanup or producer exhaustion therefore either changes the predicate before
// this check, or broadcasts after cond.Wait has atomically registered the
// waiter and released gs.mu.
func (gs *GlobalStats) waitForStatsUpdate(
	ctx context.Context,
	key pb.StatsInfoKey,
	generation *updateRecord,
	tableDefVersion *uint32,
	rejectBoundWithoutVersion bool,
) *pb.StatsInfo {
	stopWake := context.AfterFunc(ctx, gs.notifyStatsWaiters)
	defer stopWake()

	gs.mu.Lock()
	defer gs.mu.Unlock()
	for {
		if info, complete, _ := gs.statsInfoForTableVersionLocked(
			key, tableDefVersion, rejectBoundWithoutVersion); complete {
			return info
		}
		if ctx.Err() != nil {
			return nil
		}
		if gs.ctx != nil && gs.ctx.Err() != nil {
			return nil
		}
		if !gs.statsUpdateProducerActive(key, generation) {
			return nil
		}
		if gs.beforeStatsWait != nil {
			gs.beforeStatsWait(key, generation)
		}
		gs.mu.cond.Wait()
	}
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

				case job := <-gs.updateC:
					// after dequeue from the chan, remove the table ID from the queue watcher.
					gs.queueWatcher.del(job.wrapKey.Key.TableID)

					v2.StatsTriggerConsumeCounter.Add(1)
					gs.coordinateStatsUpdateJob(job)
				}
			}
		}()
	}
}

func (gs *GlobalStats) enqueueStatsUpdateForRecord(
	key pb.StatsInfoKeyWithContext,
	force bool,
	expectedRecord *updateRecord,
) bool {
	if expectedRecord == nil {
		return false
	}
	defer func() {
		v2.StatsTriggerQueueSizeGauge.Set(float64(len(gs.updateC)))
	}()
	gs.registerStatsUpdateJob(expectedRecord)
	job := statsUpdateJob{
		wrapKey:        key,
		expectedRecord: expectedRecord,
		registered:     true,
	}
	if force {
		select {
		case gs.updateC <- job:
			gs.queueWatcher.add(key.Key.TableID)
			v2.StatsTriggerForcedCounter.Add(1)
			return true
		case <-key.Ctx.Done():
			if gs.unregisterStatsUpdateJob(key.Key, expectedRecord) {
				gs.notifyStatsWaiters()
			}
			return false
		case <-gs.lifecycleDone():
			if gs.unregisterStatsUpdateJob(key.Key, expectedRecord) {
				gs.notifyStatsWaiters()
			}
			return false
		}
	}

	select {
	case gs.updateC <- job:
		gs.queueWatcher.add(key.Key.TableID)
		v2.StatsTriggerUnforcedCounter.Add(1)
		return true
	default:
		if gs.unregisterStatsUpdateJob(key.Key, expectedRecord) {
			gs.notifyStatsWaiters()
		}
		return false
	}
}

func (gs *GlobalStats) registerStatsUpdateJob(generation *updateRecord) {
	gs.updatingMu.Lock()
	generation.queued++
	gs.updatingMu.Unlock()
}

// unregisterStatsUpdateJob rolls back a job that never reached worker
// admission. It returns true only when the current generation has no remaining
// queued or running producer and waiters must re-evaluate their predicate.
func (gs *GlobalStats) unregisterStatsUpdateJob(
	key pb.StatsInfoKey,
	generation *updateRecord,
) bool {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	if generation.queued > 0 {
		generation.queued--
	}
	current, ok := gs.updatingMu.updating[key]
	return ok && current == generation && generation.queued == 0 && !generation.inProgress
}

func (gs *GlobalStats) notifyStatsWaiters() {
	gs.mu.Lock()
	if gs.mu.cond != nil {
		gs.mu.cond.Broadcast()
	}
	gs.mu.Unlock()
}

func (gs *GlobalStats) lifecycleDone() <-chan struct{} {
	if gs.ctx == nil {
		return nil
	}
	return gs.ctx.Done()
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
		if cmd := tail.Commands[i]; cmd.EntryType == api.Entry_DataObject || logtailreplay.IsMetaEntry(tail.Commands[i].TableName) {
			if tail.Commands[i].Bat != nil && len(tail.Commands[i].Bat.Vecs) > 0 {
				metaChanges += int(tail.Commands[i].Bat.Vecs[0].Len)
			}
		}
	}

	if len(tail.CkpLocation) > 0 || metaChanges > 0 {
		record, ok := gs.shouldEnqueueExistingStatsUpdateGeneration(
			key, metaChanges, len(tail.CkpLocation) > 0,
		)
		if ok {
			gs.enqueueStatsUpdateForRecord(pb.StatsInfoKeyWithContext{
				Ctx: ctx,
				Key: key,
			}, false, record)
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
	_, ok := gs.shouldEnqueueUpdateGeneration(key, metaChanges, hasCheckpoint)
	return ok
}

func (gs *GlobalStats) shouldEnqueueUpdateGeneration(
	key pb.StatsInfoKey,
	metaChanges int,
	hasCheckpoint bool,
) (*updateRecord, bool) {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	return gs.shouldEnqueueUpdateGenerationLocked(key, metaChanges, hasCheckpoint)
}

// shouldEnqueueExistingStatsUpdateGeneration links the logtail producer to the
// same table-lifetime boundary as RemoveTid. The cache-existence check and
// scheduling-record capture are atomic in the established gs.mu -> updatingMu
// lock order, so cleanup cannot fall between them.
func (gs *GlobalStats) shouldEnqueueExistingStatsUpdateGeneration(
	key pb.StatsInfoKey,
	metaChanges int,
	hasCheckpoint bool,
) (*updateRecord, bool) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if _, ok := gs.mu.statsInfoMap[key]; !ok {
		return nil, false
	}
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	return gs.shouldEnqueueUpdateGenerationLocked(key, metaChanges, hasCheckpoint)
}

func (gs *GlobalStats) shouldEnqueueUpdateGenerationLocked(
	key pb.StatsInfoKey,
	metaChanges int,
	hasCheckpoint bool,
) (*updateRecord, bool) {
	rec, ok := gs.updatingMu.updating[key]
	if !ok {
		// First time: create record and enqueue
		rec = &updateRecord{
			pendingChanges: metaChanges,
		}
		gs.updatingMu.updating[key] = rec
		return rec, true
	}

	// Accumulate pending changes
	rec.pendingChanges += metaChanges

	// Small table: enqueue on any change
	if rec.baseObjectCount < LargeTableThreshold {
		return rec, metaChanges > 0 || hasCheckpoint
	}

	// Large table: check two conditions (enqueue if either is true)
	// Condition 1: Change rate >= 5%
	if rec.baseObjectCount > 0 {
		changeRate := float64(rec.pendingChanges) / float64(rec.baseObjectCount)
		if changeRate >= LargeTableChangeRateThreshold {
			return rec, true
		}
	}

	// Condition 2: Time since last update > 30min
	if time.Since(rec.lastUpdate) > LargeTableMaxUpdateInterval {
		return rec, true
	}

	return rec, false
}

func (gs *GlobalStats) startAutomaticUpdate(
	key pb.StatsInfoKey,
	expectedRecord *updateRecord,
) (*updateRecord, bool) {
	if expectedRecord == nil {
		return nil, false
	}
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	return gs.startAutomaticUpdateLocked(key, expectedRecord)
}

func (gs *GlobalStats) startAutomaticUpdateJob(
	job statsUpdateJob,
) (*updateRecord, bool, bool) {
	if job.expectedRecord == nil {
		return nil, false, false
	}
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	if job.registered && job.expectedRecord.queued > 0 {
		job.expectedRecord.queued--
	}
	generation, started :=
		gs.startAutomaticUpdateLocked(job.wrapKey.Key, job.expectedRecord)
	if started {
		return generation, true, false
	}
	current, ok := gs.updatingMu.updating[job.wrapKey.Key]
	noProducer := ok && current == job.expectedRecord &&
		job.expectedRecord.queued == 0 && !job.expectedRecord.inProgress
	return nil, false, noProducer
}

func (gs *GlobalStats) startAutomaticUpdateLocked(
	key pb.StatsInfoKey,
	expectedRecord *updateRecord,
) (*updateRecord, bool) {
	rec, ok := gs.updatingMu.updating[key]
	if !ok || rec != expectedRecord {
		return nil, false
	}
	if rec.inProgress {
		return nil, false
	}
	if time.Since(rec.lastUpdate) > MinUpdateInterval {
		rec.inProgress = true
		return rec, true
	}
	return nil, false
}

func (gs *GlobalStats) statsUpdateGenerationActive(key pb.StatsInfoKey, generation *updateRecord) bool {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	return ok && rec == generation
}

func (gs *GlobalStats) statsUpdateProducerActive(key pb.StatsInfoKey, generation *updateRecord) bool {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	return ok && rec == generation && (rec.queued > 0 || rec.inProgress)
}

// markExplicitUpdateComplete advances the refresh baseline without stealing
// the in-progress bit from an automatic refresh that was admitted before the
// explicit refresh acquired the shared table stripe.
func (gs *GlobalStats) markExplicitUpdateComplete(
	key pb.StatsInfoKey,
	generation *updateRecord,
	actualObjectCount int64,
	samplingRatio float64,
) {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok || rec != generation {
		return
	}
	rec.lastUpdate = time.Now()
	rec.baseObjectCount = actualObjectCount
	rec.pendingChanges = 0
	rec.samplingRatio = samplingRatio
}

// markAutomaticUpdateComplete closes a generation opened by
// shouldExecuteUpdate. Unlike the explicit RefreshWithMode completion path, it
// must not recreate scheduling metadata that table-lifetime cleanup removed
// while an old worker was still unwinding.
func (gs *GlobalStats) markAutomaticUpdateComplete(
	key pb.StatsInfoKey,
	generation *updateRecord,
	updated bool,
	actualObjectCount int64,
	samplingRatio float64,
) {
	gs.updatingMu.Lock()
	defer gs.updatingMu.Unlock()
	rec, ok := gs.updatingMu.updating[key]
	if !ok || rec != generation {
		return
	}
	completeUpdateRecord(rec, updated, actualObjectCount, samplingRatio)
}

func completeUpdateRecord(rec *updateRecord, updated bool, actualObjectCount int64, samplingRatio float64) {
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

// completeAutomaticStatsCacheUpdate is the only automatic-refresh transition
// for statsInfoMap. A successful generation replaces the published value. A
// failed generation never destroys the last successful value; it installs a
// nil sentinel only when no generation has completed before, so synchronous
// first-read waiters can terminate without treating failure as publication.
func (gs *GlobalStats) completeAutomaticStatsCacheUpdate(
	key pb.StatsInfoKey,
	generation *updateRecord,
	stats *pb.StatsInfo,
	updated bool,
) bool {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	// GlobalStats shutdown is an authoritative failed-publication predicate.
	// In particular, do not install a first-generation nil sentinel here: the
	// lifecycle watcher already wakes waiters, and shutdown must leave the cache
	// and its scheduling baseline unchanged.
	if gs.ctx != nil && context.Cause(gs.ctx) != nil {
		if gs.mu.cond != nil {
			gs.mu.cond.Broadcast()
		}
		return false
	}
	// The update record is also the automatic generation's table-lifetime
	// token. RemoveTid deletes it under the same gs.mu -> updatingMu order; an
	// old worker that completes afterward must not resurrect either cache.
	if !gs.statsUpdateGenerationActive(key, generation) {
		if gs.mu.cond != nil {
			gs.mu.cond.Broadcast()
		}
		return false
	}
	if updated {
		gs.mu.statsInfoMap[key] = stats
		delete(gs.mu.tableDefVersions, key)
		gs.broadcastStats(key)
	} else if _, ok := gs.mu.statsInfoMap[key]; !ok {
		gs.mu.statsInfoMap[key] = nil
	}
	if gs.mu.cond != nil {
		gs.mu.cond.Broadcast()
	}
	return updated
}

func (gs *GlobalStats) coordinateStatsUpdateJob(job statsUpdateJob) {
	wrapKey := job.wrapKey
	statser := statistic.StatsInfoFromContext(wrapKey.Ctx)
	crs := new(perfcounter.CounterSet)
	generation, ok, noProducer := gs.startAutomaticUpdateJob(job)
	if !ok {
		if noProducer {
			gs.notifyStatsWaiters()
		}
		return
	}
	if gs.afterAutomaticUpdateStarted != nil {
		gs.afterAutomaticUpdateStarted(wrapKey.Key, generation)
	}

	// updated is used to mark that the stats info is updated.
	var updated bool
	var actualObjectCount int64
	var samplingRatio float64
	var stats *pb.StatsInfo
	release, err := gs.acquireStatsRefresh(wrapKey.Ctx, wrapKey.Key)
	if err != nil {
		// Worker admission opened this generation before refresh admission. Close it
		// even when cancellation prevents this worker from acquiring the stripe.
		gs.markAutomaticUpdateComplete(wrapKey.Key, generation, false, 0, 0)
		gs.notifyStatsWaiters()
		return
	}
	if !gs.statsUpdateGenerationActive(wrapKey.Key, generation) {
		// Table cleanup removed this queued generation while it waited for
		// admission. Avoid re-subscribing and doing object work for stale state.
		release()
		return
	}
	defer func() {
		gs.completeAutomaticStatsRefresh(
			wrapKey.Key, generation, stats, updated,
			actualObjectCount, samplingRatio, release)
	}()
	refreshCtx, stopRefresh, err := gs.newStatsRefreshContext(wrapKey.Ctx)
	if err != nil {
		return
	}
	defer stopRefresh()

	// Get the latest partition state of the table.
	//Notice that for snapshot read, subscribing the table maybe failed since the invalid table id,
	//We should handle this case in next PR if needed.
	ps, err := gs.engine.pClient.toSubscribeTable(
		refreshCtx,
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
		return
	}
	stats = plan2.NewStatsInfo()

	newCtx := perfcounter.AttachS3RequestKey(refreshCtx, crs)
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

}

// completeAutomaticStatsRefresh is the sole terminal owner for an admitted
// automatic refresh. Cache publication decides whether the result committed;
// only that committed result may advance scheduling metadata, and both happen
// before the table-scoped admission token is released.
func (gs *GlobalStats) completeAutomaticStatsRefresh(
	key pb.StatsInfoKey,
	generation *updateRecord,
	stats *pb.StatsInfo,
	calculated bool,
	actualObjectCount int64,
	samplingRatio float64,
	release func(),
) {
	committed := gs.completeAutomaticStatsCacheUpdate(
		key, generation, stats, calculated)
	gs.markAutomaticUpdateComplete(
		key, generation, committed, actualObjectCount, samplingRatio)
	release()
}

// RefreshWithMode triggers a stats refresh with the specified sampling mode
func (gs *GlobalStats) RefreshWithMode(ctx context.Context, key pb.StatsInfoKey, samplingMode string) error {
	_, err := gs.refreshStatsWithMode(ctx, key, samplingMode, engine.StatsRefreshOptions{})
	return err
}

// refreshStatsWithMode returns the exact statistics object published while
// same-table refresh admission is still held. Callers that define a synchronous
// publication boundary must use this result instead of re-reading the map after
// admission has been released.
func (gs *GlobalStats) refreshStatsWithMode(
	ctx context.Context,
	key pb.StatsInfoKey,
	samplingMode string,
	options engine.StatsRefreshOptions,
) (*pb.StatsInfo, error) {
	release, err := gs.acquireStatsRefresh(ctx, key)
	if err != nil {
		return nil, err
	}
	defer release()
	refreshCtx, stopRefresh, err := gs.newStatsRefreshContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stopRefresh()

	// Get partition state
	ps, err := gs.engine.pClient.toSubscribeTable(
		refreshCtx,
		uint64(key.AccId),
		key.TableID,
		key.TableName,
		key.DatabaseID,
		key.DbName)
	if err != nil {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInternalErrorNoCtxf("failed to subscribe table: %v", err)
	}

	// Get table definition
	table := gs.engine.GetLatestCatalogCache().GetTableById(key.AccId, key.DatabaseID, key.TableID)
	if table == nil || table.TableDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("table not found")
	}

	// The subscription owns eventual RemoveTid cleanup. Capture the refresh
	// generation only after subscription and catalog resolution succeed, and
	// only while that exact subscription lifetime is still current.
	generation, ok := gs.currentOrCreateSubscribedUpdateRecord(key)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf(
			"table statistics refresh crossed subscription boundary for table %d", key.TableID)
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
	samplingRatio, err := CollectAndCalculateStats(refreshCtx, req, gs.concurrentExecutor)
	if err != nil {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return nil, cause
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, moerr.NewInternalErrorNoCtxf("failed to update stats: %v", err)
	}
	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		return nil, cause
	}
	// Validate the observation against the schema snapshot that owned the
	// collection. Publication below validates that same version again while
	// holding the catalog change lock through the cache swap.
	if err := applyStatsRefreshOptions(stats, table.TableDef, options); err != nil {
		return nil, err
	}

	published := false
	if options.TableRowCount != nil || len(options.ColumnNDVs) > 0 {
		published, err = gs.publishStatsForGenerationAtTableVersion(
			ctx, key, generation, stats, *options.TableDefVersion)
	} else {
		published, err = gs.publishStatsForGeneration(ctx, key, generation, stats)
	}
	if err != nil {
		return nil, err
	}
	if !published {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInternalErrorNoCtxf(
			"table statistics refresh crossed cleanup boundary for table %d", key.TableID)
	}
	// Record the baseline only if this exact table lifetime is still current.
	// Preserve a concurrently admitted automatic refresh's in-progress bit.
	gs.markExplicitUpdateComplete(
		key, generation, stats.AccurateObjectNumber, samplingRatio)

	return stats, nil
}

// publishAnalyzedStats publishes one coherent manual-collection generation in
// one cache transition. Fields absent from this collection stay absent so a
// planner can use its ordinary fallback instead of mixing statistics produced
// from different table populations.
func (gs *GlobalStats) publishAnalyzedStats(
	ctx context.Context,
	key pb.StatsInfoKey,
	tableDefVersion uint32,
	collected *pb.StatsInfo,
) (*pb.StatsInfo, error) {
	if collected == nil {
		return nil, moerr.NewInternalErrorNoCtx("cannot publish nil analyzed statistics")
	}
	release, err := gs.acquireStatsRefresh(ctx, key)
	if err != nil {
		return nil, err
	}
	defer release()
	refreshCtx, stopRefresh, err := gs.newStatsRefreshContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stopRefresh()

	if _, err = gs.engine.pClient.toSubscribeTable(
		refreshCtx,
		uint64(key.AccId),
		key.TableID,
		key.TableName,
		key.DatabaseID,
		key.DbName,
	); err != nil {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInternalErrorNoCtxf("failed to subscribe table: %v", err)
	}
	generation, ok := gs.currentOrCreateSubscribedUpdateRecord(key)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf(
			"manual statistics publication crossed subscription boundary for table %d", key.TableID)
	}

	published := newAnalyzedStatsGeneration(collected)

	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		return nil, cause
	}
	publishedOK, err := gs.publishStatsForGenerationAtTableVersion(
		ctx, key, generation, published, tableDefVersion)
	if err != nil {
		return nil, err
	}
	if !publishedOK {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInternalErrorNoCtxf(
			"manual statistics publication crossed cleanup boundary for table %d", key.TableID)
	}
	gs.markExplicitUpdateComplete(
		key, generation, published.AccurateObjectNumber, gs.GetSamplingRatio(key))
	return published, nil
}

func newAnalyzedStatsGeneration(collected *pb.StatsInfo) *pb.StatsInfo {
	if collected == nil {
		return nil
	}
	return proto.Clone(collected).(*pb.StatsInfo)
}

func applyStatsRefreshOptions(
	stats *pb.StatsInfo,
	tableDef *plan2.TableDef,
	options engine.StatsRefreshOptions,
) error {
	if options.TableRowCount == nil && len(options.ColumnNDVs) == 0 {
		return nil
	}
	if stats == nil || tableDef == nil {
		return moerr.NewInternalErrorNoCtx("cannot apply statistics refresh options without table statistics")
	}
	if options.TableDefVersion == nil {
		return moerr.NewInternalErrorNoCtx(
			"cannot apply a table-wide statistics observation without its schema version")
	}
	if *options.TableDefVersion != tableDef.Version {
		return moerr.NewInternalErrorNoCtxf(
			"cannot apply statistics observation from table schema version %d to current version %d for table %q",
			*options.TableDefVersion, tableDef.Version, tableDef.Name)
	}
	knownColumns := make(map[string]struct{}, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col == nil {
			return moerr.NewInternalErrorNoCtxf(
				"cannot apply statistics observation to table %q with an invalid column definition", tableDef.Name)
		}
		knownColumns[col.Name] = struct{}{}
	}
	rowCount := stats.TableCnt
	if options.TableRowCount != nil {
		rowCount = *options.TableRowCount
	}
	if math.IsNaN(rowCount) || math.IsInf(rowCount, 0) || rowCount < 0 ||
		math.Trunc(rowCount) != rowCount || rowCount >= math.Exp2(64) {
		return moerr.NewInternalErrorNoCtxf(
			"invalid row count %v for table %q", rowCount, tableDef.Name)
	}
	for column, ndv := range options.ColumnNDVs {
		if _, ok := knownColumns[column]; !ok {
			return moerr.NewInternalErrorNoCtxf(
				"cannot apply NDV for unknown column %q in table %q", column, tableDef.Name)
		}
		if math.IsNaN(ndv) || math.IsInf(ndv, 0) || ndv < 0 {
			return moerr.NewInternalErrorNoCtxf(
				"invalid NDV %v for column %q in table %q", ndv, column, tableDef.Name)
		}
	}
	if stats.NdvMap == nil {
		stats.NdvMap = make(map[string]float64, len(options.ColumnNDVs)+1)
	}
	stats.TableName = tableDef.Name
	stats.TableCnt = rowCount
	if _, hasFakePrimaryKey := knownColumns[catalog.FakePrimaryKeyColName]; options.TableRowCount != nil && hasFakePrimaryKey {
		stats.NdvMap[catalog.FakePrimaryKeyColName] = rowCount
	}
	for column, ndv := range options.ColumnNDVs {
		stats.NdvMap[column] = min(ndv, rowCount)
	}
	if options.TableRowCount != nil {
		// A partial ANALYZE intentionally retains unselected estimates, but no
		// distinct or NULL count may exceed the now-exact table cardinality.
		for column, ndv := range stats.NdvMap {
			if ndv > rowCount {
				stats.NdvMap[column] = rowCount
			}
		}
		for column, nullCount := range stats.NullCntMap {
			if float64(nullCount) > rowCount {
				stats.NullCntMap[column] = uint64(rowCount)
			}
		}
	}
	return nil
}

// publishStatsForGeneration replaces the cache only while the exact table
// lifetime captured by the refresh remains current and neither the request nor
// GlobalStats owner has been canceled. The gs.mu -> updatingMu order matches
// RemoveTid, making validation and publication atomic with respect to cleanup.
// A late explicit refresh therefore cannot resurrect an unsubscribed table,
// publish into a replacement generation, or commit after observing
// cancellation.
func (gs *GlobalStats) publishStatsForGeneration(
	ctx context.Context,
	key pb.StatsInfoKey,
	generation *updateRecord,
	stats *pb.StatsInfo,
) (bool, error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	published, err := gs.publishStatsForGenerationLocked(ctx, key, generation, stats)
	if published {
		gs.broadcastStats(key)
	}
	if gs.mu.cond != nil {
		gs.mu.cond.Broadcast()
	}
	return published, err
}

// publishStatsForGenerationLocked performs the final cancellation/generation
// checks and bounded cache swap. The caller owns gs.mu, so this check-and-swap
// is the publication linearization point. Cancellation observed here wins and
// returns its original cause; cancellation that becomes visible after this
// point races after a completed publication. The caller is responsible for
// wakeup and gossip after any outer catalog critical section has ended.
func (gs *GlobalStats) publishStatsForGenerationLocked(
	ctx context.Context,
	key pb.StatsInfoKey,
	generation *updateRecord,
	stats *pb.StatsInfo,
) (bool, error) {
	if generation == nil || stats == nil {
		return false, nil
	}
	generationActive := gs.statsUpdateGenerationActive(key, generation)
	// Generation validation is the last operation that can wait on another
	// application lock. Check cancellation after it so a request canceled while
	// waiting for updatingMu cannot cross the cache-swap commit point.
	if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
		return false, cause
	}
	if !generationActive {
		return false, nil
	}
	gs.mu.statsInfoMap[key] = stats
	delete(gs.mu.tableDefVersions, key)
	return true, nil
}

// publishStatsForGenerationAtTableVersion makes schema validation and stats
// publication atomic with catalog table changes. If publication wins the
// catalog lock it linearizes before ALTER; if ALTER wins, the old observation
// is rejected and can never be published into the new schema. Request and
// owner cancellation use the same final check-and-swap linearization point.
func (gs *GlobalStats) publishStatsForGenerationAtTableVersion(
	ctx context.Context,
	key pb.StatsInfoKey,
	generation *updateRecord,
	stats *pb.StatsInfo,
	expectedVersion uint32,
) (bool, error) {
	if gs.engine == nil {
		return false, moerr.NewInternalErrorNoCtx("cannot validate table schema without an engine")
	}
	cc := gs.engine.GetLatestCatalogCache()
	if cc == nil {
		return false, moerr.NewInternalErrorNoCtx("cannot validate table schema without a catalog cache")
	}

	published := false
	var publicationErr error
	statsLocked := false
	actualVersion, found, matched := cc.WithTableVersion(
		key.AccId, key.DatabaseID, key.TableID, expectedVersion,
		func() {
			if gs.beforeVersionedStatsPublish != nil {
				gs.beforeVersionedStatsPublish(key, expectedVersion)
			}
			gs.mu.Lock()
			statsLocked = true
			published, publicationErr = gs.publishStatsForGenerationLocked(
				ctx, key, generation, stats)
			if published {
				if gs.mu.tableDefVersions == nil {
					gs.mu.tableDefVersions = make(map[pb.StatsInfoKey]uint32)
				}
				gs.mu.tableDefVersions[key] = expectedVersion
			}
		},
	)
	if statsLocked {
		// The cache swap above was protected by the catalog lock. Release that
		// lock before gossip, while retaining gs.mu so cleanup cannot remove
		// the entry between publication and notification.
		if published {
			gs.broadcastStats(key)
		}
		if gs.mu.cond != nil {
			gs.mu.cond.Broadcast()
		}
		gs.mu.Unlock()
	}
	if publicationErr != nil {
		return false, publicationErr
	}
	// WithTableVersion does not invoke the callback when the table disappeared
	// or its schema already changed. Preserve cancellation as the public result
	// when it became durable while this request was waiting for the catalog
	// fence; a successful publication must not be reclassified after its commit
	// point.
	if !published {
		if cause := gs.statsRefreshCancellationCause(ctx); cause != nil {
			return false, cause
		}
	}
	if !found {
		return false, moerr.NewInternalErrorNoCtxf(
			"table %d no longer exists while publishing statistics", key.TableID)
	}
	if !matched {
		return false, moerr.NewInternalErrorNoCtxf(
			"cannot publish statistics observation from table schema version %d to current version %d for table %d",
			expectedVersion, actualVersion, key.TableID)
	}
	return published, nil
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
	if context.Cause(ctx) != nil {
		return false, 0
	}
	v2.StatsUpdateDurationHistogram.Observe(time.Since(start).Seconds())
	v2.StatsUpdateBlockCounter.Add(float64(stats.BlockNumber))
	return true, samplingRatio
}

func getMinMaxValueByFloat64(typ types.Type, buf []byte) float64 {
	value, ok := tryGetMinMaxValueByFloat64(typ, buf)
	if !ok {
		panic("unsupported type")
	}
	return value
}

func tryGetMinMaxValueByFloat64(typ types.Type, buf []byte) (float64, bool) {
	switch typ.Oid {
	case types.T_bit:
		return float64(types.DecodeUint64(buf)), true
	case types.T_int8:
		return float64(types.DecodeInt8(buf)), true
	case types.T_int16:
		return float64(types.DecodeInt16(buf)), true
	case types.T_int32:
		return float64(types.DecodeInt32(buf)), true
	case types.T_int64:
		return float64(types.DecodeInt64(buf)), true
	case types.T_uint8:
		return float64(types.DecodeUint8(buf)), true
	case types.T_uint16:
		return float64(types.DecodeUint16(buf)), true
	case types.T_uint32:
		return float64(types.DecodeUint32(buf)), true
	case types.T_uint64:
		return float64(types.DecodeUint64(buf)), true
	case types.T_float32:
		return float64(types.DecodeFloat32(buf)), true
	case types.T_float64:
		return types.DecodeFloat64(buf), true
	case types.T_date:
		return float64(types.DecodeDate(buf)), true
	case types.T_time:
		return float64(types.DecodeTime(buf)), true
	case types.T_timestamp:
		return float64(types.DecodeTimestamp(buf)), true
	case types.T_datetime:
		return float64(types.DecodeDatetime(buf)), true
	case types.T_year:
		return float64(types.DecodeMoYear(buf)), true
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		return float64(plan2.ByteSliceToUint64(buf)), true
	case types.T_decimal64:
		// Fix: Use Decimal64ToFloat64 to handle negative values correctly
		dec := types.DecodeDecimal64(buf)
		return types.Decimal64ToFloat64(dec, typ.Scale), true
	case types.T_decimal128:
		// Fix: Use Decimal128ToFloat64 to handle negative values correctly
		dec := types.DecodeDecimal128(buf)
		return types.Decimal128ToFloat64(dec, typ.Scale), true
	default:
		return 0, false
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
	// Candidate2: 10% of objects
	targetCount = max(targetCount, int(float64(approxObjectNum)*0.1))
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

	onObjFn := func(objCtx context.Context, obj objectio.ObjectEntry) error {
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
		// When sampling is enabled (approxObjectNum > 100), we randomly skip objects to reduce IO.
		// We must ensure at least one object is chosen so that Phase 2 runs and ColumnNDVs/ShuffleRanges
		// get populated. Otherwise the table would end up with all-zero NDV, empty ShuffleRangeMap, and
		// point queries would use a high block_sel (e.g. 0.5) leading to ~611 blocks instead of 1.
		// So: if we have not yet sampled any object for this table, force this object into Phase 2.
		// Lock is held only to read sampledObjectCount; we do not hold across IO or shouldSampleObject.
		if isSampling {
			updateMu.Lock()
			forceOne := sampledObjectCount == 0
			updateMu.Unlock()
			if !forceOne && !shouldSampleObject(objName, samplingThreshold) {
				return nil // Skip non-sampled objects, no IO
			}
		}

		// Sampled object: read ObjectMeta (requires IO)
		location := obj.Location()
		objMeta, err := objectio.FastLoadObjectMeta(objCtx, &location, false, fs)
		if err != nil {
			return err
		}

		updateMu.Lock()
		defer updateMu.Unlock()

		meta := objMeta.MustDataMeta()
		sampledObjectCount++
		objectRowCount := meta.BlockHeader().Rows()
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
		ctx,
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
	for _, r := range info.ShuffleRanges {
		if r != nil {
			r.SampleRatio = actualSamplingRatio
		}
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
	// The zero-object fast path below does not enter ForeachVisibleObjects. Keep
	// executor shutdown as a failed refresh by checking its authoritative
	// lifecycle before any successful early return.
	if executor != nil {
		if lifecycle := executor.LifecycleContext(); lifecycle != nil {
			if cause := context.Cause(lifecycle); cause != nil {
				return 0, cause
			}
		}
	}
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
