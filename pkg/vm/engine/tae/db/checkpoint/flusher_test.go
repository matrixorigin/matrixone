// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/stretchr/testify/assert"
)

func Test_RestartFlusher(t *testing.T) {
	var cfg FlushCfg
	cfg.ForceFlushTimeout = time.Millisecond * 7
	cfg.ForceFlushCheckInterval = time.Millisecond * 9
	cfg.FlushInterval = time.Millisecond * 11
	cfg.CronPeriod = time.Millisecond * 2
	f := NewFlusher(
		nil, nil, nil, nil, false,
		WithFlusherInterval(cfg.FlushInterval),
		WithFlusherCronPeriod(cfg.CronPeriod),
		WithFlusherForceTimeout(cfg.ForceFlushTimeout),
		WithFlusherForceCheckInterval(cfg.ForceFlushCheckInterval),
	)
	f.Start()

	fCfg := f.GetCfg()
	assert.Equal(t, cfg, fCfg)
	assert.False(t, f.IsNoop())

	f.Stop()
	assert.True(t, f.IsNoop())

	ctx := context.Background()
	var ts types.TS

	assert.Equal(t, ErrFlusherStopped, f.FlushTable(ctx, 0, 0, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlush(ctx, ts))
	assert.Equal(t, ErrFlusherStopped, f.ForceFlushWithInterval(ctx, ts, time.Millisecond))
	f.ChangeForceCheckInterval(time.Millisecond)
	f.ChangeForceFlushTimeout(time.Millisecond)

	f.Restart(WithFlusherCfg(cfg))
	assert.False(t, f.IsNoop())
	fCfg = f.GetCfg()
	assert.Equal(t, cfg, fCfg)
}

func TestShouldScheduleStalledCheckpointFlush(t *testing.T) {
	threshold := 5 * time.Minute

	assert.False(t, shouldScheduleStalledCheckpointFlush(nil, threshold))

	intent := NewCheckpointEntry("", types.TS{}, types.BuildTS(100, 0), ET_Incremental)
	intent.bornTime = time.Now().Add(-threshold + time.Second)
	assert.False(t, shouldScheduleStalledCheckpointFlush(intent, threshold))

	intent.bornTime = time.Now().Add(-threshold - time.Second)
	assert.True(t, shouldScheduleStalledCheckpointFlush(intent, threshold))

	intent.SetFlushChecked()
	assert.False(t, shouldScheduleStalledCheckpointFlush(intent, threshold))

	intent = NewCheckpointEntry("", types.TS{}, types.BuildTS(100, 0), ET_Incremental)
	intent.bornTime = time.Now().Add(-threshold - time.Second)
	intent.SetState(ST_Running)
	assert.False(t, shouldScheduleStalledCheckpointFlush(intent, threshold))
}

func TestStalledCheckpointFlushAgeMatchesIntentOldAge(t *testing.T) {
	assert.Equal(t, checkpointIntentOldAge, stalledCheckpointFlushAge)
}

func TestMakeStalledCheckpointFlushEntry(t *testing.T) {
	threshold := 5 * time.Minute
	start := types.BuildTS(100, 0)
	end := types.BuildTS(200, 0)
	intent := NewCheckpointEntry("", start, end, ET_Incremental)
	intent.bornTime = time.Now().Add(-threshold - time.Second)

	sourcer := &stalledCheckpointFlushCollector{}
	entry := makeStalledCheckpointFlushEntry(sourcer, intent, threshold)

	assert.NotNil(t, entry)
	assert.True(t, sourcer.scanCalled)
	assert.Equal(t, start, sourcer.scanFrom)
	assert.Equal(t, end, sourcer.scanTo)
	requestStart, requestEnd := entry.GetTimeRange()
	assert.Equal(t, start, requestStart)
	assert.Equal(t, end, requestEnd)
	assert.Equal(t, 1, entry.GetTree().TableCount())

	intent.bornTime = time.Now().Add(-threshold + time.Second)
	sourcer = &stalledCheckpointFlushCollector{}
	assert.Nil(t, makeStalledCheckpointFlushEntry(sourcer, intent, threshold))
	assert.False(t, sourcer.scanCalled)

	intent.bornTime = time.Now().Add(-threshold - time.Second)
	sourcer = &stalledCheckpointFlushCollector{empty: true}
	assert.Nil(t, makeStalledCheckpointFlushEntry(sourcer, intent, threshold))
	assert.True(t, sourcer.scanCalled)
}

func TestPickStalledCheckpointFlushEntryAlternatesWithNormal(t *testing.T) {
	intent := NewCheckpointEntry("", types.BuildTS(2, 0), types.BuildTS(5, 0), ET_Incremental)
	intent.bornTime = time.Now().Add(-stalledCheckpointFlushAge - time.Second)
	flusher := &flushImpl{}

	assert.True(t, flusher.pickStalledCheckpointFlushEntry(intent))
	assert.False(t, flusher.pickStalledCheckpointFlushEntry(intent))
	assert.True(t, flusher.pickStalledCheckpointFlushEntry(intent))
	assert.False(t, flusher.pickStalledCheckpointFlushEntry(intent))

	assert.False(t, flusher.pickStalledCheckpointFlushEntry(nil))
	assert.False(t, flusher.stalledCheckpointPickBounded)
	assert.True(t, flusher.pickStalledCheckpointFlushEntry(intent))
}

func TestEnqueueCronFlushEnqueuesNonEmptyEntries(t *testing.T) {
	queue := newFlushRequestQueue()
	flusher := &flushImpl{flushRequestQ: queue}
	first := newTestDirtyTreeEntry(types.BuildTS(1, 0), types.BuildTS(10, 0), 1)
	second := newTestDirtyTreeEntry(types.BuildTS(2, 0), types.BuildTS(5, 0), 2)

	flusher.enqueueCronFlush(first)
	flusher.enqueueCronFlush(logtail.NewEmptyDirtyTreeEntry())
	flusher.enqueueCronFlush(second)

	assert.Len(t, queue.items, 2)
	assert.Same(t, first, queue.items[0].(*FlushRequest).tree)
	assert.Same(t, second, queue.items[1].(*FlushRequest).tree)
	assert.Equal(t, flushRequestCron, queue.items[0].(*FlushRequest).mode)
	assert.Equal(t, flushRequestCron, queue.items[1].(*FlushRequest).mode)
}

func TestMergeFlushRequestsKeepsBoundedSeparateFromCron(t *testing.T) {
	cron1 := newTestDirtyTreeEntry(types.BuildTS(1, 0), types.BuildTS(10, 0), 1)
	cron2 := newTestDirtyTreeEntry(types.BuildTS(10, 0), types.BuildTS(20, 0), 2)
	bounded := newTestDirtyTreeEntry(types.BuildTS(2, 0), types.BuildTS(5, 0), 3)
	force := newTestDirtyTreeEntry(types.BuildTS(3, 0), types.BuildTS(8, 0), 4)

	fromCrons, fromForce, fromCheckpointBounded := mergeFlushRequests(
		&FlushRequest{mode: flushRequestCron, tree: cron1},
		&FlushRequest{mode: flushRequestCheckpointBounded, tree: bounded},
		&FlushRequest{mode: flushRequestCron, tree: cron2},
		&FlushRequest{mode: flushRequestForce, tree: force},
	)

	assert.Len(t, fromCheckpointBounded, 1)
	assert.Same(t, bounded, fromCheckpointBounded[0])
	cronStart, cronEnd := fromCrons.GetTimeRange()
	assert.Equal(t, types.BuildTS(1, 0), cronStart)
	assert.Equal(t, types.BuildTS(20, 0), cronEnd)
	assert.Equal(t, 2, fromCrons.GetTree().TableCount())
	forceStart, forceEnd := fromForce.GetTimeRange()
	assert.Equal(t, types.BuildTS(3, 0), forceStart)
	assert.Equal(t, types.BuildTS(8, 0), forceEnd)
	assert.Equal(t, 1, fromForce.GetTree().TableCount())
}

func TestCheckpointBoundedScheduleBypassesFlushReady(t *testing.T) {
	assert.False(t, flushScheduleCron.bypassFlushReady())
	assert.True(t, flushScheduleCheckpointBounded.bypassFlushReady())
	assert.True(t, flushScheduleForce.bypassFlushReady())
}

func TestTriggerJobFallsBackToNormalWhenBoundedEntryIsEmpty(t *testing.T) {
	intentStart := types.BuildTS(2, 0)
	intentEnd := types.BuildTS(5, 0)
	intent := NewCheckpointEntry("", intentStart, intentEnd, ET_Incremental)
	intent.bornTime = time.Now().Add(-stalledCheckpointFlushAge - time.Second)

	normal := newTestDirtyTreeEntry(types.BuildTS(1, 0), types.BuildTS(10, 0), 1)
	sourcer := &triggerJobFallbackCollector{normal: normal}
	scheduler := &triggerJobCheckpointScheduler{pending: intent}
	queue := newFlushRequestQueue()
	flusher := &flushImpl{
		sourcer:            sourcer,
		checkpointSchduler: scheduler,
		flushRequestQ:      queue,
	}

	flusher.triggerJob(context.Background())

	assert.True(t, sourcer.runCalled)
	assert.True(t, sourcer.scanCalled)
	assert.Equal(t, intentStart, sourcer.scanFrom)
	assert.Equal(t, intentEnd, sourcer.scanTo)
	assert.True(t, sourcer.getMergedCalled)
	assert.Len(t, queue.items, 1)
	assert.Same(t, normal, queue.items[0].(*FlushRequest).tree)
	assert.True(t, scheduler.scheduleCalled)
	_, normalEnd := normal.GetTimeRange()
	assert.Equal(t, normalEnd, scheduler.scheduledTS)
	assert.False(t, scheduler.force)
}

func TestTriggerJobEnqueuesBoundedFlushWhenStalledCheckpointHasDirtyEntry(t *testing.T) {
	intentStart := types.BuildTS(2, 0)
	intentEnd := types.BuildTS(5, 0)
	intent := NewCheckpointEntry("", intentStart, intentEnd, ET_Incremental)
	intent.bornTime = time.Now().Add(-stalledCheckpointFlushAge - time.Second)

	sourcer := &triggerJobBoundedCollector{}
	scheduler := &triggerJobCheckpointScheduler{pending: intent}
	queue := newFlushRequestQueue()
	flusher := &flushImpl{
		sourcer:            sourcer,
		checkpointSchduler: scheduler,
		flushRequestQ:      queue,
	}

	flusher.triggerJob(context.Background())

	assert.True(t, sourcer.runCalled)
	assert.True(t, sourcer.scanCalled)
	assert.Equal(t, intentStart, sourcer.scanFrom)
	assert.Equal(t, intentEnd, sourcer.scanTo)
	assert.False(t, sourcer.getMergedCalled)
	assert.Len(t, queue.items, 1)
	request := queue.items[0].(*FlushRequest)
	assert.Equal(t, flushRequestCheckpointBounded, request.mode)
	requestStart, requestEnd := request.tree.GetTimeRange()
	assert.Equal(t, intentStart, requestStart)
	assert.Equal(t, intentEnd, requestEnd)
	assert.True(t, scheduler.scheduleCalled)
	assert.Equal(t, intentEnd, scheduler.scheduledTS)
	assert.False(t, scheduler.force)
}

func newTestDirtyTreeEntry(start, end types.TS, tableID uint64) *logtail.DirtyTreeEntry {
	tree := model.NewTree()
	tree.AddTable(1, tableID)
	return logtail.NewDirtyTreeEntry(start, end, tree)
}

type stalledCheckpointFlushCollector struct {
	scanCalled bool
	scanFrom   types.TS
	scanTo     types.TS
	empty      bool
}

func (c *stalledCheckpointFlushCollector) String() string    { return "" }
func (c *stalledCheckpointFlushCollector) Run(time.Duration) {}
func (c *stalledCheckpointFlushCollector) ScanInRange(from, to types.TS) (*logtail.DirtyTreeEntry, int) {
	return c.ScanInRangePruned(from, to), 1
}
func (c *stalledCheckpointFlushCollector) ScanInRangePruned(from, to types.TS) *logtail.DirtyTreeEntry {
	c.scanCalled = true
	c.scanFrom = from
	c.scanTo = to
	tree := model.NewTree()
	if !c.empty {
		tree.AddTable(1, 2)
	}
	return logtail.NewDirtyTreeEntry(from, to, tree)
}
func (c *stalledCheckpointFlushCollector) GetAndRefreshMerged() *logtail.DirtyTreeEntry {
	return logtail.NewEmptyDirtyTreeEntry()
}
func (c *stalledCheckpointFlushCollector) Merge() *logtail.DirtyTreeEntry {
	return logtail.NewEmptyDirtyTreeEntry()
}
func (c *stalledCheckpointFlushCollector) GetMaxLSN(types.TS, types.TS) uint64 { return 0 }
func (c *stalledCheckpointFlushCollector) Init(types.TS)                       {}

type flushRequestQueue struct {
	items []any
}

func newFlushRequestQueue() *flushRequestQueue {
	return &flushRequestQueue{items: make([]any, 0)}
}

func (q *flushRequestQueue) Start() {}
func (q *flushRequestQueue) Stop()  {}
func (q *flushRequestQueue) Enqueue(item any) (any, error) {
	q.items = append(q.items, item)
	return nil, nil
}

type triggerJobFallbackCollector struct {
	normal          *logtail.DirtyTreeEntry
	runCalled       bool
	scanCalled      bool
	getMergedCalled bool
	scanFrom        types.TS
	scanTo          types.TS
}

func (c *triggerJobFallbackCollector) String() string { return "" }
func (c *triggerJobFallbackCollector) Run(time.Duration) {
	c.runCalled = true
}
func (c *triggerJobFallbackCollector) ScanInRange(from, to types.TS) (*logtail.DirtyTreeEntry, int) {
	return c.ScanInRangePruned(from, to), 1
}
func (c *triggerJobFallbackCollector) ScanInRangePruned(from, to types.TS) *logtail.DirtyTreeEntry {
	c.scanCalled = true
	c.scanFrom = from
	c.scanTo = to
	return logtail.NewDirtyTreeEntry(from, to, model.NewTree())
}
func (c *triggerJobFallbackCollector) GetAndRefreshMerged() *logtail.DirtyTreeEntry {
	c.getMergedCalled = true
	return c.normal
}
func (c *triggerJobFallbackCollector) Merge() *logtail.DirtyTreeEntry {
	return c.normal
}
func (c *triggerJobFallbackCollector) GetMaxLSN(types.TS, types.TS) uint64 { return 0 }
func (c *triggerJobFallbackCollector) Init(types.TS)                       {}

type triggerJobBoundedCollector struct {
	runCalled       bool
	scanCalled      bool
	getMergedCalled bool
	scanFrom        types.TS
	scanTo          types.TS
}

func (c *triggerJobBoundedCollector) String() string { return "" }
func (c *triggerJobBoundedCollector) Run(time.Duration) {
	c.runCalled = true
}
func (c *triggerJobBoundedCollector) ScanInRange(from, to types.TS) (*logtail.DirtyTreeEntry, int) {
	return c.ScanInRangePruned(from, to), 1
}
func (c *triggerJobBoundedCollector) ScanInRangePruned(from, to types.TS) *logtail.DirtyTreeEntry {
	c.scanCalled = true
	c.scanFrom = from
	c.scanTo = to
	tree := model.NewTree()
	tree.AddTable(1, 7)
	return logtail.NewDirtyTreeEntry(from, to, tree)
}
func (c *triggerJobBoundedCollector) GetAndRefreshMerged() *logtail.DirtyTreeEntry {
	c.getMergedCalled = true
	return newTestDirtyTreeEntry(types.BuildTS(1, 0), types.BuildTS(10, 0), 8)
}
func (c *triggerJobBoundedCollector) Merge() *logtail.DirtyTreeEntry {
	return logtail.NewEmptyDirtyTreeEntry()
}
func (c *triggerJobBoundedCollector) GetMaxLSN(types.TS, types.TS) uint64 { return 0 }
func (c *triggerJobBoundedCollector) Init(types.TS)                       {}

type triggerJobCheckpointScheduler struct {
	pending        *CheckpointEntry
	scheduleCalled bool
	scheduledTS    types.TS
	force          bool
}

func (s *triggerJobCheckpointScheduler) TryScheduleCheckpoint(ts types.TS, force bool) (Intent, error) {
	s.scheduleCalled = true
	s.scheduledTS = ts
	s.force = force
	return nil, nil
}
func (s *triggerJobCheckpointScheduler) String() string { return "" }
func (s *triggerJobCheckpointScheduler) GetAllIncrementalCheckpoints() []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetAllGlobalCheckpoints() []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetIncrementalCountAfterGlobal() int {
	return 0
}
func (s *triggerJobCheckpointScheduler) CollectCheckpointsInRange(
	context.Context,
	types.TS,
	types.TS,
) (string, types.TS, error) {
	return "", types.TS{}, nil
}
func (s *triggerJobCheckpointScheduler) ICKPSeekLT(types.TS, int) []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetLowWaterMark() types.TS {
	return types.TS{}
}
func (s *triggerJobCheckpointScheduler) MaxLSN() uint64 {
	return 0
}
func (s *triggerJobCheckpointScheduler) GetCatalog() *catalog.Catalog {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetCheckpointMetaFiles() map[string]struct{} {
	return nil
}
func (s *triggerJobCheckpointScheduler) ICKPRange(*types.TS, *types.TS, int) []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetCompacted() *CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetAllCheckpoints() []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetAllCheckpointsForBackup(*CheckpointEntry) []*CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) MaxGlobalCheckpoint() *CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) MaxIncrementalCheckpoint() *CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) PendingIncrementalCheckpoint() *CheckpointEntry {
	return s.pending
}
func (s *triggerJobCheckpointScheduler) MinIncrementalCheckpoint() *CheckpointEntry {
	return nil
}
func (s *triggerJobCheckpointScheduler) GetDirtyCollector() logtail.Collector {
	return nil
}
