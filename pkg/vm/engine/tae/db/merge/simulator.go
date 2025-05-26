// Copyright 2025 Matrix Origin
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

package merge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"

	"github.com/jonboulle/clockwork"
	"github.com/tidwall/btree"
)

// region: clock

type Clock interface {
	clockwork.Clock
	Until(t time.Time) time.Duration
}

type Ticker interface {
	clockwork.Ticker
}

type stdClock struct {
	clockwork.Clock
}

func NewStdClock() *stdClock {
	return &stdClock{
		Clock: clockwork.NewRealClock(),
	}
}

func (c stdClock) Until(t time.Time) time.Duration {
	return time.Until(t)
}

type fakeClock struct {
	clockwork.FakeClock
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		FakeClock: clockwork.NewFakeClock(),
	}
}

func (c *fakeClock) Until(t time.Time) time.Duration {
	return t.Sub(c.FakeClock.Now())
}

// endregion: Clock

// region: resource controller

type simRscController struct {
	sync.Mutex
	limit    atomic.Int64
	reserved int64
}

func newSimRscController(initLimit int64) *simRscController {
	c := &simRscController{
		limit:    atomic.Int64{},
		reserved: 0,
	}
	c.setMemLimit(initLimit)
	return c
}

// for testing
func (c *simRscController) setMemLimit(limit int64) {
	c.limit.Store(limit)
}

func (c *simRscController) refresh() {}

func (c *simRscController) printMemUsage() {}

func (c *simRscController) reserveResources(estMem int64) {
	c.reserved += estMem
}

func (c *simRscController) releaseResources(estMem int64) {
	c.reserved -= estMem
	if c.reserved < 0 {
		c.reserved = 0
		logutil.Warnf("simRscController: releaseResources: %d", estMem)
	}
}

func (c *simRscController) availableMem() int64 {
	avail := c.limit.Load() - c.reserved
	if avail < 0 {
		avail = 0
	}
	return avail
}

func (c *simRscController) resourceAvailable(estMem int64) bool {
	mem := c.availableMem()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	return estMem <= 2*mem/3
}

// endregion: resource controller

// region: executor

func iterSDAsStats(objs []SData) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, obj := range objs {
			yield(obj.stats)
		}
	}
}

func iterSTAsStats(objs []STombstone) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, obj := range objs {
			yield(obj.stats)
		}
	}
}

type SExecutor struct {
	clock    Clock
	scatalog *SCatalog

	taskId uint64

	logEnabled bool

	// stats
	dataMergedSize      int64
	tombstoneMergedSize int64
}

func NewSExecutor(c Clock, scatalog *SCatalog) *SExecutor {
	return &SExecutor{
		clock:    c,
		scatalog: scatalog,
	}
}

func (e *SExecutor) SetLogEnabled(enabled bool) {
	e.logEnabled = enabled
}

type objsInfo struct {
	totalOsize, totalCsize, totalRowCount int
	avgROsize, avgRCsize                  int
}

func sizeInfo(objs []*objectio.ObjectStats) (info objsInfo) {
	for _, stat := range objs {
		info.totalOsize += int(stat.OriginSize())
		info.totalCsize += int(stat.Size())
		info.totalRowCount += int(stat.Rows())
	}

	info.avgROsize = info.totalOsize / info.totalRowCount
	info.avgRCsize = info.totalCsize / info.totalRowCount
	return
}

func mergeDataLocked(
	stable *STable,
	task mergeTask,
	clock Clock,
) (newObjs []SData) {

	info := sizeInfo(task.objs)

	zm := index.NewZM(task.objs[0].SortKeyZoneMap().GetType(), 0)
	dels := 0
	for _, stat := range task.objs {
		zm.Update(stat.SortKeyZoneMap().GetMin())
		zm.Update(stat.SortKeyZoneMap().GetMax())
		for _, tombstone := range stable.tombstone {
			dels += tombstone.distro[stat.ObjectLocation().ObjectId()]
		}
	}

	leftRows := info.totalRowCount - dels
	objRows := common.DefaultMaxOsizeObjBytes/info.avgROsize + 100
	mergedSize := 0

	rowSplit := make([]int, 0)

	for leftRows > 0 {
		// try to merge a full 128 MB object
		mergedSize += objRows * info.avgROsize
		if info.totalOsize-mergedSize > common.DefaultMaxOsizeObjBytes {
			rowSplit = append(rowSplit, objRows)
			leftRows -= objRows
		} else {
			rowSplit = append(rowSplit, leftRows)
			leftRows = 0
		}
	}

	createdSegId := objectio.NewSegmentid()
	zmSplit := splitZM(zm, rowSplit)

	for i, zm := range zmSplit {
		newObj := SData{
			stats:      objectio.NewObjectStats(),
			createTime: types.BuildTS(clock.Now().UnixNano(), 0),
		}
		row := rowSplit[i]
		name := objectio.BuildObjectName(createdSegId, uint16(i))
		objectio.SetObjectStatsObjectName(newObj.stats, name)
		objectio.SetObjectStatsOriginSize(newObj.stats, uint32(row*info.avgROsize))
		objectio.SetObjectStatsSize(newObj.stats, uint32(row*info.avgRCsize))
		objectio.SetObjectStatsSortKeyZoneMap(newObj.stats, zm)
		objectio.SetObjectStatsRowCnt(newObj.stats, uint32(row))

		if task.level > 0 ||
			newObj.stats.OriginSize() > common.DefaultMinOsizeQualifiedBytes {
			if task.level < 7 {
				newObj.stats.SetLevel(int8(task.level + 1))
			} else {
				newObj.stats.SetLevel(7)
			}
		}

		newObjs = append(newObjs, newObj)
	}

	for _, obj := range task.objs {
		stable.DeleteDataLocked(obj)
	}

	for _, obj := range newObjs {
		stable.AddDataLocked(obj)
	}

	return
}

type oidCount struct {
	oid   objectio.ObjectId
	count int
}

func mergeTombstoneLocked(
	stable *STable,
	task mergeTask,
	clock Clock,
) (newTombstones []STombstone) {

	info := sizeInfo(task.objs)

	// target disto is tombstone waiting to be flushed
	targetDistro := btree.NewBTreeG(func(a, b oidCount) bool {
		return a.oid.Compare(&b.oid) < 0
	})

	// only keep the tombstone targeting alive data object
	for _, stat := range task.objs {
		obj := stable.tombstone[stat.ObjectLocation().ObjectId()]
		for dataid, delcnt := range obj.distro {
			for i := range 8 {
				if _, ok := stable.data[i][dataid]; ok {
					v, _ := targetDistro.Get(oidCount{oid: dataid})
					v.count += delcnt
					targetDistro.Set(oidCount{oid: dataid, count: v.count})
					break
				}
			}
		}
	}

	iter := targetDistro.Iter()
	defer iter.Release()

	currentObjRow := 0
	currentObjOsize := 0
	currentObjCsize := 0
	mergedSize := 0

	createdSegId := objectio.NewSegmentid()
	createdObjIdx := uint16(0)

	var newTombstone STombstone
	var currentZM index.ZM

	writeTombstone := func() {
		// set stat
		newStat := newTombstone.stats
		objectio.SetObjectStatsOriginSize(newStat, uint32(currentObjOsize))
		objectio.SetObjectStatsSize(newStat, uint32(currentObjCsize))
		objectio.SetObjectStatsRowCnt(newStat, uint32(currentObjRow))
		objectio.SetObjectStatsSortKeyZoneMap(newStat, currentZM)

		newTombstones = append(newTombstones, newTombstone)
		newTombstone = STombstone{}
		createdObjIdx++
		mergedSize += currentObjOsize
		currentObjRow = 0
		currentObjOsize = 0
		currentObjCsize = 0
		currentZM = index.NewZM(types.T_Rowid, 0)
	}

	for iter.Next() {
		if newTombstone.distro == nil {
			newTombstone.stats = objectio.NewObjectStats()
			objectio.SetObjectStatsObjectName(
				newTombstone.stats,
				objectio.BuildObjectName(createdSegId, createdObjIdx),
			)
			newTombstone.createTime = types.BuildTS(clock.Now().UnixNano(), 0)
			newTombstone.distro = make(map[objectio.ObjectId]int)
			currentZM = index.NewZM(types.T_Rowid, 0)
		}
		item := iter.Item()
		currentObjRow += item.count
		currentObjOsize += item.count * info.avgROsize
		currentObjCsize += item.count * info.avgRCsize
		newTombstone.distro[item.oid] = item.count

		// tombstone's zm does not matter for merging
		currentZM.Update(types.NewRowIDWithObjectIDBlkNumAndRowID(item.oid, 0, 0))
		currentZM.Update(types.NewRowIDWithObjectIDBlkNumAndRowID(item.oid, 2, 8192))

		if currentObjRow*info.avgROsize > common.DefaultMaxOsizeObjBytes &&
			info.totalOsize-mergedSize > common.DefaultMaxOsizeObjBytes {
			writeTombstone()
		}
	}

	if currentObjRow > 0 {
		writeTombstone()
	}

	for _, obj := range task.objs {
		delete(stable.tombstone, obj.ObjectLocation().ObjectId())
	}

	for _, obj := range newTombstones {
		stable.tombstone[obj.stats.ObjectLocation().ObjectId()] = obj
	}

	return newTombstones
}

func logTask(
	taskId uint64,
	task mergeTask,
	cost time.Duration,
	toObjs iter.Seq[*objectio.ObjectStats]) {
	var fromDescBuilder strings.Builder
	var toDescBuilder strings.Builder
	name := fmt.Sprintf("[MT-%d]1000-merge-hero", taskId)
	if task.isTombstone {
		name = fmt.Sprintf("[MT-%d]1000-tombstone", taskId)
	}
	buildObjsString := func(
		builder *strings.Builder,
		objs iter.Seq[*objectio.ObjectStats]) (rows, objCnt int) {
		pad := " | "
		strType := false
		for obj := range objs {
			rows += int(obj.Rows())
			objCnt++
			zm := obj.SortKeyZoneMap()
			if objCnt > 1 {
				builder.WriteString(pad)
			} else {
				_, strType = zm.GetMin().([]byte)
			}
			if task.isTombstone || strType {
				builder.WriteString(fmt.Sprintf("%s(%s)Rows(%v)",
					obj.ObjectName().ObjectId().ShortStringEx(),
					units.BytesSize(float64(obj.OriginSize())),
					obj.Rows()))
			} else {
				builder.WriteString(fmt.Sprintf("%s(%s)Rows(%v)[%v, %v]",
					obj.ObjectName().ObjectId().ShortStringEx(),
					units.BytesSize(float64(obj.OriginSize())),
					obj.Rows(),
					zm.GetMin(),
					zm.GetMax()))
			}
		}
		return
	}

	fromRows, fromObjCnt := buildObjsString(&fromDescBuilder, IterStats(task.objs))
	toRows, toObjCnt := buildObjsString(&toDescBuilder, toObjs)

	logutil.Info(
		"[MERGE-TASK]",
		zap.String("task", name),
		zap.String("from-size", units.BytesSize(float64(task.oSize))),
		zap.String("est-size", units.BytesSize(float64(task.eSize))),
		zap.Int("from-obj", fromObjCnt),
		zap.Int("to-obj", toObjCnt),
		zap.Int("from-rows", fromRows),
		zap.Int("to-rows", toRows),
		zap.Int8("level", task.level),
		zap.String("task-source-note", task.note),
		zap.String("cost", cost.String()),
		zap.String("from-objs", fromDescBuilder.String()),
		zap.String("to-objs", toDescBuilder.String()),
	)
}

func (e *SExecutor) ExecuteFor(target catalog.MergeTable, task mergeTask) bool {
	stable := target.(*STable)
	stable.Lock()
	defer stable.Unlock()

	// baseline: 2MB oringnal size -> 150ms
	taskCost := time.Duration(float64(time.Millisecond) * 150 *
		float64(task.oSize) / common.Const1MBytes / 2)

	newObjCount := 0
	if task.isTombstone {
		objs := mergeTombstoneLocked(stable, task, e.clock)
		if e.logEnabled {
			logTask(e.taskId, task, taskCost, iterSTAsStats(objs))
		}
		e.tombstoneMergedSize += int64(task.oSize)
		newObjCount = len(objs)
	} else {
		objs := mergeDataLocked(stable, task, e.clock)
		if e.logEnabled {
			logTask(e.taskId, task, taskCost, iterSDAsStats(objs))
		}
		e.dataMergedSize += int64(task.oSize)
		newObjCount = len(objs)
	}
	e.taskId++

	e.clock.AfterFunc(taskCost, func() {
		if task.doneCB != nil {
			task.doneCB.f()
		}
		for range newObjCount {
			e.scatalog.mergeSched.OnCreateNonAppendObject(target)
		}
	})

	return true
}

func updateNumberTypeZM[T constraints.Integer | constraints.Float](
	zmSplit []index.ZM,
	ratio []float64,
	rowsSplit []int,
	l, r T,
) {
	span := float64(r - l)
	for i := range zmSplit {
		zmSplit[i].Update(l)
		if i == len(zmSplit)-1 {
			zmSplit[i].Update(r)
		} else if rowsSplit[i] > 1 {
			piece := T(ratio[i] * span)
			l = l + piece
			zmSplit[i].Update(l)
		}
		l += 1
	}
}

func fillZero(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func updateStringTypeZM(
	zmSplit []index.ZM,
	ratio []float64,
	rowsPerObj []int,
	l, r []byte,
) {
	buf := make([]byte, 8)
	getPartU64 := func(b []byte, i int) uint64 {
		if i*8 < len(b) && (i+1)*8 > len(b) {
			fillZero(buf)
			copy(buf, b[i*8:])
			return binary.BigEndian.Uint64(buf)
		} else if i*8 >= len(b) {
			return 0
		} else {
			return binary.BigEndian.Uint64(b[i*8 : (i+1)*8])
		}
	}
	var diffs = [4]uint64{0, 0, 0, 0}
	for i := range diffs {
		diffs[i] = getPartU64(r, i) - getPartU64(l, i)
	}

	minbuf := make([]byte, 30)
	copy(minbuf, l)

	for i := range zmSplit {
		if i == 0 {
			zmSplit[i].Update(l)
		} else {
			zmSplit[i].Update(minbuf)
		}
		if i == len(zmSplit)-1 {
			zmSplit[i].Update(r)
			return
		}

		if rowsPerObj[i] > 1 {
			for j := range diffs {
				piece := uint64(math.Floor(float64(ratio[i]) * float64(diffs[j])))
				v := getPartU64(minbuf, j) + piece
				if j < 3 {
					binary.BigEndian.PutUint64(minbuf[j*8:(j+1)*8], v)
				} else {
					binary.BigEndian.PutUint64(buf, v)
					copy(minbuf[j*8:30], buf)
				}
			}
			zmSplit[i].Update(minbuf)
		}
	}
}

func splitZM(zm index.ZM, rowsPerObj []int) []index.ZM {
	ratio := make([]float64, len(rowsPerObj))
	total := 0

	for _, row := range rowsPerObj {
		total += row
	}

	for i, row := range rowsPerObj {
		ratio[i] = float64(row) / float64(total)
	}

	zmSplit := make([]index.ZM, len(rowsPerObj))
	for i := range zmSplit {
		zmSplit[i] = index.NewZM(zm.GetType(), 0)
	}

	_min := zm.GetMin()
	_max := zm.GetMax()

	// case types.T_int8:
	// case types.T_int16:
	// case types.T_int32:
	// case types.T_int64:
	// case types.T_uint8:
	// case types.T_uint16:
	// case types.T_uint32:
	// case types.T_uint64:
	// case types.T_float32:
	// case types.T_float64:
	// case types.T_date:
	// case types.T_time:
	// case types.T_datetime:
	// case types.T_timestamp:
	// case types.T_enum:
	// case types.T_decimal64:
	// case types.T_decimal128:
	// case types.T_json:
	// case types.T_blob:
	// case types.T_text:
	// case types.T_char:
	switch zm.GetType() {
	case types.T_int32:
		l, r := _min.(int32), _max.(int32)
		updateNumberTypeZM(zmSplit, ratio, rowsPerObj, l, r)
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink:
		l, r := _min.([]byte), _max.([]byte)
		updateStringTypeZM(zmSplit, ratio, rowsPerObj, l, r)
	default:
		panic(fmt.Sprintf("unsupported type: %s", zm.GetType()))
	}

	return zmSplit
}

// endregion: executor

// region: Catalog

type SCatalog struct {
	mergeSched catalog.MergeNotifierOnCatalog
	hero       *STable

	inputDataSize      int64
	inputTombstoneSize int64
}

func (c *SCatalog) AddData(data SData) {
	c.hero.Lock()
	defer c.hero.Unlock()
	c.hero.AddDataLocked(data)
	c.mergeSched.OnCreateNonAppendObject(c.hero)
	c.inputDataSize += int64(data.stats.OriginSize())
}

func (c *SCatalog) AddTombstone(tombstone STombstone) {
	c.hero.Lock()
	defer c.hero.Unlock()
	c.hero.tombstone[tombstone.stats.ObjectLocation().ObjectId()] = tombstone
	c.mergeSched.OnCreateNonAppendObject(c.hero)
	c.inputTombstoneSize += int64(tombstone.stats.OriginSize())
}

func (c *SCatalog) AddTombstoneByDesc(desc STombstoneDesc) {
	c.hero.Lock()
	defer c.hero.Unlock()
	dist := make(map[objectio.ObjectId]int)

	generatedRowCnt := 0
	for _, distDesc := range desc.desc {
		lv := distDesc.Lv
		totalCnt := len(c.hero.data[lv])
		selected := int(float64(totalCnt) * distDesc.ObjCntProportion)
		if selected == 0 {
			continue
		}

		if selected > distDesc.ObjCnt {
			selected = distDesc.ObjCnt
		}

		avg, variance := distDesc.DelAvg, distDesc.DelVar
		// Create a slice of selected size with random integers
		// based on normal distribution with given avg and variance
		delCounts := make([]int, selected)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Using Box-Muller transform to generate normally distributed values
		for i := 0; i < selected; i++ {
			// Generate random number from normal distribution
			u1 := r.Float64()
			u2 := r.Float64()
			z := math.Sqrt(-2.0*math.Log(u1)) * math.Cos(2.0*math.Pi*u2)

			// Scale by standard deviation (sqrt of variance) and add mean
			val := z*math.Sqrt(variance) + float64(avg)

			// Ensure value is at least 1 (can't have less than 1 deletion)
			delCounts[i] = int(math.Max(1, math.Floor(val)))
			generatedRowCnt += delCounts[i]
		}

		var idx int
		for id := range c.hero.data[lv] {
			dist[id] = delCounts[idx]
			idx++
			if idx >= selected {
				break
			}
		}
	}

	missed := int(desc.GetObjectStats().Rows()) - generatedRowCnt
	if missed > 0 {
		dist[objectio.NewObjectid()] = missed
	}

	tombstone := STombstone{
		SData:  desc.SData,
		distro: dist,
	}

	c.hero.tombstone[tombstone.stats.ObjectLocation().ObjectId()] = tombstone
	c.mergeSched.OnCreateNonAppendObject(c.hero)
	c.inputTombstoneSize += int64(desc.stats.OriginSize())
}

var (
	MergeHeroID   = uint64(1000)
	MergeHeroDesc = "1000-merge-hero"
)

func NewSCatalog() *SCatalog {
	data := [8]map[objectio.ObjectId]SData{}
	for i := range data {
		data[i] = make(map[objectio.ObjectId]SData)
	}
	return &SCatalog{
		hero: &STable{
			id:        MergeHeroID,
			desc:      MergeHeroDesc,
			data:      data,
			tombstone: make(map[objectio.ObjectId]STombstone),
		},
	}
}

func (c *SCatalog) InitSource() iter.Seq[catalog.MergeTable] {
	return func(yield func(catalog.MergeTable) bool) {
		yield(c.hero)
	}
}

func (c *SCatalog) SetMergeNotifier(scheduler catalog.MergeNotifierOnCatalog) {
	c.mergeSched = scheduler
}

func (c *SCatalog) GetMergeSettingsBatchFn() func() (*batch.Batch, func()) {
	return func() (*batch.Batch, func()) {
		return nil, func() {}
	}
}

type STable struct {
	sync.RWMutex
	id   uint64
	desc string

	data      [8]map[objectio.ObjectId]SData
	tombstone map[objectio.ObjectId]STombstone
}

func (t *STable) ID() uint64 { return t.id }

func (t *STable) GetNameDesc() string { return t.desc }

func (t *STable) HasDropCommitted() bool { return false }

func (t *STable) IsSpecialBigTable() bool { return false }

func (t *STable) AddDataLocked(data SData) {
	stats := data.GetObjectStats()
	lv := stats.GetLevel()
	t.data[lv][stats.ObjectLocation().ObjectId()] = data
}

func (t *STable) DeleteDataLocked(stats *objectio.ObjectStats) {
	lv := stats.GetLevel()
	delete(t.data[lv], stats.ObjectLocation().ObjectId())
}

func (t *STable) IterDataItem() iter.Seq[catalog.MergeDataItem] {
	return func(yield func(catalog.MergeDataItem) bool) {
		t.RLock()
		defer t.RUnlock()
		for i := range t.data {
			for _, obj := range t.data[i] {
				yield(&obj)
			}
		}
	}
}

func (t *STable) IterTombstoneItem() iter.Seq[catalog.MergeTombstoneItem] {
	return func(yield func(catalog.MergeTombstoneItem) bool) {
		t.RLock()
		defer t.RUnlock()
		for _, obj := range t.tombstone {
			yield(&obj)
		}
	}
}

type SData struct {
	stats      *objectio.ObjectStats
	createTime types.TS
}

func (o *SData) GetObjectStats() *objectio.ObjectStats {
	return o.stats
}

func (o *SData) GetCreatedAt() types.TS {
	return o.createTime
}

type STombstoneDesc struct {
	SData
	desc []catalog.LevelDist
}

type STombstone struct {
	SData
	distro map[objectio.ObjectId]int
}

func (o *STombstone) ForeachRowid(
	ctx context.Context,
	reuseBatch any,
	each func(rowid types.Rowid, isNull bool, rowIdx int) error,
) error {
	for id, count := range o.distro {
		rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(id, 0, 0)
		for i := 0; i < count; i++ {
			each(rowid, false, i)
		}
	}
	return nil
}

func (o *STombstone) MakeBufferBatch() (any, func()) {
	return nil, func() {}
}

// endregion: Catalog

// region: SimPalyer

type playerSettings struct {
	tickInterval time.Duration `json:"tick_interval"`
	tickStride   time.Duration `json:"tick_stride"`
}

type SimPlayer struct {
	sclock *fakeClock
	scata  *SCatalog
	sexec  *SExecutor
	sched  *MergeScheduler
	srsc   *simRscController

	datai           int
	datasource      []SData
	tombstonei      int
	tombstoneSource []STombstoneDesc

	sourceExhaustedCh chan struct{}

	cancel func()
	ticker *time.Ticker // std ticker to drive the simulation

	tickInRealTime time.Duration
	tickInSimTime  time.Duration
}

func (p *SimPlayer) AddData(data SData) {
	p.scata.AddData(data)
}

func (p *SimPlayer) AddTombstoneByDesc(tombstone STombstoneDesc) {
	p.scata.AddTombstoneByDesc(tombstone)
}

func (p *SimPlayer) AddTombstone(tombstone STombstone) {
	p.scata.AddTombstone(tombstone)
}

func NewSimPlayer() *SimPlayer {
	sclock := newFakeClock()
	scatalog := NewSCatalog()
	sexecutor := NewSExecutor(sclock, scatalog)
	srsc := newSimRscController(8 * common.Const1GBytes)

	sched := NewMergeScheduler(
		5*time.Second,
		scatalog,
		sexecutor,
		sclock,
	)
	sched.PatchTestRscController(srsc)

	return &SimPlayer{
		sclock: sclock,
		scata:  scatalog,
		sexec:  sexecutor,
		sched:  sched,
		srsc:   srsc,

		sourceExhaustedCh: make(chan struct{}),

		tickInRealTime: 100 * time.Millisecond,
		tickInSimTime:  4 * time.Second,
	}
}

func (p *SimPlayer) SetEventSource(
	datasource []SData,
	tombstoneSource []STombstoneDesc,
) {
	p.datasource = datasource
	p.tombstoneSource = tombstoneSource
	p.datai = 0
	p.tombstonei = 0
}

func (p *SimPlayer) WaitEventSourceExhaustedAndHoldFor(durationInSim time.Duration) {
	waitInRealTime := time.Duration(
		float64(durationInSim) *
			float64(p.tickInRealTime) /
			float64(p.tickInSimTime),
	)

	timer := time.NewTimer(20 * time.Minute)
	select {
	case <-p.sourceExhaustedCh:
		time.Sleep(waitInRealTime)
	case <-timer.C:
		logutil.Infof("timeout")
	}
}

func (p *SimPlayer) fillData() {
	now := p.sclock.Now().UTC().UnixNano()
	for p.datai < len(p.datasource) &&
		p.datasource[p.datai].createTime.Physical() < now {
		p.AddData(p.datasource[p.datai])
		p.datai++
	}
}

func (p *SimPlayer) fillTombstone() {
	now := p.sclock.Now().UTC().UnixNano()
	for p.tombstonei < len(p.tombstoneSource) &&
		p.tombstoneSource[p.tombstonei].createTime.Physical() < now {
		p.AddTombstoneByDesc(p.tombstoneSource[p.tombstonei])
		p.tombstonei++
	}
}

func (p *SimPlayer) runTicker(trueInterval, simInterval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(trueInterval)
	go func() {
		closed := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.fillData()
				p.fillTombstone()
				if !closed && p.datai >= len(p.datasource) &&
					p.tombstonei >= len(p.tombstoneSource) {
					close(p.sourceExhaustedCh)
					closed = true
				}
				p.sclock.Advance(simInterval)
			}
		}
	}()
	return cancel
}

func (p *SimPlayer) Start() {
	p.sched.Start()
	p.cancel = p.runTicker(p.tickInRealTime, p.tickInSimTime)
}

func (p *SimPlayer) ResetPace(trueInterval, simInterval time.Duration) {
	if p.cancel != nil {
		p.cancel()
	}
	p.tickInRealTime = trueInterval
	p.tickInSimTime = simInterval
}

func (p *SimPlayer) Stop() {
	p.sched.Stop()
	p.cancel()
}

func (p *SimPlayer) ReportString() string {
	supp := p.sched.supps[p.scata.hero.id]
	repo := &MergeReport{
		NextSchedCheck:          supp.nextDue.String(),
		LastMerge:               p.sclock.Since(supp.lastMergeTime).String(),
		LastVacuumCheck:         p.sclock.Since(supp.lastVacuumCheckTime).String(),
		VacuumCheckCount:        supp.totalVacuumCheckCnt,
		InputDataSize:           units.BytesSize(float64(p.scata.inputDataSize)),
		InputTombstoneSize:      units.BytesSize(float64(p.scata.inputTombstoneSize)),
		DataMergedSize:          units.BytesSize(float64(p.sexec.dataMergedSize)),
		TombstoneMergedSize:     units.BytesSize(float64(p.sexec.tombstoneMergedSize)),
		DataMergeCount:          supp.totalDataMergeCnt,
		TombstoneMergeCount:     supp.totalTombstoneMergeCnt,
		DataSourceProgress:      fmt.Sprintf("%d/%d", p.datai, len(p.datasource)),
		TombstoneSourceProgress: fmt.Sprintf("%d/%d", p.tombstonei, len(p.tombstoneSource)),
	}
	if p.scata.inputDataSize > 0 {
		repo.DataWA = float64(p.sexec.dataMergedSize) /
			float64(p.scata.inputDataSize)
	}
	if p.scata.inputTombstoneSize > 0 {
		repo.TombstoneWA = float64(p.sexec.tombstoneMergedSize) /
			float64(p.scata.inputTombstoneSize)
	}
	j, _ := json.MarshalIndent(repo, "", "  ")

	b := strings.Builder{}
	b.WriteString(string(j))
	b.WriteString("\n")

	{
		ctx := context.Background()
		p.sched.pad.Reset()

		p.sched.pad.InitWithTrigger(
			NewMMsgTaskTrigger(p.scata.hero).
				WithL0(DefaultLayerZeroOpts).
				WithTombstone(DefaultTombstoneOpts),
			supp.lastMergeTime,
		)

		layerZeroStats := CalculateLayerZeroStats(
			ctx,
			p.sched.pad.leveledObjects[0],
			p.sclock.Since(supp.lastMergeTime),
			DefaultLayerZeroOpts,
		)
		b.WriteString(fmt.Sprintf(
			"level 0 basic stats  : %s\n",
			layerZeroStats.String()))

		for i := 1; i < len(p.sched.pad.leveledObjects); i++ {
			if len(p.sched.pad.leveledObjects[i]) == 0 {
				b.WriteString(fmt.Sprintf("level %d no data\n", i))
				continue
			}
			overlapStats, _ := CalculateOverlapStats(
				ctx,
				p.sched.pad.leveledObjects[i],
				DefaultOverlapOpts.Clone().WithFurtherStat(true),
			)
			b.WriteString(fmt.Sprintf(
				"level %d overlap stats : %s\n",
				i,
				overlapStats.String(),
			))
		}

		vacuumStats, _ := CalculateVacuumStats(
			ctx,
			p.scata.hero,
			DefaultVacuumOpts.Clone().WithCheckBigOnly(false),
			p.sclock.Now(),
		)
		b.WriteString(fmt.Sprintf("vacuum stats : %s\n", vacuumStats.String()))
	}

	return b.String()
}

type MergeReport struct {
	NextSchedCheck  string `json:"next_sched_check"`
	LastMerge       string `json:"last_merge"`
	LastVacuumCheck string `json:"last_vacuum_check"`

	VacuumCheckCount int `json:"vacuum_check_count"`

	InputDataSize       string  `json:"input_data_size"`
	InputTombstoneSize  string  `json:"input_tombstone_size"`
	DataMergedSize      string  `json:"data_merged_size"`
	TombstoneMergedSize string  `json:"tombstone_merged_size"`
	DataWA              float64 `json:"data_wa"`
	TombstoneWA         float64 `json:"tombstone_wa"`
	DataMergeCount      int     `json:"data_merge_count"`
	TombstoneMergeCount int     `json:"tombstone_merge_count"`

	DataSourceProgress      string `json:"data_source_progress"`
	TombstoneSourceProgress string `json:"tombstone_source_progress"`
}

// endregion: SimPalyer
