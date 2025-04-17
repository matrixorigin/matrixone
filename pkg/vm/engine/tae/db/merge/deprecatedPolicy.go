// Copyright 2024 Matrix Origin
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

package merge

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	"go.uber.org/zap"
)

// region: overlap policy
var _ policy = (*objOverlapPolicy)(nil)

var levels = [6]int{
	1, 2, 4, 16, 64, 256,
}

type objOverlapPolicy struct {
	leveledObjects [len(levels)][]*catalog.ObjectEntry

	segments map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}

	config *BasicPolicyConfig
	tid    uint64
	name   string
}

func newObjOverlapPolicy() *objOverlapPolicy {
	return &objOverlapPolicy{
		segments: make(map[objectio.Segmentid]map[*catalog.ObjectEntry]struct{}),
	}
}

func (m *objOverlapPolicy) onObject(obj *catalog.ObjectEntry) bool {
	if obj.IsTombstone {
		return false
	}
	if m.segments[obj.ObjectName().SegmentId()] == nil {
		m.segments[obj.ObjectName().SegmentId()] = make(map[*catalog.ObjectEntry]struct{})
	}
	m.segments[obj.ObjectName().SegmentId()][obj] = struct{}{}
	return true
}

func IsSameSegment(objs []*catalog.ObjectEntry) bool {
	if len(objs) < 2 {
		return true
	}
	segId := objs[0].ObjectName().SegmentId()
	for _, obj := range objs {
		if !obj.ObjectName().SegmentId().Eq(segId) {
			return false
		}
	}
	return true
}

func hasHundredSmallObjs(objs []*catalog.ObjectEntry, small uint32) bool {
	if len(objs) < 100 {
		return false
	}
	cnt := 0
	for _, obj := range objs {
		if obj.OriginSize() < small {
			cnt++
		}
		if cnt > 100 {
			return true
		}
	}
	return false
}

func isAllGreater(objs []*objectio.ObjectStats, size uint32) bool {
	for _, obj := range objs {
		if obj.OriginSize() < size {
			return false
		}
	}
	return true
}

func (m *objOverlapPolicy) revise(rc *resourceController) []mergeTask {
	if rc.cpuPercent > 80 {
		return nil
	}

	for _, objects := range m.segments {
		l := segLevel(len(objects))
		for obj := range objects {
			m.leveledObjects[l] = append(m.leveledObjects[l], obj)
		}
	}

	reviseResults := make([]mergeTask, 0, len(levels))

	checkNonoverlapObj := func(objs []*catalog.ObjectEntry, note string) {
		tmp := make([]*catalog.ObjectEntry, 0)
		sum := uint32(0)
		for _, obj := range objs {
			if obj.OriginSize() < m.config.MaxOsizeMergedObj/2 || sum < m.config.MaxOsizeMergedObj/2 {
				sum += obj.OriginSize()
				tmp = append(tmp, obj)
			} else {
				if len(tmp) > 1 {
					task := mergeTask{kind: taskHostDN, note: note}
					for _, obj := range removeOversize(slices.Clone(tmp)) {
						task.objs = append(task.objs, obj.GetObjectStats())
					}
					reviseResults = append(reviseResults, task)
				}
				tmp = tmp[:0]
				sum = 0
			}
		}
		if len(tmp) > 100 { // let the little objs accumulate to a certain number
			task := mergeTask{kind: taskHostDN, note: "end: " + note}
			for _, obj := range removeOversize(slices.Clone(tmp)) {
				task.objs = append(task.objs, obj.GetObjectStats())
			}
			reviseResults = append(reviseResults, task)
		}
	}

	for i, lv := range m.leveledObjects {
		if len(lv) < 2 {
			continue
		}
		logutil.Infof("---- bb level %d: %v", i, len(lv))
	}

	for i := range 4 {
		if len(m.leveledObjects[i]) < 2 {
			continue
		}

		points := makeEndPoints(m.leveledObjects[i])
		if res := objectsWithGivenOverlaps(points, 5, i); len(res) != 0 {
			for j, objs := range res {
				if i == 2 {
					logutil.Infof("---- tt result %d: %v", j, len(objs))
				}
				objs = removeOversize(objs)
				if len(objs) < 2 || score(objs) < 1.1 {
					if i == 2 {
						logutil.Infof("---- tt skip result %d: %v, %v", j, len(objs), score(objs))
					}
					continue
				}
				if i >= 1 && IsSameSegment(objs) {
					if i == 2 {
						logutil.Infof("---- tt skip result %d: %v, %v", j, len(objs), IsSameSegment(objs))
					}
					continue
				}
				task := mergeTask{kind: taskHostDN, note: "overlap"}
				for _, obj := range objs {
					task.objs = append(task.objs, obj.GetObjectStats())
				}
				reviseResults = append(reviseResults, task)
			}
		} else if len(points) > 0 { // zm is inited
			viewed := make(map[*catalog.ObjectEntry]struct{})
			tmp := make([]*catalog.ObjectEntry, 0)
			for _, p := range points {
				if _, ok := viewed[p.obj]; ok {
					continue
				}
				viewed[p.obj] = struct{}{}
				tmp = append(tmp, p.obj)
			}
			checkNonoverlapObj(tmp, "nonoverlap")
		} else { // no sort key
			checkNonoverlapObj(m.leveledObjects[i], "noSortKey")
		}

		if (len(reviseResults) == 0 || (len(reviseResults) == 1 && len(reviseResults[0].objs) == 2)) && hasHundredSmallObjs(m.leveledObjects[i], m.config.MaxOsizeMergedObj/2) {
			// try merge small objs
			var oldtask mergeTask
			if len(reviseResults) == 1 {
				oldtask = reviseResults[0]
				reviseResults = reviseResults[:0]
			}
			checkNonoverlapObj(m.leveledObjects[i], "check")
			// restore the old task
			if len(reviseResults) == 0 {
				reviseResults = append(reviseResults, oldtask)
			}
		}
	}

	for i := range reviseResults {
		original := len(reviseResults[i].objs)
		for !rc.resourceAvailable(int64(mergesort.EstimateMergeSize(IterStats(reviseResults[i].objs)))) && len(reviseResults[i].objs) > 1 {
			reviseResults[i].objs = reviseResults[i].objs[:len(reviseResults[i].objs)/2]
		}
		if original-len(reviseResults[i].objs) > 100 {
			tablename := "unknown"
			if len(reviseResults[i].objs) > 0 {
				tablename = fmt.Sprintf("%v-%v", m.tid, m.name)
			}
			logutil.Info("MergeExecutorEvent",
				zap.String("event", "Popout"),
				zap.String("table", tablename),
				zap.Int("original", original),
				zap.Int("revised", len(reviseResults[i].objs)),
				zap.String("createNote", string(reviseResults[i].note)),
				zap.String("avail", common.HumanReadableBytes(int(rc.availableMem()))))
		}

		if len(reviseResults[i].objs) < 2 {
			continue
		}

		if original-len(reviseResults[i].objs) > 0 && isAllGreater(reviseResults[i].objs, m.config.ObjectMinOsize) {
			// avoid zm infinited loop
			continue
		}

		rc.reserveResources(int64(mergesort.EstimateMergeSize(IterStats(reviseResults[i].objs))))
	}

	return slices.DeleteFunc(reviseResults, func(result mergeTask) bool {
		return len(result.objs) < 2
	})
}

func (m *objOverlapPolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	for i := range m.leveledObjects {
		m.leveledObjects[i] = m.leveledObjects[i][:0]
	}
	m.config = config
	if entry != nil {
		m.tid = entry.ID
		m.name = entry.GetLastestSchema(false).Name
	}
	clear(m.segments)
}

func segLevel(length int) int {
	l := len(levels) - 1
	for i, level := range levels {
		if length < level {
			l = i - 1
			break
		}
	}
	return l
}

type endPoint struct {
	val []byte
	s   bool

	obj *catalog.ObjectEntry
}

func objectsWithGivenOverlaps(points []endPoint, overlaps int, level int) [][]*catalog.ObjectEntry {
	res := make([][]*catalog.ObjectEntry, 0)
	tmp := make(map[*catalog.ObjectEntry]struct{})
	for {
		clear(tmp)
		count := 0
		globalMax := 0
		for _, p := range points {
			if p.s {
				count++
			} else {
				count--
			}
			if count > globalMax {
				globalMax = count
			}
		}

		for _, p := range points {
			if p.s {
				tmp[p.obj] = struct{}{}
			} else {
				delete(tmp, p.obj)
			}
			if len(tmp) == globalMax {
				break
			}
		}

		if level == 2 {
			logutil.Infof("---- xx level %d: %v, %v", level, globalMax, len(tmp))
		}

		if len(tmp) < overlaps {
			return res
		}

		if len(tmp) > 1 {
			res = append(res, slices.Collect(maps.Keys(tmp)))
		}

		points = slices.DeleteFunc(points, func(point endPoint) bool {
			_, ok := tmp[point.obj]
			return ok
		})
	}
}

func makeEndPoints(objects []*catalog.ObjectEntry) []endPoint {
	points := make([]endPoint, 0, 2*len(objects))
	for _, obj := range objects {
		zm := obj.SortKeyZoneMap()
		if !zm.IsInited() {
			continue
		}
		if obj.OriginSize() >= common.DefaultMinOsizeQualifiedMB*common.Const1MBytes {
			if !zm.IsString() {
				if zm.GetMin() == zm.GetMax() {
					continue
				}
			} else {
				if zm.MaxTruncated() {
					continue
				}
				minBuf := zm.GetMinBuf()
				maxBuf := zm.GetMaxBuf()
				if len(minBuf) == len(maxBuf) && len(minBuf) == 30 {
					copiedMin := make([]byte, len(minBuf))
					copiedMax := make([]byte, len(maxBuf))

					copy(copiedMin, minBuf)
					copy(copiedMax, maxBuf)
					for i := 29; i >= 0; i-- {
						if copiedMax[i] != 0 {
							copiedMax[i]--
							break
						}
						copiedMax[i]--
					}
					if bytes.Equal(copiedMin, copiedMax) {
						continue
					}
				} else if bytes.Equal(minBuf, maxBuf) {
					continue
				}
			}
		}
		points = append(points, endPoint{val: zm.GetMinBuf(), s: true, obj: obj})
		points = append(points, endPoint{val: zm.GetMaxBuf(), s: false, obj: obj})
	}
	slices.SortFunc(points, func(a, b endPoint) int {
		c := compute.Compare(a.val, b.val, objects[0].SortKeyZoneMap().GetType(),
			a.obj.SortKeyZoneMap().GetScale(), b.obj.SortKeyZoneMap().GetScale())
		if c != 0 {
			return c
		}
		// left node is first
		if a.s {
			return -1
		}
		return 1
	})
	return points
}

// region: tombstone policy

var _ policy = (*tombstonePolicy)(nil)

type tombstonePolicy struct {
	tombstones []*catalog.ObjectEntry
	config     *BasicPolicyConfig
}

func (t *tombstonePolicy) onObject(entry *catalog.ObjectEntry) bool {
	if !entry.IsTombstone {
		return false
	}
	if len(t.tombstones) == t.config.MergeMaxOneRun {
		return false
	}
	t.tombstones = append(t.tombstones, entry)
	return true
}

func (t *tombstonePolicy) revise(*resourceController) []mergeTask {
	if len(t.tombstones) < 2 {
		return nil
	}
	task := mergeTask{kind: taskHostDN, note: "tombstone", isTombstone: true}
	for _, obj := range t.tombstones {
		task.objs = append(task.objs, obj.GetObjectStats())
	}
	return []mergeTask{task}
}

func (t *tombstonePolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	t.tombstones = t.tombstones[:0]
	t.config = config
}

func newTombstonePolicy() policy {
	return &tombstonePolicy{
		tombstones: make([]*catalog.ObjectEntry, 0, 5),
	}
}

// endregion

// region: compact policy
var _ policy = (*objCompactPolicy)(nil)

type objCompactPolicy struct {
	tblEntry *catalog.TableEntry
	fs       fileservice.FileService

	objects []*catalog.ObjectEntry

	tombstoneMetas []objectio.ObjectDataMeta

	config *BasicPolicyConfig
}

func newObjCompactPolicy(fs fileservice.FileService) *objCompactPolicy {
	return &objCompactPolicy{fs: fs}
}

func (o *objCompactPolicy) onObject(entry *catalog.ObjectEntry) bool {
	if o.tblEntry == nil {
		return false
	}
	if entry.IsTombstone {
		return false
	}
	if entry.OriginSize() < o.config.ObjectMinOsize {
		return false
	}
	if len(o.tombstoneMetas) == 0 {
		return false
	}

	for _, meta := range o.tombstoneMetas {
		if !checkTombstoneMeta(meta, entry.ID()) {
			continue
		}
		o.objects = append(o.objects, entry)
	}
	return false
}

func (o *objCompactPolicy) revise(rc *resourceController) []mergeTask {
	if o.tblEntry == nil {
		return nil
	}
	results := make([]mergeTask, 0, len(o.objects))
	for _, obj := range o.objects {
		if rc.resourceAvailable(int64(mergesort.EstimateMergeSize(IterEntryAsStats([]*catalog.ObjectEntry{obj})))) {
			rc.reserveResources(int64(mergesort.EstimateMergeSize(IterEntryAsStats([]*catalog.ObjectEntry{obj}))))
			results = append(results, mergeTask{objs: []*objectio.ObjectStats{obj.GetObjectStats()}, kind: taskHostDN})
		}
	}
	return results
}

func (o *objCompactPolicy) resetForTable(entry *catalog.TableEntry, config *BasicPolicyConfig) {
	o.tblEntry = entry
	o.tombstoneMetas = o.tombstoneMetas[:0]
	o.objects = o.objects[:0]
	o.config = config

	tIter := entry.MakeTombstoneVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	defer tIter.Release()
	for tIter.Next() {
		tEntry := tIter.Item()

		if !ObjectValid(tEntry) {
			continue
		}

		if tEntry.OriginSize() > common.DefaultMaxOsizeObjBytes {
			meta, err := loadTombstoneMeta(tEntry.GetObjectStats(), o.fs)
			if err != nil {
				continue
			}
			o.tombstoneMetas = append(o.tombstoneMetas, meta)
		}
	}
}

func loadTombstoneMeta(tombstoneObject *objectio.ObjectStats, fs fileservice.FileService) (objectio.ObjectDataMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	location := tombstoneObject.ObjectLocation()
	objMeta, err := objectio.FastLoadObjectMeta(
		ctx, &location, false, fs,
	)
	if err != nil {
		return nil, err
	}
	return objMeta.MustDataMeta(), nil
}

func checkTombstoneMeta(tombstoneMeta objectio.ObjectDataMeta, objectId *objectio.ObjectId) bool {
	prefixPattern := objectId[:]
	blkCnt := int(tombstoneMeta.BlockCount())

	startIdx := sort.Search(blkCnt, func(i int) bool {
		return tombstoneMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(prefixPattern)
	})

	for pos := startIdx; pos < blkCnt; pos++ {
		blkMeta := tombstoneMeta.GetBlockMeta(uint32(pos))
		columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
		// block id is the prefixPattern of the rowid and zonemap is min-max of rowid
		// !PrefixEq means there is no rowid of this block in this zonemap, so skip
		if columnZonemap.RowidPrefixEq(prefixPattern) {
			return true
		}
		if columnZonemap.RowidPrefixGT(prefixPattern) {
			// all zone maps are sorted by the rowid
			// if the block id is less than the prefixPattern of the min rowid, skip the rest blocks
			break
		}
	}
	return false
}

// region: policy group
type policyGroup struct {
	policies []policy

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func newPolicyGroup(policies ...policy) *policyGroup {
	return &policyGroup{
		policies:       policies,
		configProvider: newCustomConfigProvider(),
	}
}

func (g *policyGroup) onObject(obj *catalog.ObjectEntry) {
	for _, p := range g.policies {
		if p.onObject(obj) { // ???
			return
		}
	}
}

func (g *policyGroup) revise(rc *resourceController) []mergeTask {
	results := make([]mergeTask, 0, len(g.policies))
	for _, p := range g.policies {
		pResult := p.revise(rc)
		for _, r := range pResult {
			if len(r.objs) > 0 {
				results = append(results, r)
			}
		}
	}
	return results
}

func (g *policyGroup) resetForTable(entry *catalog.TableEntry) {
	g.config = g.configProvider.getConfig(entry)
	for _, p := range g.policies {
		p.resetForTable(entry, g.config)
	}
}

func (g *policyGroup) setConfig(tbl *catalog.TableEntry, txn txnif.AsyncTxn, cfg *BasicPolicyConfig) (err error) {
	if tbl == nil || txn == nil {
		return
	}
	schema := tbl.GetLastestSchema(false)
	ctx := context.Background()
	defer func() {
		if err != nil {
			logutil.Error(
				"Policy-SetConfig-Error",
				zap.Error(err),
				zap.Uint64("table-id", tbl.ID),
				zap.String("table-name", schema.Name),
			)
			txn.Rollback(ctx)
		} else {
			err = txn.Commit(ctx)
			logger := logutil.Info
			if err != nil {
				logger = logutil.Error
			}
			logger(
				"Policy-SetConfig-Commit",
				zap.Error(err),
				zap.String("commit-ts", txn.GetCommitTS().ToString()),
				zap.Uint64("table-id", tbl.ID),
				zap.String("table-name", schema.Name),
			)
			g.configProvider.invalidCache(tbl)
		}
	}()

	moCatalog, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return
	}

	moTables, err := moCatalog.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		return
	}

	moColumns, err := moCatalog.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return
	}

	packer := types.NewPacker()
	defer packer.Close()

	packer.Reset()
	packer.EncodeUint32(schema.AcInfo.TenantID)
	packer.EncodeStringType([]byte(tbl.GetDB().GetName()))
	packer.EncodeStringType([]byte(schema.Name))
	pk := packer.Bytes()
	cloned := schema.Clone()
	cloned.Extra.MaxOsizeMergedObj = cfg.MaxOsizeMergedObj
	cloned.Extra.MinOsizeQuailifed = cfg.ObjectMinOsize
	cloned.Extra.MaxObjOnerun = uint32(cfg.MergeMaxOneRun)
	cloned.Extra.MinCnMergeSize = cfg.MinCNMergeSize
	cloned.Extra.Hints = cfg.MergeHints
	err = moTables.UpdateByFilter(ctx, handle.NewEQFilter(pk), uint16(pkgcatalog.MO_TABLES_EXTRA_INFO_IDX), cloned.MustGetExtraBytes(), false)
	if err != nil {
		return
	}

	for _, col := range schema.ColDefs {
		packer.Reset()
		packer.EncodeUint32(schema.AcInfo.TenantID)
		packer.EncodeStringType([]byte(tbl.GetDB().GetName()))
		packer.EncodeStringType([]byte(schema.Name))
		packer.EncodeStringType([]byte(col.Name))
		pk := packer.Bytes()
		err = moColumns.UpdateByFilter(ctx, handle.NewEQFilter(pk), uint16(pkgcatalog.MO_COLUMNS_ATT_CPKEY_IDX), pk, false)
		if err != nil {
			return
		}
	}

	db, err := txn.GetDatabaseByID(tbl.GetDB().ID)
	if err != nil {
		return
	}
	tblHandle, err := db.GetRelationByID(tbl.ID)
	if err != nil {
		return
	}
	return tblHandle.AlterTable(
		ctx,
		newUpdatePolicyReq(cfg),
	)
}

func (g *policyGroup) getConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	r := g.configProvider.getConfig(tbl)
	if r == nil {
		r = &BasicPolicyConfig{
			ObjectMinOsize:    common.RuntimeOsizeRowsQualified.Load(),
			MaxOsizeMergedObj: common.RuntimeMaxObjOsize.Load(),
			MergeMaxOneRun:    int(common.RuntimeMaxMergeObjN.Load()),
			MinCNMergeSize:    common.RuntimeMinCNMergeSize.Load(),
		}
	}
	return r
}
