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

package txnimpl

import (
	"context"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var (
	ErrDuplicateNode = moerr.NewInternalErrorNoCtx("tae: duplicate node")
)

type baseTable struct {
	txnTable    *txnTable
	schema      *catalog.Schema
	isTombstone bool

	// dedupTS covers completed incremental checks for this stream only.
	dedupTS types.TS

	tableSpace *tableSpace
}

// duplicatedRowIDs keeps candidates from appendable and non-appendable
// objects separate. A merge output can be found before the replacement row in
// an appendable object, but its row may be removed by a transferred tombstone.
// Keeping both candidates until tombstone filtering is complete lets the
// caller fall back to the live replacement row instead of treating the key as
// absent.
type duplicatedRowIDs struct {
	appendable         containers.Vector
	nonAppendable      containers.Vector
	appendableByKey    [][]int
	nonAppendableByKey [][]int
}

func newDuplicatedRowIDs(
	pool *containers.VectorPool,
	length int,
) (*duplicatedRowIDs, error) {
	rowIDs := &duplicatedRowIDs{
		appendable:         pool.GetVector(&objectio.RowidType),
		nonAppendable:      pool.GetVector(&objectio.RowidType),
		appendableByKey:    make([][]int, length),
		nonAppendableByKey: make([][]int, length),
	}
	return rowIDs, nil
}

func (r *duplicatedRowIDs) Close() {
	if r == nil {
		return
	}
	if r.appendable != nil {
		r.appendable.Close()
		r.appendable = nil
	}
	if r.nonAppendable != nil {
		r.nonAppendable.Close()
		r.nonAppendable = nil
	}
}

func (r *duplicatedRowIDs) add(appendable bool, key int, rowID types.Rowid) {
	var (
		rows containers.Vector
		keys *[][]int
	)
	if appendable {
		rows = r.appendable
		keys = &r.appendableByKey
	} else {
		rows = r.nonAppendable
		keys = &r.nonAppendableByKey
	}
	idx := rows.Length()
	rows.Append(rowID, false)
	(*keys)[key] = append((*keys)[key], idx)
}

func (r *duplicatedRowIDs) HasCandidate() bool {
	for i := range r.appendableByKey {
		for _, idx := range r.appendableByKey[i] {
			if !r.appendable.IsNull(idx) {
				return true
			}
		}
		for _, idx := range r.nonAppendableByKey[i] {
			if !r.nonAppendable.IsNull(idx) {
				return true
			}
		}
	}
	return false
}

func (r *duplicatedRowIDs) firstLive(
	rows containers.Vector,
	indexes []int,
) (types.Rowid, bool) {
	for _, idx := range indexes {
		if !rows.IsNull(idx) {
			return vector.GetFixedAtNoTypeCheck[types.Rowid](rows.GetDownstreamVector(), idx), true
		}
	}
	return types.EmptyRowid, false
}

// Merge chooses an effective row only after all object candidates have been
// checked against tombstones. Appendable rows are preferred because they are
// the replacement rows created after a merge source was written. Multiple
// candidates from the same object class are retained until that filtering is
// complete; the first surviving candidate preserves the existing object-list
// traversal order.
func (r *duplicatedRowIDs) Merge(pool *containers.VectorPool) containers.Vector {
	rowIDs := pool.GetVector(&objectio.RowidType)
	for i := range r.appendableByKey {
		if rowID, ok := r.firstLive(r.appendable, r.appendableByKey[i]); ok {
			rowIDs.Append(rowID, false)
			continue
		}
		if rowID, ok := r.firstLive(r.nonAppendable, r.nonAppendableByKey[i]); ok {
			rowIDs.Append(rowID, false)
			continue
		}
		rowIDs.Append(nil, true)
	}
	return rowIDs
}

func newBaseTable(schema *catalog.Schema, isTombstone bool, txnTable *txnTable) *baseTable {
	return &baseTable{
		schema:      schema,
		txnTable:    txnTable,
		isTombstone: isTombstone,
	}
}
func (tbl *baseTable) collectCmd(cmdMgr *commandManager) (err error) {
	if tbl.tableSpace != nil {
		err = tbl.tableSpace.CollectCmd(cmdMgr)
	}
	return
}
func (tbl *baseTable) Close() error {
	if tbl.tableSpace != nil {
		err := tbl.tableSpace.Close()
		if err != nil {
			return err
		}
		tbl.tableSpace = nil
	}
	return nil
}
func (tbl *baseTable) DedupWorkSpace(key containers.Vector) (err error) {
	if tbl.tableSpace != nil {
		if err = tbl.tableSpace.BatchDedup(key); err != nil {
			return
		}
	}
	return
}
func (tbl *baseTable) approxSize() int {
	if tbl == nil || tbl.tableSpace == nil || tbl.tableSpace.node == nil {
		return 0
	}
	return tbl.tableSpace.node.data.ApproxSize()
}
func (tbl *baseTable) BatchDedupLocal(bat *containers.Batch) error {
	if tbl.tableSpace == nil || !tbl.schema.HasPK() {
		return nil
	}
	return tbl.DedupWorkSpace(bat.GetVectorByName(tbl.schema.GetPrimaryKey().Name))
}

func (tbl *baseTable) addObjsWithMetaLoc(ctx context.Context, stats objectio.ObjectStats) (err error) {
	var pkVecs []containers.Vector
	var closeFuncs []func()
	defer func() {
		for _, v := range pkVecs {
			v.Close()
		}
		for _, f := range closeFuncs {
			f()
		}
	}()
	if tbl.tableSpace != nil && tbl.tableSpace.isStatsExisted(stats) {
		return nil
	}
	metaLocs := make([]objectio.Location, 0)
	blkCount := stats.BlkCnt()
	totalRow := stats.Rows()
	blkMaxRows := tbl.schema.Extra.BlockMaxRows
	for i := uint16(0); i < uint16(blkCount); i++ {
		var blkRow uint32
		if totalRow > blkMaxRows {
			blkRow = blkMaxRows
		} else {
			blkRow = totalRow
		}
		totalRow -= blkRow
		metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)

		metaLocs = append(metaLocs, metaloc)
	}
	schema := tbl.schema
	if schema.HasPK() && !tbl.schema.IsSecondaryIndexTable() {
		dedupType := tbl.txnTable.store.txn.GetDedupType()
		if !dedupType.SkipSourcePersisted() {
			for _, loc := range metaLocs {
				var vectors []containers.Vector
				var closeFunc func()
				vectors, closeFunc, err = ioutil.LoadColumns2(
					ctx,
					[]uint16{uint16(schema.GetSingleSortKeyIdx())},
					nil,
					tbl.txnTable.store.rt.Fs,
					loc,
					fileservice.Policy(0),
					false,
					nil,
				)
				if err != nil {
					return err
				}
				closeFuncs = append(closeFuncs, closeFunc)
				pkVecs = append(pkVecs, vectors[0])
				err = tbl.txnTable.dedup(ctx, vectors[0], tbl.isTombstone)
				if err != nil {
					return
				}
			}
		}
	}
	if tbl.tableSpace == nil {
		tbl.tableSpace = newTableSpace(tbl.txnTable, tbl.isTombstone)
	}
	return tbl.tableSpace.AddDataFiles(pkVecs, stats)
}
func (tbl *baseTable) getRowsByPK(ctx context.Context, pks containers.Vector) (rowIDs *duplicatedRowIDs, err error) {
	var it *catalog.VisibleCommittedObjectIt
	if tbl.isTombstone {
		it = tbl.txnTable.entry.MakeTombstoneVisibleObjectIt(tbl.txnTable.store.txn)
	} else {
		it = tbl.txnTable.entry.MakeDataVisibleObjectIt(tbl.txnTable.store.txn)
	}
	defer it.Release()
	rowIDs, err = newDuplicatedRowIDs(
		tbl.txnTable.store.rt.VectorPool.Small,
		pks.Length(),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// GetByFilter intentionally continues with the candidates returned before
		// a WW conflict so it can still resolve the visible row after waiting.
		if err != nil && !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			rowIDs.Close()
			rowIDs = nil
		}
	}()
	pkType := pks.GetType()
	keysZM := index.NewZM(pkType.Oid, pkType.Scale)
	if err = index.BatchUpdateZM(keysZM, pks.GetDownstreamVector()); err != nil {
		return
	}
	for it.Next() {
		obj := it.Item()
		if isEmptyDroppedAppendableObject(obj) {
			continue
		}
		objData := obj.GetObjectData()
		if objData == nil {
			continue
		}
		if obj.HasCommittedPersistedData() {
			var skip bool
			if skip, err = quickSkipThisObject(ctx, keysZM, obj); err != nil {
				return
			} else if skip {
				continue
			}
		}
		objectRowIDs := tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
		err = vector.AppendMultiFixed[types.Rowid](
			objectRowIDs.GetDownstreamVector(),
			types.EmptyRowid,
			true,
			pks.Length(),
			common.WorkspaceAllocator,
		)
		if err == nil {
			err = obj.GetObjectData().GetDuplicatedRows(
				ctx,
				tbl.txnTable.store.txn,
				pks,
				nil,
				types.TS{}, types.MaxTs(),
				objectRowIDs,
				common.WorkspaceAllocator,
			)
		}
		if err == nil || moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			for i := 0; i < pks.Length(); i++ {
				if !objectRowIDs.IsNull(i) {
					rowIDs.add(
						obj.IsAppendable(),
						i,
						vector.GetFixedAtNoTypeCheck[types.Rowid](objectRowIDs.GetDownstreamVector(), i),
					)
				}
			}
		}
		objectRowIDs.Close()
		if err != nil {
			logutil.Infof("getRowsByPK failed GetDuplicate: %v, obj %v", err, obj.ID().String())
			return
		}
	}
	return
}

/*
similar to findDeletes
*/
func foreachIncrementalObject(
	snapshot catalog.ObjectListSnapshot,
	from, to types.TS,
	fn func(*catalog.ObjectEntry) error,
) error {
	_, err := foreachIncrementalObjectWithStats(snapshot, false, from, to, 0, fn)
	return err
}

// incrementalObjectScanStats separates time spent by the object-list iterator
// from time spent in the callback. filter is the per-entry catalog checks
// excluding maxCommit; maxCommit is tracked separately, and operation is the
// object-level GetDuplicatedRows/Contains work.
type incrementalObjectScanStats struct {
	scan        time.Duration
	filter      time.Duration
	createdAt   time.Duration
	deletedAt   time.Duration
	visibleByTS time.Duration
	groupState  time.Duration
	maxCommit   time.Duration
	operation   time.Duration
}

func incrementalObjectGroupName(group catalog.ObjectListGroup) string {
	switch group {
	case catalog.ObjectListGroupAppendableCreate:
		return "appendable_create"
	case catalog.ObjectListGroupAppendableCreateWithDrop:
		return "appendable_create_with_drop"
	case catalog.ObjectListGroupAppendableDrop:
		return "appendable_drop"
	case catalog.ObjectListGroupNonAppendableCreate:
		return "non_appendable_create"
	case catalog.ObjectListGroupNonAppendableCreateWithDrop:
		return "non_appendable_create_with_drop"
	case catalog.ObjectListGroupNonAppendableDrop:
		return "non_appendable_drop"
	default:
		return "unknown"
	}
}

func prePrepareObjectKind(obj *catalog.ObjectEntry) string {
	if obj.IsAppendable() {
		return "aobject"
	}
	return "naobject"
}

func prePrepareObjectHasDrop(group catalog.ObjectListGroup) string {
	switch group {
	case catalog.ObjectListGroupAppendableCreateWithDrop,
		catalog.ObjectListGroupAppendableDrop,
		catalog.ObjectListGroupNonAppendableCreateWithDrop,
		catalog.ObjectListGroupNonAppendableDrop:
		return "true"
	default:
		return "false"
	}
}

func observePrePrepareObjectScan(
	tableID uint64,
	isTombstone bool,
	obj *catalog.ObjectEntry,
	result string,
) {
	if tableID == 0 {
		return
	}
	typ := "data"
	if isTombstone {
		typ = "tombstone"
	}
	group := obj.ObjectListGroup()
	v2.TxnTNPrePrepareObjectScanCounter.WithLabelValues(
		strconv.FormatUint(tableID, 10),
		typ,
		prePrepareObjectKind(obj),
		prePrepareObjectHasDrop(group),
		incrementalObjectGroupName(group),
		result,
	).Inc()
}

func observeIncrementalObjectScanStats(isTombstone bool, stats incrementalObjectScanStats) {
	observePrePrepareDedupStepDuration(isTombstone, "object_list_scan", stats.scan)
	observePrePrepareDedupStepDuration(isTombstone, "object_entry_filter", stats.filter)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_created_at", stats.createdAt)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_deleted_at", stats.deletedAt)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_visible_by_ts", stats.visibleByTS)
	observePrePrepareDedupStepDuration(isTombstone, "object_filter_group_state", stats.groupState)
	observePrePrepareDedupStepDuration(isTombstone, "object_max_commit_lookup", stats.maxCommit)
	observePrePrepareDedupStepDuration(isTombstone, "object_operation", stats.operation)

	// The iterator duration includes callback execution. Subtracting the
	// measured entry work leaves the time spent in the ObjectList traversal
	// itself (tree navigation, callback dispatch, and group boundaries).
	iterator := stats.scan - stats.filter - stats.maxCommit - stats.operation
	if iterator > 0 {
		observePrePrepareDedupStepDuration(isTombstone, "object_list_iterator", iterator)
	}
}

func observeIncrementalObjectGroupScanStats(
	isTombstone bool,
	group catalog.ObjectListGroup,
	stats incrementalObjectScanStats,
) {
	typ := "data"
	if isTombstone {
		typ = "tombstone"
	}
	groupName := incrementalObjectGroupName(group)
	observeObjectStep := func(step string, duration time.Duration) {
		v2.TxnTNPrePrepareObjectStepDurationHistogram.WithLabelValues(typ, groupName, step).Observe(duration.Seconds())
	}
	observeObjectStep("object_list_scan", stats.scan)
	observeObjectStep("object_list_iterator", maxDuration(0, stats.scan-stats.filter-stats.maxCommit-stats.operation))
	observeObjectStep("object_entry_filter", stats.filter)
	observeObjectStep("object_filter_created_at", stats.createdAt)
	observeObjectStep("object_filter_deleted_at", stats.deletedAt)
	observeObjectStep("object_filter_visible_by_ts", stats.visibleByTS)
	observeObjectStep("object_filter_group_state", stats.groupState)
	observeObjectStep("object_max_commit_lookup", stats.maxCommit)
	observeObjectStep("object_operation", stats.operation)
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}

func foreachIncrementalObjectWithStats(
	snapshot catalog.ObjectListSnapshot,
	isTombstone bool,
	from, to types.TS,
	tableID uint64,
	fn func(*catalog.ObjectEntry) error,
) (stats incrementalObjectScanStats, err error) {
	groupStats := make(map[catalog.ObjectListGroup]*incrementalObjectScanStats)
	groupTypes := make(map[catalog.ObjectListGroup]string)
	var activeStats *incrementalObjectScanStats
	getGroupStats := func(group catalog.ObjectListGroup) *incrementalObjectScanStats {
		if groupStats[group] == nil {
			groupStats[group] = &incrementalObjectScanStats{}
		}
		return groupStats[group]
	}
	mergeStats := func(dst *incrementalObjectScanStats, src *incrementalObjectScanStats) {
		dst.scan += src.scan
		dst.filter += src.filter
		dst.createdAt += src.createdAt
		dst.deletedAt += src.deletedAt
		dst.visibleByTS += src.visibleByTS
		dst.groupState += src.groupState
		dst.maxCommit += src.maxCommit
		dst.operation += src.operation
	}
	observeObject := func(obj *catalog.ObjectEntry, result string) {
		observePrePrepareObjectScan(tableID, isTombstone, obj, result)
		kind := "data"
		if obj.IsTombstone {
			kind = "tombstone"
		}
		groupTypes[obj.ObjectListGroup()] = kind
		v2.TxnTNPrePrepareObjectCounter.WithLabelValues(
			kind, incrementalObjectGroupName(obj.ObjectListGroup()), result).Inc()
		if result == "object_list_visited" && obj.ObjectListGroup() == catalog.ObjectListGroupAppendableCreate {
			v2.TxnTNPrePrepareObjectBookmarkCounter.WithLabelValues(
				kind, incrementalObjectGroupName(obj.ObjectListGroup()), "range_visited").Inc()
		}
	}
	shouldSkipAObject := func(obj *catalog.ObjectEntry) bool {
		if !obj.IsAppendable() {
			return false
		}
		observeObject(obj, "aobject_candidate")
		v2.TxnAObjectDedupCandidateCounter.Inc()
		objData := obj.GetObjectData()
		maxCommitter, ok := objData.(interface {
			GetAppendMaxCommitTS() (types.TS, bool)
		})
		if !ok {
			// An appendable catalog entry should normally be backed by an
			// aobject. Keep the conservative behavior if it is not.
			v2.TxnAObjectDedupMaxCommitUnavailableCounter.Inc()
			v2.TxnAObjectDedupScannedCounter.Inc()
			observeObject(obj, "aobject_max_commit_unavailable")
			observeObject(obj, "aobject_scanned")
			return false
		}
		maxCommitStart := time.Now()
		maxCommit, finalized := maxCommitter.GetAppendMaxCommitTS()
		activeStats.maxCommit += time.Since(maxCommitStart)
		if !finalized {
			// Unsealed append history has no safe upper bound. It must not be
			// skipped, even when the currently visible rows look old.
			v2.TxnAObjectDedupMaxCommitUnavailableCounter.Inc()
			v2.TxnAObjectDedupScannedCounter.Inc()
			observeObject(obj, "aobject_max_commit_unavailable")
			observeObject(obj, "aobject_scanned")
			return false
		}
		if maxCommit.LT(&from) {
			v2.TxnAObjectDedupMaxCommitSkippedCounter.Inc()
			observeObject(obj, "aobject_max_commit_skipped")
			return true
		}
		v2.TxnAObjectDedupScannedCounter.Inc()
		observeObject(obj, "aobject_scanned")
		return false
	}
	recordFilter := func(filterStart time.Time, maxCommitBefore time.Duration, createdAt, deletedAt, visibleByTS time.Duration) {
		filterElapsed := time.Since(filterStart) - (activeStats.maxCommit - maxCommitBefore)
		if filterElapsed > 0 {
			activeStats.filter += filterElapsed
			groupState := filterElapsed - createdAt - deletedAt - visibleByTS
			if groupState > 0 {
				activeStats.groupState += groupState
			}
		}
	}
	visitCreateGroup := func(group catalog.ObjectListGroup, appendable bool) error {
		// An appendable object created before from can still contain an append
		// prepared in [from, to]. Do not use the lower CreatedAt bound to
		// truncate an appendable create group. The upper bound is safe because
		// the group's tree is ordered by CreatedAt.
		var err error
		localStats := getGroupStats(group)
		activeStats = localStats
		visitEntry := func(obj *catalog.ObjectEntry) bool {
			filterStart := time.Now()
			maxCommitBefore := activeStats.maxCommit
			observeObject(obj, "object_list_visited")
			createdAtStart := time.Now()
			createdAfterTo := obj.CreatedAt.GT(&to)
			createdAtElapsed := time.Since(createdAtStart)
			activeStats.createdAt += createdAtElapsed
			if createdAfterTo {
				observeObject(obj, "created_after_to")
				recordFilter(filterStart, maxCommitBefore, createdAtElapsed, 0, 0)
				return false
			}
			if shouldSkipAObject(obj) {
				recordFilter(filterStart, maxCommitBefore, createdAtElapsed, 0, 0)
				return true
			}
			// A create-only group contains serving C entries without a D
			// counterpart. With CreatedAt <= to established above,
			// VisibleByTS(to) cannot reject the entry.
			recordFilter(filterStart, maxCommitBefore, createdAtElapsed, 0, 0)
			operationStart := time.Now()
			err = fn(obj)
			activeStats.operation += time.Since(operationStart)
			return err == nil
		}
		scanStart := time.Now()
		bookmarkStart, bookmarkValid, bookmarkSkipped := snapshot.CommitBookmarkStartWithCount(group, from)
		if from == txnif.UncommitTS {
			bookmarkValid = false
			bookmarkSkipped = 0
		}
		if group == catalog.ObjectListGroupAppendableCreate {
			kind := "data"
			if isTombstone {
				kind = "tombstone"
			}
			groupName := incrementalObjectGroupName(group)
			bookmarkResult := "invalid"
			if bookmarkValid {
				bookmarkResult = "valid"
			}
			v2.TxnTNPrePrepareObjectBookmarkCounter.WithLabelValues(
				kind, groupName, bookmarkResult).Inc()
			if bookmarkSkipped > 0 {
				v2.TxnTNPrePrepareObjectBookmarkCounter.WithLabelValues(
					kind, groupName, "prefix_skipped").Add(float64(bookmarkSkipped))
			}
			startID := "<none>"
			if bookmarkStart != nil {
				startID = bookmarkStart.ID().ShortStringEx()
			}
			logutil.Debugf(
				"[PrePrepareDedup] object-list bookmark group=%s type=%s valid=%v prefix-skipped=%d start=%s from=%s to=%s",
				groupName, kind, bookmarkValid, bookmarkSkipped, startID, from.ToString(), to.ToString())
		}
		if bookmarkValid {
			// The prefix bookmark proves that every entry before bookmarkStart
			// has maxCommitTS < from. This is the only path that can avoid
			// rechecking those objects one by one.
			if bookmarkStart != nil {
				snapshot.ScanGroupFrom(group, bookmarkStart, visitEntry)
			}
		} else if appendable {
			snapshot.ScanGroup(group, visitEntry)
		} else {
			snapshot.AscendGroup(group, from, visitEntry)
		}
		localStats.scan += time.Since(scanStart)
		observeIncrementalObjectGroupScanStats(groupTypes[group] == "tombstone", group, *localStats)
		mergeStats(&stats, localStats)
		return err
	}
	if err := visitCreateGroup(catalog.ObjectListGroupAppendableCreate, true); err != nil {
		return stats, err
	}
	visitDropGroup := func(group catalog.ObjectListGroup) error {
		var err error
		localStats := getGroupStats(group)
		activeStats = localStats
		scanStart := time.Now()
		visitEntry := func(obj *catalog.ObjectEntry) bool {
			filterStart := time.Now()
			maxCommitBefore := activeStats.maxCommit
			observeObject(obj, "object_list_visited")
			deletedAtStart := time.Now()
			deletedAfterFrom := obj.DeletedAt.GT(&from)
			deletedAtElapsed := time.Since(deletedAtStart)
			activeStats.deletedAt += deletedAtElapsed
			// Ascend is inclusive. Drop processing starts strictly after from.
			if !deletedAfterFrom {
				observeObject(obj, "deleted_at_or_before_from")
				recordFilter(filterStart, maxCommitBefore, 0, deletedAtElapsed, 0)
				return true
			}
			createdAtStart := time.Now()
			createdAfterTo := obj.CreatedAt.GT(&to)
			createdAtElapsed := time.Since(createdAtStart)
			activeStats.createdAt += createdAtElapsed
			if createdAfterTo {
				observeObject(obj, "created_after_to")
				recordFilter(filterStart, maxCommitBefore, createdAtElapsed, deletedAtElapsed, 0)
				return true
			}
			visibleStart := time.Now()
			visible := obj.VisibleByTS(to)
			visibleElapsed := time.Since(visibleStart)
			activeStats.visibleByTS += visibleElapsed
			if !visible {
				observeObject(obj, "not_visible_at_to")
				recordFilter(filterStart, maxCommitBefore, createdAtElapsed, deletedAtElapsed, visibleElapsed)
				return true
			}
			if shouldSkipAObject(obj) {
				recordFilter(filterStart, maxCommitBefore, createdAtElapsed, deletedAtElapsed, visibleElapsed)
				return true
			}
			recordFilter(filterStart, maxCommitBefore, createdAtElapsed, deletedAtElapsed, visibleElapsed)
			operationStart := time.Now()
			err = fn(obj)
			activeStats.operation += time.Since(operationStart)
			return err == nil
		}
		bookmarkStart, bookmarkValid := snapshot.CommitBookmarkStart(group, from)
		if from == txnif.UncommitTS {
			bookmarkValid = false
		}
		if bookmarkValid {
			if bookmarkStart != nil {
				snapshot.ScanGroupFrom(group, bookmarkStart, visitEntry)
			}
		} else {
			snapshot.AscendGroup(group, from, visitEntry)
		}
		localStats.scan += time.Since(scanStart)
		observeIncrementalObjectGroupScanStats(groupTypes[group] == "tombstone", group, *localStats)
		mergeStats(&stats, localStats)
		return err
	}
	if err := visitDropGroup(catalog.ObjectListGroupAppendableDrop); err != nil {
		return stats, err
	}

	if err := visitCreateGroup(catalog.ObjectListGroupNonAppendableCreate, false); err != nil {
		return stats, err
	}
	err = visitDropGroup(catalog.ObjectListGroupNonAppendableDrop)
	return stats, err
}

// incrementalGetRowsByPK checks the inclusive logical interval [from, to].
// Callers that hold an exclusive dedup watermark must pass watermark.Next().
func (tbl *baseTable) incrementalGetRowsByPK(ctx context.Context, pks containers.Vector, from, to types.TS, inQueue bool) (rowIDs containers.Vector, err error) {
	stepStart := time.Now()
	var snapshot catalog.ObjectListSnapshot
	if tbl.isTombstone {
		tbl.txnTable.entry.WaitTombstoneObjectCommitted(to)
		snapshot = tbl.txnTable.entry.MakeTombstoneObjectSnapshot()
	} else {
		tbl.txnTable.entry.WaitDataObjectCommitted(to)
		snapshot = tbl.txnTable.entry.MakeDataObjectSnapshot()
	}
	observePrePrepareDedupStep(tbl.isTombstone, "wait_object_committed", stepStart)
	rowIDs = tbl.txnTable.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	defer func() {
		// Ownership transfers to the caller only on success. In particular,
		// lazy commit-TS reads add cancellable I/O errors after allocation.
		if err != nil {
			rowIDs.Close()
			rowIDs = nil
		}
	}()
	vector.AppendMultiFixed[types.Rowid](
		rowIDs.GetDownstreamVector(),
		types.EmptyRowid,
		true,
		pks.Length(),
		common.WorkspaceAllocator,
	)
	var scanStats incrementalObjectScanStats
	scanStats, err = foreachIncrementalObjectWithStats(snapshot, tbl.isTombstone, from, to, tbl.txnTable.entry.ID, func(obj *catalog.ObjectEntry) error {
		if isEmptyDroppedAppendableObject(obj) {
			kind := "data"
			if tbl.isTombstone {
				kind = "tombstone"
			}
			group := obj.ObjectListGroup()
			groupNames := [...]string{
				"appendable_create", "appendable_create_with_drop", "appendable_drop",
				"non_appendable_create", "non_appendable_create_with_drop", "non_appendable_drop",
			}
			v2.TxnTNPrePrepareObjectCounter.WithLabelValues(kind, groupNames[group], "empty_dropped_skipped").Inc()
			return nil
		}
		objData := obj.GetObjectData()
		objectStepStart := time.Now()
		err := objData.GetDuplicatedRows(
			ctx,
			tbl.txnTable.store.txn,
			pks,
			nil,
			from, to,
			rowIDs,
			common.WorkspaceAllocator,
		)
		observePrePrepareDedupStep(tbl.isTombstone, "object_get_duplicated_rows", objectStepStart)
		kind := "data"
		if tbl.isTombstone {
			kind = "tombstone"
		}
		groupNames := [...]string{
			"appendable_create", "appendable_create_with_drop", "appendable_drop",
			"non_appendable_create", "non_appendable_create_with_drop", "non_appendable_drop",
		}
		v2.TxnTNPrePrepareObjectCounter.WithLabelValues(
			kind, groupNames[obj.ObjectListGroup()], "object_get_duplicated_rows").Inc()
		return err
	})
	observeIncrementalObjectScanStats(tbl.isTombstone, scanStats)
	if err != nil {
		return
	}
	return
}

func isEmptyDroppedAppendableObject(obj *catalog.ObjectEntry) bool {
	stats := obj.GetObjectStats()
	if !obj.IsAppendable() || stats.Rows() != 0 || stats.BlkCnt() != 0 {
		return false
	}
	dropCommitted := obj.HasDropCommitted()
	if !dropCommitted && obj.IsCEntry() && obj.HasDCounterpart() {
		dropCommitted = obj.GetNextVersion().HasDropCommitted()
	}
	if !dropCommitted {
		return false
	}
	objData := obj.GetObjectData()
	if objData == nil {
		return false
	}
	rows, err := objData.Rows()
	if err != nil || rows != 0 {
		return false
	}
	return true
}

func (tbl *baseTable) CleanUp() {
	if tbl.tableSpace != nil {
		tbl.tableSpace.CloseAppends()
	}
}

func (tbl *baseTable) PrePrepare() error {
	if tbl.tableSpace != nil {
		return tbl.tableSpace.PrepareApply()
	}
	return nil
}

func quickSkipThisObject(
	ctx context.Context,
	keysZM index.ZM,
	meta *catalog.ObjectEntry,
) (ok bool, err error) {
	ok = !meta.SortKeyZoneMap().FastIntersect(keysZM)
	return
}
