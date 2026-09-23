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

package catalog

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	ObjectState_Create_Active uint8 = iota
	ObjectState_Create_PrepareCommit
	ObjectState_Create_ApplyCommit
	ObjectState_Delete_Active
	ObjectState_Delete_PrepareCommit
	ObjectState_Delete_ApplyCommit
)

/*
ObjectList keeps entries in six independent B-trees:

 1. appendable serving C entries
 2. appendable C entries with a D counterpart
 3. appendable D entries
 4. non-appendable serving C entries
 5. non-appendable C entries with a D counterpart
 6. non-appendable D entries

Each tree is ordered by the entry commit timestamp
(CreatedAt for C entries and DeletedAt for D entries), then by object name.
Uncommitted entries therefore sit at the end of their group.

The C and D entries for a dropped object remain separate tree items. Callers
that need only the latest version must skip C entries having a D counterpart.

All-object compatibility scans use a combined tree with the same group order.
Visible scans use another copy-on-write B-tree containing only C entries,
globally ordered by CreatedAt and object name. The combined, grouped, and
visible indexes are published in one atomic snapshot so readers never observe
mismatched catalog indexes.
*/

type ObjectList struct {
	isTombstone bool
	sync.RWMutex
	objectID_index    map[objectio.ObjectId]objectListIndex
	trees             atomic.Pointer[objectListTrees]
	appendMaxes       map[objectio.ObjectId]objectListAppendMax
	appendMaxStates   map[objectio.ObjectId]objectListAppendMaxState
	appendMaxCounts   [3]int
	tableID           uint64
	restartSealed     map[objectio.ObjectId]types.TS
	bookmarkSchedule  func(func() error) error
	bookmarkPending   bool
	bookmarkPendingAt time.Time
	bookmarkVersion   uint64
}

type objectListIndex struct {
	ts    types.TS
	group ObjectListGroup
}

type objectListTrees struct {
	all       *btree.BTreeG[*ObjectEntry]
	groups    [ObjectListGroupNonAppendableDrop + 1]*btree.BTreeG[*ObjectEntry]
	visible   *btree.BTreeG[*ObjectEntry]
	bookmarks [ObjectListGroupNonAppendableDrop + 1]*objectListCommitBookmark
}

type objectListAppendMax struct {
	max       types.TS
	finalized bool
}

type objectListAppendMaxState uint8

const (
	appendMaxStatePending objectListAppendMaxState = iota
	appendMaxStateSealedWaiting
	appendMaxStateFinalized
)

// objectListCommitBookmark records a prefix maximum in group-tree order.
// maxThrough includes the current entry and all preceding entries.
type objectListCommitBookmark struct {
	entries []objectListCommitBookmarkEntry
}

type objectListCommitBookmarkEntry struct {
	entry      *ObjectEntry
	maxThrough types.TS
}

type mutableObjectListTrees struct {
	old    *objectListTrees
	next   *objectListTrees
	copied [ObjectListGroupNonAppendableDrop + 1]bool
}

func newObjectEntryTreeWithLess(less func(a, b *ObjectEntry) bool) *btree.BTreeG[*ObjectEntry] {
	opts := btree.Options{
		Degree:  64,
		NoLocks: true,
	}
	return btree.NewBTreeGOptions(less, opts)
}

func newObjectEntryTree() *btree.BTreeG[*ObjectEntry] {
	return newObjectEntryTreeWithLess((*ObjectEntry).Less)
}

func objectEntryGroupLess(a, b *ObjectEntry) bool {
	t1, t2 := a.ObjectListCommitTS(), b.ObjectListCommitTS()
	if !t1.EQ(&t2) {
		return t1.LT(&t2)
	}
	return bytes.Compare(a.ObjectShortName()[:], b.ObjectShortName()[:]) < 0
}

func newObjectEntryGroupTree() *btree.BTreeG[*ObjectEntry] {
	return newObjectEntryTreeWithLess(objectEntryGroupLess)
}

func visibleObjectEntryLess(a, b *ObjectEntry) bool {
	if !a.CreatedAt.EQ(&b.CreatedAt) {
		return a.CreatedAt.LT(&b.CreatedAt)
	}
	return bytes.Compare(a.ObjectShortName()[:], b.ObjectShortName()[:]) < 0
}

func NewObjectList(isTombstone bool) *ObjectList {
	return NewObjectListWithTableID(0, isTombstone)
}

func NewObjectListWithTableID(tableID uint64, isTombstone bool) *ObjectList {
	list := &ObjectList{
		objectID_index:  make(map[types.Objectid]objectListIndex),
		appendMaxes:     make(map[objectio.ObjectId]objectListAppendMax),
		appendMaxStates: make(map[objectio.ObjectId]objectListAppendMaxState),
		tableID:         tableID,
		restartSealed:   make(map[objectio.ObjectId]types.TS),
		isTombstone:     isTombstone,
	}
	trees := &objectListTrees{
		all:     newObjectEntryTree(),
		visible: newObjectEntryTreeWithLess(visibleObjectEntryLess),
	}
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		trees.groups[group] = newObjectEntryGroupTree()
	}
	list.trees.Store(trees)
	return list
}

// SetTableID is used by replay-created TableEntry values whose ID is assigned
// after the ObjectList is constructed.
func (l *ObjectList) SetTableID(tableID uint64) {
	l.Lock()
	l.tableID = tableID
	currentTableID, counts := l.appendMaxMetricSnapshotLocked()
	l.Unlock()
	l.publishAppendMaxMetrics(currentTableID, counts)
}

func (l *ObjectList) appendMaxMetricSnapshotLocked() (uint64, [3]int) {
	return l.tableID, l.appendMaxCounts
}

func (l *ObjectList) publishAppendMaxMetrics(tableID uint64, counts [3]int) {
	v2.UpdateTxnAObjectMaxCommitState(
		tableID,
		l.isTombstone,
		counts[appendMaxStatePending],
		counts[appendMaxStateSealedWaiting],
		counts[appendMaxStateFinalized],
	)
}

func (l *ObjectList) setAppendMaxStateLocked(id *objectio.ObjectId, state objectListAppendMaxState) {
	old, exists := l.appendMaxStates[*id]
	if exists && old == state {
		return
	}
	if exists {
		l.appendMaxCounts[old]--
	}
	l.appendMaxStates[*id] = state
	l.appendMaxCounts[state]++
}

func (l *ObjectList) deleteAppendMaxStateLocked(id *objectio.ObjectId) {
	state, exists := l.appendMaxStates[*id]
	if !exists {
		return
	}
	delete(l.appendMaxStates, *id)
	l.appendMaxCounts[state]--
}

func (l *ObjectList) loadTrees() *objectListTrees {
	return l.trees.Load()
}

func (l *ObjectList) loadTree() *btree.BTreeG[*ObjectEntry] {
	return l.loadTrees().all
}

func (trees *objectListTrees) group(group ObjectListGroup) *btree.BTreeG[*ObjectEntry] {
	if group > ObjectListGroupNonAppendableDrop {
		panic("invalid object list group")
	}
	return trees.groups[group]
}

func newMutableObjectListTrees(old *objectListTrees) *mutableObjectListTrees {
	next := &objectListTrees{
		all:       old.all.Copy(),
		groups:    old.groups,
		visible:   old.visible.Copy(),
		bookmarks: old.bookmarks,
	}
	return &mutableObjectListTrees{old: old, next: next}
}

func (trees *mutableObjectListTrees) group(group ObjectListGroup) *btree.BTreeG[*ObjectEntry] {
	if !trees.copied[group] {
		trees.next.groups[group] = trees.old.groups[group].Copy()
		trees.copied[group] = true
	}
	return trees.next.groups[group]
}

type appendMaxCommitter interface {
	GetAppendMaxCommitTS() (types.TS, bool)
}

func appendMaxStateForEntry(entry *ObjectEntry) objectListAppendMax {
	if data := entry.GetObjectData(); data != nil {
		if committer, ok := data.(appendMaxCommitter); ok {
			max, finalized := committer.GetAppendMaxCommitTS()
			return objectListAppendMax{max: max, finalized: finalized}
		}
	}
	// Persisted objects do not retain AppendNode history.  The object-list
	// commit timestamp is the conservative upper bound for their materialized
	// rows.
	return objectListAppendMax{max: entry.ObjectListCommitTS(), finalized: true}
}

func (l *ObjectList) ensureAppendMaxStateLocked(entry *ObjectEntry) {
	if _, ok := l.appendMaxes[*entry.ID()]; ok {
		return
	}
	l.appendMaxes[*entry.ID()] = appendMaxStateForEntry(entry)
}

func clearObjectListBookmarks(trees *objectListTrees) {
	for i := range trees.bookmarks {
		trees.bookmarks[i] = nil
	}
}

// SetBookmarkScheduler installs the one executor used to rebuild prefix
// bookmarks. The catalog does not own a goroutine; the object runtime owns
// the scheduler and its lifecycle.
func (l *ObjectList) SetBookmarkScheduler(schedule func(func() error) error) {
	l.Lock()
	l.bookmarkSchedule = schedule
	need := !l.bookmarkPending
	if need {
		l.bookmarkPending = true
		l.bookmarkPendingAt = time.Now()
	}
	logutil.Debugf(
		"[ObjectListBookmark] scheduler installed tombstone=%v initial_rebuild=%v pending=%v",
		l.isTombstone, need, l.bookmarkPending)
	l.Unlock()
	if need {
		l.scheduleBookmarkRebuild(schedule)
	}
}

func (l *ObjectList) requestBookmarkRebuild() {
	l.Lock()
	if l.bookmarkSchedule == nil || l.bookmarkPending {
		if l.bookmarkSchedule == nil {
			logutil.Debugf(
				"[ObjectListBookmark] rebuild request dropped tombstone=%v reason=no_scheduler",
				l.isTombstone)
		}
		l.Unlock()
		return
	}
	l.bookmarkPending = true
	l.bookmarkPendingAt = time.Now()
	schedule := l.bookmarkSchedule
	logutil.Debugf(
		"[ObjectListBookmark] rebuild requested tombstone=%v version=%d",
		l.isTombstone, l.bookmarkVersion)
	l.Unlock()
	l.scheduleBookmarkRebuild(schedule)
}

func (l *ObjectList) scheduleBookmarkRebuild(schedule func(func() error) error) {
	if schedule == nil {
		l.Lock()
		l.bookmarkPending = false
		l.Unlock()
		return
	}
	if err := schedule(func() error {
		l.rebuildCommitBookmarks()
		return nil
	}); err != nil {
		l.Lock()
		l.bookmarkPending = false
		l.Unlock()
	}
}

func (l *ObjectList) rebuildCommitBookmarks() {
	rebuildStart := time.Now()
	l.RLock()
	trees := l.loadTrees()
	version := l.bookmarkVersion
	pendingAt := l.bookmarkPendingAt
	states := make(map[objectio.ObjectId]objectListAppendMax, len(l.appendMaxes))
	restartSealed := make(map[objectio.ObjectId]types.TS, len(l.restartSealed))
	for id, state := range l.appendMaxes {
		states[id] = state
	}
	for id, ts := range l.restartSealed {
		restartSealed[id] = ts
	}
	l.RUnlock()
	logutil.Debugf(
		"[ObjectListBookmark] rebuild started tombstone=%v version=%d pending_age=%s state_count=%d",
		l.isTombstone, version, rebuildStart.Sub(pendingAt), len(states))

	var bookmarks [ObjectListGroupNonAppendableDrop + 1]*objectListCommitBookmark
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		bookmark := &objectListCommitBookmark{}
		it := trees.groups[group].Iter()
		maxCommit := types.TS{}
		groupEntries := 0
		firstUnfinalized := -1
		var firstUnfinalizedEntry *ObjectEntry
		var firstUnfinalizedState objectListAppendMax
		for ok := it.First(); ok; ok = it.Next() {
			entry := it.Item()
			groupEntries++
			state, exists := states[*entry.ID()]
			if !exists {
				state = appendMaxStateForEntry(entry)
			}
			if restartTS, ok := restartSealed[*entry.ID()]; ok {
				// Replayed appendable objects do not retain their AppendNode
				// history.  At restart, a frozen prefix is nevertheless known to
				// contain only rows committed before this restart timestamp.
				// Publish that boundary as a conservative bookmark instead of
				// treating the recovered history as permanently unbounded.
				if maxCommit.LT(&restartTS) {
					maxCommit = restartTS
				}
				state = objectListAppendMax{max: restartTS, finalized: true}
			}
			if !state.finalized {
				// An unfinalized append history is equivalent to UncommitTS:
				// it is an upper bound that cannot be below any real `from`.
				// Keep the bookmark and continue so the consumer can skip the
				// proven prefix and start at this first uncertain object.
				maxCommit = txnif.UncommitTS
				if firstUnfinalized == -1 {
					firstUnfinalized = groupEntries - 1
					firstUnfinalizedEntry = entry
					firstUnfinalizedState = state
				}
			} else if maxCommit != txnif.UncommitTS && state.max.GT(&maxCommit) {
				maxCommit = state.max
			}
			bookmark.entries = append(bookmark.entries, objectListCommitBookmarkEntry{
				entry:      entry,
				maxThrough: maxCommit,
			})
		}
		it.Release()
		if firstUnfinalizedEntry != nil {
			logutil.Debugf(
				"[ObjectListBookmark] group has unfinalized suffix tombstone=%v group=%d entries=%d known_prefix_entries=%d first_unfinalized_index=%d first_unfinalized_object=%s max_commit=%s finalized=%v elapsed=%s",
				l.isTombstone, group, groupEntries, firstUnfinalized,
				firstUnfinalized, firstUnfinalizedEntry.ID().ShortStringEx(),
				firstUnfinalizedState.max.ToString(), firstUnfinalizedState.finalized,
				time.Since(rebuildStart))
		} else {
			logutil.Debugf(
				"[ObjectListBookmark] group evaluated tombstone=%v group=%d entries=%d valid=true elapsed=%s",
				l.isTombstone, group, groupEntries, time.Since(rebuildStart))
		}
		bookmarks[group] = bookmark
	}

	l.Lock()
	if l.loadTrees() == trees && l.bookmarkVersion == version {
		next := &objectListTrees{
			all:       trees.all,
			groups:    trees.groups,
			visible:   trees.visible,
			bookmarks: bookmarks,
		}
		l.trees.Store(next)
		l.bookmarkPending = false
		l.bookmarkPendingAt = time.Time{}
		logutil.Debugf(
			"[ObjectListBookmark] rebuild published tombstone=%v version=%d elapsed=%s",
			l.isTombstone, version, time.Since(rebuildStart))
		l.Unlock()
		return
	}
	l.bookmarkPending = false
	l.bookmarkPendingAt = time.Time{}
	logutil.Debugf(
		"[ObjectListBookmark] rebuild discarded tombstone=%v built_version=%d current_version=%d elapsed=%s",
		l.isTombstone, version, l.bookmarkVersion, time.Since(rebuildStart))
	l.Unlock()
	l.requestBookmarkRebuild()
}

// MarkAppendMaxPending invalidates all bookmarks until this live object has
// reached a terminal append state.
func (l *ObjectList) MarkAppendMaxPending(id *objectio.ObjectId) {
	l.Lock()
	delete(l.restartSealed, *id)
	l.appendMaxes[*id] = objectListAppendMax{finalized: false}
	l.setAppendMaxStateLocked(id, appendMaxStatePending)
	l.bookmarkVersion++
	logutil.Debugf(
		"[ObjectListBookmark] object pending tombstone=%v object=%s version=%d",
		l.isTombstone, id.ShortStringEx(), l.bookmarkVersion)
	trees := l.loadTrees()
	next := &objectListTrees{all: trees.all, groups: trees.groups, visible: trees.visible, bookmarks: trees.bookmarks}
	clearObjectListBookmarks(next)
	l.trees.Store(next)
	tableID, counts := l.appendMaxMetricSnapshotLocked()
	l.Unlock()
	l.publishAppendMaxMetrics(tableID, counts)
	l.requestBookmarkRebuild()
}

// MarkAppendMaxSealedWaiting records that no more AppendNodes can be added to
// the object, but at least one existing AppendNode still has to reach commit
// or rollback before the exact max commit timestamp is available.
func (l *ObjectList) MarkAppendMaxSealedWaiting(id *objectio.ObjectId) {
	l.Lock()
	l.setAppendMaxStateLocked(id, appendMaxStateSealedWaiting)
	tableID, counts := l.appendMaxMetricSnapshotLocked()
	l.Unlock()
	l.publishAppendMaxMetrics(tableID, counts)
}

// UpdateAppendMaxCommit publishes an exact max for one object. Rebuilding the
// prefix values is intentionally asynchronous and coalesced per ObjectList.
func (l *ObjectList) UpdateAppendMaxCommit(id *objectio.ObjectId, max types.TS) {
	l.Lock()
	delete(l.restartSealed, *id)
	l.appendMaxes[*id] = objectListAppendMax{max: max, finalized: true}
	l.setAppendMaxStateLocked(id, appendMaxStateFinalized)
	l.bookmarkVersion++
	logutil.Debugf(
		"[ObjectListBookmark] object finalized tombstone=%v object=%s max_commit=%s version=%d",
		l.isTombstone, id.ShortStringEx(), max.ToString(), l.bookmarkVersion)
	trees := l.loadTrees()
	next := &objectListTrees{all: trees.all, groups: trees.groups, visible: trees.visible, bookmarks: trees.bookmarks}
	clearObjectListBookmarks(next)
	l.trees.Store(next)
	tableID, counts := l.appendMaxMetricSnapshotLocked()
	l.Unlock()
	l.publishAppendMaxMetrics(tableID, counts)
	l.requestBookmarkRebuild()
}

type appendFrozenObject interface {
	IsAppendFrozen() bool
}

// MarkRestartSealedPrefix records the contiguous frozen prefix of a group at
// restart. Replayed appendable objects do not have their AppendNode history,
// so their normal max-commit state is intentionally unknown. The restart
// timestamp is a safe upper bound for that recovered prefix; the first object
// that is not frozen terminates the prefix.
func (l *ObjectList) MarkRestartSealedPrefix(restartTS types.TS) {
	l.Lock()
	trees := l.loadTrees()
	marked := 0
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupAppendableDrop; group++ {
		it := trees.groups[group].Iter()
		for ok := it.First(); ok; ok = it.Next() {
			entry := it.Item()
			data := entry.GetObjectData()
			frozen, ok := data.(appendFrozenObject)
			if !ok || !frozen.IsAppendFrozen() {
				break
			}
			l.restartSealed[*entry.ID()] = restartTS
			l.setAppendMaxStateLocked(entry.ID(), appendMaxStateFinalized)
			marked++
		}
		it.Release()
	}
	if marked > 0 {
		l.bookmarkVersion++
		next := &objectListTrees{
			all:       trees.all,
			groups:    trees.groups,
			visible:   trees.visible,
			bookmarks: trees.bookmarks,
		}
		clearObjectListBookmarks(next)
		l.trees.Store(next)
	}
	tableID, counts := l.appendMaxMetricSnapshotLocked()
	l.Unlock()
	l.publishAppendMaxMetrics(tableID, counts)
	if marked > 0 {
		logutil.Debugf(
			"[ObjectListBookmark] restart sealed prefix tombstone=%v restart_ts=%s objects=%d",
			l.isTombstone, restartTS.ToString(), marked)
		l.requestBookmarkRebuild()
	}
}

func (trees *mutableObjectListTrees) delete(entry *ObjectEntry) (*ObjectEntry, bool) {
	trees.next.all.Delete(entry)
	return trees.group(entry.ObjectListGroup()).Delete(entry)
}

func (trees *mutableObjectListTrees) set(entry *ObjectEntry) (*ObjectEntry, bool) {
	trees.next.all.Set(entry)
	return trees.group(entry.ObjectListGroup()).Set(entry)
}

func (trees *mutableObjectListTrees) deleteFromGroup(
	group ObjectListGroup,
	entry *ObjectEntry,
) (*ObjectEntry, bool) {
	trees.next.all.Delete(entry)
	return trees.group(group).Delete(entry)
}

//// read part

func getObjectEntry(it *btree.IterG[*ObjectEntry], pivot *ObjectEntry) *ObjectEntry {
	ok := it.Seek(pivot)
	if !ok {
		logutil.Errorf("object not found seek: %s", pivot.ID().ShortStringEx())
		return nil
	}
	obj := it.Item()
	if !obj.ID().EQ(pivot.ID()) {
		logutil.Errorf("object not found cmp: %s %s", obj.ID().ShortStringEx(), pivot.ID().ShortStringEx())
		return nil
	}
	return obj
}

func (l *ObjectList) getNodes(id *objectio.ObjectId, latestOnly bool) []*ObjectEntry {
	l.RLock()
	index, ok := l.objectID_index[*id]
	trees := l.loadTrees()
	l.RUnlock()
	if !ok {
		return nil
	}
	return l.getNodesSnap(trees, index, id, latestOnly)
}

// getNodes returns the create and delete (if exists) entries of the object with the given objectID
func (l *ObjectList) getNodesSnap(
	trees *objectListTrees,
	index objectListIndex,
	id *objectio.ObjectId,
	latestOnly bool,
) []*ObjectEntry {
	it := trees.group(index.group).Iter()
	defer it.Release()

	var key ObjectEntry
	initObjectListKey(&key, index.group, index.ts, id)

	obj := getObjectEntry(&it, &key)
	if obj == nil {
		return nil
	}

	ret := []*ObjectEntry{obj}

	// the obj is a del Entry, try to find the create entry
	if !latestOnly && obj.prevVersion != nil {
		if !obj.prevVersion.ID().EQ(id) {
			panic("logic error")
		}
		ret = append(ret, obj.prevVersion)
	}
	return ret
}

func (l *ObjectList) GetLastestNode(id *objectio.ObjectId) *ObjectEntry {
	nodes := l.getNodes(id, true)
	if len(nodes) == 0 {
		return nil
	}
	return nodes[0]
}

func (l *ObjectList) GetAllNodes(id *objectio.ObjectId) []*ObjectEntry {
	return l.getNodes(id, false)
}

func (l *ObjectList) GetObjectByID(objectID *objectio.ObjectId) (obj *ObjectEntry, err error) {
	obj = l.GetLastestNode(objectID)
	if obj == nil {
		logutil.Debug("GetObjectByID not found", zap.String("obj", objectID.ShortStringEx()))
		err = moerr.GetOkExpectedEOB()
	}
	return
}

/// write part

func (l *ObjectList) UpdateReplayTs(entry *ObjectEntry, ts types.TS) *ObjectEntry {
	l.Lock()
	defer func() {
		l.Unlock()
		l.requestBookmarkRebuild()
	}()
	oldIndex, ok := l.objectID_index[*entry.ID()]
	if !ok {
		panic("replay object index not found")
	}
	oldTrees := l.loadTrees()
	mutableTrees := newMutableObjectListTrees(oldTrees)
	l.ensureAppendMaxStateLocked(entry)
	clearObjectListBookmarks(mutableTrees.next)
	l.bookmarkVersion++
	newVisibleTree := mutableTrees.next.visible
	var oldKey ObjectEntry
	initObjectListKey(&oldKey, oldIndex.group, oldIndex.ts, entry.ID())
	if _, deleted := mutableTrees.deleteFromGroup(oldIndex.group, &oldKey); !deleted {
		panic("replay object not found")
	}
	if !entry.IsDEntry() {
		oldVisibleKey := makeVisibleObjectListKey(oldIndex.ts, entry.ID())
		if _, deleted := newVisibleTree.Delete(oldVisibleKey); !deleted {
			panic("replay visible object not found")
		}
	}

	updated := entry
	if err := updated.EntryMVCCNode.ApplyCommit(ts); err != nil {
		panic(err)
	}
	if entry.IsDEntry() {
		if _, deleted := mutableTrees.delete(entry.prevVersion); !deleted {
			panic("replay object create entry not found")
		}
		mutableTrees.set(entry.prevVersion)
		newVisibleTree.Set(entry.prevVersion)
	} else {
		newVisibleTree.Set(updated)
	}
	mutableTrees.set(updated)
	if updated.objData != nil {
		updated.objData.UpdateMeta(updated)
	}
	l.objectID_index[*entry.ID()] = objectListIndex{
		ts:    updated.ObjectListCommitTS(),
		group: updated.ObjectListGroup(),
	}
	if !l.trees.CompareAndSwap(oldTrees, mutableTrees.next) {
		panic("concurrent mutation")
	}
	return updated
}

// 1. del\ins\updated should all belong to the same object
// 2. del and ins should be two entry with different sort key, like different DeleteAt, so modify deletes the del entry (if not nil), inserts the ins entry and updates index map according to the ins entry
// 3. updated will be inserted into the tree, and the index map WON'T be updated. The Caller make sure the updated entry has the same sort key as the target entry.
// 4. all operations are atomic from the view of the caller of modify
func (l *ObjectList) modify(del, ins, updated *ObjectEntry) (deleted, replaced1, replaced2 bool) {
	l.Lock()
	defer func() {
		l.Unlock()
		l.requestBookmarkRebuild()
	}()
	oldIndex, existed := l.objectID_index[*ins.ID()]
	l.objectID_index[*ins.ID()] = objectListIndex{
		ts:    ins.ObjectListCommitTS(),
		group: ins.ObjectListGroup(),
	}

	oldTrees := l.loadTrees()
	mutableTrees := newMutableObjectListTrees(oldTrees)
	l.ensureAppendMaxStateLocked(ins)
	clearObjectListBookmarks(mutableTrees.next)
	l.bookmarkVersion++
	newVisibleTree := mutableTrees.next.visible

	if del != nil {
		if del.IsTombstone != l.isTombstone {
			panic("logic error")
		}
		_, deleted = mutableTrees.delete(del)
		if !del.IsDEntry() {
			newVisibleTree.Delete(del)
		}
	}
	// The first D entry moves its C counterpart from the create-only group to
	// the create-with-drop group. The old implementation shared one timestamp
	// ordering for both forms; the grouped ordering requires an explicit move.
	if existed &&
		(oldIndex.group == ObjectListGroupAppendableCreate ||
			oldIndex.group == ObjectListGroupNonAppendableCreate) &&
		ins.IsDEntry() && ins.prevVersion != nil {
		var oldC ObjectEntry
		initObjectListKey(&oldC, oldIndex.group, oldIndex.ts, ins.ID())
		mutableTrees.deleteFromGroup(oldIndex.group, &oldC)
		mutableTrees.set(ins.prevVersion)
	}
	// Rolling back a drop performs the inverse transition. Remove the
	// create-with-drop counterpart before restoring the serving C entry.
	if existed &&
		(oldIndex.group == ObjectListGroupAppendableDrop ||
			oldIndex.group == ObjectListGroupNonAppendableDrop) &&
		!ins.HasDropIntent() && del != nil && del.prevVersion != nil {
		mutableTrees.delete(del.prevVersion)
	}
	if updated != nil {
		_, replaced2 = mutableTrees.set(updated)
		if !updated.IsDEntry() {
			newVisibleTree.Set(updated)
		}
	}
	_, replaced1 = mutableTrees.set(ins)
	if ins.IsDEntry() {
		if existed && ins.prevVersion != nil {
			newVisibleTree.Set(ins.prevVersion)
		}
	} else {
		newVisibleTree.Set(ins)
	}
	ok := l.trees.CompareAndSwap(oldTrees, mutableTrees.next)
	if !ok {
		panic("concurrent mutation")
	}
	return
}

// Set inserts a brand the objectstate, used in CreateObject
func (l *ObjectList) Set(object *ObjectEntry) {
	_, replaced, _ := l.modify(nil, object, nil)
	if replaced {
		logutil.Error("Object list Set replaced", zap.String("obj", object.ID().ShortStringEx()), zap.Uint64("tableID", object.table.ID))
	}
}

// DropObjectByID appends a delete node as a marker, used in SoftDeleteObject
func (l *ObjectList) DropObjectByID(
	objectID *objectio.ObjectId,
	txn txnif.TxnReader,
) (*ObjectEntry, bool, error) {
	return l.dropObjectByID(objectID, txn, false)
}

func (l *ObjectList) dropObjectByID(
	objectID *objectio.ObjectId,
	txn txnif.TxnReader,
	deleteByCN bool,
) (
	droppedObj *ObjectEntry,
	isNew bool,
	err error,
) {
	obj, err := l.GetObjectByID(objectID)
	if err != nil {
		return
	}
	if obj.HasDropIntent() {
		logutil.Error("DropObjectByID HasDropIntent", zap.String("obj", objectID.ShortStringEx()))
		return nil, false, moerr.GetOkExpectedEOB()
	}
	if !obj.DeleteNode.IsEmpty() {
		panic("logic error")
	}
	needWait, txnToWait := obj.CreateNode.NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.CreateNode.CheckConflict(txn); err != nil {
		return nil, false, err
	}
	droppedObj, updatedCEntry, isNew := obj.GetDropEntry(txn, deleteByCN)
	if !isNew && obj.IsCreating() {
		tableDesc := fmt.Sprintf("%v-%s", obj.table.ID, obj.table.GetLastestSchema(false).Name)
		logutil.Error("DropObjectByID IsCreating", zap.String("obj", objectID.ShortStringEx()), zap.String("table", tableDesc))
		return nil, false, moerr.NewNYINoCtx("DropObjectByID creating obj.")
	}
	// insert the D Entry and update the C Entry
	l.modify(nil, droppedObj, updatedCEntry)
	return
}

// UpdateObjectInfo must be called after DropObjectByID in a txn refer to flushTableTail
func (l *ObjectList) UpdateObjectInfo(
	obj *ObjectEntry,
	txn txnif.TxnReader,
	stats *objectio.ObjectStats,
) (isNew bool, err error) {
	needWait, txnToWait := obj.GetLastMVCCNode().NeedWaitCommitting(txn.GetStartTS())
	if needWait {
		txnToWait.GetTxnState(true)
	}
	if err := obj.GetLastMVCCNode().CheckConflict(txn); err != nil {
		return false, err
	}
	newDroppedObj, udpateCEntry, isNew := obj.GetUpdateEntry(txn, stats)
	if isNew {
		tableDesc := fmt.Sprintf("%v-%s", obj.table.ID, obj.table.GetLastestSchema(false).Name)
		logutil.Error("UpdateObjectInfo Before Deleting", zap.String("obj", obj.ID().ShortStringEx()), zap.String("table", tableDesc))
		return false, moerr.NewNYINoCtx("UpdateObjectInfo before deleting.")
	}
	// replace the D entry and update the C entry
	l.modify(nil, newDroppedObj, udpateCEntry)
	return
}

// deleteEntryLocked deletes all entries with the given objectID, used in GC & Rollback
func (l *ObjectList) DeleteAllEntries(id *objectio.ObjectId) error {
	l.Lock()
	defer func() {
		tableID, counts := l.appendMaxMetricSnapshotLocked()
		l.Unlock()
		l.publishAppendMaxMetrics(tableID, counts)
		l.requestBookmarkRebuild()
	}()
	index, ok := l.objectID_index[*id]
	if !ok {
		return nil
	}
	oldTrees := l.loadTrees()
	mutableTrees := newMutableObjectListTrees(oldTrees)
	clearObjectListBookmarks(mutableTrees.next)
	l.bookmarkVersion++
	newVisibleTree := mutableTrees.next.visible
	objs := l.getNodesSnap(oldTrees, index, id, false)
	for _, obj := range objs {
		mutableTrees.delete(obj)
		if !obj.IsDEntry() {
			newVisibleTree.Delete(obj)
		}
		delete(l.objectID_index, *obj.ID())
		delete(l.appendMaxes, *obj.ID())
		l.deleteAppendMaxStateLocked(obj.ID())
		delete(l.restartSealed, *obj.ID())
	}
	l.bookmarkVersion++
	clearObjectListBookmarks(mutableTrees.next)
	ok = l.trees.CompareAndSwap(oldTrees, mutableTrees.next)
	if !ok {
		panic("concurrent mutation")
	}
	return nil
}

func (l *ObjectList) UpdateCreateTS(id *objectio.ObjectId, ts types.TS) (*ObjectEntry, error) {
	l.Lock()
	defer func() {
		l.Unlock()
		l.requestBookmarkRebuild()
	}()
	oldIndex, ok := l.objectID_index[*id]
	if !ok {
		return nil, moerr.GetOkExpectedEOB()
	}
	oldTrees := l.loadTrees()
	mutableTrees := newMutableObjectListTrees(oldTrees)
	clearObjectListBookmarks(mutableTrees.next)
	l.bookmarkVersion++
	newVisibleTree := mutableTrees.next.visible
	nodes := l.getNodesSnap(oldTrees, oldIndex, id, true)
	if len(nodes) == 0 {
		return nil, moerr.GetOkExpectedEOB()
	}
	oldNode := nodes[0]
	newNode := oldNode.Clone()
	if oldNode.IsDEntry() {
		newPrev := oldNode.prevVersion.Clone()
		newPrev.CreatedAt = ts
		newPrev.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		newPrev.nextVersion = newNode
		newNode.CreatedAt = ts
		newNode.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		newNode.prevVersion = newPrev
		mutableTrees.delete(oldNode)
		mutableTrees.delete(oldNode.prevVersion)
		mutableTrees.set(newNode)
		mutableTrees.set(newPrev)
		newVisibleTree.Delete(oldNode.prevVersion)
		newVisibleTree.Set(newPrev)
	} else {
		newNode.CreatedAt = ts
		newNode.CreateNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		mutableTrees.delete(oldNode)
		mutableTrees.set(newNode)
		newVisibleTree.Delete(oldNode)
		newVisibleTree.Set(newNode)
	}
	l.objectID_index[*id] = objectListIndex{
		ts:    newNode.ObjectListCommitTS(),
		group: newNode.ObjectListGroup(),
	}
	if !l.trees.CompareAndSwap(oldTrees, mutableTrees.next) {
		panic("concurrent mutation")
	}
	return newNode, nil
}

// WaitUntilCommitted checks the uncommitted tail of every group. When it
// returns, all creating objects that can be visible to ts have committed.
func (l *ObjectList) WaitUntilCommitted(ts types.TS) {
	it := l.loadTree().Iter()
	defer it.Release()
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		for ok := SeekObjectListGroup(&it, group, txnif.UncommitTS); ok; ok = it.Next() {
			obj := it.Item()
			if obj.ObjectListGroup() != group {
				break
			}
			if obj.IsCommitted() {
				continue
			}
			if needWait, txn := obj.CreateNode.NeedWaitCommitting(ts); needWait {
				txn.GetTxnState(true)
			}
			if needWait, txn := obj.DeleteNode.NeedWaitCommitting(ts); needWait {
				txn.GetTxnState(true)
			}
		}
	}
}

// Iterator part

// ObjectListSnapshot pins the atomically published set of group trees so a
// multi-group scan cannot mix catalog generations.
type ObjectListSnapshot struct {
	trees *objectListTrees
}

func (snapshot ObjectListSnapshot) Group(group ObjectListGroup) btree.IterG[*ObjectEntry] {
	return snapshot.trees.group(group).Iter()
}

func (snapshot ObjectListSnapshot) ScanGroup(
	group ObjectListGroup,
	fn func(*ObjectEntry) bool,
) {
	snapshot.trees.group(group).Scan(fn)
}

// CommitBookmarkStart returns the first object whose prefix max commit can
// reach from. A valid bookmark with a nil entry means the whole group is
// older than from and can be skipped.
func (snapshot ObjectListSnapshot) CommitBookmarkStart(
	group ObjectListGroup, from types.TS,
) (*ObjectEntry, bool) {
	start, valid, _ := snapshot.CommitBookmarkStartWithCount(group, from)
	return start, valid
}

// CommitBookmarkStartWithCount is the instrumentable form of
// CommitBookmarkStart. skipped is the number of group entries proven to have
// max commit timestamps below from by the prefix bookmark.
func (snapshot ObjectListSnapshot) CommitBookmarkStartWithCount(
	group ObjectListGroup, from types.TS,
) (*ObjectEntry, bool, int) {
	bookmark := snapshot.trees.bookmarks[group]
	if bookmark == nil {
		return nil, false, 0
	}
	lo, hi := 0, len(bookmark.entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bookmark.entries[mid].maxThrough.LT(&from) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(bookmark.entries) {
		return nil, true, lo
	}
	return bookmark.entries[lo].entry, true, lo
}

// ScanGroupFrom scans a group from an entry retained by the same immutable
// snapshot. The entry is obtained from CommitBookmarkStart, so this avoids a
// second seek/key construction in the hot dedup path.
func (snapshot ObjectListSnapshot) ScanGroupFrom(
	group ObjectListGroup,
	start *ObjectEntry,
	fn func(*ObjectEntry) bool,
) {
	if start == nil {
		return
	}
	snapshot.trees.group(group).Ascend(start, fn)
}

func (snapshot ObjectListSnapshot) AscendGroup(
	group ObjectListGroup,
	ts types.TS,
	fn func(*ObjectEntry) bool,
) {
	var minID objectio.ObjectId
	var key ObjectEntry
	initObjectListKey(&key, group, ts, &minID)
	snapshot.trees.group(group).Ascend(&key, fn)
}

var _iterPool = sync.Pool{New: func() any {
	return &VisibleCommittedObjectIt{}
}}

type VisibleCommittedObjectIt struct {
	iter        btree.IterG[*ObjectEntry]
	curr        *ObjectEntry
	txn         txnif.TxnReader
	isMockTxn   bool
	firstCalled bool
}

// MakeVisibleCommittedObjectIt returns an iterator that iterates over committed objects visible to the given txn
// two cases:
// 2. normal txn, wait if needed, return committed non-dropped objects
// 1. txn is mock txn, no waiting, only return committed non-dropped objects, used for status check

func (l *ObjectList) MakeVisibleCommittedObjectIt(txn txnif.TxnReader) *VisibleCommittedObjectIt {
	it := _iterPool.Get().(*VisibleCommittedObjectIt)
	it.iter = l.loadTrees().visible.Iter()
	it.txn = txn
	it.isMockTxn = len(txn.GetCtx()) == 0
	return it
}

func (it *VisibleCommittedObjectIt) Next() bool {
	var ok bool
	for {
		if !it.firstCalled {
			ok = it.iter.Last()
			it.firstCalled = true
		} else {
			ok = it.iter.Prev()
		}
		if !ok {
			return false
		}
		entry := it.iter.Item()
		if it.isMockTxn {
			if !entry.IsCreating() && !entry.HasDCounterpart() {
				it.curr = entry
				return true
			}
		} else if entry.IsVisible(it.txn) {
			if !entry.HasDCounterpart() || !entry.GetNextVersion().IsVisible(it.txn) {
				it.curr = entry
				return true
			}
		}
	}
}

func (it *VisibleCommittedObjectIt) Item() *ObjectEntry {
	return it.curr
}

func (it *VisibleCommittedObjectIt) Release() {
	if it.txn == nil {
		logutil.Errorf("attempt to put iter %p into pool twice", it)
		return
	}
	it.iter.Release()
	it.curr = nil
	it.txn = nil
	it.firstCalled = false
	it.isMockTxn = false
	_iterPool.Put(it)
}

// utils

// Show returns a string representation of the objectlist
func (l *ObjectList) Show() string {
	l.RLock()
	defer l.RUnlock()
	it := l.loadTree().Iter()
	defer it.Release()
	ret := ""
	for it.Next() {
		ret += " " + it.Item().StringWithLevel(common.PPL2) + "\n"
	}
	ret += "objectID_index:\n"
	for id, index := range l.objectID_index {
		ret += fmt.Sprintf(" %s: %s-%d\n", id.ShortStringEx(), index.ts.ToString(), index.group)
	}
	return ret
}

func makeObjectListKey(group ObjectListGroup, ts types.TS, id *objectio.ObjectId) *ObjectEntry {
	key := &ObjectEntry{}
	initObjectListKey(key, group, ts, id)
	return key
}

func initObjectListKey(key *ObjectEntry, group ObjectListGroup, ts types.TS, id *objectio.ObjectId) {
	appendable := group < ObjectListGroupNonAppendableCreate
	var stats objectio.ObjectStats
	copy(stats[:objectio.ObjectIDSize], id[:])
	objectio.SetObjectStatsAppendable(&stats, appendable)
	*key = ObjectEntry{
		EntryMVCCNode: EntryMVCCNode{CreatedAt: ts},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: stats,
		},
	}
	switch group {
	case ObjectListGroupAppendableCreateWithDrop, ObjectListGroupNonAppendableCreateWithDrop:
		key.nextVersion = objectListVersionMarker
	case ObjectListGroupAppendableDrop, ObjectListGroupNonAppendableDrop:
		key.CreatedAt = types.TS{}
		key.DeletedAt = ts
		key.prevVersion = objectListVersionMarker
	}
}

func makeVisibleObjectListKey(ts types.TS, id *objectio.ObjectId) *ObjectEntry {
	var stats objectio.ObjectStats
	copy(stats[:objectio.ObjectIDSize], id[:])
	return &ObjectEntry{
		EntryMVCCNode: EntryMVCCNode{CreatedAt: ts},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: stats,
		},
	}
}

var (
	// Sort pivots only need the version link's nil/non-nil state.
	objectListVersionMarker = &ObjectEntry{}

	// Dynamic group seeks cannot share a pivot because their timestamps vary.
	// BTree seek does not retain or mutate its key, so recycle these large
	// ObjectEntry-shaped pivots instead of allocating one for every seek.
	objectListSeekKeyPool = sync.Pool{New: func() any {
		return &ObjectEntry{}
	}}

	// These immutable pivots are shared by hot visibility scans. BTree Seek only
	// compares a pivot and never retains or mutates it.
	objectListUncommittedMinKeys = makeObjectListUncommittedKeys(false)
	objectListUncommittedMaxKeys = makeObjectListUncommittedKeys(true)
)

func makeObjectListUncommittedKeys(maxID bool) [ObjectListGroupNonAppendableDrop + 1]*ObjectEntry {
	var id objectio.ObjectId
	if maxID {
		for i := range id {
			id[i] = 0xff
		}
	}
	var keys [ObjectListGroupNonAppendableDrop + 1]*ObjectEntry
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		keys[group] = makeObjectListKey(group, txnif.UncommitTS, &id)
	}
	return keys
}

func acquireObjectListSeekKey(
	group ObjectListGroup,
	ts types.TS,
	id *objectio.ObjectId,
) *ObjectEntry {
	key := objectListSeekKeyPool.Get().(*ObjectEntry)
	initObjectListKey(key, group, ts, id)
	return key
}

func releaseObjectListSeekKey(key *ObjectEntry) {
	objectListSeekKeyPool.Put(key)
}

func SeekObjectListGroup(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var key *ObjectEntry
	if ts == txnif.UncommitTS && group <= ObjectListGroupNonAppendableDrop {
		key = objectListUncommittedMinKeys[group]
	} else {
		var minID objectio.ObjectId
		key = acquireObjectListSeekKey(group, ts, &minID)
		defer releaseObjectListSeekKey(key)
	}
	if !it.Seek(key) {
		return false
	}
	return it.Item().ObjectListGroup() == group
}

func SeekObjectListGroupBefore(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var minID objectio.ObjectId
	key := acquireObjectListSeekKey(group, ts, &minID)
	defer releaseObjectListSeekKey(key)
	if it.Seek(key) {
		if !it.Prev() {
			return false
		}
	} else if !it.Last() {
		return false
	}
	return it.Item().ObjectListGroup() == group
}

func SeekObjectListGroupReverse(
	it *btree.IterG[*ObjectEntry],
	group ObjectListGroup,
	ts types.TS,
) bool {
	var key *ObjectEntry
	if ts == txnif.UncommitTS && group <= ObjectListGroupNonAppendableDrop {
		key = objectListUncommittedMaxKeys[group]
	} else {
		var maxID objectio.ObjectId
		for i := range maxID {
			maxID[i] = 0xff
		}
		key = acquireObjectListSeekKey(group, ts, &maxID)
		defer releaseObjectListSeekKey(key)
	}
	if it.Seek(key) {
		item := it.Item()
		if item.ObjectListGroup() == group {
			commitTS := item.ObjectListCommitTS()
			if commitTS.LE(&ts) {
				return true
			}
		}
		if !it.Prev() {
			return false
		}
	} else if !it.Last() {
		return false
	}
	return it.Item().ObjectListGroup() == group
}
