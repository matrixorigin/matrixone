// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	defaultExportedDirMode = 0755
	snapshotPathPattern    = "snapshot-%016X"
)

// snapshotIndex is type indicates the index of snapshot.
type snapshotIndex uint64

// nodeID contains shardID and replicaID.
type nodeID struct {
	shardID   uint64
	replicaID uint64
}

// snapshotItem represents a snapshot item, with index and directory in it.
type snapshotItem struct {
	fs    vfs.FS
	index snapshotIndex
	dir   string
}

var snapshotItemPool = sync.Pool{
	New: func() interface{} {
		return new(snapshotItem)
	},
}

func getSnapshotItem(si snapshotItem) *snapshotItem {
	item := snapshotItemPool.Get().(*snapshotItem)
	*item = si
	return item
}

func putSnapshotItem(item *snapshotItem) {
	*item = snapshotItem{}
	snapshotItemPool.Put(item)
}

// Exists implements the ISnapshotItem interface.
func (si *snapshotItem) Exists() bool {
	_, err := si.fs.Stat(si.dir)
	return err == nil
}

// Valid implements the ISnapshotItem interface.
func (si *snapshotItem) Valid() (bool, error) {
	names, err := si.fs.List(si.dir)
	if err != nil {
		return false, err
	}
	if len(names) != 2 {
		return false, moerr.NewInternalErrorNoCtxf("file number is not correct: %d", len(names))
	}
	sort.Strings(names)
	var index uint64
	_, err = fmt.Sscanf(names[0], snapshotPathPattern+".gbsnap", &index)
	if err != nil {
		return false, err
	}
	if snapshotIndex(index) != si.index {
		return false, moerr.NewInternalErrorNoCtxf("index of dir %d and file %d are different",
			si.index, index)
	}
	if names[1] != "snapshot.metadata" {
		return false, moerr.NewInternalErrorNoCtx("no snapshot.metadata file")
	}
	return true, nil
}

// snapshotRecord contains snapshot items for the specified shard.
type snapshotRecord struct {
	fs     vfs.FS
	nodeID nodeID
	// items are sorted by .index
	items []*snapshotItem
	// An export request can return before Dragonboat finishes writing it.
	// Keep checking disk on later truncation ticks after that result is unknown.
	uncertainExport bool
	// An admitted export can outlive its RPC. A request that returned early may still
	// publish a final directory, even after StopReplica returns.
	activeExports      int
	lateExportPossible bool
	// Keep the Dragonboat result after the caller's shorter context expires.
	// The truncation worker owns and consumes this single pending result.
	pendingExport *dragonboat.RequestState
	// StartReplica has confirmed that the previous replica's snapshot worker
	// has stopped. Wait for its caller to return before reconciling old files.
	previousReplicaStopped bool
	// A confirmed failed export left an abandoned directory that could not
	// be removed. Retry cleanup before the next export at the same index.
	failedExportCleanup bool
}

var errSnapshotExportPending = moerr.NewInternalErrorNoCtx("exported snapshot may still be written")

func newNodeSnapshot(fs vfs.FS, nodeID nodeID) *snapshotRecord {
	return &snapshotRecord{
		fs:     fs,
		nodeID: nodeID,
		items:  make([]*snapshotItem, 0),
	}
}

func (ss *snapshotRecord) first() *snapshotItem {
	if len(ss.items) == 0 {
		return nil
	}
	return ss.items[0]
}

func (ss *snapshotRecord) last() *snapshotItem {
	l := len(ss.items)
	if l == 0 {
		return nil
	}
	return ss.items[l-1]
}

func (ss *snapshotRecord) release() {
	for _, item := range ss.items {
		putSnapshotItem(item)
	}
	ss.items = nil
}

func (ss *snapshotRecord) add(index snapshotIndex, dir string) error {
	last := ss.last()
	if last != nil {
		if index < last.index {
			return moerr.NewInternalErrorNoCtxf("snapshot with smaller index %d than current biggest one %d",
				index, last.index)
		}
		if index == last.index {
			return nil
		}
	}
	si := getSnapshotItem(snapshotItem{fs: ss.fs, index: index, dir: dir})
	if !si.Exists() {
		return moerr.NewInternalErrorNoCtxf("snapshot file does not exist for shard-replica %d-%d, index %d, dir %s",
			ss.nodeID.shardID, ss.nodeID.replicaID, index, dir)
	}
	v, err := si.Valid()
	if err != nil {
		return err
	}
	if v {
		ss.items = append(ss.items, si)
	}
	return nil
}

// removeAt renames a completed export before deleting its contents. Once the
// canonical name is gone, the item must leave the inventory even if cleanup
// fails: the durable .removing name owns the retry across a process restart.
func (ss *snapshotRecord) removeAt(i int) error {
	item := ss.items[i]
	parent := filepath.Dir(item.dir)
	removing := ss.fs.PathJoin(parent,
		fmt.Sprintf(snapshotPathPattern+"-%d.removing", item.index, ss.nodeID.replicaID))
	renamed := true
	if err := ss.fs.Rename(item.dir, removing); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// A missing export cannot be renamed. Confirm the canonical path is
		// gone before releasing its inventory slot; an ENOENT from Rename
		// while the source still exists or a transient Stat error must retry.
		if _, statErr := ss.fs.Stat(item.dir); !errors.Is(statErr, os.ErrNotExist) {
			return errors.Join(err, statErr)
		}
		renamed = false
	}
	copy(ss.items[i:], ss.items[i+1:])
	ss.items[len(ss.items)-1] = nil
	ss.items = ss.items[:len(ss.items)-1]
	putSnapshotItem(item)
	if !renamed {
		return nil
	}
	if err := syncExportDir(ss.fs, parent); err != nil {
		ss.failedExportCleanup = true
		return err
	}
	if err := ss.fs.RemoveAll(removing); err != nil {
		ss.failedExportCleanup = true
		return err
	}
	return nil
}

// removeFirst removes the first snapshot from snapshot item list.
func (ss *snapshotRecord) removeFirst() error {
	if len(ss.items) == 0 {
		return nil
	}
	return ss.removeAt(0)
}

// removeLast removes the last (newest) snapshot from the snapshot item
// list. Used by the truncation loop's timeout-path escape valve to
// prefer preserving older items, which are more likely to be importable
// once the next SyncRead recovers. See issue #24315.
func (ss *snapshotRecord) removeLast() error {
	l := len(ss.items)
	if l == 0 {
		return nil
	}
	return ss.removeAt(l - 1)
}

// removeIndex removes a known unusable export without disturbing newer or
// older candidates. The Raft log is compacted only after a successful import.
func (ss *snapshotRecord) removeIndex(index snapshotIndex) (bool, error) {
	for i, item := range ss.items {
		if item.index != index {
			continue
		}
		if err := ss.removeAt(i); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// remove the snapshots whose index is LE than the index.
func (ss *snapshotRecord) remove(index snapshotIndex) error {
	var cleanupErr error
	for first := ss.first(); first != nil && first.index <= index; first = ss.first() {
		count := len(ss.items)
		if err := ss.removeFirst(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			if len(ss.items) == count {
				// Rename failed, so the current item is still canonical and
				// retained in the inventory for an explicit retry.
				return cleanupErr
			}
			// A tombstone owns this item's retry. Finish removing the other
			// images already covered by the successful Raft import.
		}
	}
	return cleanupErr
}

// snapshotManager manages the exported snapshots created by dragonboat.
// In dragonboat, snapshot are taken with method SyncRequestSnapshot,
// which accepts a snapshot option. By default, a new snapshot will be
// taken at the applied index and log entries are removed at the LSN
// parameter which is in snapshot option. LSN must be less than the applied
// index. This works in normal state machine.
// Unfortunately, there are two separate state machines in MO. One is
// in logservice and the other is in DN. The snapshot cannot be taken
// like that in this case, because when a replica starts, it applies the
// latest snapshot, whose index is greater than truncate LSN. As a result,
// the TN cannot read the log entries between truncate LSN and snapshot
// index to replay.
// The solution is, set Exported to true in snapshot option. This prevents
// taking an active snapshot, and just export the snapshot files to an external
// directory. When truncate LSN is greater than any snapshot index in the
// external directory, the snapshot would be imported to system. And then,
// the log entries can be removed safely.
type snapshotManager struct {
	mu        sync.RWMutex
	cfg       *Config
	snapshots map[nodeID]*snapshotRecord // shardID => *snapshotRecord
}

// newSnapshotManager makes a new snapshot.
func newSnapshotManager(cfg *Config) *snapshotManager {
	return &snapshotManager{
		cfg:       cfg,
		snapshots: make(map[nodeID]*snapshotRecord),
	}
}

func (sm *snapshotManager) exportPath(shardID uint64, replicaID uint64) string {
	parts := make([]string, 0, 3)
	dir := sm.cfg.SnapshotExportDir
	shardPart := fmt.Sprintf("shard-%d", shardID)
	replicaPart := fmt.Sprintf("replica-%d", replicaID)
	parts = append(parts, dir, shardPart, replicaPart)
	return filepath.Join(parts...)
}

func (sm *snapshotManager) snapshotPath(nodeID nodeID, index snapshotIndex) string {
	parts := make([]string, 0, 2)
	snapshotPart := fmt.Sprintf(snapshotPathPattern, index)
	parts = append(parts, sm.exportPath(nodeID.shardID, nodeID.replicaID), snapshotPart)
	return filepath.Join(parts...)
}

func (sm *snapshotManager) prepareDir(path string) error {
	s, err := sm.cfg.FS.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if e := sm.cfg.FS.MkdirAll(path, defaultExportedDirMode); e != nil {
			return e
		}
		return nil
	}
	if !s.IsDir() {
		return moerr.NewInternalErrorNoCtxf("%s is not a dir", path)
	}
	return nil
}

// ReconcileInterruptedExport removes exports abandoned by a stopped worker.
// A final directory is renamed before deletion so partial deletion remains
// discoverable on the next retry or after a process restart.
func (sm *snapshotManager) ReconcileInterruptedExport(shardID, replicaID uint64) error {
	path := sm.exportPath(shardID, replicaID)
	if err := sm.prepareDir(path); err != nil {
		return err
	}
	names, err := sm.cfg.FS.List(path)
	if err != nil {
		return err
	}
	// Finish removals from earlier attempts before renaming new candidates.
	for _, name := range names {
		var index uint64
		var from uint64
		count, err := fmt.Sscanf(name, snapshotPathPattern+"-%d.removing", &index, &from)
		if err == nil && count == 2 && from == replicaID &&
			name == fmt.Sprintf(snapshotPathPattern+"-%d.removing", index, replicaID) {
			// A previous parent sync may have failed. Make the tombstone durable
			// before a retry can remove the flag and metadata beneath it.
			if err := syncExportDir(sm.cfg.FS, path); err != nil {
				return err
			}
			if err := sm.cfg.FS.RemoveAll(sm.cfg.FS.PathJoin(path, name)); err != nil {
				return err
			}
			continue
		}
		count, err = fmt.Sscanf(name, snapshotPathPattern+"-%d.generating", &index, &from)
		if err == nil && count == 2 && from == replicaID &&
			name == fmt.Sprintf(snapshotPathPattern+"-%d.generating", index, replicaID) {
			if err := sm.cfg.FS.RemoveAll(sm.cfg.FS.PathJoin(path, name)); err != nil {
				return err
			}
		}
	}
	for _, name := range names {
		var index uint64
		count, err := fmt.Sscanf(name, snapshotPathPattern, &index)
		if err != nil || count != 1 || name != fmt.Sprintf(snapshotPathPattern, index) {
			continue
		}
		dir := sm.cfg.FS.PathJoin(path, name)
		_, err = sm.cfg.FS.Stat(sm.cfg.FS.PathJoin(dir, "dragonboat.snapshot.message"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		removing := sm.cfg.FS.PathJoin(path,
			fmt.Sprintf(snapshotPathPattern+"-%d.removing", index, replicaID))
		if err := sm.cfg.FS.Rename(dir, removing); err != nil {
			return err
		}
		// Make the recovery name durable before RemoveAll can erase the flag
		// and metadata that identified the original directory.
		if err := syncExportDir(sm.cfg.FS, path); err != nil {
			return err
		}
		if err := sm.cfg.FS.RemoveAll(removing); err != nil {
			return err
		}
	}
	return nil
}

func syncExportDir(fs vfs.FS, path string) error {
	parent, err := fs.OpenDir(path)
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

// Init implements the ISnapshotManager interface.
func (sm *snapshotManager) Init(shardID uint64, replicaID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.initLocked(shardID, replicaID)
}

func (sm *snapshotManager) initLocked(shardID uint64, replicaID uint64) error {
	snapshots, err := sm.loadCompletedSnapshots(shardID, replicaID)
	if err != nil {
		return err
	}
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if current, ok := sm.snapshots[nid]; ok {
		snapshots.uncertainExport = current.uncertainExport
		snapshots.activeExports = current.activeExports
		snapshots.lateExportPossible = current.lateExportPossible
		snapshots.pendingExport = current.pendingExport
		snapshots.previousReplicaStopped = current.previousReplicaStopped
		snapshots.failedExportCleanup = current.failedExportCleanup
		current.release()
	}
	sm.snapshots[nid] = snapshots
	return nil
}

// loadCompletedSnapshots builds a fresh view without changing the published
// one. Dragonboat can be writing while a runtime reconciliation scans, so
// only canonical final directories with the complete two-file layout enter
// the view; marker-bearing or incomplete directories remain for later scans.
func (sm *snapshotManager) loadCompletedSnapshots(shardID uint64, replicaID uint64) (*snapshotRecord, error) {
	path := sm.exportPath(shardID, replicaID)
	if err := sm.prepareDir(path); err != nil {
		return nil, err
	}
	names, err := sm.cfg.FS.List(path)
	if err != nil {
		return nil, err
	}
	indexes := make([]snapshotIndex, 0, len(names))
	for _, name := range names {
		var index uint64
		count, err := fmt.Sscanf(name, snapshotPathPattern, &index)
		if err != nil || count != 1 || index == 0 || name != fmt.Sprintf(snapshotPathPattern, index) {
			continue
		}
		indexes = append(indexes, snapshotIndex(index))
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	snapshots := newNodeSnapshot(sm.cfg.FS, nid)
	for _, index := range indexes {
		dir := sm.snapshotPath(nid, index)
		info, err := sm.cfg.FS.Stat(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			snapshots.release()
			return nil, err
		}
		if !info.IsDir() {
			continue
		}
		files, err := sm.cfg.FS.List(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			snapshots.release()
			return nil, err
		}
		sort.Strings(files)
		if len(files) == 2 && files[0] == fmt.Sprintf(snapshotPathPattern+".gbsnap", index) &&
			files[1] == "snapshot.metadata" {
			snapshots.items = append(snapshots.items, getSnapshotItem(snapshotItem{
				fs: sm.cfg.FS, index: index, dir: dir,
			}))
		}
	}
	return snapshots, nil
}

// MarkExportUncertain records that a failed request may still finish writing
// an exported snapshot after its caller has returned.
func (sm *snapshotManager) MarkExportUncertain(shardID, replicaID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		ss = newNodeSnapshot(sm.cfg.FS, nid)
		sm.snapshots[nid] = ss
	}
	ss.uncertainExport = true
}

// RetainPendingExport transfers a timed-out caller's request to the
// truncation worker. Its Dragonboat result remains authoritative even though
// the caller no longer waits for it.
func (sm *snapshotManager) RetainPendingExport(shardID, replicaID uint64, rs *dragonboat.RequestState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	ss.pendingExport = rs
	ss.activeExports--
	ss.uncertainExport = true
	ss.lateExportPossible = true
}

func (sm *snapshotManager) HasUnresolvedExport(shardID, replicaID uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	return ss != nil && (ss.activeExports > 0 || ss.pendingExport != nil || ss.lateExportPossible)
}

// MarkPreviousReplicaStopped is called only after Dragonboat accepts a new
// incarnation. StartReplica rejects it while the old snapshot worker remains
// loaded, so this is a safe point to schedule cleanup of the old export.
func (sm *snapshotManager) MarkPreviousReplicaStopped(shardID, replicaID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	if ss == nil || (ss.activeExports == 0 && ss.pendingExport == nil && !ss.lateExportPossible) {
		return
	}
	ss.previousReplicaStopped = true
	ss.failedExportCleanup = true
	ss.uncertainExport = true
}

// PollPendingExport returns only a final Dragonboat result. An unresolved
// request keeps the replica fenced so cleanup cannot race its worker.
func (sm *snapshotManager) PollPendingExport(shardID, replicaID uint64) (dragonboat.RequestResult, bool, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	if ss == nil {
		return dragonboat.RequestResult{}, false, false
	}
	if ss.previousReplicaStopped {
		if ss.activeExports > 0 {
			return dragonboat.RequestResult{}, false, true
		}
		if ss.pendingExport != nil {
			ss.pendingExport.Release()
			ss.pendingExport = nil
		}
		ss.lateExportPossible = false
		ss.previousReplicaStopped = false
		return dragonboat.RequestResult{}, false, false
	}
	if ss.pendingExport == nil {
		return dragonboat.RequestResult{}, false, ss.lateExportPossible
	}
	select {
	case result := <-ss.pendingExport.AppliedC():
		ss.pendingExport.Release()
		ss.pendingExport = nil
		if result.Completed() || result.Rejected() || result.Aborted() || result.Error() != nil {
			ss.lateExportPossible = false
		}
		return result, true, ss.lateExportPossible
	default:
		return dragonboat.RequestResult{}, false, true
	}
}

func (sm *snapshotManager) MarkFailedExportCleanup(shardID, replicaID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		ss = newNodeSnapshot(sm.cfg.FS, nid)
		sm.snapshots[nid] = ss
	}
	ss.failedExportCleanup = true
}

func (sm *snapshotManager) NeedsFailedExportCleanup(shardID, replicaID uint64) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	return ss != nil && ss.failedExportCleanup
}

func (sm *snapshotManager) ClearFailedExportCleanup(shardID, replicaID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]; ss != nil {
		ss.failedExportCleanup = false
	}
}

// BeginExport is called while store.mu protects the local retirement marker.
// Only one request may own a replica's export directory at a time.
func (sm *snapshotManager) BeginExport(shardID, replicaID uint64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		ss = newNodeSnapshot(sm.cfg.FS, nid)
		sm.snapshots[nid] = ss
	}
	if ss.activeExports > 0 || ss.pendingExport != nil || ss.lateExportPossible || ss.failedExportCleanup {
		return false
	}
	ss.activeExports++
	return true
}

// FinishExport keeps a request with an unknown result fenced until process
// restart. Asynchronous requests are instead retained and polled to completion.
func (sm *snapshotManager) FinishExport(shardID, replicaID uint64, mayFinishLater bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	if mayFinishLater {
		ss.uncertainExport = true
		ss.lateExportPossible = true
	}
	ss.activeExports--
}

// ReconcileUncertainExports reloads completed snapshots after an export with
// an unknown result. Other callers can mark an export uncertain without
// retaining its request, so keep scanning for their late completion.
func (sm *snapshotManager) ReconcileUncertainExports(shardID, replicaID uint64, maxCount int) (uint64, bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok || !ss.uncertainExport {
		return 0, false, nil
	}
	next, err := sm.loadCompletedSnapshots(shardID, replicaID)
	if err != nil {
		return 0, false, err
	}
	limit := maxCount
	if limit < 1 {
		limit = 1 // preserve the oldest import candidate even with quota zero
	}
	// A late completion can put disk over quota before the next scan. Drop
	// newest exports first, retaining the oldest importable candidate.
	trimmed := false
	for len(next.items) > limit {
		if err := next.removeLast(); err != nil {
			if next.failedExportCleanup {
				ss.failedExportCleanup = true
			}
			next.release()
			return 0, trimmed, err
		}
		trimmed = true
	}
	next.uncertainExport = true
	next.activeExports = ss.activeExports
	next.lateExportPossible = ss.lateExportPossible
	next.pendingExport = ss.pendingExport
	next.previousReplicaStopped = ss.previousReplicaStopped
	next.failedExportCleanup = ss.failedExportCleanup
	sm.snapshots[nid] = next
	ss.release()
	if last := next.last(); last != nil {
		return uint64(last.index), trimmed, nil
	}
	return 0, trimmed, nil
}

// Count implements the ISnapshotManager interface.
func (sm *snapshotManager) Count(shardID uint64, replicaID uint64) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if s, ok := sm.snapshots[nid]; ok {
		return len(s.items)
	}
	return 0
}

// Add implements the ISnapshotManager interface.
func (sm *snapshotManager) Add(shardID uint64, replicaID uint64, index uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.addLocked(shardID, replicaID, index)
}

func (sm *snapshotManager) addLocked(shardID uint64, replicaID uint64, index uint64) error {
	si := snapshotIndex(index)
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	dir := sm.snapshotPath(nid, si)
	_, ok := sm.snapshots[nid]
	if !ok {
		sm.snapshots[nid] = newNodeSnapshot(sm.cfg.FS, nid)
	}
	return sm.snapshots[nid].add(si, dir)
}

// Remove implements the ISnapshotManager interface.
func (sm *snapshotManager) Remove(shardID uint64, replicaID uint64, index uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	si := snapshotIndex(index)
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if ss, ok := sm.snapshots[nid]; ok {
		return ss.remove(si)
	}
	return nil
}

// RemoveReplica removes all exported snapshots tracked for the specified
// replica. It is used when local metadata still references a replica that has
// already been superseded on this store.
func (sm *snapshotManager) RemoveReplica(shardID uint64, replicaID uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if ss, ok := sm.snapshots[nid]; ok && (ss.activeExports > 0 || ss.lateExportPossible) {
		return errSnapshotExportPending
	}
	if err := sm.cfg.FS.RemoveAll(sm.exportPath(shardID, replicaID)); err != nil {
		return err
	}
	if ss, ok := sm.snapshots[nid]; ok {
		ss.release()
	}
	delete(sm.snapshots, nid)
	return nil
}

// NewestIndex returns the index of the newest exported snapshot tracked
// by the manager for (shardID, replicaID), or 0 if no items exist.
// Callers use this to realign shardSnapshotInfo.snapshotIndex after an
// escape-valve deletion so shouldDoExport does not suppress a needed
// re-export on a quiescent shard. See issue #24315.
func (sm *snapshotManager) NewestIndex(shardID uint64, replicaID uint64) uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		return 0
	}
	last := ss.last()
	if last == nil {
		return 0
	}
	return uint64(last.index)
}

// DropNewest drops the latest exported snapshot for (shardID, replicaID)
// from both memory and disk. Returns the index of the dropped item, or 0
// if no items exist.
//
// This is the escape valve used by the truncation loop when getTruncatedLsn
// times out while the export quota is full. Dropping an exported snapshot
// is always safe: the underlying raft log range is still present (tan WAL
// compaction only happens as a side effect of a successful
// importSnapshot), so a subsequent exportSnapshot can reproduce the same
// content. We drop the *newest* rather than the oldest because the
// oldest item is the most likely to be importable (its index <= current
// lsnInSM) once SyncRead recovers; preserving it keeps the shard on a
// direct path back to a successful import. The cost is at most one
// redundant export round. See issue #24315.
func (sm *snapshotManager) DropNewest(shardID uint64, replicaID uint64) (uint64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		return 0, nil
	}
	last := ss.last()
	if last == nil {
		return 0, nil
	}
	idx := uint64(last.index)
	if err := ss.removeLast(); err != nil {
		return 0, err
	}
	return idx, nil
}

// DropInvalid removes an exported snapshot whose contents have been checked
// and found unusable. A failed filesystem removal keeps it tracked for retry.
func (sm *snapshotManager) DropInvalid(shardID, replicaID, index uint64) (bool, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ss := sm.snapshots[nodeID{shardID: shardID, replicaID: replicaID}]
	if ss == nil {
		return false, nil
	}
	return ss.removeIndex(snapshotIndex(index))
}

// EvalImportSnapshot implements the ISnapshotManager interface.
func (sm *snapshotManager) EvalImportSnapshot(shardID uint64, replicaID uint64, index uint64) (string, uint64) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	ss, ok := sm.snapshots[nid]
	if !ok {
		return "", 0
	}

	var dir string
	var si snapshotIndex
	for _, item := range ss.items {
		if item == nil {
			return "", 0
		}
		// Find the bigger one, break and return the smaller one.
		if uint64(item.index) > index {
			break
		}
		dir = item.dir
		si = item.index
	}
	return dir, uint64(si)
}
