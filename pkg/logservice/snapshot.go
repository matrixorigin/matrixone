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
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	defaultExportedDirMode = 0755
	snapshotPathPattern    = "snapshot-%016X"
)

// ISnapshotManager is an interface that managers snapshots.
type ISnapshotManager interface {
	// Init initialize snapshots by loading exported snapshots.
	Init(shardID uint64, replicaID uint64) error
	// Count returns the number of snapshots in the manager.
	Count(shardID uint64, replicaID uint64) int
	// Add adds a new snapshot for specified shard.
	Add(shardID uint64, replicaID uint64, index uint64) error
	// Remove removes snapshots whose index is LE than index.
	Remove(shardID uint64, replicaID uint64, index uint64) error
	// EvalImportSnapshot returns the source directory and index of
	// the biggest snapshot of the shard.
	EvalImportSnapshot(shardID uint64, replicaID uint64, index uint64) (string, uint64)
}

// ISnapshotItem is an interface that represents a snapshot item.
type ISnapshotItem interface {
	// Exists returns if the snapshot item exists.
	Exists() bool
	// Remove removes the snapshot item.
	Remove() error
	// Valid check the legality of the snapshot item. Only the names of
	// the files in it are checked.
	Valid() (bool, error)
}

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

// Remove implements the ISnapshotItem interface.
func (si *snapshotItem) Remove() error {
	return si.fs.RemoveAll(si.dir)
}

// Valid implements the ISnapshotItem interface.
func (si *snapshotItem) Valid() (bool, error) {
	names, err := si.fs.List(si.dir)
	if err != nil {
		return false, err
	}
	if len(names) != 2 {
		return false, moerr.NewInternalErrorNoCtx("file number is not correct: %d", len(names))
	}
	sort.Strings(names)
	var index uint64
	_, err = fmt.Sscanf(names[0], snapshotPathPattern+".gbsnap", &index)
	if err != nil {
		return false, err
	}
	if snapshotIndex(index) != si.index {
		return false, moerr.NewInternalErrorNoCtx("index of dir %d and file %d are different",
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
}

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

func (ss *snapshotRecord) add(index snapshotIndex, dir string) error {
	last := ss.last()
	if last != nil && index < last.index {
		return moerr.NewInternalErrorNoCtx("snapshot with smaller index %d than current biggest one %d",
			index, last.index)
	}
	si := getSnapshotItem(snapshotItem{fs: ss.fs, index: index, dir: dir})
	if !si.Exists() {
		return moerr.NewInternalErrorNoCtx("snapshot file does not exist for shard-replica %d-%d, index %d, dir %s",
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

// removeFirst removes the first snapshot from snapshot item list.
func (ss *snapshotRecord) removeFirst() error {
	if first := ss.first(); first != nil {
		if err := first.Remove(); err != nil {
			return err
		}
	}
	putSnapshotItem(ss.items[0])
	ss.items = ss.items[1:]
	return nil
}

// remove the snapshots whose index is LE than the index.
func (ss *snapshotRecord) remove(index snapshotIndex) error {
	items := make([]*snapshotItem, len(ss.items))
	copy(items, ss.items)
	for _, si := range items {
		if si.index <= index {
			if err := ss.removeFirst(); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
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
// the DN cannot read the log entries between truncate LSN and snapshot
// index to replay.
// The solution is, set Exported to true in snapshot option. This prevents
// taking an active snapshot, and just export the snapshot files to an external
// directory. When truncate LSN is greater than any snapshot index in the
// external directory, the snapshot would be imported to system. And then,
// the log entries can be removed safely.
type snapshotManager struct {
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
	parts := make([]string, 3)
	dir := sm.cfg.SnapshotExportDir
	shardPart := fmt.Sprintf("shard-%d", shardID)
	replicaPart := fmt.Sprintf("replica-%d", replicaID)
	parts = append(parts, dir, shardPart, replicaPart)
	return filepath.Join(parts...)
}

func (sm *snapshotManager) snapshotPath(nodeID nodeID, index snapshotIndex) string {
	parts := make([]string, 2)
	snapshotPart := fmt.Sprintf(snapshotPathPattern, index)
	parts = append(parts, sm.exportPath(nodeID.shardID, nodeID.replicaID), snapshotPart)
	return filepath.Join(parts...)
}

func (sm *snapshotManager) prepareDir(path string) error {
	s, err := sm.cfg.FS.Stat(path)
	if err != nil {
		if e := sm.cfg.FS.MkdirAll(path, defaultExportedDirMode); e != nil {
			return e
		}
		return nil
	}
	if !s.IsDir() {
		return moerr.NewInternalErrorNoCtx("%s is not a dir", path)
	}
	return nil
}

func (sm *snapshotManager) parse(dir string) (int, error) {
	var index int
	_, err := fmt.Sscanf(dir, snapshotPathPattern, &index)
	if err != nil {
		return 0, err
	}
	return index, nil
}

// Init implements the ISnapshotManager interface.
func (sm *snapshotManager) Init(shardID uint64, replicaID uint64) error {
	path := sm.exportPath(shardID, replicaID)
	if err := sm.prepareDir(path); err != nil {
		return err
	}
	names, err := sm.cfg.FS.List(path)
	if err != nil {
		return err
	}
	indexes := make([]int, len(names))
	for _, name := range names {
		index, err := sm.parse(name)
		if err != nil {
			continue
		}
		indexes = append(indexes, index)
	}
	// The snapshots in the manager must be sorted.
	sort.Ints(indexes)
	for _, idx := range indexes {
		if idx > 0 {
			_ = sm.Add(shardID, replicaID, uint64(idx))
		}
	}
	return nil
}

// Count implements the ISnapshotManager interface.
func (sm *snapshotManager) Count(shardID uint64, replicaID uint64) int {
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if s, ok := sm.snapshots[nid]; ok {
		return len(s.items)
	}
	return 0
}

// Add implements the ISnapshotManager interface.
func (sm *snapshotManager) Add(shardID uint64, replicaID uint64, index uint64) error {
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
	si := snapshotIndex(index)
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	if ss, ok := sm.snapshots[nid]; ok {
		return ss.remove(si)
	}
	return nil
}

// EvalImportSnapshot implements the ISnapshotManager interface.
func (sm *snapshotManager) EvalImportSnapshot(shardID uint64, replicaID uint64, index uint64) (string, uint64) {
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
