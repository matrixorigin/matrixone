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
	"testing"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
)

func TestSnapshotItemValid(t *testing.T) {
	si := &snapshotItem{
		fs:    vfs.NewStrictMem(),
		index: 100,
		dir:   "/tmp/exported",
	}
	err := si.fs.MkdirAll(si.dir, defaultExportedDirMode)
	assert.NoError(t, err)

	_, err = si.fs.Create(si.dir + "/snapshot.metadata")
	assert.NoError(t, err)
	_, err = si.fs.Create(si.dir + "/snapshot-0000000000000064.gbsnap")
	assert.NoError(t, err)

	v, err := si.Valid()
	assert.Equal(t, true, v)
	assert.NoError(t, err)
}

func TestSnapshotItemInValid(t *testing.T) {
	si := &snapshotItem{
		fs:    vfs.NewStrictMem(),
		index: 100,
		dir:   "/tmp/exported",
	}
	err := si.fs.MkdirAll(si.dir, defaultExportedDirMode)
	assert.NoError(t, err)

	_, err = si.fs.Create(si.dir + "/snapshot.m")
	assert.NoError(t, err)
	_, err = si.fs.Create(si.dir + "/snapshot-0000000000000064.gbsnap")
	assert.NoError(t, err)

	v, err := si.Valid()
	assert.Equal(t, false, v)
	assert.Error(t, err)
}

func testPrepareSnapshot(
	t *testing.T, mgr *snapshotManager, nid nodeID, index snapshotIndex,
) {
	metadataFile := "/snapshot.metadata"
	err := mgr.cfg.FS.MkdirAll(mgr.snapshotPath(nid, index), defaultExportedDirMode)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(mgr.snapshotPath(nid, index) + metadataFile)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(
		fmt.Sprintf("%s/snapshot-%016X.gbsnap", mgr.snapshotPath(nid, index), index))
	assert.NoError(t, err)
}

func TestInitSnapshotMgr(t *testing.T) {
	var (
		shardID   uint64 = 1
		replicaID uint64 = 1
	)
	cfg := &Config{
		FS:                vfs.NewStrictMem(),
		SnapshotExportDir: "/tmp/exported",
	}
	mgr := newSnapshotManager(cfg)
	err := mgr.cfg.FS.MkdirAll(mgr.exportPath(shardID, replicaID), defaultExportedDirMode)
	assert.NoError(t, err)

	nid := nodeID{shardID: shardID, replicaID: replicaID}
	indexes := []snapshotIndex{
		snapshotIndex(310),
		snapshotIndex(1324),
		snapshotIndex(1211),
		snapshotIndex(1203),
	}
	for _, index := range indexes {
		testPrepareSnapshot(t, mgr, nid, index)
	}
	// add a bad dir
	err = mgr.cfg.FS.MkdirAll(mgr.exportPath(shardID, replicaID)+"/bad-dir", defaultExportedDirMode)
	assert.NoError(t, err)

	err = mgr.Init(shardID, replicaID)
	assert.NoError(t, err)
	assert.Equal(t, 4, mgr.Count(shardID, replicaID))
	assert.Equal(t, mgr.snapshots[nid].items[0].index, snapshotIndex(310))
	assert.Equal(t, mgr.snapshots[nid].items[1].index, snapshotIndex(1203))
	assert.Equal(t, mgr.snapshots[nid].items[2].index, snapshotIndex(1211))
	assert.Equal(t, mgr.snapshots[nid].items[3].index, snapshotIndex(1324))
}

func TestAddSnapshot(t *testing.T) {
	var (
		shardID   uint64 = 1
		replicaID uint64 = 1
	)
	cfg := &Config{
		FS:                vfs.NewStrictMem(),
		SnapshotExportDir: "",
	}
	mgr := newSnapshotManager(cfg)
	err := mgr.cfg.FS.MkdirAll(mgr.exportPath(shardID, replicaID), defaultExportedDirMode)
	assert.NoError(t, err)

	err = mgr.Init(shardID, replicaID)
	assert.NoError(t, err)
	assert.Equal(t, 0, mgr.Count(shardID, replicaID))

	// add one
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	index := snapshotIndex(621)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.NoError(t, err)
	assert.Equal(t, 1, mgr.Count(shardID, replicaID))

	// add another one, but smaller index, should get an error
	index = snapshotIndex(96)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.Error(t, err)
	assert.Equal(t, 1, mgr.Count(shardID, replicaID))

	// add another one
	index = snapshotIndex(794)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.NoError(t, err)
	assert.Equal(t, 2, mgr.Count(shardID, replicaID))

	// add an invalid one
	index = snapshotIndex(795)
	metadataFile := "/snapshot.bad"
	err = mgr.cfg.FS.MkdirAll(mgr.snapshotPath(nid, index), defaultExportedDirMode)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(mgr.snapshotPath(nid, index) + metadataFile)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(
		fmt.Sprintf("%s/snapshot-%016X.gbsnap", mgr.snapshotPath(nid, index), index))
	assert.NoError(t, err)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.Error(t, err)
	assert.Equal(t, 2, mgr.Count(shardID, replicaID))

	// add an invalid one
	index = snapshotIndex(797)
	metadataFile = "/snapshot.metadata"
	err = mgr.cfg.FS.MkdirAll(mgr.snapshotPath(nid, index), defaultExportedDirMode)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(mgr.snapshotPath(nid, index) + metadataFile)
	assert.NoError(t, err)
	_, err = mgr.cfg.FS.Create(
		fmt.Sprintf("%s/snapshot.gbsnap", mgr.snapshotPath(nid, index)))
	assert.NoError(t, err)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.Error(t, err)
	assert.Equal(t, 2, mgr.Count(shardID, replicaID))
}

func getInitializedMgr(t *testing.T, shardID uint64, replicaID uint64) *snapshotManager {
	cfg := &Config{
		FS:                vfs.NewStrictMem(),
		SnapshotExportDir: "",
	}
	mgr := newSnapshotManager(cfg)
	err := mgr.Init(shardID, replicaID)
	assert.NoError(t, err)
	assert.Equal(t, 0, mgr.Count(shardID, replicaID))

	nid := nodeID{shardID: shardID, replicaID: replicaID}
	index := snapshotIndex(100)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.NoError(t, err)
	assert.Equal(t, 1, mgr.Count(shardID, replicaID))
	assert.Equal(t, mgr.snapshots[nid].items[0].index, index)

	index = snapshotIndex(200)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.NoError(t, err)
	assert.Equal(t, 2, mgr.Count(shardID, replicaID))
	assert.Equal(t, mgr.snapshots[nid].items[1].index, index)

	index = snapshotIndex(300)
	testPrepareSnapshot(t, mgr, nid, index)
	err = mgr.Add(shardID, replicaID, uint64(index))
	assert.NoError(t, err)
	assert.Equal(t, 3, mgr.Count(shardID, replicaID))
	assert.Equal(t, mgr.snapshots[nid].items[2].index, index)

	return mgr
}

func TestRemoveSnapshot(t *testing.T) {
	var (
		shardID   uint64 = 1
		replicaID uint64 = 1
	)
	mgr := getInitializedMgr(t, shardID, replicaID)
	err := mgr.Remove(shardID, replicaID, 200)
	nid := nodeID{shardID: shardID, replicaID: replicaID}
	assert.NoError(t, err)
	assert.Equal(t, 1, mgr.Count(shardID, replicaID))
	assert.Equal(t, mgr.snapshots[nid].items[0].index, snapshotIndex(300))
}

func TestEvalImportSnapshot(t *testing.T) {
	var (
		shardID   uint64 = 1
		replicaID uint64 = 1
	)
	mgr := getInitializedMgr(t, shardID, replicaID)

	dir, index := mgr.EvalImportSnapshot(shardID, replicaID, 50)
	assert.Equal(t, "", dir)
	assert.Equal(t, uint64(0), index)

	dir, index = mgr.EvalImportSnapshot(shardID, replicaID, 110)
	assert.Equal(t, "shard-1/replica-1/snapshot-0000000000000064", dir)
	assert.Equal(t, uint64(100), index)

	dir, index = mgr.EvalImportSnapshot(shardID, replicaID, 210)
	assert.Equal(t, "shard-1/replica-1/snapshot-00000000000000C8", dir)
	assert.Equal(t, uint64(200), index)

	err := mgr.Remove(shardID, replicaID, 150)
	assert.NoError(t, err)

	dir, index = mgr.EvalImportSnapshot(shardID, replicaID, 110)
	assert.Equal(t, "", dir)
	assert.Equal(t, uint64(0), index)

	dir, index = mgr.EvalImportSnapshot(shardID, replicaID, 210)
	assert.Equal(t, "shard-1/replica-1/snapshot-00000000000000C8", dir)
	assert.Equal(t, uint64(200), index)

	dir, index = mgr.EvalImportSnapshot(shardID, replicaID, 10000)
	assert.Equal(t, "shard-1/replica-1/snapshot-000000000000012C", dir)
	assert.Equal(t, uint64(300), index)
}
