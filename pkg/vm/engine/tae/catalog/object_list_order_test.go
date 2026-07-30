// Copyright 2026 Matrix Origin
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

package catalog

import (
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func makeObjectListOrderTestEntry(
	marker byte,
	group ObjectListGroup,
	commitTS int64,
) *ObjectEntry {
	var id objectio.ObjectId
	id[0] = marker
	appendable := group < ObjectListGroupNonAppendableCreate
	ts := types.BuildTS(commitTS, 0)
	entry := &ObjectEntry{
		EntryMVCCNode: EntryMVCCNode{CreatedAt: ts},
		ObjectMVCCNode: ObjectMVCCNode{
			ObjectStats: *objectio.NewObjectStatsWithObjectID(&id, appendable, false, false),
		},
		CreateNode:  txnbase.NewTxnMVCCNodeWithTS(ts),
		ObjectState: ObjectState_Create_ApplyCommit,
	}
	switch group {
	case ObjectListGroupAppendableCreateWithDrop, ObjectListGroupNonAppendableCreateWithDrop:
		entry.nextVersion = &ObjectEntry{}
	case ObjectListGroupAppendableDrop, ObjectListGroupNonAppendableDrop:
		entry.DeletedAt = ts
		entry.DeleteNode = txnbase.NewTxnMVCCNodeWithTS(ts)
		entry.prevVersion = &ObjectEntry{}
		entry.ObjectState = ObjectState_Delete_ApplyCommit
	}
	return entry
}

func collectVisibleObjectMarkers(list *ObjectList) []byte {
	it := list.MakeVisibleCommittedObjectIt(txnbase.MockTxnReaderWithNow())
	defer it.Release()
	var markers []byte
	for it.Next() {
		markers = append(markers, it.Item().ID()[0])
	}
	return markers
}

func TestObjectListOrder(t *testing.T) {
	list := NewObjectList(false)
	entries := []*ObjectEntry{
		makeObjectListOrderTestEntry(12, ObjectListGroupNonAppendableDrop, 2),
		makeObjectListOrderTestEntry(6, ObjectListGroupAppendableDrop, 2),
		makeObjectListOrderTestEntry(8, ObjectListGroupNonAppendableCreate, 2),
		makeObjectListOrderTestEntry(4, ObjectListGroupAppendableCreateWithDrop, 2),
		makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 2),
		makeObjectListOrderTestEntry(10, ObjectListGroupNonAppendableCreateWithDrop, 2),
		makeObjectListOrderTestEntry(5, ObjectListGroupAppendableDrop, 1),
		makeObjectListOrderTestEntry(11, ObjectListGroupNonAppendableDrop, 1),
		makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1),
		makeObjectListOrderTestEntry(7, ObjectListGroupNonAppendableCreate, 1),
		makeObjectListOrderTestEntry(3, ObjectListGroupAppendableCreateWithDrop, 1),
		makeObjectListOrderTestEntry(9, ObjectListGroupNonAppendableCreateWithDrop, 1),
	}
	for _, entry := range entries {
		list.Set(entry)
	}

	var markers []byte
	it := list.loadTree().Iter()
	for ok := it.First(); ok; ok = it.Next() {
		markers = append(markers, it.Item().ID()[0])
	}
	it.Release()
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, markers)

	for _, entry := range entries {
		require.Same(t, entry, list.GetLastestNode(entry.ID()))
	}
	require.Equal(t, []byte{8, 2, 7, 1}, collectVisibleObjectMarkers(list))
}

func TestObjectListGroupSeek(t *testing.T) {
	list := NewObjectList(false)
	for marker, ts := range []int64{1, 3, 5} {
		list.Set(makeObjectListOrderTestEntry(
			byte(marker+1),
			ObjectListGroupAppendableCreate,
			ts,
		))
	}
	list.Set(makeObjectListOrderTestEntry(4, ObjectListGroupAppendableDrop, 2))

	it := list.loadTree().Iter()
	defer it.Release()

	require.True(t, SeekObjectListGroup(&it, ObjectListGroupAppendableCreate, types.BuildTS(2, 0)))
	require.Equal(t, types.BuildTS(3, 0), it.Item().CreatedAt)

	require.True(t, SeekObjectListGroupBefore(&it, ObjectListGroupAppendableCreate, types.BuildTS(3, 0)))
	require.Equal(t, types.BuildTS(1, 0), it.Item().CreatedAt)

	require.True(t, SeekObjectListGroupReverse(&it, ObjectListGroupAppendableCreate, types.BuildTS(4, 0)))
	require.Equal(t, types.BuildTS(3, 0), it.Item().CreatedAt)

	require.True(t, SeekObjectListGroup(&it, ObjectListGroupAppendableDrop, types.TS{}))
	require.Equal(t, types.BuildTS(2, 0), it.Item().DeletedAt)

	require.False(t, SeekObjectListGroup(&it, ObjectListGroupNonAppendableDrop, types.TS{}))
}

func TestObjectListUncommittedSeekKeysMatchFreshKeys(t *testing.T) {
	tree := newObjectEntryTree()
	marker := byte(1)
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		committed := makeObjectListOrderTestEntry(marker, group, 1)
		marker++
		uncommitted := makeObjectListOrderTestEntry(marker, group, 2)
		marker++
		if uncommitted.IsDEntry() {
			uncommitted.DeletedAt = txnif.UncommitTS
		} else {
			uncommitted.CreatedAt = txnif.UncommitTS
		}
		tree.Set(committed)
		tree.Set(uncommitted)
	}

	assertSameSeek := func(fresh, cached *ObjectEntry) {
		freshIt := tree.Iter()
		defer freshIt.Release()
		cachedIt := tree.Iter()
		defer cachedIt.Release()

		freshOK := freshIt.Seek(fresh)
		cachedOK := cachedIt.Seek(cached)
		require.Equal(t, freshOK, cachedOK)
		if freshOK {
			require.Same(t, freshIt.Item(), cachedIt.Item())
		}
	}

	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		var minID, maxID objectio.ObjectId
		for i := range maxID {
			maxID[i] = 0xff
		}
		assertSameSeek(
			makeObjectListKey(group, txnif.UncommitTS, &minID),
			objectListUncommittedMinKeys[group],
		)
		assertSameSeek(
			makeObjectListKey(group, txnif.UncommitTS, &maxID),
			objectListUncommittedMaxKeys[group],
		)
	}
}

func TestObjectListUncommittedSeekKeysConcurrent(t *testing.T) {
	tree := newObjectEntryTree()
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		tree.Set(makeObjectListOrderTestEntry(byte(group+1), group, 1))
	}

	const workers = 16
	errC := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group := ObjectListGroup(worker % len(objectListUncommittedMaxKeys))
			it := tree.Iter()
			defer it.Release()
			for range 100 {
				if !SeekObjectListGroupReverse(&it, group, txnif.UncommitTS) {
					errC <- fmt.Errorf("group %d not found", group)
					return
				}
				if actual := it.Item().ObjectListGroup(); actual != group {
					errC <- fmt.Errorf("got group %d, want %d", actual, group)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		require.NoError(t, err)
	}
}

func TestObjectListDynamicSeekKeysConcurrent(t *testing.T) {
	tree := newObjectEntryTree()
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		tree.Set(makeObjectListOrderTestEntry(byte(group+1), group, 1))
	}

	const workers = 16
	errC := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group := ObjectListGroup(worker % len(objectListUncommittedMaxKeys))
			it := tree.Iter()
			defer it.Release()
			for range 100 {
				if !SeekObjectListGroup(&it, group, types.BuildTS(1, 0)) {
					errC <- fmt.Errorf("group %d not found", group)
					return
				}
				if actual := it.Item().ObjectListGroup(); actual != group {
					errC <- fmt.Errorf("got group %d, want %d", actual, group)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		require.NoError(t, err)
	}
}

func TestObjectListGroupUsesVersionLink(t *testing.T) {
	entry := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	entry.DeletedAt = types.BuildTS(2, 0)

	require.Equal(t, ObjectListGroupAppendableCreate, entry.ObjectListGroup())
	require.Equal(t, entry.CreatedAt, entry.ObjectListCommitTS())

	entry.prevVersion = &ObjectEntry{}
	require.Equal(t, ObjectListGroupAppendableDrop, entry.ObjectListGroup())
	require.Equal(t, entry.DeletedAt, entry.ObjectListCommitTS())
}

func TestVisibleObjectIteratorMergesGroupsByCreateTS(t *testing.T) {
	list := NewObjectList(false)
	for _, entry := range []*ObjectEntry{
		makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 4),
		makeObjectListOrderTestEntry(2, ObjectListGroupNonAppendableCreate, 2),
		makeObjectListOrderTestEntry(3, ObjectListGroupAppendableCreate, 1),
		makeObjectListOrderTestEntry(4, ObjectListGroupNonAppendableCreate, 3),
	} {
		list.Set(entry)
	}

	require.Equal(t, []byte{1, 4, 2, 3}, collectVisibleObjectMarkers(list))
}

func TestObjectListMovesCreateEntryWhenDropStarts(t *testing.T) {
	list := NewObjectList(false)
	created := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	list.Set(created)
	list.Set(makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 2))

	updatedCreate := created.Clone()
	dropped := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableDrop, 3)
	dropped.CreatedAt = created.CreatedAt
	updatedCreate.nextVersion = dropped
	dropped.prevVersion = updatedCreate
	list.modify(nil, dropped, updatedCreate)

	var groups []ObjectListGroup
	var markers []byte
	it := list.loadTree().Iter()
	for ok := it.First(); ok; ok = it.Next() {
		groups = append(groups, it.Item().ObjectListGroup())
		markers = append(markers, it.Item().ID()[0])
	}
	it.Release()

	require.Equal(t, []ObjectListGroup{
		ObjectListGroupAppendableCreate,
		ObjectListGroupAppendableCreateWithDrop,
		ObjectListGroupAppendableDrop,
	}, groups)
	require.Equal(t, []byte{2, 1, 1}, markers)
	require.Len(t, list.GetAllNodes(created.ID()), 2)
	require.Equal(t, []byte{2}, collectVisibleObjectMarkers(list))

	restored := dropped.Clone()
	restored.DeletedAt = types.TS{}
	restored.prevVersion = nil
	restored.nextVersion = nil
	list.modify(dropped, restored, nil)

	require.Equal(t, 2, list.loadTree().Len())
	require.Same(t, restored, list.GetLastestNode(restored.ID()))
	it = list.loadTree().Iter()
	for ok := it.First(); ok; ok = it.Next() {
		require.Equal(t, ObjectListGroupAppendableCreate, it.Item().ObjectListGroup())
	}
	it.Release()
	require.Equal(t, []byte{2, 1}, collectVisibleObjectMarkers(list))
}

func TestObjectListReplayTimestampReordersEntry(t *testing.T) {
	list := NewObjectList(false)
	committed := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 10)
	replayed := makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 1)
	replayed.CreatedAt = txnif.UncommitTS
	list.Set(committed)
	list.Set(replayed)

	updated := list.UpdateReplayTs(replayed, types.BuildTS(5, 0))

	it := list.loadTree().Iter()
	defer it.Release()
	require.True(t, it.First())
	require.Same(t, updated, it.Item())
	require.True(t, it.Next())
	require.Same(t, committed, it.Item())
	require.Same(t, updated, list.GetLastestNode(replayed.ID()))
	require.Equal(t, types.BuildTS(5, 0), replayed.CreatedAt)
	require.Equal(t, []byte{1, 2}, collectVisibleObjectMarkers(list))
}

func TestObjectListReplayTimestampReordersDropEntry(t *testing.T) {
	list := NewObjectList(false)
	created := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	list.Set(created)

	updatedCreate := created.Clone()
	dropped := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableDrop, 1)
	dropped.CreatedAt = created.CreatedAt
	dropped.DeletedAt = txnif.UncommitTS
	updatedCreate.nextVersion = dropped
	dropped.prevVersion = updatedCreate
	list.Set(dropped)
	require.Empty(t, collectVisibleObjectMarkers(list))

	updatedDrop := list.UpdateReplayTs(dropped, types.BuildTS(3, 0))
	require.Equal(t, types.BuildTS(3, 0), dropped.DeletedAt)
	require.Equal(t, types.BuildTS(3, 0), updatedDrop.DeletedAt)
	require.Same(t, updatedDrop, updatedDrop.prevVersion.nextVersion)

	nodes := list.GetAllNodes(created.ID())
	require.Len(t, nodes, 2)
	require.Same(t, updatedDrop, nodes[0])
	require.Same(t, updatedDrop.prevVersion, nodes[1])
	require.Empty(t, collectVisibleObjectMarkers(list))
}
