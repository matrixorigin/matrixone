// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/stretchr/testify/require"
)

type restartBookmarkTestObject struct {
	data.Object
	frozen bool
}

func (o *restartBookmarkTestObject) IsAppendFrozen() bool {
	return o.frozen
}

func TestObjectListCommitBookmarkPrefix(t *testing.T) {
	list := NewObjectList(false)
	first := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	second := makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 2)
	list.Set(first)
	list.Set(second)

	list.Lock()
	list.appendMaxes[*first.ID()] = objectListAppendMax{max: types.BuildTS(10, 0), finalized: true}
	list.appendMaxes[*second.ID()] = objectListAppendMax{max: types.BuildTS(20, 0), finalized: true}
	list.bookmarkVersion++
	list.Unlock()
	list.rebuildCommitBookmarks()

	snapshot := ObjectListSnapshot{trees: list.loadTrees()}
	start, valid, skipped := snapshot.CommitBookmarkStartWithCount(ObjectListGroupAppendableCreate, types.BuildTS(15, 0))
	require.True(t, valid)
	require.Same(t, second, start)
	require.Equal(t, 1, skipped)

	start, valid, skipped = snapshot.CommitBookmarkStartWithCount(ObjectListGroupAppendableCreate, types.BuildTS(25, 0))
	require.True(t, valid)
	require.Nil(t, start)
	require.Equal(t, 2, skipped)
}

func TestObjectListCommitBookmarkPendingIsUnbounded(t *testing.T) {
	list := NewObjectList(false)
	entry := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	list.Set(entry)

	list.Lock()
	list.appendMaxes[*entry.ID()] = objectListAppendMax{finalized: false}
	list.bookmarkVersion++
	list.Unlock()
	list.rebuildCommitBookmarks()

	snapshot := ObjectListSnapshot{trees: list.loadTrees()}
	start, valid, skipped := snapshot.CommitBookmarkStartWithCount(
		ObjectListGroupAppendableCreate, types.BuildTS(1, 0))
	require.True(t, valid)
	require.Same(t, entry, start)
	require.Zero(t, skipped)
}

func TestObjectListCommitBookmarkKeepsFinalizedPrefixBeforePending(t *testing.T) {
	list := NewObjectList(false)
	first := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	pending := makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 2)
	last := makeObjectListOrderTestEntry(3, ObjectListGroupAppendableCreate, 3)
	list.Set(first)
	list.Set(pending)
	list.Set(last)

	list.Lock()
	list.appendMaxes[*first.ID()] = objectListAppendMax{max: types.BuildTS(10, 0), finalized: true}
	list.appendMaxes[*pending.ID()] = objectListAppendMax{finalized: false}
	list.appendMaxes[*last.ID()] = objectListAppendMax{max: types.BuildTS(30, 0), finalized: true}
	list.bookmarkVersion++
	list.Unlock()
	list.rebuildCommitBookmarks()

	snapshot := ObjectListSnapshot{trees: list.loadTrees()}
	start, valid, skipped := snapshot.CommitBookmarkStartWithCount(
		ObjectListGroupAppendableCreate, types.BuildTS(15, 0))
	require.True(t, valid)
	require.Same(t, pending, start)
	require.Equal(t, 1, skipped)

	start, valid, skipped = snapshot.CommitBookmarkStartWithCount(
		ObjectListGroupAppendableCreate, types.BuildTS(40, 0))
	require.True(t, valid)
	require.Same(t, pending, start)
	require.Equal(t, 1, skipped)
}

func TestObjectListRestartBookmarkUsesFrozenPrefix(t *testing.T) {
	list := NewObjectList(false)
	first := makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1)
	second := makeObjectListOrderTestEntry(2, ObjectListGroupAppendableCreate, 2)
	third := makeObjectListOrderTestEntry(3, ObjectListGroupAppendableCreate, 3)
	first.objData = &restartBookmarkTestObject{frozen: true}
	second.objData = &restartBookmarkTestObject{frozen: true}
	third.objData = &restartBookmarkTestObject{frozen: false}
	list.Set(first)
	list.Set(second)
	list.Set(third)
	list.Lock()
	list.appendMaxes[*first.ID()] = objectListAppendMax{finalized: false}
	list.appendMaxes[*second.ID()] = objectListAppendMax{finalized: false}
	list.appendMaxes[*third.ID()] = objectListAppendMax{finalized: false}
	list.Unlock()

	restartTS := types.BuildTS(100, 0)
	list.MarkRestartSealedPrefix(restartTS)
	list.rebuildCommitBookmarks()

	snapshot := ObjectListSnapshot{trees: list.loadTrees()}
	start, valid, skipped := snapshot.CommitBookmarkStartWithCount(
		ObjectListGroupAppendableCreate, types.BuildTS(101, 0))
	require.True(t, valid)
	require.Same(t, third, start)
	require.Equal(t, 2, skipped)

	start, valid, skipped = snapshot.CommitBookmarkStartWithCount(
		ObjectListGroupAppendableCreate, types.BuildTS(100, 0))
	require.True(t, valid)
	require.Same(t, first, start)
	require.Zero(t, skipped)
}
