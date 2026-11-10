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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestRenameUndoLifecycle(t *testing.T) {
	for _, mode := range []string{"cycle", "later-duplicate", "first-duplicate", "commit", "replay"} {
		t.Run(mode, func(t *testing.T) {
			db := NewReplayDBEntry()
			db.catalog = &Catalog{}
			self := MockTableEntryWithDB(db, 101)
			moved := MockTableEntryWithDB(db, 202)
			occupied := MockTableEntryWithDB(db, 303)
			db.Lock()
			for table, name := range map[*TableEntry]string{self: "live", moved: "moved", occupied: "occupied"} {
				table.CreateWithTSLocked(types.BuildTS(1, 0), &TableMVCCNode{Schema: NewEmptySchema(name)})
				db.entries[table.ID] = db.link.Insert(table)
			}
			live := genTblFullName(0, "live")
			db.addNameIndexLocked(live, self.ID)
			db.addNameIndexLocked(live, moved.ID)
			// A failed first admission must not delete this older self binding.
			db.addNameIndexLocked(genTblFullName(0, "occupied"), self.ID)
			db.addNameIndexLocked(genTblFullName(0, "occupied"), occupied.ID)
			before := db.PrettyNameIndex()
			db.Unlock()
			txn := txnbase.NewTxn(nil, nil, []byte("rename"), types.BuildTS(2, 0), types.BuildTS(2, 0))
			rename := func(old, next string, first bool) error {
				self.Lock()
				_, node := self.getOrSetUpdateNodeLocked(txn)
				node.BaseNode.Schema.Name = next
				if first {
					node.BaseNode.Schema.Extra.OldName = old
				}
				self.Unlock()
				if mode == "replay" {
					return db.replayRenameTable(old, next, self.ID, 0, txn, first)
				}
				return db.RenameTableInTxn(old, next, self.ID, 0, txn, first)
			}
			if mode == "first-duplicate" {
				require.True(t, moerr.IsMoErrCode(rename("live", "occupied", true), moerr.OkExpectedDup))
				require.Empty(t, db.renameUndo[self.ID].mutations)
			} else {
				require.NoError(t, rename("live", "tmp", true))
				if mode == "replay" {
					require.Empty(t, db.renameUndo, "replay has no TableEntry commit callback to release undo")
					return
				}
				require.NoError(t, rename("tmp", "live", false), "admit historical self behind another binding")
				if mode == "later-duplicate" {
					require.True(t, moerr.IsMoErrCode(rename("live", "occupied", false), moerr.OkExpectedDup))
				} else {
					require.NoError(t, rename("live", "again", false))
					require.NoError(t, rename("again", "live", false))
				}
			}
			if mode == "commit" {
				// A failed callback must not discard the original owner's journal.
				require.Error(t, self.ApplyCommit("other-transaction"))
				require.NotEmpty(t, db.renameUndo)
				require.NoError(t, self.ApplyCommit(txn.GetID()))
			} else {
				require.NoError(t, self.PrepareRollback())
				require.Equal(t, before, db.PrettyNameIndex(), "abort restores all original historical claims")
				reader := txnbase.NewTxn(nil, nil, []byte("reader"), types.BuildTS(3, 0), types.BuildTS(3, 0))
				table, err := db.GetTableEntryByName(0, "live", reader)
				require.NoError(t, err)
				require.Equal(t, self.ID, table.ID)
			}
			require.Empty(t, db.renameUndo, "terminal callback releases runtime ownership")
		})
	}
}

func TestRenameUndoBindingOrder(t *testing.T) {
	for _, emptyList := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(map[bool]string{false: "foreign-tail", true: "recreated-list"}[emptyList]+map[bool]string{false: "/forward", true: "/reverse"}[reverse], func(t *testing.T) {
				db := NewReplayDBEntry()
				db.Lock()
				defer db.Unlock()
				name := genTblFullName(0, "shared")
				if !emptyList {
					db.addNameIndexLocked(name, 3)
				}
				db.addNameIndexLocked(name, 1)
				db.addNameIndexLocked(name, 2)
				db.renameUndo = make(map[uint64]*tableRenameUndo)
				for _, id := range []uint64{1, 2} {
					undo := &tableRenameUndo{txnID: "writer"}
					db.renameUndo[id] = undo
					_, empty := db.nameNodes[name].deleteNode(id, nil, func(node *nameNode[*TableEntry]) {
						undo.mutations = append(undo.mutations, renameNameMutation{name: name, node: node})
					})
					if empty {
						delete(db.nameNodes, name)
					}
				}
				// A distinct owner claims the free position while old nodes are detached.
				db.addNameIndexLocked(name, 4)
				order := []uint64{1, 2}
				if reverse {
					order = []uint64{2, 1}
				}
				for _, id := range order {
					journaled, err := db.undoRenameLocked(id, "writer")
					require.NoError(t, err)
					require.True(t, journaled)
					delete(db.renameUndo, id)
				}
				var ids []uint64
				last := ^uint64(0)
				db.nameNodes[name].ForEachNodes(func(node *nameNode[*TableEntry]) bool {
					require.Less(t, node.order, last)
					last = node.order
					ids = append(ids, node.id)
					return true
				})
				want := []uint64{4, 2, 1}
				if !emptyList {
					want = append(want, 3)
				}
				require.Equal(t, want, ids)
			})
		}
	}
}

func TestRenameUndoNodeIdentity(t *testing.T) {
	var mu sync.RWMutex
	list := newNodeList[*TableEntry](nil, nil, &mu, "identity")
	old := list.createOrderedNode(1, 1, nil)
	list.deleteNode(1, old, nil)
	newer := list.createOrderedNode(1, 2, nil)
	// Undo cannot erase a later node merely because its ID matches.
	removed, _ := list.deleteNode(1, old, nil)
	require.Nil(t, removed)
	require.Error(t, list.restoreNode(old))
	require.Equal(t, newer, list.GetNext())
	list.deleteNode(1, newer, nil)
	require.NoError(t, list.restoreNode(old))
	require.NoError(t, list.restoreNode(old), "repeated restore cannot duplicate identity")
	require.Equal(t, 1, list.Length())
}

func TestNameConflictHistory(t *testing.T) {
	for _, selfHead := range []bool{false, true} {
		t.Run(map[bool]string{false: "selfBehindHead", true: "selfAtHead"}[selfHead], func(t *testing.T) {
			db := NewReplayDBEntry()
			db.Lock()
			defer db.Unlock()
			self := MockTableEntryWithDB(db, 101)
			other := MockTableEntryWithDB(db, 202)
			self.CreateWithTSLocked(types.BuildTS(1, 0), &TableMVCCNode{Schema: NewEmptySchema("shadow")})
			other.CreateWithTSLocked(types.BuildTS(1, 0), &TableMVCCNode{Schema: NewEmptySchema("moved_away")})
			db.entries[self.ID] = db.link.Insert(self)
			db.entries[other.ID] = db.link.Insert(other)
			name := genTblFullName(0, "reused")
			if selfHead {
				db.addNameIndexLocked(name, other.ID)
				db.addNameIndexLocked(name, self.ID)
			} else {
				db.addNameIndexLocked(name, self.ID)
				db.addNameIndexLocked(name, other.ID)
			}
			txn := txnbase.NewTxn(nil, nil, []byte("reader"), types.BuildTS(2, 0), types.BuildTS(2, 0))
			// Rename mutates the target's private schema before DB admission.
			self.Lock()
			_, selfNode := self.getOrSetUpdateNodeLocked(txn)
			selfNode.BaseNode.Schema.Name = "reused"
			self.Unlock()
			names := db.nameNodes[name]
			visible, wait := db.tryGetTableNodeByNameLocked(names, txn, "reused", 0)
			require.Nil(t, wait)
			require.Same(t, self, visible.GetPayload())
			require.NoError(t, db.checkAddNameConflictLocked("reused", self.ID, name, txn, 0))

			require.NoError(t, db.checkAddNameConflictLocked("reused", 303, name, nil, 0))

			// The historical owner starts a schema ALTER unrelated to this name.
			writer := txnbase.NewTxn(nil, nil, []byte("writer"), types.BuildTS(3, 0), types.BuildTS(3, 0))
			other.Lock()
			_, node := other.getOrSetUpdateNodeLocked(writer)
			node.BaseNode.Schema.Comment = "unrelated schema ALTER"
			other.Unlock()
			err := db.checkAddNameConflictLocked("reused", self.ID, name, txn, 0)
			if selfHead {
				require.NoError(t, err, "an older moved-away owner is not a name reservation")
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "retain head reservation conflict")
			}

			// Roll back the unrelated ALTER, then let the distinct owner claim
			// the name through the same serialized admission path.
			_, err = other.MVCCChain.PrepareRollback()
			require.NoError(t, err)
			// Release this fixture's DB lock before calling RenameTableInTxn.
			db.Unlock()
			other.Lock()
			_, node = other.getOrSetUpdateNodeLocked(txn)
			node.BaseNode.Schema.Name = "occupied"
			other.Unlock()
			err = db.RenameTableInTxn("moved_away", "occupied", other.ID, 0, txn, true)
			db.Lock()
			require.NoError(t, err)
			require.True(t, moerr.IsMoErrCode(db.checkAddNameConflictLocked("occupied", self.ID, genTblFullName(0, "occupied"), txn, 0), moerr.OkExpectedDup))
			require.True(t, moerr.IsMoErrCode(db.checkAddNameConflictLocked("occupied", 303, genTblFullName(0, "occupied"), txn, 0), moerr.OkExpectedDup))
		})
	}
}
