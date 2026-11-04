// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

// The barrier exposes a real namespace transaction wait. Completion is only
// released after the actual TableEntry callback has finished, as in DoneApply.
type namespaceWaitTxn struct {
	txnif.AsyncTxn
	state       atomic.Int32
	waited      chan struct{}
	release     chan struct{}
	waitOnce    sync.Once
	releaseOnce sync.Once
}

func newNamespaceWaitTxn(t *testing.T) *namespaceWaitTxn {
	txn := &namespaceWaitTxn{
		AsyncTxn: txnbase.NewTxn(nil, nil, []byte("namespace-owner"), types.BuildTS(2, 0), types.BuildTS(3, 0)),
		waited:   make(chan struct{}),
		release:  make(chan struct{}),
	}
	txn.state.Store(int32(txnif.TxnStateActive))
	t.Cleanup(func() { txn.finish(true) })
	return txn
}

func (txn *namespaceWaitTxn) GetPrepareTS() types.TS { return types.BuildTS(3, 0) }
func (txn *namespaceWaitTxn) GetCommitTS() types.TS  { return types.BuildTS(3, 0) }
func (txn *namespaceWaitTxn) GetTxnState(wait bool) txnif.TxnState {
	if wait {
		txn.waitOnce.Do(func() { close(txn.waited) })
		<-txn.release
	}
	return txnif.TxnState(txn.state.Load())
}

func (txn *namespaceWaitTxn) finish(abort bool) {
	txn.releaseOnce.Do(func() {
		state := txnif.TxnStateCommitted
		if abort {
			state = txnif.TxnStateRollbacked
		}
		txn.state.Store(int32(state))
		close(txn.release)
	})
}

func namespaceTable(db *DBEntry, id uint64, name string, ts types.TS) *TableEntry {
	table := MockTableEntryWithDB(db, id)
	schema := NewEmptySchema(name)
	table.TableNode.schema.Store(schema)
	table.fullName = genTblFullName(0, name)
	table.CreateWithTSLocked(ts, &TableMVCCNode{Schema: schema})
	db.Lock()
	db.entries[id] = db.link.Insert(table)
	db.addNameIndexLocked(table.fullName, id)
	db.Unlock()
	return table
}

func namespaceRename(t *testing.T, table *TableEntry, txn txnif.TxnReader, old, next string, first bool) {
	t.Helper()
	table.Lock()
	_, node := table.getOrSetUpdateNodeLocked(txn)
	node.BaseNode.Schema.Name = next
	if first {
		node.BaseNode.Schema.Extra.OldName = old
	}
	table.Unlock()
	require.NoError(t, table.db.RenameTableInTxn(old, next, table.ID, 0, txn, first))
}

func awaitNamespaceWait(t *testing.T, txn *namespaceWaitTxn, db *DBEntry, table *TableEntry) {
	t.Helper()
	select {
	case <-txn.waited:
	case <-time.After(3 * time.Second):
		t.Fatal("namespace operation did not reach transaction wait")
	}
	require.True(t, db.TryLock(), "transaction waiter retained the DB lock")
	db.Unlock()
	require.True(t, db.nodesMu.TryLock(), "transaction waiter retained the name-list lock")
	db.nodesMu.Unlock()
	require.True(t, table.TryLock(), "transaction waiter retained the MVCC lock")
	table.Unlock()
}

func finishNamespaceOwner(t *testing.T, txn *namespaceWaitTxn, table *TableEntry, abort bool) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		if abort {
			done <- table.PrepareRollback()
		} else {
			done <- table.ApplyCommit(txn.GetID())
		}
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("terminal callback cannot finish before namespace waiter is released")
	}
	txn.finish(abort)
}

func TestNamespaceLookupWaitCompletion(t *testing.T) {
	for _, kind := range []string{"rename", "create", "drop"} {
		for _, abort := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/commit", true: "/abort"}[abort], func(t *testing.T) {
				db := NewReplayDBEntry()
				db.catalog = &Catalog{}
				owner := newNamespaceWaitTxn(t)
				var table *TableEntry
				if kind == "create" {
					var err error
					table, err = db.CreateTableEntryWithTableId(NewEmptySchema("old"), owner, nil, 100001)
					require.NoError(t, err)
				} else {
					table = namespaceTable(db, 101, "old", types.BuildTS(1, 0))
					if kind == "rename" {
						// Abort removes the new list and recreates the original list.
						namespaceRename(t, table, owner, "old", "tmp", true)
						namespaceRename(t, table, owner, "tmp", "old", false)
					} else {
						table.Lock()
						_, err := table.DropEntryLocked(owner)
						table.Unlock()
						require.NoError(t, err)
					}
				}
				owner.state.Store(int32(txnif.TxnStatePreparing))
				require.NoError(t, table.PrepareCommit())
				oldReader := txnbase.NewTxn(nil, nil, []byte("old-reader"), types.BuildTS(2, 0), types.TS{})
				oldTable, oldErr := db.GetTableEntryByName(0, "old", oldReader)
				if kind == "create" {
					require.True(t, moerr.IsMoErrCode(oldErr, moerr.OkExpectedEOB))
				} else {
					require.NoError(t, oldErr)
					require.Same(t, table, oldTable)
				}
				select {
				case <-owner.waited:
					t.Fatal("older snapshot unexpectedly waited")
				default:
				}
				reader := txnbase.NewTxn(nil, nil, []byte("reader"), types.BuildTS(4, 0), types.TS{})
				type result struct {
					table *TableEntry
					err   error
				}
				done := make(chan result, 1)
				go func() {
					found, err := db.GetTableEntryByName(0, "old", reader)
					done <- result{found, err}
				}()
				awaitNamespaceWait(t, owner, db, table)
				finishNamespaceOwner(t, owner, table, abort)
				select {
				case got := <-done:
					if kind == "create" && abort || kind == "drop" && !abort {
						require.True(t, moerr.IsMoErrCode(got.err, moerr.OkExpectedEOB))
					} else {
						require.NoError(t, got.err)
						require.Same(t, table, got.table)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("namespace lookup did not finish")
				}
				db.RLock()
				require.Empty(t, db.renameUndo)
				require.Nil(t, db.nameNodes[genTblFullName(0, "tmp")])
				db.RUnlock()
			})
		}
	}
}

func TestNamespaceAdmissionWaitRevalidation(t *testing.T) {
	for _, mode := range []string{"commit-duplicate", "history-wait-duplicate", "abort-admits", "newer-head-ww", "explicit-id-duplicate"} {
		t.Run(mode, func(t *testing.T) {
			db := NewReplayDBEntry()
			db.catalog = &Catalog{}
			owner := newNamespaceWaitTxn(t)
			blocker := namespaceTable(db, 101, "away", types.BuildTS(1, 0))
			db.Lock()
			db.addNameIndexLocked(genTblFullName(0, "target"), blocker.ID)
			db.Unlock()
			namespaceRename(t, blocker, owner, "away", "target", true)
			owner.state.Store(int32(txnif.TxnStatePreparing))
			require.NoError(t, blocker.PrepareCommit())
			if mode == "history-wait-duplicate" {
				// The admitted head passes WW and has moved away; the relevant
				// prepared owner is reached only by visibility traversal.
				head := namespaceTable(db, 404, "moved", types.BuildTS(1, 0))
				db.Lock()
				db.addNameIndexLocked(genTblFullName(0, "target"), head.ID)
				db.Unlock()
			}
			contender := txnbase.NewTxn(nil, nil, []byte("contender"), types.BuildTS(4, 0), types.TS{})
			done := make(chan error, 1)
			var self *TableEntry
			if mode == "explicit-id-duplicate" {
				go func() {
					_, err := db.CreateTableEntryWithTableId(NewEmptySchema("target"), contender, nil, 100001)
					done <- err
				}()
			} else {
				self = namespaceTable(db, 202, "source", types.BuildTS(1, 0))
				namespaceRename(t, self, contender, "source", "tmp", true)
				self.Lock()
				self.GetLatestNodeLocked().BaseNode.Schema.Name = "target"
				self.Unlock()
				go func() { done <- db.RenameTableInTxn("tmp", "target", self.ID, 0, contender, false) }()
			}
			awaitNamespaceWait(t, owner, db, blocker)
			if self != nil {
				db.RLock()
				require.Len(t, db.renameUndo[self.ID].mutations, 1, "waiting must not record intermediate removal")
				require.Equal(t, self.ID, db.nameNodes[genTblFullName(0, "tmp")].GetNode().GetPayload().ID)
				db.RUnlock()
			}
			var newer *TableEntry
			if mode == "newer-head-ww" {
				newer = namespaceTable(db, 303, "target", types.BuildTS(5, 0))
			} else if mode == "explicit-id-duplicate" {
				newer = namespaceTable(db, 100001, "alternate", types.BuildTS(1, 0))
			}
			finishNamespaceOwner(t, owner, blocker, mode != "commit-duplicate" && mode != "history-wait-duplicate")
			select {
			case err := <-done:
				switch mode {
				case "abort-admits":
					require.NoError(t, err)
				case "newer-head-ww":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
				default:
					require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedDup))
				}
			case <-time.After(3 * time.Second):
				t.Fatal("admission did not finish after transaction completion")
			}
			if self != nil {
				db.RLock()
				wantMutations := 2 // first insertion and the one intermediate removal
				if mode == "abort-admits" {
					wantMutations++ // successful final insertion
				}
				require.Len(t, db.renameUndo[self.ID].mutations, wantMutations)
				db.RUnlock()
				require.NoError(t, self.PrepareRollback())
				found, err := db.GetTableEntryByName(0, "source", contender)
				require.NoError(t, err)
				require.Same(t, self, found)
			}
			db.RLock()
			require.Empty(t, db.renameUndo)
			if newer != nil {
				require.Same(t, newer, db.entries[newer.ID].GetPayload())
				if mode == "newer-head-ww" {
					require.Same(t, newer, db.nameNodes[genTblFullName(0, "target")].GetNode().GetPayload())
				}
			}
			db.RUnlock()
		})
	}
}

func TestNamespaceSameNameRenameRetainsForeignWW(t *testing.T) {
	db := NewReplayDBEntry()
	db.catalog = &Catalog{}
	self := namespaceTable(db, 101, "source", types.BuildTS(1, 0))
	foreign := namespaceTable(db, 202, "away", types.BuildTS(1, 0))
	name := genTblFullName(0, "same")
	db.Lock()
	db.addNameIndexLocked(name, foreign.ID)
	db.addNameIndexLocked(name, self.ID)
	before := db.PrettyNameIndex()
	db.Unlock()
	txn := txnbase.NewTxn(nil, nil, []byte("self"), types.BuildTS(2, 0), types.TS{})
	namespaceRename(t, self, txn, "source", "same", true)
	writer := txnbase.NewTxn(nil, nil, []byte("foreign"), types.BuildTS(3, 0), types.TS{})
	foreign.Lock()
	_, update := foreign.getOrSetUpdateNodeLocked(writer)
	update.BaseNode.Schema.Comment = "active foreign owner"
	foreign.Unlock()
	// The original remove-before-check path exposes the foreign head when
	// old == new. Its virtual exclusion must preserve that WW rejection.
	err := db.RenameTableInTxn("same", "same", self.ID, 0, txn, false)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	require.NoError(t, self.PrepareRollback())
	require.NoError(t, foreign.PrepareRollback())
	db.RLock()
	require.Equal(t, before, db.PrettyNameIndex())
	require.Empty(t, db.renameUndo)
	db.RUnlock()
}
