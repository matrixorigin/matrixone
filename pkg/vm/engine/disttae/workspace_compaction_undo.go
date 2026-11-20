// Copyright 2022 Matrix Origin
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

package disttae

import (
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// workspaceCompactionUndo retains the physically charged original batches
// until the maintenance boundary can be committed or rolled back. Entries
// refer to stable workspace anchors; slot compaction must not run while this
// record is installed.
type workspaceCompactionUndo struct {
	entries    []workspaceCompactionOldEntry
	summaries  map[types.Objectid]workspaceCompactionOldSummary
	deleted    map[types.Blockid][]int64
	oldObjects []objectio.ObjectStats
	newNames   []string
	restored   bool
}

type workspaceCompactionOldEntry struct {
	anchor      int
	entry       Entry
	replacement *batch.Batch
}

type workspaceCompactionOldSummary struct {
	value   Summary
	present bool
}

// retireCompactionDeletionsLocked consumes only an unchanged deletion snapshot.
// New offsets for an old block cannot be applied to the rewritten object and
// require restoring and retrying the compaction instead.
func (txn *Transaction) retireCompactionDeletionsLocked(undo *workspaceCompactionUndo) error {
	txn.deletedBlocks.Lock()
	defer txn.deletedBlocks.Unlock()
	for id, captured := range undo.deleted {
		current := txn.deletedBlocks.offsets[id]
		if !slices.Equal(current, captured) {
			return moerr.NewInvalidStateNoCtx("workspace compaction deletion snapshot changed")
		}
	}
	for id := range undo.deleted {
		delete(txn.deletedBlocks.offsets, id)
	}
	return nil
}

// restoreCompactionLocked reinstalls the original entries. Readers may have
// captured replacement object names while maintenance temporarily unlocked the
// workspace, so their batches and objects stay owned until readers have exited.
func (txn *Transaction) restoreCompactionLocked() error {
	u := txn.compactionUndo
	if u == nil {
		return nil
	}
	if !u.restored {
		for _, old := range u.entries {
			if old.anchor >= len(txn.writes) || txn.writes[old.anchor].bat != old.replacement {
				return moerr.NewInvalidStateNoCtx("workspace compaction anchor changed before restore")
			}
		}
		for i := len(u.entries) - 1; i >= 0; i-- {
			old := u.entries[i]
			txn.unaccountWorkspaceEntryLocked(&txn.writes[old.anchor])
			txn.writes[old.anchor] = old.entry
			txn.accountWorkspaceEntryLocked(&txn.writes[old.anchor])
		}
		for id, old := range u.summaries {
			if old.present {
				txn.cnObjsSummary[id] = old.value
			} else {
				delete(txn.cnObjsSummary, id)
			}
		}
		u.restored = true
	}
	txn.retiredCompactions = append(txn.retiredCompactions, u)
	txn.compactionUndo = nil
	return nil
}

// finishCompactionLocked retires old objects only after all maintenance
// steps have succeeded. Validate the captured deletion prefix first so a
// concurrent mutation cannot be silently discarded.
func (txn *Transaction) finishCompactionLocked() error {
	u := txn.compactionUndo
	if u == nil {
		return nil
	}
	if u.restored {
		return txn.restoreCompactionLocked()
	}
	if err := txn.retireCompactionDeletionsLocked(u); err != nil {
		return err
	}
	txn.retiredCompactions = append(txn.retiredCompactions, u)
	txn.compactionUndo = nil
	return nil
}

// retireCompactionBatchesLocked is called only once the transaction's SQL
// producers and readers have drained. A failed GC admission keeps this and
// all later records owned by the transaction for retry.
func (txn *Transaction) retireCompactionBatchesLocked() error {
	for len(txn.retiredCompactions) > 0 {
		u := txn.retiredCompactions[0]
		if u.restored {
			var obsolete []string
			for _, name := range u.newNames {
				if !txn.hasLiveObjectStatsRefLocked(name, nil) {
					obsolete = append(obsolete, name)
				}
			}
			if err := gcFilesToOwner(txn, cloneGCIntermediate, obsolete...); err != nil {
				return err
			}
		} else {
			var obsolete []objectio.ObjectStats
			var removedProtections []string
			for _, stats := range u.oldObjects {
				name := stats.ObjectName().String()
				if !txn.hasLiveObjectStatsRefLocked(name, nil) {
					obsolete = append(obsolete, stats)
					if txn.isCloneTxn && txn.engine.cloneTxnCache != nil &&
						txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, name) {
						removedProtections = append(removedProtections, name)
					}
				}
			}
			txn.unprotectUnreferencedTxnLocalSharedFilesLocked(obsolete, nil)
			if err := txn.GCObjsByStats(obsolete...); err != nil {
				for _, name := range removedProtections {
					txn.engine.cloneTxnCache.AddTxnLocalSharedFile(txn.op.Txn().ID, name)
				}
				return err
			}
		}
		txn.releaseRetiredCompactionBuffersLocked(u)
		txn.retiredCompactions[0] = nil
		txn.retiredCompactions = txn.retiredCompactions[1:]
	}
	return nil
}

func (txn *Transaction) releaseRetiredCompactionBuffersLocked(u *workspaceCompactionUndo) {
	for _, old := range u.entries {
		bat := old.entry.bat
		if u.restored {
			bat = old.replacement
		}
		if bat != nil {
			delete(txn.batchSelectList, bat)
			bat.Clean(txn.proc.Mp())
		}
	}
}

// An unknown commit result can have reached TN. Release only local backing;
// the existing unknown-result finalization leaves recorded objects untouched.
func (txn *Transaction) releaseRetiredCompactionBuffersUnknownLocked() {
	for _, u := range txn.retiredCompactions {
		txn.releaseRetiredCompactionBuffersLocked(u)
	}
	clear(txn.retiredCompactions)
	txn.retiredCompactions = nil
}
