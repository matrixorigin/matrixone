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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

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
			visible, err := names.TxnGetNodeLocked(txn, "reused")
			require.NoError(t, err)
			require.Same(t, self, visible.GetPayload())
			require.NoError(t, db.checkAddNameConflictLocked("reused", self.ID, names, txn))

			require.NoError(t, db.checkAddNameConflictLocked("reused", 303, names, nil))

			// The historical owner starts a schema ALTER unrelated to this name.
			writer := txnbase.NewTxn(nil, nil, []byte("writer"), types.BuildTS(3, 0), types.BuildTS(3, 0))
			other.Lock()
			_, node := other.getOrSetUpdateNodeLocked(writer)
			node.BaseNode.Schema.Comment = "unrelated schema ALTER"
			other.Unlock()
			err = db.checkAddNameConflictLocked("reused", self.ID, names, txn)
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
			occupied := db.nameNodes[genTblFullName(0, "occupied")]
			require.True(t, moerr.IsMoErrCode(db.checkAddNameConflictLocked("occupied", self.ID, occupied, txn), moerr.OkExpectedDup))
			require.True(t, moerr.IsMoErrCode(db.checkAddNameConflictLocked("occupied", 303, occupied, txn), moerr.OkExpectedDup))
		})
	}
}
