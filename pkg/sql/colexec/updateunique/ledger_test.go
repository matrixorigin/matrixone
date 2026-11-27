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

package updateunique

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type updateLedgerFixture struct {
	mp       *mpool.MPool
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
	alloc    *updateKeyLedgerAllocations
	ledgers  []*updateKeyLedger
	vectors  []*vector.Vector
}

func newUpdateLedgerFixture(t *testing.T) *updateLedgerFixture {
	t.Helper()
	f := &updateLedgerFixture{mp: mpool.MustNewZero()}
	var err error
	f.registry, err = mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	f.account, err = f.registry.Open(1 << 30)
	require.NoError(t, err)
	f.alloc, err = newUpdateKeyLedgerAllocations(f.account)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, ledger := range f.ledgers {
			ledger.close(f.mp)
		}
		for _, v := range f.vectors {
			v.Free(f.mp)
		}
		require.Zero(t, f.account.Snapshot().Used)
		_, _, err := f.registry.CompleteTerminal(f.account)
		require.NoError(t, err)
		require.Zero(t, f.mp.CurrNB())
	})
	return f
}

func (f *updateLedgerFixture) ledger(t *testing.T) *updateKeyLedger {
	t.Helper()
	ledger, err := newUpdateKeyLedger(f.mp, f.alloc)
	require.NoError(t, err)
	f.ledgers = append(f.ledgers, ledger)
	return ledger
}

func (f *updateLedgerFixture) ints(t *testing.T, values []int32, nulls ...int) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_int32.ToType())
	f.vectors = append(f.vectors, v)
	for i, value := range values {
		isNull := false
		for _, pos := range nulls {
			isNull = isNull || pos == i
		}
		require.NoError(t, vector.AppendFixed(v, value, isNull, f.mp))
	}
	return v
}

func updateLedgerRow(id uint32) types.Rowid {
	var block types.Blockid
	return types.NewRowid(&block, id)
}

func TestUpdateKeyLedgerRejectsAllKeysAtomically(t *testing.T) {
	f := newUpdateLedgerFixture(t)
	u, v := f.ledger(t), f.ledger(t)
	oldU := f.ints(t, []int32{10, 20})
	oldV := f.ints(t, []int32{100, 200})
	newU := f.ints(t, []int32{500, 500})
	newV := f.ints(t, []int32{300, 400})
	for row := 0; row < 2; row++ {
		owner := updateLedgerRow(uint32(row + 1))
		require.NoError(t, u.addParticipant(f.mp, oldU, oldU, row, owner))
		require.NoError(t, v.addParticipant(f.mp, oldV, oldV, row, owner))
	}
	candidate := func(row int, vHit bool) []updateKeyCandidate {
		return []updateKeyCandidate{
			{ledger: u, current: oldU, proposed: newU, row: row},
			{ledger: v, current: oldV, proposed: newV, row: row, snapshotHit: vHit},
		}
	}
	accepted, err := acceptUpdateKeys(f.mp, updateLedgerRow(1), candidate(0, true))
	require.NoError(t, err)
	require.False(t, accepted, "the outside owner of v=300 rejects the complete contribution")
	accepted, err = acceptUpdateKeys(f.mp, updateLedgerRow(2), candidate(1, false))
	require.NoError(t, err)
	require.True(t, accepted, "a rejected contribution must not reserve u=500")
	conflict, err := u.conflicts(newU, 0, updateLedgerRow(1), false)
	require.NoError(t, err)
	require.True(t, conflict, "the accepted second row now owns u=500")
}

func TestUpdateKeyLedgerParticipantVacancyAndNull(t *testing.T) {
	f := newUpdateLedgerFixture(t)
	ledger := f.ledger(t)
	old := f.ints(t, []int32{10, 20, 0}, 2)
	proposed := f.ints(t, []int32{40, 10, 0}, 2)
	for row := 0; row < 3; row++ {
		require.NoError(t, ledger.addParticipant(f.mp, old, old, row, updateLedgerRow(uint32(row+1))))
	}

	// Before row 1 moves, row 2 cannot claim its key, even though the key is
	// present in the starting snapshot and belongs to a participant.
	conflict, err := ledger.conflicts(proposed, 1, updateLedgerRow(2), true)
	require.NoError(t, err)
	require.True(t, conflict)
	accepted, err := acceptUpdateKeys(f.mp, updateLedgerRow(1), []updateKeyCandidate{{
		ledger: ledger, current: old, proposed: proposed, row: 0,
	}})
	require.NoError(t, err)
	require.True(t, accepted)
	accepted, err = acceptUpdateKeys(f.mp, updateLedgerRow(2), []updateKeyCandidate{{
		ledger: ledger, current: old, proposed: proposed, row: 1, snapshotHit: true,
	}})
	require.NoError(t, err)
	require.True(t, accepted, "a key released by a participant is available")
	accepted, err = acceptUpdateKeys(f.mp, updateLedgerRow(3), []updateKeyCandidate{{
		ledger: ledger, current: old, proposed: proposed, row: 2, snapshotHit: true,
	}})
	require.NoError(t, err)
	require.True(t, accepted, "NULL does not own or conflict with a UNIQUE key")
}

func TestUpdateKeyLedgerOutsideOwnerAndInvalidInput(t *testing.T) {
	f := newUpdateLedgerFixture(t)
	ledger := f.ledger(t)
	old := f.ints(t, []int32{10})
	outside := f.ints(t, []int32{30})
	require.NoError(t, ledger.addParticipant(f.mp, old, old, 0, updateLedgerRow(1)))
	conflict, err := ledger.conflicts(outside, 0, updateLedgerRow(1), true)
	require.NoError(t, err)
	require.True(t, conflict, "a snapshot owner outside the participant set remains occupied")
	conflict, err = ledger.conflicts(old, 0, updateLedgerRow(1), true)
	require.NoError(t, err)
	require.False(t, conflict, "a row may keep its own key")
	require.Error(t, ledger.addParticipant(f.mp, old, old, 0, updateLedgerRow(2)))
}
