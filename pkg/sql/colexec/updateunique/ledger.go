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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const updateLedgerAllocationOwner mpool.AllocationOwner = 1

const (
	updateLedgerHashCell mpool.AllocationSite = iota + 1
	updateLedgerHashDescriptor
	updateLedgerVectorData
	updateLedgerVectorArea
	updateLedgerVectorNulls
	updateLedgerVectorGrouping
	updateLedgerIteratorScratch
)

const (
	updateKeyOriginal uint8 = 1 << iota
	updateKeyLive
)

// updateKeyLedgerAllocations are shared by the key ledgers of one owner.
type updateKeyLedgerAllocations struct {
	hash     *hashtable.AllocationAccountSelection
	vector   *vector.AllocationAccountSelection
	iterator *hashmap.IteratorAllocation
}

func newUpdateKeyLedgerAllocations(account *mpool.AllocationAccount) (*updateKeyLedgerAllocations, error) {
	hash, err := hashtable.NewAllocationAccountSelection(account, updateLedgerAllocationOwner, updateLedgerHashCell, updateLedgerHashDescriptor)
	if err != nil {
		return nil, err
	}
	vectors, err := vector.NewAllocationAccountSelection(account, updateLedgerAllocationOwner,
		updateLedgerVectorData, updateLedgerVectorArea, updateLedgerVectorNulls, updateLedgerVectorGrouping)
	if err != nil {
		return nil, err
	}
	iterator, err := hashmap.NewIteratorAllocation(account, updateLedgerAllocationOwner, updateLedgerIteratorScratch)
	if err != nil {
		return nil, err
	}
	return &updateKeyLedgerAllocations{hash: hash, vector: vectors, iterator: iterator}, nil
}

// updateKeyLedger tracks one UNIQUE key domain for all rows participating in
// one alias stage. The snapshot lookup supplies whether a proposed key exists;
// this ledger distinguishes an outside owner from a participant that released
// the key and tracks keys accepted earlier in the stage.
//
// All participants must be added before deciding any candidate. A ledger is
// owned by one execution and is not safe for concurrent use.
type updateKeyLedger struct {
	keys    *hashmap.StrHashMap
	iter    hashmap.Iterator
	owners  *vector.Vector
	flags   *vector.Vector
	keyType types.Type
	hasType bool
}

func newUpdateKeyLedger(mp *mpool.MPool, alloc *updateKeyLedgerAllocations) (*updateKeyLedger, error) {
	if mp == nil || alloc == nil || alloc.hash == nil || alloc.vector == nil || alloc.iterator == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	keys, err := hashmap.NewStrHashMapWithAllocations(false, mp, alloc.hash, alloc.iterator)
	if err != nil {
		return nil, err
	}
	owners, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_Rowid.ToType(), alloc.vector)
	if err != nil {
		keys.Free()
		return nil, err
	}
	flags, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_uint8.ToType(), alloc.vector)
	if err != nil {
		owners.Free(mp)
		keys.Free()
		return nil, err
	}
	return &updateKeyLedger{keys: keys, iter: keys.NewIterator(), owners: owners, flags: flags}, nil
}

func (l *updateKeyLedger) close(mp *mpool.MPool) {
	if l == nil {
		return
	}
	if l.iter != nil {
		hashmap.IteratorClearOwner(l.iter)
		l.iter = nil
	}
	if l.keys != nil {
		l.keys.Free()
		l.keys = nil
	}
	if l.owners != nil {
		l.owners.Free(mp)
		l.owners = nil
	}
	if l.flags != nil {
		l.flags.Free(mp)
		l.flags = nil
	}
}

func (l *updateKeyLedger) checkKey(key *vector.Vector, row int) error {
	if l == nil || l.iter == nil || key == nil || row < 0 || row >= key.Length() {
		return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE key")
	}
	if !l.hasType {
		l.keyType = *key.GetType()
		l.hasType = true
	} else if !l.keyType.Eq(*key.GetType()) {
		return moerr.NewInternalErrorNoCtx("repeated UPDATE key type changed")
	}
	return nil
}

// find returns -1 for a NULL or absent key. NULL never reserves a UNIQUE key.
func (l *updateKeyLedger) find(key *vector.Vector, row int) (int, error) {
	if err := l.checkKey(key, row); err != nil {
		return -1, err
	}
	if key.IsNull(uint64(row)) {
		return -1, nil
	}
	groups, nonnull, err := l.iter.Find(row, 1, []*vector.Vector{key})
	if err != nil {
		return -1, err
	}
	if nonnull[0] == 0 || groups[0] == 0 {
		return -1, nil
	}
	group := int(groups[0] - 1)
	if group < 0 || group >= l.owners.Length() || group >= l.flags.Length() {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE key owner")
	}
	return group, nil
}

func (l *updateKeyLedger) insert(mp *mpool.MPool, key *vector.Vector, row int) (int, error) {
	if err := l.checkKey(key, row); err != nil {
		return -1, err
	}
	if key.IsNull(uint64(row)) {
		return -1, nil
	}
	groups, nonnull, err := l.iter.Insert(row, 1, []*vector.Vector{key})
	if err != nil {
		return -1, err
	}
	if nonnull[0] == 0 || groups[0] == 0 {
		return -1, nil
	}
	group := int(groups[0] - 1)
	if group == l.owners.Length() {
		if err = vector.AppendFixed(l.owners, types.Rowid{}, false, mp); err != nil {
			return -1, err
		}
		if err = vector.AppendFixed(l.flags, uint8(0), false, mp); err != nil {
			return -1, err
		}
	}
	if group < 0 || group >= l.owners.Length() || group >= l.flags.Length() {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE key owner")
	}
	return group, nil
}

// addParticipant registers the snapshot and current key of a physical row.
// A duplicate non-NULL key indicates invalid input to the stage.
func (l *updateKeyLedger) addParticipant(mp *mpool.MPool, original, current *vector.Vector, row int, owner types.Rowid) error {
	group, err := l.insert(mp, original, row)
	if err != nil {
		return err
	}
	if group >= 0 {
		flags := vector.GetFixedAtNoTypeCheck[uint8](l.flags, group)
		if flags&updateKeyOriginal != 0 {
			return moerr.NewInternalErrorNoCtx("duplicate original repeated UPDATE key")
		}
		if err = vector.SetFixedAtNoTypeCheck(l.flags, group, flags|updateKeyOriginal); err != nil {
			return err
		}
	}
	group, err = l.insert(mp, current, row)
	if err != nil || group < 0 {
		return err
	}
	flags := vector.GetFixedAtNoTypeCheck[uint8](l.flags, group)
	if flags&updateKeyLive != 0 && vector.GetFixedAtNoTypeCheck[types.Rowid](l.owners, group) != owner {
		return moerr.NewInternalErrorNoCtx("duplicate current repeated UPDATE key")
	}
	if err = vector.SetFixedAtNoTypeCheck(l.owners, group, owner); err != nil {
		return err
	}
	return vector.SetFixedAtNoTypeCheck(l.flags, group, flags|updateKeyLive)
}

func (l *updateKeyLedger) conflicts(proposed *vector.Vector, row int, owner types.Rowid, snapshotHit bool) (bool, error) {
	group, err := l.find(proposed, row)
	if err != nil || proposed.IsNull(uint64(row)) {
		return false, err
	}
	if group < 0 {
		return snapshotHit, nil
	}
	flags := vector.GetFixedAtNoTypeCheck[uint8](l.flags, group)
	if flags&updateKeyLive != 0 && vector.GetFixedAtNoTypeCheck[types.Rowid](l.owners, group) != owner {
		return true, nil
	}
	return snapshotHit && flags&updateKeyOriginal == 0, nil
}

func (l *updateKeyLedger) move(mp *mpool.MPool, current, proposed *vector.Vector, row int, owner types.Rowid) error {
	group, err := l.find(current, row)
	if err != nil {
		return err
	}
	if group < 0 && !current.IsNull(uint64(row)) {
		return moerr.NewInternalErrorNoCtx("missing current repeated UPDATE key")
	}
	if group >= 0 {
		flags := vector.GetFixedAtNoTypeCheck[uint8](l.flags, group)
		if flags&updateKeyLive == 0 || vector.GetFixedAtNoTypeCheck[types.Rowid](l.owners, group) != owner {
			return moerr.NewInternalErrorNoCtx("invalid current repeated UPDATE key owner")
		}
		if err = vector.SetFixedAtNoTypeCheck(l.flags, group, flags&^updateKeyLive); err != nil {
			return err
		}
	}
	group, err = l.insert(mp, proposed, row)
	if err != nil || group < 0 {
		return err
	}
	flags := vector.GetFixedAtNoTypeCheck[uint8](l.flags, group)
	if flags&updateKeyLive != 0 && vector.GetFixedAtNoTypeCheck[types.Rowid](l.owners, group) != owner {
		return moerr.NewInternalErrorNoCtx("repeated UPDATE key was not admitted")
	}
	if err = vector.SetFixedAtNoTypeCheck(l.owners, group, owner); err != nil {
		return err
	}
	return vector.SetFixedAtNoTypeCheck(l.flags, group, flags|updateKeyLive)
}

type updateKeyCandidate struct {
	ledger      *updateKeyLedger
	current     *vector.Vector
	proposed    *vector.Vector
	row         int
	snapshotHit bool
}

// acceptUpdateKeys checks all constraints before releasing or reserving any
// key. If an allocation fails while applying an accepted candidate, the caller
// must discard the stage without publishing its output.
func acceptUpdateKeys(mp *mpool.MPool, owner types.Rowid, candidates []updateKeyCandidate) (bool, error) {
	for _, candidate := range candidates {
		conflict, err := candidate.ledger.conflicts(candidate.proposed, candidate.row, owner, candidate.snapshotHit)
		if err != nil || conflict {
			return false, err
		}
	}
	for _, candidate := range candidates {
		if err := candidate.ledger.move(mp, candidate.current, candidate.proposed, candidate.row, owner); err != nil {
			return false, err
		}
	}
	return true, nil
}
