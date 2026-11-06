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

package preinsertunique

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func odkuKeyTypeEqual(a, b types.Type) bool {
	// Integer widths/scales may differ between table metadata and expressions;
	// Type.Eq deliberately ignores those attributes. Physical size must still
	// agree. Decimal and floating scales remain part of Type.Eq.
	return a.Eq(b) && a.TypeSize() == b.TypeSize()
}

func odkuIntKey(v *vector.Vector) bool {
	if v.HasGrouping() || v.GetType().IsVarlen() {
		return false
	}
	switch v.GetType().TypeSize() {
	case 1, 2, 4, 8:
		return true
	default:
		return false
	}
}

func (preInsertUnique *PreInsertUnique) initODKUKey(proc *process.Process, slot int) error {
	return preInsertUnique.initODKUKeyIndex(proc, &preInsertUnique.ctr.acceptedKeys[slot])
}

func (preInsertUnique *PreInsertUnique) initODKUKeyIndex(proc *process.Process, key *odkuKeyIndex) error {
	key.keyType = *key.key[0].GetType()
	if key.keyType.Oid == types.T_float32 || key.keyType.Oid == types.T_float64 {
		var err error
		key.identity, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_varbinary.ToType(), preInsertUnique.retainedAllocation)
		if err != nil {
			return err
		}
	}
	if key.identity == nil && odkuIntKey(key.key[0]) {
		h, err := hashmap.NewIntHashMapWithAllocation(false, proc.Mp(), preInsertUnique.hashAllocation)
		if err != nil {
			return err
		}
		key.index = h
		key.iterator = h.NewIterator()
		key.fixedKeys, err = vector.NewOffHeapVecWithTypeAndAllocation(key.keyType, preInsertUnique.retainedAllocation)
		if err != nil {
			return err
		}
	} else if err := preInsertUnique.newODKUKeyStrMap(proc, key); err != nil {
		return err
	}
	var err error
	key.rows, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), preInsertUnique.retainedAllocation)
	return err
}

// Generic SQL hashmaps normalize floating values. Constraint owners instead
// hash their existing physical bytes, with the raw typed key kept for locks,
// retained images and duplicate errors. Scratch is owned and reused by this
// index; borrowed vector metadata is never changed.
func (preInsertUnique *PreInsertUnique) hashODKUKey(proc *process.Process, key *odkuKeyIndex, row int) (int, error) {
	key.hashKey = key.key
	if key.identity == nil {
		return row, nil
	}
	key.identity.CleanOnlyData()
	source := key.key[0]
	if err := vector.AppendBytes(key.identity, source.GetRawBytesAt(row), source.IsNull(uint64(row)), proc.Mp()); err != nil {
		return 0, err
	}
	if source.GetGrouping().Contains(uint64(row)) {
		key.identity.GetGrouping().Add(0)
	}
	key.hashKey[0] = key.identity
	return 0, nil
}

func (preInsertUnique *PreInsertUnique) newODKUKeyStrMap(proc *process.Process, key *odkuKeyIndex) error {
	h, err := hashmap.NewStrHashMapWithAllocations(false, proc.Mp(), preInsertUnique.hashAllocation, preInsertUnique.iteratorAllocation)
	if err != nil {
		return err
	}
	key.index = h
	key.iterator = h.NewIterator()
	return nil
}

// A later batch may carry GROUPING even when the first batch did not. Keep
// group-aligned fixed keys until a slot switches permanently to the generic
// encoder; rows continues to reference the same group identities.
func (preInsertUnique *PreInsertUnique) ensureODKUKeyMap(proc *process.Process, slot int) error {
	return preInsertUnique.ensureODKUKeyIndex(proc, &preInsertUnique.ctr.acceptedKeys[slot])
}

func (preInsertUnique *PreInsertUnique) ensureODKUKeyIndex(proc *process.Process, key *odkuKeyIndex) error {
	keys := key.fixedKeys
	if keys == nil || !key.key[0].HasGrouping() {
		return nil
	}
	if keys.Length() != key.rows.Length() || uint64(keys.Length()) != key.index.GroupCount() || !odkuKeyTypeEqual(*keys.GetType(), key.keyType) {
		return moerr.NewInternalError(proc.Ctx, "ODKU fixed-key history does not match groups")
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	hashmap.IteratorClearOwner(key.iterator)
	key.iterator = nil
	key.index.Free()
	key.index = nil
	if err := preInsertUnique.newODKUKeyStrMap(proc, key); err != nil {
		return err
	}
	vecs := []*vector.Vector{keys}
	for start := 0; start < keys.Length(); start += hashmap.UnitLimit {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		count := min(hashmap.UnitLimit, keys.Length()-start)
		groups, nonNull, err := key.iterator.Insert(start, count, vecs)
		if err != nil {
			return err
		}
		for i, g := range groups {
			if nonNull[i] == 0 || g != uint64(start+i+1) {
				return moerr.NewInternalError(proc.Ctx, "ODKU fixed-key rebuild changed group identity")
			}
		}
	}
	keys.Free(proc.Mp())
	key.fixedKeys = nil
	return nil
}

// lookupODKUKeyIndex returns the current logical owner. Vacated groups remain in
// the hash table until the owner rebuilds it from its live row images.
func (preInsertUnique *PreInsertUnique) lookupODKUKeyIndex(proc *process.Process, key *odkuKeyIndex, row int) (int64, bool, error) {
	if key.key[0].IsNull(uint64(row)) || key.iterator == nil {
		return 0, false, nil
	}
	if key.fixedKeys != nil && key.key[0].HasGrouping() {
		if err := preInsertUnique.ensureODKUKeyIndex(proc, key); err != nil {
			return 0, false, err
		}
	}
	if key.rows != nil && key.rows.Length() == 0 {
		return 0, false, nil
	}
	hashRow, err := preInsertUnique.hashODKUKey(proc, key, row)
	if err != nil {
		return 0, false, err
	}
	groups, nonNull, err := key.iterator.Find(hashRow, 1, key.hashKey[:])
	if err != nil {
		return 0, false, err
	}
	if nonNull[0] == 0 || groups[0] == 0 {
		return 0, false, nil
	}
	group := int(groups[0] - 1)
	if key.rows == nil || group >= key.rows.Length() {
		return 0, false, moerr.NewInternalError(proc.Ctx, "ODKU key owner is missing")
	}
	owner := vector.GetFixedAtNoTypeCheck[int64](key.rows, group)
	return owner, owner >= 0, nil
}

// claimODKUKey publishes a non-NULL key, reusing a previously vacated group
// when possible. A failed claim makes the enclosing attempt terminal.
func (preInsertUnique *PreInsertUnique) claimODKUKey(proc *process.Process, slot, row int, owner int64) error {
	return preInsertUnique.claimODKUKeyIndex(proc, &preInsertUnique.ctr.acceptedKeys[slot], row, owner)
}

func (preInsertUnique *PreInsertUnique) claimODKUKeyIndex(proc *process.Process, key *odkuKeyIndex, row int, owner int64) error {
	keyVec := key.key[0]
	if owner < 0 {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU key claim")
	}
	if keyVec.IsNull(uint64(row)) {
		return nil
	}
	if key.iterator == nil || key.rows == nil {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU key claim")
	}
	hashRow, err := preInsertUnique.hashODKUKey(proc, key, row)
	if err != nil {
		return err
	}
	isNew, err := key.iterator.DetectDup(key.hashKey[:], hashRow)
	if err != nil {
		return err
	}
	if !isNew {
		groups, nonNull, e := key.iterator.Find(hashRow, 1, key.hashKey[:])
		if e != nil {
			return e
		}
		if nonNull[0] == 0 || groups[0] == 0 || groups[0] > uint64(key.rows.Length()) {
			return moerr.NewInternalError(proc.Ctx, "ODKU key claim has no owner group")
		}
		group := int(groups[0] - 1)
		if vector.GetFixedAtNoTypeCheck[int64](key.rows, group) >= 0 || key.dead == 0 {
			return moerr.NewInternalError(proc.Ctx, "ODKU key already has an owner")
		}
		if e := vector.SetFixedAtNoTypeCheck[int64](key.rows, group, owner); e != nil {
			return e
		}
		key.dead--
		return nil
	}
	if key.fixedKeys != nil {
		if err := key.fixedKeys.UnionOne(keyVec, int64(row), proc.Mp()); err != nil {
			return err
		}
	}
	return vector.AppendFixed(key.rows, owner, false, proc.Mp())
}

// releaseODKUKey vacates exactly the current owner's key. It never removes a
// hash group, since the underlying map assigns stable group ordinals.
func (preInsertUnique *PreInsertUnique) releaseODKUKey(proc *process.Process, slot, row int, owner int64) error {
	key := &preInsertUnique.ctr.acceptedKeys[slot]
	if owner < 0 || key.key[0].IsNull(uint64(row)) || key.iterator == nil {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU key release")
	}
	if err := preInsertUnique.ensureODKUKeyMap(proc, slot); err != nil {
		return err
	}
	hashRow, err := preInsertUnique.hashODKUKey(proc, key, row)
	if err != nil {
		return err
	}
	groups, nonNull, err := key.iterator.Find(hashRow, 1, key.hashKey[:])
	if err != nil {
		return err
	}
	if nonNull[0] == 0 || groups[0] == 0 || key.rows == nil || groups[0] > uint64(key.rows.Length()) {
		return moerr.NewInternalError(proc.Ctx, "ODKU key release has no owner group")
	}
	group := int(groups[0] - 1)
	if vector.GetFixedAtNoTypeCheck[int64](key.rows, group) != owner {
		return moerr.NewInternalError(proc.Ctx, "ODKU key release owner mismatch")
	}
	if err := vector.SetFixedAtNoTypeCheck[int64](key.rows, group, -1); err != nil {
		return err
	}
	key.dead++
	return nil
}

// rebuildODKUKeyIndex discards vacated groups and rebuilds from the action
// owner's current physical keys. Input vectors must not alias the old index.
// On an error the enclosing attempt must be discarded and reset.
func (preInsertUnique *PreInsertUnique) rebuildODKUKeyIndex(proc *process.Process, slot int, keys *vector.Vector, owners []int64) error {
	key := &preInsertUnique.ctr.acceptedKeys[slot]
	if keys == nil || keys == key.fixedKeys || keys == key.rows || (owners != nil && keys.Length() != len(owners)) || !odkuKeyTypeEqual(*keys.GetType(), key.keyType) {
		return moerr.NewInternalError(proc.Ctx, "invalid ODKU key rebuild input")
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return err
	}
	current := key.key
	key.free(proc)
	key.key[0] = keys
	defer func() { key.key = current }()
	if err := preInsertUnique.initODKUKey(proc, slot); err != nil {
		return err
	}
	for row := 0; row < keys.Length(); row++ {
		if row&1023 == 0 {
			if err := context.Cause(proc.Ctx); err != nil {
				return err
			}
		}
		owner := int64(row)
		if owners != nil {
			owner = owners[row]
		}
		if err := preInsertUnique.claimODKUKey(proc, slot, row, owner); err != nil {
			return err
		}
	}
	return nil
}

// shouldRebuild limits retained vacated groups when the action owner checks it
// after a key move. The fixed slack avoids rebuilding for short statements.
func (key *odkuKeyIndex) shouldRebuild() bool {
	return key.rows != nil && key.dead >= 64 && key.dead > uint64(key.rows.Length())-key.dead
}

func (key *odkuKeyIndex) free(proc *process.Process) {
	hashmap.IteratorClearOwner(key.iterator)
	if key.index != nil {
		key.index.Free()
	}
	if key.fixedKeys != nil {
		key.fixedKeys.Free(proc.Mp())
	}
	if key.identity != nil {
		key.identity.Free(proc.Mp())
	}
	if key.rows != nil {
		key.rows.Free(proc.Mp())
	}
	*key = odkuKeyIndex{}
}
