// Copyright 2026 Matrix Origin
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

package databranchutils

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// This file is the one shared lock-key contract of the pk-move link
// retention ledger's row-lock serialization protocol (design D168): the
// EXTERNAL genesis probe's shared writer-fair acquisition, the retirement
// critical section's exclusive acquisition, and the fencing scratch
// admissions all address the SAME key space as the ordinary row locks the
// storage layer takes for the relation's own inserts and deletes, so every
// party must derive the key from the relation's live primary-key shape, not
// from a private assumption pinned at one schema generation.
//
// Two shapes are live in the field. The current wide ledger declares
// primary key(row_kind, table_id), which the storage layer materializes
// through the hidden composite-pk column: the row's lock key is the serial
// encoding of the components in declared order, byte-identical to the
// producer's insert encoding. The pre-upgrade dev-window ledger declares
// primary key(table_id): the row's lock key is the raw table_id value.
// Because lock keys of different bytes simply never meet, a shape mistake
// is silent serialization loss; derivations therefore READ the primary key
// definition off the resolved relation and refuse loudly on an unexpected
// shape.

// PkMoveRetentionRowLockKey composes the one-row key batch that locks the
// real retention identity row (row_kind = DataBranchLinkRetentionRowKindReal,
// table_id) of tableID under the primary-key shape def declares. def must be
// the retention relation's own live TableDef (the relation's GetTableDef).
// The returned batch and vector are owned by the caller (mp-allocated);
// pkType is the type the lock pipeline must be told the keys carry.
//
// A nil def, a missing primary key, an unexpected composite component, or a
// single-column key on any column but table_id fails loudly: locking the
// wrong key would silently unpair the probe/retirement serialization.
func PkMoveRetentionRowLockKey(
	mp *mpool.MPool,
	def *plan.TableDef,
	tableID uint64,
) (*batch.Batch, types.Type, error) {
	return pkMoveRetentionRowLockKeyForKind(mp, def, tableID, catalog.DataBranchLinkRetentionRowKindReal)
}

// PkMoveRetentionScratchLockKey composes the one-row key batch that locks
// the FENCING SCRATCH identity row (row_kind =
// DataBranchLinkRetentionRowKindFencingScratch, table_id) of tableID under
// the primary-key shape def declares — the disjoint-by-prefix sibling of
// the real row (design D168/D176): the scratch row shares the relation and
// the key-shape derivation, so a fencing round's exclusive admission on it
// serializes with the storage layer's own row locks on the same relation,
// and the same loud-shape guarantees apply. tableID here is the round's
// freshly drawn scratch table id (DataBranchFencingScratchDraw).
func PkMoveRetentionScratchLockKey(
	mp *mpool.MPool,
	def *plan.TableDef,
	tableID uint64,
) (*batch.Batch, types.Type, error) {
	return pkMoveRetentionRowLockKeyForKind(mp, def, tableID, catalog.DataBranchLinkRetentionRowKindFencingScratch)
}

func pkMoveRetentionRowLockKeyForKind(
	mp *mpool.MPool,
	def *plan.TableDef,
	tableID uint64,
	rowKind int64,
) (*batch.Batch, types.Type, error) {
	if def == nil || def.Pkey == nil {
		return nil, types.Type{}, moerr.NewInternalErrorNoCtx(
			"pk-move retention lock key: retention ledger primary key absent")
	}
	keys := batch.NewWithSize(1)
	fail := func(err error) (*batch.Batch, types.Type, error) {
		keys.Clean(mp)
		return nil, types.Type{}, err
	}
	if def.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		// Wide shape: the materialized composite key. Encode the components
		// in the declared Names order so the bytes are identical to the
		// producer's insert (row_kind first, then table_id in the current
		// DDL).
		var cpkeyType types.Type
		found := false
		for _, col := range def.Cols {
			if col.Name == catalog.CPrimaryKeyColName {
				cpkeyType = types.MustTypeFromPlan(col.Typ)
				found = true
				break
			}
		}
		if !found {
			return fail(moerr.NewInternalErrorNoCtx(
				"pk-move retention lock key: composite primary key column absent"))
		}
		rowKindVec := vector.NewVec(types.T_int8.ToType())
		defer rowKindVec.Free(mp)
		if err := vector.AppendFixed[int8](
			rowKindVec, int8(rowKind), false, mp); err != nil {
			return fail(err)
		}
		tableIDVec := vector.NewVec(types.T_uint64.ToType())
		defer tableIDVec.Free(mp)
		if err := vector.AppendFixed[uint64](tableIDVec, tableID, false, mp); err != nil {
			return fail(err)
		}
		packer := types.NewPacker()
		defer packer.Close()
		packer.Reset()
		for _, name := range def.Pkey.Names {
			var encVec *vector.Vector
			switch name {
			case catalog.DataBranchLinkRetentionColRowKind:
				encVec = rowKindVec
			case catalog.DataBranchLinkRetentionColTableID:
				encVec = tableIDVec
			default:
				return fail(moerr.NewInternalErrorNoCtxf(
					"pk-move retention lock key: unexpected composite component %q", name))
			}
			enc, encErr := planfunction.NewSerialValueEncoder(encVec)
			if encErr != nil {
				return fail(encErr)
			}
			enc(encVec, 0, packer)
		}
		keys.Vecs[0] = vector.NewVec(cpkeyType)
		if err := vector.AppendBytes(
			keys.Vecs[0], append([]byte(nil), packer.Bytes()...), false, mp); err != nil {
			return fail(err)
		}
		keys.SetRowCount(1)
		return keys, cpkeyType, nil
	}
	// Legacy dev-window shape: single-column primary key on table_id. Names
	// is either empty or the one-column echo on that shape; anything wider
	// on a non-composite key declaration is an unexpected shape.
	if def.Pkey.PkeyColName != catalog.DataBranchLinkRetentionColTableID ||
		len(def.Pkey.Names) > 1 ||
		(len(def.Pkey.Names) == 1 && def.Pkey.Names[0] != catalog.DataBranchLinkRetentionColTableID) {
		return fail(moerr.NewInternalErrorNoCtxf(
			"pk-move retention lock key: unexpected primary key shape %q", def.Pkey.PkeyColName))
	}
	pkType := types.T_uint64.ToType()
	keys.Vecs[0] = vector.NewVec(pkType)
	if err := vector.AppendFixed[uint64](keys.Vecs[0], tableID, false, mp); err != nil {
		return fail(err)
	}
	keys.SetRowCount(1)
	return keys, pkType, nil
}
