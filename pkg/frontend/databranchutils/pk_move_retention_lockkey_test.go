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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// wideRetentionLockDef mirrors the wide ledger's primary-key declaration:
// primary key(row_kind, table_id) realized through the hidden composite-pk
// column.
func wideRetentionLockDef() *plan.TableDef {
	return &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: catalog.DataBranchLinkRetentionColRowKind, Typ: plan.Type{Id: int32(types.T_int8)}},
			{Name: catalog.DataBranchLinkRetentionColTableID, Typ: plan.Type{Id: int32(types.T_uint64)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names: []string{
				catalog.DataBranchLinkRetentionColRowKind,
				catalog.DataBranchLinkRetentionColTableID,
			},
		},
	}
}

func legacyRetentionLockDef() *plan.TableDef {
	return &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: catalog.DataBranchLinkRetentionColTableID, Typ: plan.Type{Id: int32(types.T_uint64)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.DataBranchLinkRetentionColTableID},
	}
}

// TestPkMoveRetentionRowLockKeyWide pins the byte contract that makes the
// serialization real: the lock key of the REAL identity row equals the
// cpkey the producer's own insert encoding materializes for the same
// (row_kind=0, table_id) pair. Different bytes would land on a key nothing
// else ever locks and the whole protocol would silently never meet.
func TestPkMoveRetentionRowLockKeyWide(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	keys, pkType, err := PkMoveRetentionRowLockKey(mp, wideRetentionLockDef(), 4242)
	require.NoError(t, err)
	require.NotNil(t, keys)
	defer keys.Clean(mp)
	require.Equal(t, 1, keys.Vecs[0].Length())
	require.Equal(t, types.T_varchar, pkType.Oid)
	require.Equal(t, pkType.Oid, keys.Vecs[0].GetType().Oid)

	// Reference encoding: the producer's insert path packs serial(row_kind,
	// table_id) in CompPkeyColNames order for the REAL row kind.
	rowKindVec := vector.NewVec(types.T_int8.ToType())
	defer rowKindVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int8](
		rowKindVec, int8(catalog.DataBranchLinkRetentionRowKindReal), false, mp))
	tableIDVec := vector.NewVec(types.T_uint64.ToType())
	defer tableIDVec.Free(mp)
	require.NoError(t, vector.AppendFixed[uint64](tableIDVec, 4242, false, mp))
	packer := types.NewPacker()
	defer packer.Close()
	packer.Reset()
	encKind, err := planfunction.NewSerialValueEncoder(rowKindVec)
	require.NoError(t, err)
	encTable, err := planfunction.NewSerialValueEncoder(tableIDVec)
	require.NoError(t, err)
	encKind(rowKindVec, 0, packer)
	encTable(tableIDVec, 0, packer)
	require.Equal(t, packer.Bytes(), keys.Vecs[0].GetBytesAt(0),
		"lock bytes equal the insert-materialized cpkey bytes")

	// A different table id or row kind must produce different bytes: the
	// fencing scratch namespace (row_kind=1) must never collide with the
	// real identity row this key addresses.
	other, _, err := PkMoveRetentionRowLockKey(mp, wideRetentionLockDef(), 4243)
	require.NoError(t, err)
	defer other.Clean(mp)
	require.NotEqual(t, keys.Vecs[0].GetBytesAt(0), other.Vecs[0].GetBytesAt(0))
}

// TestPkMoveRetentionRowLockKeyLegacy covers the pre-upgrade dev-window
// shape: the raw table_id is the row's key.
func TestPkMoveRetentionRowLockKeyLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	keys, pkType, err := PkMoveRetentionRowLockKey(mp, legacyRetentionLockDef(), 900)
	require.NoError(t, err)
	require.NotNil(t, keys)
	defer keys.Clean(mp)
	require.Equal(t, types.T_uint64, pkType.Oid)
	require.Equal(t, uint64(900), vector.MustFixedColNoTypeCheck[uint64](keys.Vecs[0])[0])
}

// TestPkMoveRetentionScratchLockKeyWide pins the fencing scratch sibling of
// the wide-shape contract (D168/D176 disjoint-by-prefix): the scratch
// identity row (row_kind=1, table_id) encodes as the producer's own serial
// insert encoding for row_kind=1, and its bytes never collide with the real
// identity row for the SAME table id — the fencing round's exclusive
// admission serializes with storage's own row locks, and no scratch key can
// ever alias a real identity row.
func TestPkMoveRetentionScratchLockKeyWide(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	scratch, pkType, err := PkMoveRetentionScratchLockKey(mp, wideRetentionLockDef(), 4242)
	require.NoError(t, err)
	require.NotNil(t, scratch)
	defer scratch.Clean(mp)
	require.Equal(t, 1, scratch.Vecs[0].Length())
	require.Equal(t, types.T_varchar, pkType.Oid)

	// Reference encoding: serial(row_kind=1, table_id).
	rowKindVec := vector.NewVec(types.T_int8.ToType())
	defer rowKindVec.Free(mp)
	require.NoError(t, vector.AppendFixed[int8](
		rowKindVec, int8(catalog.DataBranchLinkRetentionRowKindFencingScratch), false, mp))
	tableIDVec := vector.NewVec(types.T_uint64.ToType())
	defer tableIDVec.Free(mp)
	require.NoError(t, vector.AppendFixed[uint64](tableIDVec, 4242, false, mp))
	packer := types.NewPacker()
	defer packer.Close()
	packer.Reset()
	encKind, err := planfunction.NewSerialValueEncoder(rowKindVec)
	require.NoError(t, err)
	encTable, err := planfunction.NewSerialValueEncoder(tableIDVec)
	require.NoError(t, err)
	encKind(rowKindVec, 0, packer)
	encTable(tableIDVec, 0, packer)
	require.Equal(t, packer.Bytes(), scratch.Vecs[0].GetBytesAt(0),
		"scratch lock bytes equal the insert-materialized cpkey bytes for row_kind=1")

	// Disjoint-by-prefix against the REAL identity row of the same id.
	real, _, err := PkMoveRetentionRowLockKey(mp, wideRetentionLockDef(), 4242)
	require.NoError(t, err)
	defer real.Clean(mp)
	require.NotEqual(t, real.Vecs[0].GetBytesAt(0), scratch.Vecs[0].GetBytesAt(0),
		"scratch and real identity keys of one table id must be disjoint")

	// And the shared shape guards fail loudly on the scratch path too.
	_, _, err = PkMoveRetentionScratchLockKey(mp, nil, 1)
	require.ErrorContains(t, err, "primary key absent")
}

// TestPkMoveRetentionRowLockKeyRefusals covers the loud shape failures:
// serialization loss must be an error, never a silently mis-keyed lock.
func TestPkMoveRetentionRowLockKeyRefusals(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	_, _, err := PkMoveRetentionRowLockKey(mp, nil, 1)
	require.ErrorContains(t, err, "primary key absent")

	_, _, err = PkMoveRetentionRowLockKey(mp, &plan.TableDef{}, 1)
	require.ErrorContains(t, err, "primary key absent")

	// A composite key over unexpected components.
	bogus := wideRetentionLockDef()
	bogus.Pkey.Names = []string{"something_else"}
	_, _, err = PkMoveRetentionRowLockKey(mp, bogus, 1)
	require.ErrorContains(t, err, "unexpected composite component")

	// A composite key declaration whose cpkey column is missing.
	bogus2 := wideRetentionLockDef()
	bogus2.Cols = bogus2.Cols[1:]
	_, _, err = PkMoveRetentionRowLockKey(mp, bogus2, 1)
	require.ErrorContains(t, err, "composite primary key column absent")

	// A single-column key on any column but table_id.
	bogus3 := legacyRetentionLockDef()
	bogus3.Pkey.PkeyColName = "database_name"
	_, _, err = PkMoveRetentionRowLockKey(mp, bogus3, 1)
	require.ErrorContains(t, err, "unexpected primary key shape")
}
