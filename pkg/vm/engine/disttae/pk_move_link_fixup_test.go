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

package disttae

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// pkMoveEncodeInt64 encodes a pk value exactly the way the link writer and
// the branch hashmaps encode keys.
func pkMoveEncodeInt64(t *testing.T, v int64) []byte {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt64(v)
	return append([]byte(nil), packer.Bytes()...)
}

// pkMoveStagedInsertEntry builds one staged INSERT workspace entry in the
// post-WriteBatch layout: rowid vector prepended at Vecs[0], pk column at
// pkCheckPos.
func pkMoveStagedInsertEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableID uint64,
	pks []int64,
	nullPKs []uint64,
	rowids []types.Rowid,
) Entry {
	t.Helper()

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{catalog.Row_ID, "pk"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	for _, rid := range rowids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rid, false, mp))
	}
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	nullSet := make(map[uint64]struct{}, len(nullPKs))
	for _, n := range nullPKs {
		nullSet[n] = struct{}{}
	}
	for i, pk := range pks {
		_, isNull := nullSet[uint64(i)]
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], pk, isNull, mp))
	}
	bat.SetRowCount(len(pks))
	return Entry{
		typ:        INSERT,
		accountId:  accountID,
		tableId:    tableID,
		bat:        bat,
		pkCheckPos: 1,
	}
}

func TestPkMoveHarvestNewRowIDs(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	r1 := types.BuildTestRowid(1, 1)
	r2 := types.BuildTestRowid(1, 2)
	r3 := types.BuildTestRowid(2, 1)

	t.Run("maps encoded pk bytes to staged row ids", func(t *testing.T) {
		entries := []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3, 4}, nil, []types.Rowid{r1, r2}),
		}
		harvest := pkMoveHarvestNewRowIDs(entries, 7, 42)
		require.Len(t, harvest, 2)
		require.Equal(t, r1, harvest[string(pkMoveEncodeInt64(t, 3))])
		require.Equal(t, r2, harvest[string(pkMoveEncodeInt64(t, 4))])
	})

	t.Run("freshest staged row wins for a re-staged pk", func(t *testing.T) {
		entries := []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1}),
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r3}),
		}
		require.Equal(t, r3, pkMoveHarvestNewRowIDs(entries, 7, 42)[string(pkMoveEncodeInt64(t, 3))])
	})

	t.Run("skips entries outside the requested account or table", func(t *testing.T) {
		entries := []Entry{
			pkMoveStagedInsertEntry(t, mp, 8, 42, []int64{3}, nil, []types.Rowid{r1}),
			pkMoveStagedInsertEntry(t, mp, 7, 43, []int64{3}, nil, []types.Rowid{r2}),
		}
		require.Nil(t, pkMoveHarvestNewRowIDs(entries, 7, 42))
		require.Len(t, pkMoveHarvestNewRowIDs(entries, 8, 42), 1)
	})

	t.Run("skips non-insert entries and malformed batches", func(t *testing.T) {
		deleted := pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1})
		deleted.typ = DELETE

		noBatch := Entry{typ: INSERT, accountId: 7, tableId: 42, pkCheckPos: 1}

		noRowidPrefix := pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1})
		noRowidPrefix.bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(noRowidPrefix.bat.Vecs[0], int64(1), false, mp))

		unknownPKPos := pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1})
		unknownPKPos.pkCheckPos = -1

		outOfRangePKPos := pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1})
		outOfRangePKPos.pkCheckPos = 9

		entries := []Entry{deleted, noBatch, noRowidPrefix, unknownPKPos, outOfRangePKPos}
		require.Nil(t, pkMoveHarvestNewRowIDs(entries, 7, 42))
	})

	t.Run("null pk rows carry no identity", func(t *testing.T) {
		entries := []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3, 4}, []uint64{0}, []types.Rowid{r1, r2}),
		}
		harvest := pkMoveHarvestNewRowIDs(entries, 7, 42)
		require.Len(t, harvest, 1)
		require.Equal(t, r2, harvest[string(pkMoveEncodeInt64(t, 4))])
	})
}

// pkMoveLinkStagedBatch builds the link-table batch in the post-prepend
// layout fillPkMoveLinkNewRowIDsLocked expects.
func pkMoveLinkStagedBatch(
	t *testing.T,
	mp *mpool.MPool,
	linkRowids []types.Rowid,
	tableIDs []uint64,
	newPKs [][]byte,
) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(11)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	for _, rid := range linkRowids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rid, false, mp))
	}
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	for _, tid := range tableIDs {
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], tid, false, mp))
	}
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	for i, newPK := range newPKs {
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], newPK, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint64(100+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
	}
	// Every link row starts with NULL new_row_id; the workspace back-fills it.
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	for range newPKs {
		require.NoError(t, vector.AppendBytes(bat.Vecs[8], nil, true, mp))
	}
	bat.SetRowCount(len(newPKs))
	return bat
}

func TestFillPkMoveLinkNewRowIDsLocked(t *testing.T) {
	r1 := types.BuildTestRowid(1, 1)
	r2 := types.BuildTestRowid(1, 2)
	linkRid1 := types.BuildTestRowid(9, 1)
	linkRid2 := types.BuildTestRowid(9, 2)
	linkRid3 := types.BuildTestRowid(9, 3)

	t.Run("back-fills staged replacement identities, leaves the rest NULL", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()

		txn := &Transaction{
			proc: proc,
			writes: []Entry{
				pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3, 4}, nil, []types.Rowid{r1, r2}),
			},
		}
		bat := pkMoveLinkStagedBatch(t, mp,
			[]types.Rowid{linkRid1, linkRid2, linkRid3},
			[]uint64{42, 42, 43},
			[][]byte{pkMoveEncodeInt64(t, 3), pkMoveEncodeInt64(t, 9), pkMoveEncodeInt64(t, 3)},
		)
		defer bat.Clean(mp)

		txn.fillPkMoveLinkNewRowIDsLocked(7, bat)

		newRowVec := bat.Vecs[8]
		require.Equal(t, 3, newRowVec.Length())
		require.False(t, newRowVec.IsNull(0))
		require.Equal(t, r1[:], newRowVec.GetBytesAt(0))
		// no staged insert of pk 9 for table 42: stays NULL (fail closed).
		require.True(t, newRowVec.IsNull(1))
		// table 43 in another table id: no harvest match even for pk 3.
		require.True(t, newRowVec.IsNull(2))
	})

	t.Run("schema drift leaves the batch untouched", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()

		txn := &Transaction{
			proc: proc,
			writes: []Entry{
				pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1}),
			},
		}
		// A pre-v3 catalog without the new_row_id column.
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{
			catalog.DataBranchUpdateLinkColTableID,
			catalog.DataBranchUpdateLinkColNewPK,
		})
		bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint64(42), false, mp))
		bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], pkMoveEncodeInt64(t, 3), false, mp))
		bat.SetRowCount(1)
		defer bat.Clean(mp)

		require.NotPanics(t, func() {
			txn.fillPkMoveLinkNewRowIDsLocked(7, bat)
		})
		require.Equal(t, 2, bat.VectorCount())
	})

	t.Run("unexpected table id vector type leaves the batch untouched", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		txn := &Transaction{proc: proc}
		bat := pkMoveLinkStagedBatch(t, mp,
			[]types.Rowid{linkRid1},
			[]uint64{42},
			[][]byte{pkMoveEncodeInt64(t, 3)},
		)
		defer bat.Clean(mp)

		bat.Vecs[2].Free(mp)
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("42"), false, mp))

		require.NotPanics(t, func() {
			txn.fillPkMoveLinkNewRowIDsLocked(7, bat)
		})
		require.True(t, bat.Vecs[8].IsNull(0))
	})
}

// errPkMoveInjected faults the allocation seams in the provenance paths.
var errPkMoveInjected = errors.New("injected pk-move allocation failure")

// pkMoveSwapVecSeams substitutes the vector-op seams the provenance
// bookkeeping uses to touch the mpool; the originals are restored when
// the test ends.
func pkMoveSwapVecSeams(
	t *testing.T,
	appendBytes func(*vector.Vector, []byte, bool, *mpool.MPool) error,
	setBytesAt func(*vector.Vector, int, []byte, *mpool.MPool) error,
) {
	t.Helper()
	origAppend, origSet := pkMoveVecAppendBytes, pkMoveVecSetBytesAt
	pkMoveVecAppendBytes, pkMoveVecSetBytesAt = appendBytes, setBytesAt
	t.Cleanup(func() { pkMoveVecAppendBytes, pkMoveVecSetBytesAt = origAppend, origSet })
}

func pkMoveRestoreVecSeams(t *testing.T) {
	t.Helper()
	pkMoveVecAppendBytes, pkMoveVecSetBytesAt = vector.AppendBytes, vector.SetBytesAt
}

// pkMoveLinkNamedBatch builds a two-row staged link batch whose new_row_id
// cells already name `newRow`, the state a same-key edit's retarget scans
// for. old/new pk bytes are placeholders: the retarget matches identities,
// not keys.
func pkMoveLinkNamedBatch(
	t *testing.T,
	mp *mpool.MPool,
	linkIDs []string,
	tableIDs []uint64,
	newRow types.Rowid,
) *batch.Batch {
	t.Helper()
	bat := pkMoveLinkStagedBatch(t, mp,
		[]types.Rowid{types.BuildTestRowid(9, 1), types.BuildTestRowid(9, 2)},
		tableIDs,
		[][]byte{pkMoveEncodeInt64(t, 3), pkMoveEncodeInt64(t, 4)},
	)
	// Re-stamp the link ids the fixture mints (those cells were appended
	// non-null, so an in-place rewrite takes).
	for i, id := range linkIDs {
		require.NoError(t, vector.SetBytesAt(bat.Vecs[1], i, []byte(id), mp))
	}
	// The new_row cells must be genuinely non-null (SetBytesAt cannot
	// lift the null mask): rebuild the column with the named identity
	// appended as live bytes.
	bat.Vecs[8].Free(mp)
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	for range linkIDs {
		require.NoError(t, vector.AppendBytes(bat.Vecs[8], newRow[:], false, mp))
	}
	return bat
}

// pkMoveMarkerBatch builds the one-row same-key continuity marker whose
// Pass 2 resolution retargets staged links naming `preImage`.
func pkMoveMarkerBatch(
	t *testing.T,
	mp *mpool.MPool,
	tableID uint64,
	key int64,
	preImage types.Rowid,
) *batch.Batch {
	t.Helper()
	enc := pkMoveEncodeInt64(t, key)
	bat := pkMoveLinkStagedBatch(t, mp,
		[]types.Rowid{types.BuildTestRowid(9, 9)},
		[]uint64{tableID},
		[][]byte{enc},
	)
	require.NoError(t, vector.SetBytesAt(bat.Vecs[5], 0, enc, mp))
	require.NoError(t, vector.SetBytesAt(bat.Vecs[4], 0, preImage[:], mp))
	return bat
}

func TestFillPkMoveAllocFailurePropagates(t *testing.T) {
	t.Run("fill surfaces the allocation failure and leaves the batch untouched", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		txn := &Transaction{proc: proc}
		bat := pkMoveLinkStagedBatch(t, mp,
			[]types.Rowid{types.BuildTestRowid(9, 1), types.BuildTestRowid(9, 2)},
			[]uint64{42, 42},
			[][]byte{pkMoveEncodeInt64(t, 3), pkMoveEncodeInt64(t, 4)},
		)
		defer bat.Clean(mp)
		origVec := bat.Vecs[8]

		pkMoveSwapVecSeams(t,
			func(*vector.Vector, []byte, bool, *mpool.MPool) error { return errPkMoveInjected },
			vector.SetBytesAt,
		)
		err := txn.fillPkMoveLinkNewRowIDsLocked(7, bat)
		require.ErrorIs(t, err, errPkMoveInjected)
		// The original column rides on: nothing half-filled took its place.
		require.Same(t, origVec, bat.Vecs[8])
		require.True(t, bat.Vecs[8].IsNull(0))
		require.True(t, bat.Vecs[8].IsNull(1))
	})

	t.Run("workspace staging fails the statement instead of committing partial provenance", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		txn := &Transaction{proc: proc}

		// Link-table insert arriving through the real workspace write path
		// (the rowid vector is prepended by the write itself, like the
		// production hook expects).
		bat := batch.NewWithSize(10)
		bat.SetAttributes([]string{
			catalog.DataBranchUpdateLinkColLinkID,
			catalog.DataBranchUpdateLinkColTableID,
			catalog.DataBranchUpdateLinkColOldPKSha,
			catalog.DataBranchUpdateLinkColOldRow,
			catalog.DataBranchUpdateLinkColOldPK,
			catalog.DataBranchUpdateLinkColNewPKSha,
			catalog.DataBranchUpdateLinkColNewPK,
			catalog.DataBranchUpdateLinkColNewRow,
			catalog.DataBranchUpdateLinkColWriteTS,
			catalog.DataBranchUpdateLinkColTxnID,
		})
		bat.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[7] = vector.NewVec(types.T_binary.ToType())
		bat.Vecs[8] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[9] = vector.NewVec(types.T_binary.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("L1"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(42), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], pkMoveEncodeInt64(t, 3), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], nil, true, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[8], uint64(100), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[9], testDisttaePkMoveTxnID(1), false, mp))
		bat.SetRowCount(1)
		defer bat.Clean(mp)

		pkMoveSwapVecSeams(t,
			func(*vector.Vector, []byte, bool, *mpool.MPool) error { return errPkMoveInjected },
			vector.SetBytesAt,
		)
		_, err := txn.writeBatchWithAutoIncrEpochKnown(
			context.Background(),
			INSERT, "", 7, 100, 200,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK,
			bat, DNStore{}, 0, false,
		)
		// The pre-F4 code swallowed exactly this failure and staged the
		// link without its completion; the statement must die instead.
		require.ErrorIs(t, err, errPkMoveInjected)
		require.Empty(t, txn.writes)
	})
}

func TestRetargetFaultStatementFailureAndRestore(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	preImage := types.BuildTestRowid(5, 5)
	fresh := types.BuildTestRowid(6, 6)

	linkBat := pkMoveLinkNamedBatch(t, mp, []string{"L1", "L2"}, []uint64{42, 42}, preImage)
	defer linkBat.Clean(mp)
	txn := &Transaction{
		proc: proc,
		writes: []Entry{
			{
				typ:          INSERT,
				accountId:    7,
				databaseId:   100,
				tableId:      200,
				databaseName: catalog.MO_CATALOG,
				tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
				bat:          linkBat,
			},
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{fresh}),
		},
		// The birth population is the retarget's "there is something to
		// scan" guard; both link rows were staged by an earlier statement.
		pkMoveLinkBirth: map[string]int{"L1": 1, "L2": 1},
	}
	txn.accountWorkspaceEntryLocked(&txn.writes[0])

	marker := pkMoveMarkerBatch(t, mp, 42, 3, preImage)
	defer marker.Clean(mp)

	// The first retarget mutation succeeds, the second allocation faults:
	// a half-retargeted link set is precisely the state the pre-F4 code
	// committed silently. The staged cells are non-null, so the retarget
	// rides the D148 in-place overwrite seam (r40 review F2), the same
	// seam the sibling restore test swaps.
	calls := 0
	origOverwrite := pkMoveVecOverwriteRowidAt
	pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
		calls++
		if calls == 2 {
			return errPkMoveInjected
		}
		return origOverwrite(v, idx, bs, m)
	}
	t.Cleanup(func() { pkMoveVecOverwriteRowidAt = origOverwrite })
	err := txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
	require.ErrorIs(t, err, errPkMoveInjected)
	// The seam fired exactly twice: cell 0 retargeted, cell 1 faulted
	// before mutation. This count keeps the swap from going dormant if
	// the retarget path ever leaves the overwrite seam again.
	require.Equal(t, 2, calls)
	require.Equal(t, fresh[:], linkBat.Vecs[8].GetBytesAt(0))
	require.Equal(t, preImage[:], linkBat.Vecs[8].GetBytesAt(1))

	// The workspace memory accounting must be balanced around the faulted
	// entry mutation: unaccount compares against the live batch size and
	// panics on drift.
	require.NotPanics(t, func() { txn.unaccountWorkspaceEntryLocked(&txn.writes[0]) })
	txn.accountWorkspaceEntryLocked(&txn.writes[0])

	pkMoveVecOverwriteRowidAt = origOverwrite
	require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
	// The successful half of the faulted retarget is restored by the
	// rollback; the half that never mutated was already correct.
	require.Equal(t, preImage[:], linkBat.Vecs[8].GetBytesAt(0))
	require.Equal(t, preImage[:], linkBat.Vecs[8].GetBytesAt(1))
	require.NotPanics(t, func() { txn.unaccountWorkspaceEntryLocked(&txn.writes[0]) })
	txn.accountWorkspaceEntryLocked(&txn.writes[0])
}

func TestRollbackPkMoveRestoreFaultSurfacesAndCompletes(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	preImage := types.BuildTestRowid(5, 5)
	fresh := types.BuildTestRowid(6, 6)

	linkBat := pkMoveLinkNamedBatch(t, mp, []string{"L1", "L2"}, []uint64{42, 42}, preImage)
	defer linkBat.Clean(mp)
	txn := &Transaction{
		proc: proc,
		writes: []Entry{
			{
				typ:          INSERT,
				accountId:    7,
				databaseId:   100,
				tableId:      200,
				databaseName: catalog.MO_CATALOG,
				tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
				bat:          linkBat,
			},
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{fresh}),
		},
		pkMoveLinkBirth: map[string]int{"L1": 1, "L2": 1},
	}
	txn.accountWorkspaceEntryLocked(&txn.writes[0])

	marker := pkMoveMarkerBatch(t, mp, 42, 3, preImage)
	defer marker.Clean(mp)
	require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
	require.Equal(t, fresh[:], linkBat.Vecs[8].GetBytesAt(0))
	require.Equal(t, fresh[:], linkBat.Vecs[8].GetBytesAt(1))

	// One restore allocation fails at rollback time: the failure must
	// reach the caller (it used to be discarded into `_ =`), and the
	// remaining undo records must still run to completion. The staged
	// cells are non-null, so the restore rides the D148 in-place
	// overwrite seam.
	calls := 0
	origOverwrite := pkMoveVecOverwriteRowidAt
	pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
		calls++
		if calls == 1 {
			return errPkMoveInjected
		}
		return origOverwrite(v, idx, bs, m)
	}
	t.Cleanup(func() { pkMoveVecOverwriteRowidAt = origOverwrite })
	err := txn.rollbackPkMoveStmtStateLocked()
	require.ErrorIs(t, err, errPkMoveInjected)
	// The undo list runs in reverse, so the fault hit L2's restore; L1's
	// still completed. Cell 1 stays on the faulted value so the failure
	// is observable rather than quietly committed.
	require.Equal(t, preImage[:], linkBat.Vecs[8].GetBytesAt(0))
	require.Equal(t, fresh[:], linkBat.Vecs[8].GetBytesAt(1))
	require.Empty(t, txn.pkMoveRetargetUndo)
	require.NotPanics(t, func() { txn.unaccountWorkspaceEntryLocked(&txn.writes[0]) })
}

// pkMoveWorkspaceRowid builds a pre-commit workspace identity the way the
// commit pipeline allocates staged row ids: the dedicated workspace segment
// with a per-row offset.
func pkMoveWorkspaceRowid(off uint32) types.Rowid {
	var rid types.Rowid
	rid.SetSegment(colexec.TxnWorkspaceSegment)
	rid.SetRowOffset(off)
	return rid
}

// testDisttaePkMoveTxnID returns a fabricated 16-byte recording-transaction
// identity with the tag stamped into its first byte.
func testDisttaePkMoveTxnID(tag byte) []byte {
	buf := make([]byte, catalog.DataBranchUpdateLinkTxnIDBytes)
	buf[0] = tag
	buf[1] = 0xa5
	return buf
}

// pkMovePruneLinkEntry builds one staged INSERT for the link catalog table
// with per-row control of the old_row_id and new_row_id columns; a nil
// pointer stages a NULL.
func pkMovePruneLinkEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableIDs []uint64,
	oldRows, newRows []*types.Rowid,
	writeTSes []uint64,
) Entry {
	t.Helper()
	n := len(tableIDs)
	require.Len(t, oldRows, n)
	require.Len(t, newRows, n)
	require.Len(t, writeTSes, n)

	bat := batch.NewWithSize(11)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(9, int64(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], tableIDs[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte{byte(i)}, false, mp))
		if oldRows[i] == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], oldRows[i][:], false, mp))
		}
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], []byte{byte(i)}, false, mp))
		if newRows[i] == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], newRows[i][:], false, mp))
		}
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], writeTSes[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
	}
	bat.SetRowCount(n)
	return Entry{
		typ:          INSERT,
		accountId:    accountID,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		bat:          bat,
	}
}

// pkMoveDeleteEntry builds one staged DELETE entry: the rowid vector at
// Vecs[0] is the only column pruneDeadPkMoveLinkRows reads.
func pkMoveDeleteEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableID uint64,
	rowids []types.Rowid,
) Entry {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{catalog.Row_ID, "pk"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	for _, rid := range rowids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rid, false, mp))
	}
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	for range rowids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(0), false, mp))
	}
	bat.SetRowCount(len(rowids))
	return Entry{
		typ:          DELETE,
		accountId:    accountID,
		tableId:      tableID,
		databaseName: "user_db",
		tableName:    "user_t",
		bat:          bat,
		pkCheckPos:   1,
	}
}

// pkMoveLinkWriteTSes returns the write_ts values of a staged link entry in
// row order; each test row carries a distinct marker so shrink results are
// directly assertable.
func pkMoveLinkWriteTSes(t *testing.T, e *Entry) []uint64 {
	t.Helper()
	for i, attr := range e.bat.Attrs {
		if attr == catalog.DataBranchUpdateLinkColWriteTS {
			ts := vector.MustFixedColNoTypeCheck[uint64](e.bat.Vecs[i])
			return append([]uint64(nil), ts[:e.bat.RowCount()]...)
		}
	}
	t.Fatalf("link entry has no write_ts column")
	return nil
}

// applyPkMoveKeeps mirrors pruneDeadPkMoveLinkRowsLocked's mutation shape so
// the pure planner's decisions are verified against real row contents.
func applyPkMoveKeeps(t *testing.T, writes []Entry, keeps map[int][]int64) {
	t.Helper()
	for idx, keep := range keeps {
		we := &writes[idx]
		if len(keep) == we.bat.RowCount() {
			continue
		}
		we.bat.Shrink(keep, false)
		we.bat.SetRowCount(we.bat.Vecs[0].Length())
	}
}

func TestPruneDeadPkMoveLinkRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)

	ridDead := pkMoveWorkspaceRowid(1)     // staged then deleted, no successor
	ridUnstaged := pkMoveWorkspaceRowid(2) // never staged for (7, 42)
	ridAlive := pkMoveWorkspaceRowid(3)    // staged and still alive
	ridChained := pkMoveWorkspaceRowid(4)  // staged + deleted, successor link names it
	ridAlive2 := pkMoveWorkspaceRowid(5)   // the successor link's replacement
	ridCommitted := types.BuildTestRowid(6, 6)

	t.Run("drops dead replacements, keeps live null non-workspace and chained rows", func(t *testing.T) {
		entryMain := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable, userTable, userTable, userTable, userTable, userTable},
			[]*types.Rowid{nil, nil, nil, nil, nil, nil},
			[]*types.Rowid{&ridDead, &ridUnstaged, &ridAlive, &ridChained, nil, &ridCommitted},
			[]uint64{100, 101, 102, 103, 104, 105},
		)
		entryChain := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{&ridChained},
			[]*types.Rowid{&ridAlive2},
			[]uint64{200},
		)
		writes := []Entry{
			entryMain,
			entryChain,
			pkMoveStagedInsertEntry(t, mp, 7, userTable,
				[]int64{3, 4, 5, 6}, nil,
				[]types.Rowid{ridDead, ridAlive, ridChained, ridAlive2}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridDead, ridChained}),
			// Isolation decoys: the same row ids staged for another account
			// or another table must never resurrect a replacement.
			pkMoveStagedInsertEntry(t, mp, 8, userTable, []int64{2}, nil, []types.Rowid{ridUnstaged}),
			pkMoveStagedInsertEntry(t, mp, 7, 43, []int64{3}, nil, []types.Rowid{ridDead}),
		}

		keeps := pkMoveLinkRowsToKeep(writes, nil)
		require.Equal(t, map[int][]int64{0: {2, 3, 4, 5}}, keeps)
		applyPkMoveKeeps(t, writes, keeps)
		require.Equal(t, []uint64{102, 103, 104, 105}, pkMoveLinkWriteTSes(t, &writes[0]))
		require.Equal(t, []uint64{200}, pkMoveLinkWriteTSes(t, &writes[1]))

		// Commit retries may prune again: the shrunken result is stable.
		require.Empty(t, pkMoveLinkRowsToKeep(writes, nil))
		require.Equal(t, []uint64{102, 103, 104, 105}, pkMoveLinkWriteTSes(t, &writes[0]))
		require.Equal(t, []uint64{200}, pkMoveLinkWriteTSes(t, &writes[1]))
	})

	t.Run("dead tail takes the whole chain down in one pass", func(t *testing.T) {
		// UPDATE 2->3; UPDATE 3->4; DELETE 4; INSERT (3,'unrelated') in one
		// transaction: neither link's generation makes it to commit. A
		// one-hop chained check would keep the ancestor because its dead
		// replacement is named by the (itself dead) successor link; the
		// fixpoint drops both, so the resolver can never fold 2->3 onto
		// the unrelated row now holding key 3.
		ridB := pkMoveWorkspaceRowid(21) // update 2->3 insert, deleted by update 3->4
		ridC := pkMoveWorkspaceRowid(22) // update 3->4 insert, deleted by DELETE 4
		ridD := pkMoveWorkspaceRowid(23) // the unrelated reincarnation at key 3
		deadChain := []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable, userTable},
				[]*types.Rowid{nil, &ridB},
				[]*types.Rowid{&ridB, &ridC},
				[]uint64{400, 401}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3, 4, 3}, nil,
				[]types.Rowid{ridB, ridC, ridD}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridB, ridC}),
		}
		keeps := pkMoveLinkRowsToKeep(deadChain, nil)
		require.Equal(t, map[int][]int64{0: nil}, keeps)
		applyPkMoveKeeps(t, deadChain, keeps)
		require.Equal(t, 0, deadChain[0].bat.RowCount())
		// Idempotent: a second pass over the shrunken workspace keeps
		// exactly the rows already kept.
		require.Empty(t, pkMoveLinkRowsToKeep(deadChain, nil))
	})

	t.Run("rescue propagates through a multi-hop chain", func(t *testing.T) {
		// UPDATE 2->3; UPDATE 3->4; UPDATE 4->5 in one transaction: both
		// intermediate generations die inside the transaction and only the
		// terminal replacement survives. Every link of the chain must be
		// kept — dropping any hop orphans the resolver's walk across the
		// netted generations.
		ridB := pkMoveWorkspaceRowid(31) // 2->3 insert, deleted by 3->4
		ridC := pkMoveWorkspaceRowid(32) // 3->4 insert, deleted by 4->5
		ridD := pkMoveWorkspaceRowid(33) // 4->5 insert, survives
		liveChain := []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable, userTable, userTable},
				[]*types.Rowid{nil, &ridB, &ridC},
				[]*types.Rowid{&ridB, &ridC, &ridD},
				[]uint64{500, 501, 502}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3, 4, 5}, nil,
				[]types.Rowid{ridB, ridC, ridD}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridB, ridC}),
		}
		require.Empty(t, pkMoveLinkRowsToKeep(liveChain, nil))
		require.Equal(t, 3, liveChain[0].bat.RowCount())
		require.Empty(t, pkMoveLinkRowsToKeep(liveChain, nil))
	})

	t.Run("prunes an entire entry down to zero rows", func(t *testing.T) {
		dead := pkMoveWorkspaceRowid(11)
		writes := []Entry{
			pkMovePruneLinkEntry(t, mp, 7, []uint64{userTable},
				[]*types.Rowid{nil}, []*types.Rowid{&dead}, []uint64{300}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{dead}),
		}
		keeps := pkMoveLinkRowsToKeep(writes, nil)
		require.Equal(t, map[int][]int64{0: nil}, keeps)
		applyPkMoveKeeps(t, writes, keeps)
		require.Equal(t, 0, writes[0].bat.RowCount())
	})

	t.Run("workspaces without link entries are untouched", func(t *testing.T) {
		writes := []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3, 4}, nil,
				[]types.Rowid{ridAlive, ridAlive2}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridAlive}),
		}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, nil))
		require.Equal(t, 2, writes[0].bat.RowCount())
		require.Equal(t, 1, writes[1].bat.RowCount())
	})

	t.Run("an unstaged workspace identity dies fail-closed", func(t *testing.T) {
		// The pin-keep contract (TestPkMoveDumpKeepsPinnedRows) keeps
		// every row a staged link names inside the in-memory workspace
		// until the final prune, so production never reaches this shape;
		// if it ever did, no spill supplement may resurrect the link —
		// an identity absent from the staged set is dead.
		ridSpilled := pkMoveWorkspaceRowid(41)
		writes := []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable},
				[]*types.Rowid{nil},
				[]*types.Rowid{&ridSpilled},
				[]uint64{600}),
		}
		keeps := pkMoveLinkRowsToKeep(writes, nil)
		require.Equal(t, map[int][]int64{0: nil}, keeps)
		applyPkMoveKeeps(t, writes, keeps)
		require.Equal(t, 0, writes[0].bat.RowCount())
	})

	t.Run("an unstaged replacement still dies under a staged delete", func(t *testing.T) {
		// Absence proves death, never immunity: a staged delete naming
		// the link's replacement identity retires the link whether or
		// not the identity ever sat in the workspace this pass sees.
		ridSpilled := pkMoveWorkspaceRowid(42)
		writes := []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable},
				[]*types.Rowid{nil},
				[]*types.Rowid{&ridSpilled},
				[]uint64{601}),
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridSpilled}),
		}
		keeps := pkMoveLinkRowsToKeep(writes, nil)
		require.Equal(t, map[int][]int64{0: nil}, keeps)
		applyPkMoveKeeps(t, writes, keeps)
		require.Equal(t, 0, writes[0].bat.RowCount())
	})

	t.Run("an unstaged replacement dies under the diverted death ledger", func(t *testing.T) {
		// The diverted deletion path: the delete arrives with the
		// object's physical row id and is DIVERTED (deletedBlocks + a
		// pk death entry), never appearing as a workspace delete naming
		// the recorded identity. Replacement liveness rides the
		// pk-level death ledger here: no staged row can vouch for the
		// identity, and only the ledger decides.
		ridSpilled := pkMoveWorkspaceRowid(45)
		writes := []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable},
				[]*types.Rowid{nil},
				[]*types.Rowid{&ridSpilled},
				[]uint64{602}),
		}
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{{account: 7, table: userTable}: {string([]byte{0}): pkMoveLedgerStmts(2)}},
			birth: map[string]int{string([]byte{0}): 1},
		}
		keeps := pkMoveLinkRowsToKeep(writes, deaths)
		require.Equal(t, map[int][]int64{0: nil}, keeps)
		applyPkMoveKeeps(t, writes, keeps)
		require.Equal(t, 0, writes[0].bat.RowCount())
	})
}

func TestRemapPkMoveLinkRowIDs(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)

	oldRowIdx, newRowIdx := 4, 8 // pkMovePruneLinkEntry's attr layout

	t.Run("both rowid columns ride the compaction re-key", func(t *testing.T) {
		oldB := pkMoveWorkspaceRowid(51)
		newB := pkMoveWorkspaceRowid(61)
		oldC := pkMoveWorkspaceRowid(52)
		newC := pkMoveWorkspaceRowid(62)
		untouched := pkMoveWorkspaceRowid(70)
		link := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable, userTable},
			[]*types.Rowid{&oldB, &untouched},
			[]*types.Rowid{&oldC, nil},
			[]uint64{700, 701})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{link}
		require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
			[]types.Rowid{oldB, oldC},
			[]types.Rowid{newB, newC}, nil))
		bat := txn.writes[0].bat
		require.Equal(t, newB[:], bat.Vecs[oldRowIdx].GetBytesAt(0))
		require.Equal(t, newC[:], bat.Vecs[newRowIdx].GetBytesAt(0))
		// Rows whose identities the merge did not re-key stay verbatim,
		// including NULL new_row_id cells.
		require.Equal(t, untouched[:], bat.Vecs[oldRowIdx].GetBytesAt(1))
		require.True(t, bat.Vecs[newRowIdx].IsNull(1))
	})

	t.Run("no staged link rows means no scan and no map", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil,
				[]types.Rowid{pkMoveWorkspaceRowid(80)}),
		}
		require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
			[]types.Rowid{pkMoveWorkspaceRowid(80)},
			[]types.Rowid{pkMoveWorkspaceRowid(81)}, nil))
		require.Equal(t, pkMoveWorkspaceRowid(80),
			vector.MustFixedColNoTypeCheck[types.Rowid](txn.writes[0].bat.Vecs[0])[0])
	})

	t.Run("mismatched mapping lengths are ignored", func(t *testing.T) {
		rid := pkMoveWorkspaceRowid(90)
		link := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{&rid},
			[]uint64{702})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{link}
		require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
			[]types.Rowid{rid}, nil, nil))
		require.Equal(t, rid[:], txn.writes[0].bat.Vecs[newRowIdx].GetBytesAt(0))
	})

	t.Run("accounted staged batches stay accounted after varlena area growth", func(t *testing.T) {
		// old_row_id/new_row_id are binary(24) varlenas: payload writes
		// append to the vector area, growing Batch.Size(). A staged entry
		// is already size-accounted, so the remap must re-account around
		// the mutation or delTransaction panics ("changed outside
		// accounting helpers", observed live on a compacted move commit).
		oldB := pkMoveWorkspaceRowid(91)
		newB := pkMoveWorkspaceRowid(92)
		link := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{&oldB},
			[]*types.Rowid{nil},
			[]uint64{703})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{link}
		txn.accountWorkspaceEntryLocked(&txn.writes[0])
		require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
			[]types.Rowid{oldB},
			[]types.Rowid{newB}, nil))
		bat := txn.writes[0].bat
		require.Equal(t, newB[:], bat.Vecs[oldRowIdx].GetBytesAt(0))
		require.Equal(t, uint64(bat.Size()), txn.writes[0].accountedSize)
		// The lone entry's live size is the whole workspace total.
		require.Equal(t, uint64(bat.Size()), txn.workspaceSize)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.unaccountWorkspaceEntryLocked(&txn.writes[0])
		require.Zero(t, txn.workspaceSize)
	})
}

// TestRemapFailurePropagatesThroughWorkspaceMerge injects the allocation
// failure exactly at the compaction remap boundary: the workspace merge
// has already re-keyed the user rows when the link remap faults, and only
// an ERROR propagated out of mergeTxnWorkspaceLocked keeps the statement
// (and the commit it feeds) from retiring the now-dangling link and
// committing the update without provenance. A swallowed failure here was
// the pre-fix behaviour: remap returned void, the merge reported success,
// and the prune dropped the unmatched link as dead.
func TestRemapFailurePropagatesThroughWorkspaceMerge(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)

	txn := &Transaction{
		proc:            proc,
		tablesInVain:    make(map[uint64]int),
		batchSelectList: make(map[*batch.Batch][]int64),
		deletedBlocks:   &deletedBlocks{offsets: make(map[types.Blockid][]int64)},
	}
	txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

	// Thirty one-row inserts of one table merge into a single entry and
	// trip the compaction threshold, forcing the rowid re-key path whose
	// remap this test faults.
	var victimRowid types.Rowid
	for i := 0; i < 30; i++ {
		bat := newInsertBatchWithRowIDForTest(t, proc, []int64{int64(i)})
		if i == 0 {
			victimRowid = vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])[0]
		}
		txn.appendWorkspaceEntryLocked(Entry{
			typ:        INSERT,
			databaseId: 7,
			tableId:    userTable,
			bat:        bat,
		})
	}
	// The staged link records the pre-merge workspace identity of the
	// victim row: the remap must rewrite it to the merged identity, and
	// the fault fires on exactly that write.
	link := pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{userTable},
		[]*types.Rowid{&victimRowid},
		[]*types.Rowid{nil},
		[]uint64{800})
	txn.appendWorkspaceEntryLocked(link)

	seamCalls := 0
	origOverwrite := pkMoveVecOverwriteRowidAt
	pkMoveVecOverwriteRowidAt = func(*vector.Vector, int, []byte, *mpool.MPool) error {
		seamCalls++
		return errPkMoveInjected
	}
	t.Cleanup(func() { pkMoveVecOverwriteRowidAt = origOverwrite })

	err := txn.mergeTxnWorkspaceLocked(context.Background())
	require.ErrorIs(t, err, errPkMoveInjected)
	// Fail-fast: the first remap write error aborts the merge instead of
	// sweeping more link entries.
	require.Equal(t, 1, seamCalls)
	// The user rows were merged and re-keyed before the remap ran: the
	// pre-merge identity no longer appears in the survivor batch, while
	// the link's old_row_id cell still names it verbatim — the exact
	// dangling state that must never reach a commit, and the reason the
	// error has to propagate. (The workspace region is discarded by
	// statement rollback after this error.)
	survivors := 0
	for i := range txn.writes {
		if txn.writes[i].tableName != catalog.MO_DATA_BRANCH_UPDATE_LINK &&
			txn.writes[i].bat != nil && txn.writes[i].bat.RowCount() == 30 {
			survivors++
			merged := vector.MustFixedColNoTypeCheck[types.Rowid](txn.writes[i].bat.Vecs[0])
			for _, rid := range merged {
				require.NotEqual(t, victimRowid, rid)
			}
		}
	}
	require.Equal(t, 1, survivors)
	linkBat := txn.writes[len(txn.writes)-1].bat
	oldRowIdx := -1
	for j, attr := range linkBat.Attrs {
		if attr == catalog.DataBranchUpdateLinkColOldRow {
			oldRowIdx = j
		}
	}
	require.Equal(t, victimRowid[:], linkBat.Vecs[oldRowIdx].GetBytesAt(0))
}

// pkMoveNoteStagedLinkBirths mirrors pass 4 of fillPkMoveLinkNewRowIDsLocked
// for fixtures that stage link entries directly into txn.writes: production
// only ever stages link rows THROUGH the fill hook, so every live staged row
// owns a birth record, and the retarget/continue scans short-circuit on
// exactly "no live staged link rows". A fixture that bypasses the hook must
// register the same records or the guard hides the rows it staged.
func pkMoveNoteStagedLinkBirths(t *testing.T, txn *Transaction) {
	t.Helper()
	for i := range txn.writes {
		we := &txn.writes[i]
		if !isPkMoveLinkTableInsert(we.databaseName, we.tableName) ||
			we.typ != INSERT || we.bat == nil {
			continue
		}
		idIdx := -1
		for j, attr := range we.bat.Attrs {
			if attr == catalog.DataBranchUpdateLinkColLinkID {
				idIdx = j
				break
			}
		}
		if idIdx < 0 || idIdx >= len(we.bat.Vecs) {
			continue
		}
		idVec := we.bat.Vecs[idIdx]
		for r := 0; r < we.bat.RowCount(); r++ {
			if idVec.IsNull(uint64(r)) {
				continue
			}
			if txn.pkMoveLinkBirth == nil {
				txn.pkMoveLinkBirth = make(map[string]int)
			}
			txn.pkMoveLinkBirth[string(idVec.GetBytesAt(r))] = txn.statementID
		}
	}
}

// pkMoveMarkerOrMoveBatch stages one incoming link batch with full per-row
// control of old/new pk encodings and the old row id, mimicking exactly
// what collectUpdatePkMoveLinkRows writes (new_row_id always NULL).
func pkMoveMarkerOrMoveBatch(
	t *testing.T,
	mp *mpool.MPool,
	tableID uint64,
	oldPKs, newPKs [][]byte,
	oldRows []*types.Rowid,
) *batch.Batch {
	t.Helper()
	n := len(oldPKs)
	require.Len(t, newPKs, n)
	require.Len(t, oldRows, n)

	bat := batch.NewWithSize(11)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(9, int64(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], tableID, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte{byte(i)}, false, mp))
		if oldRows[i] == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[4], oldRows[i][:], false, mp))
		}
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], oldPKs[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte{byte(i)}, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], newPKs[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[8], nil, true, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint64(100+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
	}
	bat.SetRowCount(n)
	return bat
}

// pkMoveStagedDeleteEntry builds one staged DELETE workspace entry: a lone
// rowid vector, which is how the prune's liveness scan reads deletions.
func pkMoveStagedDeleteEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableID uint64,
	rowids []types.Rowid,
) Entry {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{catalog.Row_ID})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	for _, rid := range rowids {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], rid, false, mp))
	}
	bat.SetRowCount(len(rowids))
	return Entry{
		typ:       DELETE,
		accountId: accountID,
		tableId:   tableID,
		bat:       bat,
	}
}

// pkMoveLinkEntryFromBatch wraps a filled link batch (post
// fillPkMoveLinkNewRowIDsLocked) as the workspace entry the WriteBatch path
// would have appended.
func pkMoveLinkEntryFromBatch(t *testing.T, mp *mpool.MPool, bat *batch.Batch) Entry {
	t.Helper()
	return Entry{
		typ:          INSERT,
		accountId:    7,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		bat:          bat,
	}
}

func TestFillPkMoveSameKeyMarkers(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const userTable = uint64(42)
	oldRowIdx, newRowIdx := 4, 8

	t.Run("marker retargets a recorded move and is dropped", func(t *testing.T) {
		// Statement 1: UPDATE id=3 WHERE id=2 — the move link recorded
		// replacement r1. Statement 2: UPDATE v='edited' WHERE id=3 — a
		// same-key edit replacing r1 with r2. The marker must carry the
		// move's recorded identity onto r2 and vanish; without it the
		// commit prune retires the move link and the merge falls back to
		// the FK-failing DELETE+INSERT replay.
		r1 := pkMoveWorkspaceRowid(1)
		r2 := pkMoveWorkspaceRowid(2)
		moveLink := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{&r1},
			[]uint64{500})
		// The edit's own replacement insert, staged before the link write.
		editInsert := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{3}, nil, []types.Rowid{r2})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{moveLink, editInsert}
		pkMoveNoteStagedLinkBirths(t, txn)

		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r1})
		defer marker.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)

		// The staged move link now names the fresh replacement.
		require.Equal(t, r2[:], txn.writes[0].bat.Vecs[newRowIdx].GetBytesAt(0))
		// Workspace-lineage generations (r1 was minted by the move itself)
		// record NO origin: a later stitch move of r2 must keep its literal
		// pre-image id to match this link's retargeted new_row_id, and an
		// in-txn-insert lineage anchors to the netted stream insert id.
		require.Empty(t, txn.pkMoveRowidOrigins)
		// The marker itself is consumed.
		require.Equal(t, 0, marker.RowCount())
	})

	t.Run("durable pre-image markers record the durable origin", func(t *testing.T) {
		// Same-key edit of a COMMITTED row: a later statement moving the
		// fresh generation is the chain head and must anchor to the durable
		// generation the stream shows departing.
		ridX := types.BuildTestRowid(8, 8)
		fresh := pkMoveWorkspaceRowid(41)
		editInsert := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{2}, nil, []types.Rowid{fresh})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{editInsert}
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&ridX})
		defer marker.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		require.Equal(t, map[types.Rowid]types.Rowid{fresh: ridX}, txn.pkMoveRowidOrigins)
		require.Equal(t, 0, marker.RowCount())
	})

	t.Run("markers compose durable origins transitively", func(t *testing.T) {
		ridX := types.BuildTestRowid(9, 9)
		r1 := pkMoveWorkspaceRowid(51)
		r2 := pkMoveWorkspaceRowid(52)
		editInsert := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{2}, nil, []types.Rowid{r2})
		txn := &Transaction{proc: proc}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{r1: ridX}
		txn.writes = []Entry{editInsert}
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&r1})
		defer marker.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		// r26 review F5 (D112): the composition still hands r2 the durable
		// origin TRANSITIVELY, but the superseded head r1 retires instead
		// of chaining — retention is one origin per LIVE generation
		// (statement-rollback-journaled), which keeps repeated same-key
		// updates from accumulating one entry per UPDATE forever.
		require.Equal(t, map[types.Rowid]types.Rowid{r2: ridX}, txn.pkMoveRowidOrigins)
		require.Equal(t, 0, marker.RowCount())
	})

	t.Run("stitch move keeps its literal pre-image so the prune rescues the head", func(t *testing.T) {
		// Live-covered shape (f14b/em): move 2->3, same-key edit at 3,
		// move 3->4 in one transaction. The second move's old side must
		// name the generation the first link was retargeted to, and the
		// prune must keep BOTH links through the chain rescue.
		ridD := types.BuildTestRowid(6, 6)
		r1 := pkMoveWorkspaceRowid(61)
		r2 := pkMoveWorkspaceRowid(62)
		r3 := pkMoveWorkspaceRowid(63)
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			// Statement 1: DELETE D, INSERT (3)@r1, link 2->3 (old D, new r1).
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{ridD}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r1}),
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{userTable}, []*types.Rowid{&ridD}, []*types.Rowid{&r1}, []uint64{900}),
			// Statement 2: DELETE r1, INSERT (3)@r2 (the same-key edit).
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r1}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}),
		}
		pkMoveNoteStagedLinkBirths(t, txn)
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r1})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		// Marker consumed and dropped; the first link now names r2.
		require.Equal(t, 0, marker.RowCount())
		marker.Clean(mp)
		require.Equal(t, r2[:], txn.writes[2].bat.Vecs[newRowIdx].GetBytesAt(0))

		// Statement 3: DELETE r2, INSERT (4)@r3, then the move write.
		txn.writes = append(txn.writes,
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r2}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{4}, nil, []types.Rowid{r3}))
		move2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&r2})
		txn.fillPkMoveLinkNewRowIDsLocked(7, move2)
		require.Equal(t, 1, move2.RowCount())
		// The stitch side stays literal: r2, not an "origin".
		require.Equal(t, r2[:], move2.Vecs[oldRowIdx].GetBytesAt(0))
		require.Equal(t, r3[:], move2.Vecs[newRowIdx].GetBytesAt(0))
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move2))
		// move2's storage now belongs to the workspace entry; no Clean.

		// Prune: the head's recorded replacement r2 died inside the
		// transaction, but the tail continues from r2 and lands on the
		// alive r3, so both links survive.
		keeps := pkMoveLinkRowsToKeep(txn.writes, nil)
		require.Empty(t, keeps, "no link row may be pruned from the intact chain")
	})

	t.Run("origin rewrites a later move's old row, then chains", func(t *testing.T) {
		// Edit first: X (durable) -> r1 by a non-key update. A later
		// statement moves the row: the writer names r1 as the old row,
		// but the committed stream will only show X departing, so the
		// link must name X or the anchor probe can never find it.
		ridX := types.BuildTestRowid(4, 4)
		r1 := pkMoveWorkspaceRowid(11)
		r2 := pkMoveWorkspaceRowid(12)
		txn := &Transaction{proc: proc}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{r1: ridX}
		// The move's own replacement insert (id=4) staged in-workspace.
		txn.writes = []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, userTable,
				[]int64{4}, nil, []types.Rowid{r2}),
		}
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&r1})
		defer move.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, move)

		require.Equal(t, 1, move.RowCount())
		require.Equal(t, ridX[:], move.Vecs[oldRowIdx].GetBytesAt(0))
		require.Equal(t, r2[:], move.Vecs[newRowIdx].GetBytesAt(0))
	})

	t.Run("unresolved markers drop without retargeting", func(t *testing.T) {
		r1 := pkMoveWorkspaceRowid(21)
		moveLink := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{&r1},
			[]uint64{501})
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{moveLink}
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r1})
		defer marker.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		require.Equal(t, r1[:], txn.writes[0].bat.Vecs[newRowIdx].GetBytesAt(0))
		require.Nil(t, txn.pkMoveRowidOrigins)
		require.Equal(t, 0, marker.RowCount())
	})

	t.Run("real moves are never mistaken for markers", func(t *testing.T) {
		r1 := pkMoveWorkspaceRowid(31)
		r2 := pkMoveWorkspaceRowid(32)
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, userTable,
				[]int64{4}, nil, []types.Rowid{r2}),
		}
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&r1})
		defer move.Clean(mp)
		txn.fillPkMoveLinkNewRowIDsLocked(7, move)
		require.Equal(t, 1, move.RowCount())
		require.Equal(t, r1[:], move.Vecs[oldRowIdx].GetBytesAt(0))
		require.Equal(t, r2[:], move.Vecs[newRowIdx].GetBytesAt(0))
		require.Nil(t, txn.pkMoveRowidOrigins)
	})
}

func TestPkMoveHarvestCache(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	r1 := types.BuildTestRowid(1, 1)
	r2 := types.BuildTestRowid(1, 2)
	r3 := types.BuildTestRowid(2, 1)

	t.Run("suffix scans stay equivalent to a full harvest", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3, 4}, nil, []types.Rowid{r1, r2}),
		}
		full := pkMoveHarvestNewRowIDs(txn.writes, 7, 42)
		require.Equal(t, full, txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42))

		// A later statement stages a re-key of pk 3: the suffix fold must
		// override, matching what one full rescan would answer.
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r3}))
		cached := txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42)
		require.Equal(t, pkMoveHarvestNewRowIDs(txn.writes, 7, 42), cached)
		require.Equal(t, r3, cached[string(pkMoveEncodeInt64(t, 3))])
		require.Equal(t, r2, cached[string(pkMoveEncodeInt64(t, 4))])
	})

	t.Run("compaction generation forces a rescan", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		entry := pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1})
		txn.writes = []Entry{entry}
		require.Equal(t, r1,
			txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42)[string(pkMoveEncodeInt64(t, 3))])

		// Simulate a merge re-key: mutate the staged batch in place, then
		// bump the epoch exactly like mergeTxnWorkspaceLocked does.
		we := &txn.writes[0]
		require.NoError(t, vector.SetFixedAtWithTypeCheck[types.Rowid](we.bat.Vecs[0], 0, r3))
		txn.pkMoveHarvestGen++
		require.Equal(t, r3,
			txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42)[string(pkMoveEncodeInt64(t, 3))])
	})

	t.Run("statement rollback truncation forces a rebuild", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{3}, nil, []types.Rowid{r1}),
			pkMoveStagedInsertEntry(t, mp, 7, 42, []int64{4}, nil, []types.Rowid{r2}),
		}
		require.Len(t, txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42), 2)

		// Roll back the second statement: a stale watermark must never
		// skip the rescan.
		txn.writes = txn.writes[:1]
		cached := txn.pkMoveHarvestNewRowIDsCachedLocked(7, 42)
		require.Equal(t, pkMoveHarvestNewRowIDs(txn.writes, 7, 42), cached)
		_, hasRolledBack := cached[string(pkMoveEncodeInt64(t, 4))]
		require.False(t, hasRolledBack)
	})
}

// pkMoveLedgerLinkRow describes one fabricated link row for the
// death-ledger keep tests: identity, lineage keys, and birth bookkeeping
// all explicit.
type pkMoveLedgerLinkRow struct {
	table  uint64
	oldPK  string
	newPK  string
	newRID *types.Rowid
	linkID string
}

// pkMoveLedgerLinkEntry builds one staged link-table INSERT in the
// 10-column layout with explicit lineage bytes (plain strings stand in for
// the packer-encoded keys; the keep evaluation treats them as opaque).
func pkMoveLedgerLinkEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	rows []pkMoveLedgerLinkRow,
) Entry {
	t.Helper()
	bat := batch.NewWithSize(11)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	for i, row := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(9, int64(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row.linkID), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], row.table, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte(row.oldPK), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], nil, true, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte(row.oldPK), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte(row.newPK), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], []byte(row.newPK), false, mp))
		if row.newRID == nil {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], nil, true, mp))
		} else {
			require.NoError(t, vector.AppendBytes(bat.Vecs[8], row.newRID[:], false, mp))
		}
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], uint64(100+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
	}
	bat.SetRowCount(len(rows))
	return Entry{
		typ:          INSERT,
		accountId:    accountID,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		bat:          bat,
	}
}

// TestPkMoveDeathLedgerKeep drives the NULL-identity (S3-route) death
// evaluation of pkMoveLinkRowsToKeep: diverted uncommitted-object deaths
// retire links whose replacement key died after the link was staged,
// unless a same-statement marker or a live non-reincarnated continuer
// carries the key onward.
// pkMoveLedgerStmts builds the per-key statement set the death ledger
// retains: every diverted death and every continuity marker keeps its own
// statement so the keep evaluation can match deaths to same-statement
// explanations.
func pkMoveLedgerStmts(stmts ...int) map[int]struct{} {
	set := make(map[int]struct{}, len(stmts))
	for _, stmt := range stmts {
		set[stmt] = struct{}{}
	}
	return set
}

func TestPkMoveDeathLedgerKeep(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	key := pkMoveAcctTbl{account: 7, table: userTable}

	dropRow := pkMoveLedgerLinkRow{table: userTable, oldPK: "k2", newPK: "k3", linkID: "La"}

	t.Run("unrescued death after birth drops the link", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth: map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("death that did not outlive the birth statement is ignored", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(1)}},
			birth: map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("same-statement chain continuer rescues", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{
			dropRow,
			{table: userTable, oldPK: "k3", newPK: "k4", linkID: "Lb"},
		})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth: map[string]int{"La": 1, "Lb": 2},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("dead tail takes the pk chain down to a fixpoint", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{
			dropRow,
			{table: userTable, oldPK: "k3", newPK: "k4", linkID: "Lb"},
		})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2), "k4": pkMoveLedgerStmts(3)}},
			birth: map[string]int{"La": 1, "Lb": 2},
		}
		writes := []Entry{entry}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("same-statement marker rescues its own consumption", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:    map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			markers: map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth:   map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("a later marker continues reincarnation, never the move", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:    map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			markers: map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(3)}},
			birth:   map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("successive edits keep every explanation", func(t *testing.T) {
		// Statement 2's edit consumed the recorded replacement (death 2
		// plus its own marker), then statement 3's edit continued the
		// same key: its delete of the workspace-staged generation records
		// no divert death, but its marker must NOT erase statement 2's
		// evidence. The explained death stays explained and the lineage
		// survives.
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:    map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			markers: map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2, 3)}},
			birth:   map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("a continued lineage still dies to an unexplained later death", func(t *testing.T) {
		// Statements 2 and 3 explain their own deaths, but the key
		// divert-died once more in statement 4 without a same-statement
		// marker: the whole lineage is dead even though earlier deaths
		// were covered.
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:    map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2, 4)}},
			markers: map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2, 3)}},
			birth:   map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("every recorded death explained keeps the lineage", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:    map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2, 4)}},
			markers: map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2, 4)}},
			birth:   map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("post-mortem continuer cannot rescue", func(t *testing.T) {
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{
			dropRow,
			{table: userTable, oldPK: "k3", newPK: "k4", linkID: "Lb"},
		})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth: map[string]int{"La": 1, "Lb": 3},
		}
		writes := []Entry{entry}
		// La drops (its only candidate continuer was born after the death
		// of its own old key: reincarnation lineage). Lb itself keeps —
		// nothing ever killed key k4.
		require.Equal(t, map[int][]int64{0: {1}},
			pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("healed links ride the row-id liveness logic", func(t *testing.T) {
		r1 := pkMoveWorkspaceRowid(71)
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			birth: map[string]int{"La": 1},
			heals: map[string]types.Rowid{"La": r1},
		}
		writes := []Entry{
			entry,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r1}),
		}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
		// The bound generation dies inside the transaction: the link must
		// go down exactly like a workspace-tracked one.
		writesDead := []Entry{
			entry,
			pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{r1}),
		}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writesDead, deaths))
	})

	t.Run("deaths are scoped by account and table", func(t *testing.T) {
		other := pkMoveLedgerLinkRow{table: 43, oldPK: "k2", newPK: "k3", linkID: "La"}
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{other})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth: map[string]int{"La": 1},
		}
		writes := []Entry{entry}
		require.Empty(t, pkMoveLinkRowsToKeep(writes, deaths))
	})

	t.Run("healed identity that dies diverted drops the link", func(t *testing.T) {
		// A heal carried the NULL-identity link to a live staged row id;
		// a later diverted delete of that key must still retire it — the
		// healed ownership must not shield the link from the pk ledger.
		rW := pkMoveWorkspaceRowid(73)
		entry := pkMoveLedgerLinkEntry(t, mp, 7, []pkMoveLedgerLinkRow{dropRow})
		deaths := &pkMoveDeathLedger{
			dead:  map[pkMoveAcctTbl]map[string]map[int]struct{}{key: {"k3": pkMoveLedgerStmts(2)}},
			birth: map[string]int{string("La"): 1},
			heals: map[string]types.Rowid{"La": rW},
		}
		writes := []Entry{
			entry,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{rW}),
		}
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(writes, deaths))
	})
}

// TestPkMoveDivertedDeleteOverlap drives the two real producers of the
// provenance maps against each other on one shared transaction through
// the PRODUCTION divert/delete boundary (Transaction.notePkMoveDivertedDeletes,
// the exact critical section deleteBatch calls): the staging/bookkeeping
// path holding txn.Lock on one side, diverted deletes of an uncommitted
// object on the other. Each round is a channel rendezvous, so both
// critical sections enter simultaneously by construction — no sleeps,
// guaranteed progress per round. Run under -race: losing the boundary's
// lock turns this red immediately (the map writes hit the SAME key).
func TestPkMoveDivertedDeleteOverlap(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	key := pkMoveAcctTbl{account: 7, table: userTable}
	pkVal := string(pkMoveEncodeInt64(t, 3))

	txn := &Transaction{proc: proc}
	txn.statementID = 1
	txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}

	const rows = 8
	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{catalog.Row_ID, "pk"})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], pkMoveWorkspaceRowid(uint32(40+i)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(3), false, mp))
	}
	bat.SetRowCount(rows)
	defer bat.Clean(mp)
	allOffsets := make([]int64, rows)
	for i := range allOffsets {
		allOffsets[i] = int64(i)
	}

	const rounds = 64
	// rendezvous[0] gates the divert driver, rendezvous[1] the bookkeeping
	// driver: each round both announce readiness, collide in their
	// critical sections, then both announce completion before the next
	// round.
	rendezvous := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var wg sync.WaitGroup
	wg.Add(2)
	// Staging/bookkeeping path: mutates the same maps under txn.Lock.
	go func() {
		defer wg.Done()
		other := pkMoveAcctTbl{account: 9, table: userTable}
		for r := 0; r < rounds; r++ {
			rendezvous[0] <- struct{}{}
			<-rendezvous[1]
			txn.Lock()
			txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}, other: {}}
			txn.pkMoveDeadPks = notePkMoveStatement(txn.pkMoveDeadPks, key, pkVal, 100+r)
			txn.pkMoveDeadPks = notePkMoveStatement(txn.pkMoveDeadPks, other, pkVal, 100+r)
			txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}
			txn.Unlock()
			rendezvous[0] <- struct{}{}
			<-rendezvous[1]
		}
	}()
	// Divert/delete boundary path: exactly what deleteBatch calls.
	go func() {
		defer wg.Done()
		for r := 0; r < rounds; r++ {
			<-rendezvous[0]
			rendezvous[1] <- struct{}{}
			require.NoError(t, txn.notePkMoveDivertedDeletes(userTable, bat, allOffsets[:1+r%rows]))
			<-rendezvous[0]
			rendezvous[1] <- struct{}{}
		}
	}()
	wg.Wait()
	// Deterministic outcome, post-join only: the divert driver records
	// statement 1 deaths under the shared (account, table, pk); the
	// bookkeeping driver's later statements all survive in the set; the
	// keyed tuple and the table registration must be intact.
	require.Contains(t, txn.pkMoveLinkTables, key)
	require.Contains(t, txn.pkMoveDeadPks[key][pkVal], 1,
		"the divert driver's evidence survived the overlap")
	latest, ok := pkMoveLatestStmt(txn.pkMoveDeadPks[key][pkVal])
	require.True(t, ok)
	require.Equal(t, 100+rounds-1, latest,
		"every critical section serialized: the latest statement survived")
}

// TestRollbackRestoresPkMoveState proves statement rollback restores the
// provenance side effects the writes truncation cannot reach: a marker's
// retarget of an earlier statement's link row, an origin-map write, the
// harvest cache, and every statement-tagged ledger record — and that a
// later retry re-establishes correct lineage.
func TestRollbackRestoresPkMoveState(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	ridD := types.BuildTestRowid(5, 5)
	ridX := types.BuildTestRowid(6, 6)
	r1 := pkMoveWorkspaceRowid(81)
	r2 := pkMoveWorkspaceRowid(82)
	r2b := pkMoveWorkspaceRowid(83)
	r9 := pkMoveWorkspaceRowid(89)
	newRowIdx := 8

	txn := &Transaction{proc: proc}
	// Statement 0: move 2->3 with workspace replacement r1.
	txn.writes = []Entry{
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r1}),
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{9}, nil, []types.Rowid{r9}),
	}
	move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 2)},
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[]*types.Rowid{&ridD})
	txn.fillPkMoveLinkNewRowIDsLocked(7, move)
	require.Equal(t, r1[:], move.Vecs[newRowIdx].GetBytesAt(0))
	txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move))
	endStmt0 := len(txn.writes)
	txn.statementID = 1

	// Statement 1: same-key edit 3->3 (retargets the link to r2) plus a
	// durable-lineage marker 9->9 (records origin r9 -> ridX).
	txn.writes = append(txn.writes,
		pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r1}),
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
	markers := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 3), pkMoveEncodeInt64(t, 9)},
		[][]byte{pkMoveEncodeInt64(t, 3), pkMoveEncodeInt64(t, 9)},
		[]*types.Rowid{&r1, &ridX})
	txn.fillPkMoveLinkNewRowIDsLocked(7, markers)
	require.Equal(t, 0, markers.RowCount(), "markers are always consumed")
	txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, markers))
	require.Equal(t, r2[:], txn.writes[2].bat.Vecs[newRowIdx].GetBytesAt(0))
	require.Equal(t, ridX, txn.pkMoveRowidOrigins[r9])
	require.NotEmpty(t, txn.pkMoveHarvestCache, "fills fold through the cache")
	require.NotEmpty(t, txn.pkMoveRetargetUndo)
	require.NotEmpty(t, txn.pkMoveMarkerStmts)

	// Statement-tagged bookkeeping: tag 0 must survive, tag 1 must die.
	survivorKey := pkMoveAcctTbl{account: 7, table: 4242}
	txn.pkMoveDeadPks = notePkMoveStatement(txn.pkMoveDeadPks, survivorKey, "keep", 0)
	txn.pkMoveDeadPks = notePkMoveStatement(txn.pkMoveDeadPks, survivorKey, "drop", 1)
	txn.pkMoveLinkBirth["survivor"] = 0
	if txn.pkMoveNullHeals == nil {
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{}
	}
	txn.pkMoveNullHeals["createdInStmt1"] = pkMoveNullHeal{rid: r1, stmt: 1}
	txn.pkMoveNullHeals["bornEarlier"] = pkMoveNullHeal{rid: r1, stmt: 0}

	// Roll the statement back exactly like RollbackLastStatement does:
	// truncate first, step the id back, then the provenance undo.
	txn.writes = txn.writes[:endStmt0]
	txn.statementID = 0
	txn.rollbackPkMoveStmtStateLocked()

	require.Equal(t, r1[:], txn.writes[2].bat.Vecs[newRowIdx].GetBytesAt(0),
		"retarget restored the pre-statement replacement identity")
	_, hasOrigin := txn.pkMoveRowidOrigins[r9]
	require.False(t, hasOrigin, "the statement's origin write was undone")
	require.Empty(t, txn.pkMoveHarvestCache,
		"the cache can never serve entries of the rolled-back statement")
	require.Equal(t, map[int]struct{}{0: {}}, txn.pkMoveDeadPks[survivorKey]["keep"],
		"the surviving statement's record stays as a set")
	_, dropped := txn.pkMoveDeadPks[survivorKey]["drop"]
	require.False(t, dropped)
	require.Equal(t, 0, txn.pkMoveLinkBirth["survivor"])
	_, healDropped := txn.pkMoveNullHeals["createdInStmt1"]
	require.False(t, healDropped)
	require.Contains(t, txn.pkMoveNullHeals, "bornEarlier")

	// Retry the exact statement: new replacement r2b must be resolved,
	// served by a freshly rebuilt cache, and kept by the prune.
	txn.statementID = 1
	txn.writes = append(txn.writes,
		pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r1}),
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2b}))
	retry := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[]*types.Rowid{&r1})
	txn.fillPkMoveLinkNewRowIDsLocked(7, retry)
	txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, retry))
	require.Equal(t, r2b[:], txn.writes[2].bat.Vecs[newRowIdx].GetBytesAt(0))
	require.Empty(t, pkMoveLinkRowsToKeep(txn.writes,
		txn.pkMoveLedgerForPruneLocked()),
		"the retried lineage is alive and must survive the prune")
}

// TestPkMoveNullHealLifecycle proves the ordering-checked binding of a
// NULL-identity (S3-route) link to the workspace generation a resolved
// continuity marker established: binding happens, moves with retargets,
// and is refused when the death ledger proves the marker continues a
// post-mortem reincarnation.
func TestPkMoveNullHealLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	ridD := types.BuildTestRowid(5, 5)
	ridS3 := types.BuildTestRowid(6, 6)
	r2 := pkMoveWorkspaceRowid(91)
	r3 := pkMoveWorkspaceRowid(92)
	newRowIdx := 8
	linkID := string([]byte{0})

	stage := func(t *testing.T, txn *Transaction) {
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridD})
		txn.fillPkMoveLinkNewRowIDsLocked(7, move)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move))
	}

	t.Run("resolved marker binds and retarget carries the binding", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		stage(t, txn)
		txn.statementID = 1
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridS3})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker))
		require.Equal(t, r2, txn.pkMoveNullHeals[linkID].rid)
		require.True(t, txn.writes[0].bat.Vecs[newRowIdx].IsNull(0),
			"the committed image keeps its NULL cell")

		// A second same-key edit retargets the binding onto r3.
		txn.statementID = 2
		txn.writes = append(txn.writes,
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r2}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r3}))
		marker2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r2})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker2)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker2))
		require.Equal(t, r3, txn.pkMoveNullHeals[linkID].rid)
		require.Empty(t, pkMoveLinkRowsToKeep(txn.writes,
			txn.pkMoveLedgerForPruneLocked()),
			"the re-bound lineage is alive on r3")
	})

	t.Run("binding is refused across a post-mortem gap", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		stage(t, txn)
		key := pkMoveAcctTbl{account: 7, table: userTable}
		// The replacement's staged generation died in statement 1; the
		// marker arrives only in statement 2 (an edit of whatever the
		// reinsert produced).
		txn.pkMoveDeadPks = notePkMoveStatement(
			txn.pkMoveDeadPks, key, string(pkMoveEncodeInt64(t, 3)), 1)
		txn.statementID = 2
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridS3})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker))
		require.NotContains(t, txn.pkMoveNullHeals, linkID)
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(txn.writes, txn.pkMoveLedgerForPruneLocked()),
			"the ledger retires the link the reincarnation could never continue")
	})

	t.Run("same-statement death is the marker's own consumption", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		stage(t, txn)
		key := pkMoveAcctTbl{account: 7, table: userTable}
		txn.pkMoveDeadPks = notePkMoveStatement(
			txn.pkMoveDeadPks, key, string(pkMoveEncodeInt64(t, 3)), 1)
		txn.statementID = 1
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridS3})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker))
		require.Equal(t, r2, txn.pkMoveNullHeals[linkID].rid)
	})

	t.Run("repeated edits through the diverted-delete boundary keep provenance", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		stage(t, txn)
		key := pkMoveAcctTbl{account: 7, table: userTable}
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}

		// Edit 1 (statement 1): the replaced generation is the durable
		// object row, so its delete diverts through the production
		// boundary before the edit's own marker lands.
		txn.statementID = 1
		del := batch.NewWithSize(2)
		del.SetAttributes([]string{catalog.Row_ID, "pk"})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], ridS3, false, mp))
		del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(3), false, mp))
		del.SetRowCount(1)
		defer del.Clean(mp)
		require.NoError(t, txn.notePkMoveDivertedDeletes(userTable, del, []int64{0}))
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridS3})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker))
		require.Equal(t, r2, txn.pkMoveNullHeals[linkID].rid,
			"the first edit bound the lineage to its staged replacement")
		require.Equal(t, map[int]struct{}{1: {}},
			txn.pkMoveDeadPks[key][string(pkMoveEncodeInt64(t, 3))],
			"the diverted death kept its own statement")

		// Edit 2 (statement 2) deletes the workspace generation (staged
		// delete, never diverted) and lands another marker. The second
		// marker must ADD its continuity statement, never erase the first
		// edit's: the diverted death at statement 1 stays explained only
		// when the marker set keeps statement 1. Advancing the statement
		// mirrors IncrStatementID's provenance reset: the finished
		// statement's undo records retire beyond rollback reach.
		txn.statementID = 2
		txn.pkMoveRetargetUndo = nil
		txn.pkMoveOriginUndo = nil
		txn.pkMoveRetargetUndoSeen = nil
		txn.pkMoveOriginUndoSeen = nil
		txn.writes = append(txn.writes,
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r2}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r3}))
		marker2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r2})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker2)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker2))
		require.Equal(t, r3, txn.pkMoveNullHeals[linkID].rid,
			"the retarget carried the binding onto the second replacement")
		require.Equal(t, map[int]struct{}{1: {}, 2: {}},
			txn.pkMoveMarkerStmts[key][string(pkMoveEncodeInt64(t, 3))],
			"both edits' continuity evidence survives")
		require.Empty(t, pkMoveLinkRowsToKeep(txn.writes,
			txn.pkMoveLedgerForPruneLocked()),
			"the lineage stays live through both edits")
	})

	t.Run("statement rollback restores the earlier marker evidence", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		stage(t, txn)
		key := pkMoveAcctTbl{account: 7, table: userTable}
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}

		// Edit 1 through the diverted boundary, as above.
		txn.statementID = 1
		del := batch.NewWithSize(2)
		del.SetAttributes([]string{catalog.Row_ID, "pk"})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], ridS3, false, mp))
		del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(3), false, mp))
		del.SetRowCount(1)
		defer del.Clean(mp)
		require.NoError(t, txn.notePkMoveDivertedDeletes(userTable, del, []int64{0}))
		txn.writes = append(txn.writes,
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r2}))
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&ridS3})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker))
		require.Equal(t, r2, txn.pkMoveNullHeals[linkID].rid)
		endStmt1 := len(txn.writes)

		// Edit 2 rebinds the lineage and adds its continuity statement.
		// Advancing mirrors IncrStatementID's provenance reset (the
		// finished statement's undo records retire).
		txn.statementID = 2
		txn.pkMoveRetargetUndo = nil
		txn.pkMoveOriginUndo = nil
		txn.pkMoveRetargetUndoSeen = nil
		txn.pkMoveOriginUndoSeen = nil
		txn.writes = append(txn.writes,
			pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r2}),
			pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r3}))
		marker2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&r2})
		txn.fillPkMoveLinkNewRowIDsLocked(7, marker2)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, marker2))
		require.Equal(t, r3, txn.pkMoveNullHeals[linkID].rid)

		// Roll statement 2 back exactly like RollbackLastStatement does:
		// truncate first, step the id back, then the provenance undo.
		txn.writes = txn.writes[:endStmt1]
		txn.statementID = 1
		txn.rollbackPkMoveStmtStateLocked()

		pkVal := string(pkMoveEncodeInt64(t, 3))
		require.Equal(t, map[int]struct{}{1: {}},
			txn.pkMoveMarkerStmts[key][pkVal],
			"edit 1's continuity evidence is restored exactly")
		require.Equal(t, map[int]struct{}{1: {}},
			txn.pkMoveDeadPks[key][pkVal],
			"the diverted death survives the rollback untouched")
		require.Equal(t, r2, txn.pkMoveNullHeals[linkID].rid,
			"the heal binding is back on the surviving generation")
		require.Empty(t, pkMoveLinkRowsToKeep(txn.writes,
			txn.pkMoveLedgerForPruneLocked()),
			"with the second edit gone the lineage is alive on r2 again")
	})
}

// TestRemapPkMoveOriginsAndHeals proves the compaction remap carries the
// durable-origin map and the heal bindings through a workspace re-key even
// when no link rows are staged yet (edit-first, compaction, then move).
func TestRemapPkMoveOriginsAndHeals(t *testing.T) {
	proc := testutil.NewProcess(t)
	durableX := types.BuildTestRowid(4, 4)
	wA := pkMoveWorkspaceRowid(11)
	wB := pkMoveWorkspaceRowid(12)
	wC := pkMoveWorkspaceRowid(13)
	wH := pkMoveWorkspaceRowid(14)
	txn := &Transaction{proc: proc}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
		wA: durableX,
		wB: wC,
	}
	txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
		"L1": {rid: wH, stmt: 0},
	}
	// No link entries staged anywhere: the early return must still come
	// AFTER the identity maps are remapped.
	require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
		[]types.Rowid{wA, wC, wH},
		[]types.Rowid{pkMoveWorkspaceRowid(21), pkMoveWorkspaceRowid(23), pkMoveWorkspaceRowid(24)}, nil))
	require.Equal(t, map[types.Rowid]types.Rowid{
		pkMoveWorkspaceRowid(21): durableX,
		wB:                       pkMoveWorkspaceRowid(23),
	}, txn.pkMoveRowidOrigins)
	require.Equal(t, pkMoveWorkspaceRowid(24), txn.pkMoveNullHeals["L1"].rid)
}

// TestPkMoveHarvestCacheRollbackRefill is the F3 trap: a rollback clears
// the harvest cache before a retry refills the writes slice back to (or
// past) the old watermark, so the retry's fill can never be served the
// rolled-back attempt's replacement identity.
func TestPkMoveHarvestCacheRollbackRefill(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	r1 := pkMoveWorkspaceRowid(31)
	rDead := pkMoveWorkspaceRowid(33)
	rRetry := pkMoveWorkspaceRowid(34)
	newRowIdx := 8
	ridD := types.BuildTestRowid(5, 5)

	txn := &Transaction{proc: proc}
	txn.writes = []Entry{
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3}, nil, []types.Rowid{r1}),
	}
	move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 2)},
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[]*types.Rowid{&ridD})
	txn.fillPkMoveLinkNewRowIDsLocked(7, move)
	txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move))
	require.Equal(t, r1[:], move.Vecs[newRowIdx].GetBytesAt(0))
	endStmt0 := len(txn.writes)
	txn.statementID = 1

	// A failed statement adds inserts AND its own link fill, folding its
	// entries into the cache.
	txn.writes = append(txn.writes,
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{4}, nil, []types.Rowid{rDead}))
	failed := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[][]byte{pkMoveEncodeInt64(t, 4)},
		[]*types.Rowid{&r1})
	txn.fillPkMoveLinkNewRowIDsLocked(7, failed)
	txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, failed))
	require.Equal(t, rDead[:], failed.Vecs[newRowIdx].GetBytesAt(0))

	// Rollback: truncate, step back, undo. The cache must be gone before
	// any refill can reach the old watermark.
	txn.writes = txn.writes[:endStmt0]
	txn.statementID = 0
	txn.rollbackPkMoveStmtStateLocked()
	require.Empty(t, txn.pkMoveHarvestCache)

	// Retry: the writes slice refills to the SAME length with different
	// identities; the fill must resolve the RETRY's row id.
	txn.statementID = 1
	txn.writes = append(txn.writes,
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{4}, nil, []types.Rowid{rRetry}))
	retry := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[][]byte{pkMoveEncodeInt64(t, 4)},
		[]*types.Rowid{&r1})
	txn.fillPkMoveLinkNewRowIDsLocked(7, retry)
	require.Equal(t, rRetry[:], retry.Vecs[newRowIdx].GetBytesAt(0),
		"the retry resolved its own replacement, not the rolled-back attempt's")
}

// TestPkMoveDivertedDeleteGate proves the delete path records statement-
// stamped pk deaths only for tables with staged links and only from the
// [rowid, pk] delete layout.
func TestPkMoveDivertedDeleteGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	newDelBat := func(t *testing.T, second string, pk any) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{catalog.Row_ID, second})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], pkMoveWorkspaceRowid(55), false, mp))
		if pk == nil {
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(0), true, mp))
		} else {
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], pk.(int64), false, mp))
		}
		bat.SetRowCount(1)
		return bat
	}

	t.Run("records the death for a linked table", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 4
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{{account: 7, table: userTable}: {}}
		bat := newDelBat(t, "pk", int64(3))
		defer bat.Clean(mp)
		txn.Lock()
		txn.notePkMoveDivertedDeleteLocked(userTable, bat, 0)
		txn.Unlock()
		require.Equal(t, map[int]struct{}{4: {}},
			txn.pkMoveDeadPks[pkMoveAcctTbl{account: 7, table: userTable}][string(pkMoveEncodeInt64(t, 3))])
	})

	t.Run("ignores unlinked tables and foreign layouts", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{{account: 7, table: userTable}: {}}
		other := newDelBat(t, "pk", int64(3))
		defer other.Clean(mp)
		weird := newDelBat(t, "weird", int64(3))
		defer weird.Clean(mp)
		nullPK := newDelBat(t, "pk", nil)
		defer nullPK.Clean(mp)
		txn.Lock()
		txn.notePkMoveDivertedDeleteLocked(4242, other, 0)
		txn.notePkMoveDivertedDeleteLocked(userTable, weird, 0)
		txn.notePkMoveDivertedDeleteLocked(userTable, nullPK, 0)
		txn.Unlock()
		require.Empty(t, txn.pkMoveDeadPks)
	})
}

// TestPkMoveLinkCoordinatorBudgetWaves proves the coordinator-side
// admission bound: successive ferry waves charge against the retained
// byte counter through the real workspace accounting, and the wave that
// would cross the budget fails atomically — a clear error, the counter
// untouched, and nothing appended to the workspace.
func TestPkMoveLinkCoordinatorBudgetWaves(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	wave := func() *batch.Batch {
		return pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{nil})
	}

	probe := wave()
	sz := int64(probe.Size())
	probe.Clean(mp)
	require.Positive(t, sz)
	// Two waves fit, the third crosses the budget.
	prevBudget := pkMoveLinkCoordinatorBudget
	pkMoveLinkCoordinatorBudget = sz*2 + sz/2
	t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

	txn := &Transaction{proc: proc}
	for w := 0; w < 2; w++ {
		bat := wave()
		require.NoError(t, txn.admitPkMoveLinkBatchLocked(bat))
		txn.appendWorkspaceEntryLocked(pkMoveLinkEntryFromBatch(t, mp, bat))
	}
	require.Equal(t, sz*2, txn.pkMoveLinkStagedBytes,
		"each staged wave added its bytes through append accounting")

	over := wave()
	entriesBefore := len(txn.writes)
	err := txn.admitPkMoveLinkBatchLocked(over)
	require.Error(t, err)
	require.Contains(t, err.Error(), "coordinator retention budget")
	over.Clean(mp)
	require.Equal(t, entriesBefore, len(txn.writes),
		"the rejected wave never entered the workspace")
	require.Equal(t, sz*2, txn.pkMoveLinkStagedBytes,
		"the rejected wave charged nothing")

	// A zero budget disables admission outright.
	pkMoveLinkCoordinatorBudget = 0
	unlimited := wave()
	require.NoError(t, txn.admitPkMoveLinkBatchLocked(unlimited))
	txn.appendWorkspaceEntryLocked(pkMoveLinkEntryFromBatch(t, mp, unlimited))
	require.Equal(t, sz*3, txn.pkMoveLinkStagedBytes)
}

// TestPkMoveLinkCoordinatorBudgetPruneAndRelease proves the retained
// counter tracks net bytes through the real release paths: a prune shrink
// drops exactly the pruned rows' bytes, and a statement-rollback release
// removes exactly what the surviving batches still hold — no drift, no
// double count.
func TestPkMoveLinkCoordinatorBudgetPruneAndRelease(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	ridD := types.BuildTestRowid(5, 5)
	r1 := pkMoveWorkspaceRowid(71)
	r2 := pkMoveWorkspaceRowid(72)
	newRowIdx := 8

	txn := &Transaction{proc: proc}
	// Statement 0: replacements r1 (about to die) and r2 (stays alive).
	txn.appendWorkspaceEntryLocked(
		pkMoveStagedInsertEntry(t, mp, 7, userTable, []int64{3, 5}, nil, []types.Rowid{r1, r2}))
	move1 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 2)},
		[][]byte{pkMoveEncodeInt64(t, 3)},
		[]*types.Rowid{&ridD})
	txn.fillPkMoveLinkNewRowIDsLocked(7, move1)
	require.Equal(t, r1[:], move1.Vecs[newRowIdx].GetBytesAt(0))
	txn.appendWorkspaceEntryLocked(pkMoveLinkEntryFromBatch(t, mp, move1))
	move2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 4)},
		[][]byte{pkMoveEncodeInt64(t, 5)},
		[]*types.Rowid{&ridD})
	txn.fillPkMoveLinkNewRowIDsLocked(7, move2)
	require.Equal(t, r2[:], move2.Vecs[newRowIdx].GetBytesAt(0))
	txn.appendWorkspaceEntryLocked(pkMoveLinkEntryFromBatch(t, mp, move2))

	sz1 := int64(txn.writes[1].bat.Size())
	sz2 := int64(txn.writes[2].bat.Size())
	require.Equal(t, sz1+sz2, txn.pkMoveLinkStagedBytes)

	// Statement 1 kills r1: the prune shrinks the dead replacement's
	// batch, and the counter drops to exactly the surviving bytes.
	txn.statementID = 1
	txn.appendWorkspaceEntryLocked(
		pkMoveStagedDeleteEntry(t, mp, 7, userTable, []types.Rowid{r1}))
	txn.pruneDeadPkMoveLinkRowsLocked()
	require.Zero(t, txn.writes[1].bat.RowCount(),
		"the dead replacement's link row was pruned")
	require.Equal(t, sz2+int64(txn.writes[1].bat.Size()), txn.pkMoveLinkStagedBytes,
		"the counter nets the shrunk batch's remaining bytes plus the survivor")

	// Statement rollback releases the staged batches through the real
	// release path; each release removes exactly what the batch held.
	txn.releaseWorkspaceEntryBatchLocked(1)
	require.Equal(t, sz2, txn.pkMoveLinkStagedBytes,
		"releasing the pruned batch removed only its remaining bytes")
	txn.releaseWorkspaceEntryBatchLocked(2)
	require.Zero(t, txn.pkMoveLinkStagedBytes,
		"releasing the survivor emptied the counter without underflow")
}

// pkMoveNewMarkerWriteBatch builds the WriteBatch INSERT layout (NO rowid
// vector — WriteBatch mints and prepends it) of same-key continuity
// markers: rows an ordinary non-key UPDATE emits on a linked table.
func pkMoveNewMarkerWriteBatch(t *testing.T, mp *mpool.MPool, pks []int64) *batch.Batch {
	t.Helper()
	const userTable = uint64(42)
	bat := batch.NewWithSize(10)
	bat.SetAttributes([]string{
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	})
	for i := 0; i < 10; i++ {
		switch i {
		case 1, 8:
			// table_id and write_ts are the fixed-width columns of the
			// ten-column WriteBatch layout (no rowid prefix here).
			bat.Vecs[i] = vector.NewVec(types.T_uint64.ToType())
		case 9:
			bat.Vecs[i] = vector.NewVec(types.T_binary.ToType())
		default:
			bat.Vecs[i] = vector.NewVec(types.T_varbinary.ToType())
		}
	}
	for i, pk := range pks {
		enc := pkMoveEncodeInt64(t, pk)
		durable := types.BuildTestRowid(int64(100+i), 1)
		linkID := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
		copy(linkID[8:], enc)
		txnID := make([]byte, 16)
		copy(txnID, enc)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], linkID, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], userTable, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], enc, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], durable[:], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], enc, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], enc, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], enc, false, mp))
		// new_row_id stays NULL: the harvest fills replacements, and a
		// continuity marker has none of its own to commit.
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], nil, true, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[8], uint64(1000+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[9], txnID, false, mp))
	}
	bat.SetRowCount(len(pks))
	return bat
}

// TestPkMoveConsumedMarkersKeepNoWorkspaceFootprint pins the r17 marker
// storage contract (F3): an ordinary pk-UNCHANGED update stages one
// continuity marker per row, the fill consumes every one of them, and the
// WriteBatch path must then release the shrunken batch's buffers instead of
// retaining a zero-row workspace entry. vector.Shrink keeps varlena areas,
// so retaining the entry would charge the coordinator budget for rows that
// no longer exist — with enough payload, a bulk payload UPDATE that moves
// no primary key would fail on provenance bytes.
func TestPkMoveConsumedMarkersKeepNoWorkspaceFootprint(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	t.Run("a fully consumed marker wave keeps no workspace footprint", func(t *testing.T) {
		// Under the default budget a marker-only wave succeeds with zero
		// workspace entries and zero retained link bytes. The lineage
		// bookkeeping it leaves behind DOES consume the coordinator
		// budget — TestPkMoveBookkeepBudget prices and bounds it, and
		// the r18 review showed a marker-only wave otherwise grew
		// unaccounted heap across statement boundaries.
		txn := &Transaction{proc: proc}
		bat := pkMoveNewMarkerWriteBatch(t, mp, []int64{1, 2, 3, 4, 5, 6, 7, 8})
		rids, err := txn.WriteBatch(INSERT, "", 7, 1, 99,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK, bat, DNStore{})
		require.NoError(t, err,
			"a marker-only wave must never fail the default provenance budget")
		require.NotNil(t, rids)
		require.Zero(t, rids.Length(),
			"consumed markers leave no generated rowids to expose")
		rids.Free(mp)
		require.Empty(t, txn.writes,
			"the consumed marker batch must not enter the workspace")
		require.Zero(t, txn.pkMoveLinkStagedBytes,
			"the consumed marker batch charges no retained link bytes")
		require.NotEmpty(t, txn.pkMoveKeyLineage,
			"the fill really consumed the markers (lineage bindings recorded)")
	})

	t.Run("repeated marker waves keep a zero footprint under the default budget", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		for wave := 0; wave < 3; wave++ {
			base := int64(wave * 100)
			bat := pkMoveNewMarkerWriteBatch(t, mp, []int64{base + 1, base + 2, base + 3, base + 4})
			rids, err := txn.WriteBatch(INSERT, "", 7, 1, 99,
				catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK, bat, DNStore{})
			require.NoError(t, err)
			rids.Free(mp)
			require.Empty(t, txn.writes, "wave %d retained a dead batch", wave)
			require.Zero(t, txn.pkMoveLinkStagedBytes, "wave %d leaked budget bytes", wave)
		}
	})
}

// BenchmarkPkMoveMarkerFillDurableOrigins prices the provenance bookkeeping
// of an N-row pk-UNCHANGED update (every row stages a durable-origin
// continuity marker): the txn-layer fill must stay linear in the marker
// count, or ordinary bulk updates degrade quadratically.
func BenchmarkPkMoveMarkerFillDurableOrigins(b *testing.B) {
	for _, n := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprintf("markers=%d", n), func(b *testing.B) {
			proc := testutil.NewProcess(b)
			mp := proc.Mp()
			const userTable = uint64(42)
			b.ReportAllocs()
			packer := types.NewPacker()
			defer packer.Close()
			enc := func(v int64) []byte {
				packer.Reset()
				packer.EncodeInt64(v)
				return append([]byte(nil), packer.Bytes()...)
			}
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				txn := &Transaction{proc: proc}
				txn.statementID = 1
				ins := batch.NewWithSize(2)
				ins.SetAttributes([]string{catalog.Row_ID, "pk"})
				ins.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				ins.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				for i := 0; i < n; i++ {
					require.NoError(b, vector.AppendFixed(ins.Vecs[0], pkMoveWorkspaceRowid(uint32(100000+i)), false, mp))
					require.NoError(b, vector.AppendFixed(ins.Vecs[1], int64(5000+i), false, mp))
				}
				ins.SetRowCount(n)
				txn.writes = []Entry{{typ: INSERT, accountId: 7, tableId: userTable, bat: ins, pkCheckPos: 1}}
				markers := batch.NewWithSize(11)
				markers.SetAttributes([]string{
					catalog.Row_ID,
					catalog.DataBranchUpdateLinkColLinkID,
					catalog.DataBranchUpdateLinkColTableID,
					catalog.DataBranchUpdateLinkColOldPKSha,
					catalog.DataBranchUpdateLinkColOldRow,
					catalog.DataBranchUpdateLinkColOldPK,
					catalog.DataBranchUpdateLinkColNewPKSha,
					catalog.DataBranchUpdateLinkColNewPK,
					catalog.DataBranchUpdateLinkColNewRow,
					catalog.DataBranchUpdateLinkColWriteTS,
					catalog.DataBranchUpdateLinkColTxnID,
				})
				markers.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				markers.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
				markers.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[8] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
				markers.Vecs[10] = vector.NewVec(types.T_binary.ToType())
				for i := 0; i < n; i++ {
					pkB := enc(int64(5000 + i))
					durable := types.BuildTestRowid(int64(2*i+1), 1)
					linkID := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
					copy(linkID[8:], pkB)
					require.NoError(b, vector.AppendFixed(markers.Vecs[0], pkMoveWorkspaceRowid(uint32(200000+i)), false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[1], linkID, false, mp))
					require.NoError(b, vector.AppendFixed(markers.Vecs[2], userTable, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[3], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[4], durable[:], false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[5], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[6], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[7], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[8], nil, true, mp))
					require.NoError(b, vector.AppendFixed(markers.Vecs[9], uint64(1), false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
				}
				markers.SetRowCount(n)
				b.StartTimer()
				txn.fillPkMoveLinkNewRowIDsLocked(7, markers)
				b.StopTimer()
				markers.Clean(mp)
				ins.Clean(mp)
			}
		})
	}
}

// BenchmarkPkMoveMarkerFillPopulatedLinks is the workload the F3 guard
// cannot keep linear: a transaction with N staged link rows (pk moves)
// whose replacements a same-sized marker batch then retargets — BEGIN;
// update N primary keys; edit those N payloads through the continuity
// route; COMMIT. The empty-birth early exit never fires here, so every
// resolved marker used to rescan the whole staged link set. The F5 fill
// index turns the per-fill work O(links + markers); the 256/1024/4096
// ladder records the before/after.
func BenchmarkPkMoveMarkerFillPopulatedLinks(b *testing.B) {
	for _, n := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprintf("populated=%d", n), func(b *testing.B) {
			proc := testutil.NewProcess(b)
			mp := proc.Mp()
			const userTable = uint64(42)
			b.ReportAllocs()
			packer := types.NewPacker()
			defer packer.Close()
			enc := func(v int64) []byte {
				packer.Reset()
				packer.EncodeInt64(v)
				return append([]byte(nil), packer.Bytes()...)
			}
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				// N staged link rows whose new_row cells name the durable
				// identities B_i the markers will replace.
				links := batch.NewWithSize(11)
				links.SetAttributes([]string{
					catalog.Row_ID,
					catalog.DataBranchUpdateLinkColLinkID,
					catalog.DataBranchUpdateLinkColTableID,
					catalog.DataBranchUpdateLinkColOldPKSha,
					catalog.DataBranchUpdateLinkColOldRow,
					catalog.DataBranchUpdateLinkColOldPK,
					catalog.DataBranchUpdateLinkColNewPKSha,
					catalog.DataBranchUpdateLinkColNewPK,
					catalog.DataBranchUpdateLinkColNewRow,
					catalog.DataBranchUpdateLinkColWriteTS,
					catalog.DataBranchUpdateLinkColTxnID,
				})
				links.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				links.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
				links.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
				links.Vecs[8] = vector.NewVec(types.T_binary.ToType())
				links.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
				links.Vecs[10] = vector.NewVec(types.T_binary.ToType())
				birth := make(map[string]int, n)
				for i := 0; i < n; i++ {
					pkB := enc(int64(5000 + i))
					b0 := types.BuildTestRowid(int64(2*i+1), 1)
					linkID := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
					copy(linkID[8:], pkB)
					birth[string(linkID)] = 1
					require.NoError(b, vector.AppendFixed(links.Vecs[0], pkMoveWorkspaceRowid(uint32(300000+i)), false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[1], linkID, false, mp))
					require.NoError(b, vector.AppendFixed(links.Vecs[2], userTable, false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[3], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[4], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[5], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[6], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[7], enc(int64(9000+i)), false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[8], b0[:], false, mp))
					require.NoError(b, vector.AppendFixed(links.Vecs[9], uint64(1), false, mp))
					require.NoError(b, vector.AppendBytes(links.Vecs[10], testDisttaePkMoveTxnID(1), false, mp))
				}
				links.SetRowCount(n)
				// The staged same-key replacements every marker resolves to.
				ins := batch.NewWithSize(2)
				ins.SetAttributes([]string{catalog.Row_ID, "pk"})
				ins.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				ins.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				for i := 0; i < n; i++ {
					require.NoError(b, vector.AppendFixed(ins.Vecs[0], pkMoveWorkspaceRowid(uint32(100000+i)), false, mp))
					require.NoError(b, vector.AppendFixed(ins.Vecs[1], int64(5000+i), false, mp))
				}
				ins.SetRowCount(n)
				txn := &Transaction{proc: proc}
				txn.statementID = 2
				txn.pkMoveLinkBirth = birth
				txn.writes = []Entry{
					{typ: INSERT, accountId: 7, tableId: userTable, bat: ins, pkCheckPos: 1},
					{
						typ:          INSERT,
						accountId:    7,
						tableId:      200,
						databaseName: catalog.MO_CATALOG,
						tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
						bat:          links,
					},
				}
				txn.accountWorkspaceEntryLocked(&txn.writes[1])
				// The marker batch: one continuity marker per edited row.
				markers := batch.NewWithSize(11)
				markers.SetAttributes([]string{
					catalog.Row_ID,
					catalog.DataBranchUpdateLinkColLinkID,
					catalog.DataBranchUpdateLinkColTableID,
					catalog.DataBranchUpdateLinkColOldPKSha,
					catalog.DataBranchUpdateLinkColOldRow,
					catalog.DataBranchUpdateLinkColOldPK,
					catalog.DataBranchUpdateLinkColNewPKSha,
					catalog.DataBranchUpdateLinkColNewPK,
					catalog.DataBranchUpdateLinkColNewRow,
					catalog.DataBranchUpdateLinkColWriteTS,
					catalog.DataBranchUpdateLinkColTxnID,
				})
				markers.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				markers.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
				markers.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[8] = vector.NewVec(types.T_varbinary.ToType())
				markers.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
				markers.Vecs[10] = vector.NewVec(types.T_binary.ToType())
				for i := 0; i < n; i++ {
					pkB := enc(int64(5000 + i))
					b0 := types.BuildTestRowid(int64(2*i+1), 1)
					require.NoError(b, vector.AppendFixed(markers.Vecs[0], pkMoveWorkspaceRowid(uint32(200000+i)), false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[1], pkB, false, mp))
					require.NoError(b, vector.AppendFixed(markers.Vecs[2], userTable, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[3], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[4], b0[:], false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[5], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[6], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[7], pkB, false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[8], nil, true, mp))
					require.NoError(b, vector.AppendFixed(markers.Vecs[9], uint64(1), false, mp))
					require.NoError(b, vector.AppendBytes(markers.Vecs[10], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
				}
				markers.SetRowCount(n)
				b.StartTimer()
				if err := txn.fillPkMoveLinkNewRowIDsLocked(7, markers); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				markers.Clean(mp)
				links.Clean(mp)
				ins.Clean(mp)
			}
		})
	}
}

// The undo bookkeeping guards that keep bulk pk-unchanged updates linear:
// an N-row update stages one continuity marker per row, and every marker
// funnels through these noters; the paired seen-sets make the
// first-record-wins dedup constant-time per marker.
func TestPkMoveUndoBookkeepingDedup(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	var ridA, ridB types.Rowid
	ridA[0] = 0xa1
	ridB[0] = 0xb2

	linkIDVecBatch := func(t *testing.T, ids ...[]byte) *batch.Batch {
		t.Helper()
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
		for _, id := range ids {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], id, false, mp))
		}
		bat.SetRowCount(len(ids))
		return bat
	}

	t.Run("first retarget record wins per link per statement", func(t *testing.T) {
		txn := &Transaction{}
		bat := linkIDVecBatch(t, []byte("L1"))
		defer bat.Clean(mp)
		txn.Lock()
		txn.pkMoveNoteRetargetLocked(bat, 0, 0, ridA)
		txn.pkMoveNoteRetargetLocked(bat, 0, 0, ridB)
		txn.Unlock()
		require.Len(t, txn.pkMoveRetargetUndo, 1)
		require.Equal(t, ridA, txn.pkMoveRetargetUndo[0].prev)
		require.False(t, txn.pkMoveRetargetUndo[0].heal)
	})

	t.Run("heal and retarget records share one dedup per link", func(t *testing.T) {
		// Heal first: a later retarget note for the same link is the dup.
		txn := &Transaction{}
		bat := linkIDVecBatch(t, []byte("L2"))
		defer bat.Clean(mp)
		txn.Lock()
		txn.pkMoveNoteHealsLocked("L2", types.Rowid{}, 0, true)
		txn.pkMoveNoteRetargetLocked(bat, 0, 0, ridA)
		txn.Unlock()
		require.Len(t, txn.pkMoveRetargetUndo, 1)
		require.True(t, txn.pkMoveRetargetUndo[0].heal)
		require.True(t, txn.pkMoveRetargetUndo[0].created)

		// Retarget first: a later heal note for the same link is the dup.
		txn2 := &Transaction{}
		txn2.Lock()
		txn2.pkMoveNoteRetargetLocked(bat, 0, 0, ridA)
		txn2.pkMoveNoteHealsLocked("L2", ridB, 0, false)
		txn2.Unlock()
		require.Len(t, txn2.pkMoveRetargetUndo, 1)
		require.False(t, txn2.pkMoveRetargetUndo[0].heal)
		require.Equal(t, ridA, txn2.pkMoveRetargetUndo[0].prev)
	})

	t.Run("first origin-write record captures the pre-state once", func(t *testing.T) {
		txn := &Transaction{}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{ridA: ridB}
		txn.Lock()
		txn.pkMoveNoteOriginWriteLocked(ridA)
		txn.pkMoveRowidOrigins[ridA] = types.Rowid{}
		txn.pkMoveNoteOriginWriteLocked(ridA)
		txn.Unlock()
		require.Len(t, txn.pkMoveOriginUndo, 1)
		require.True(t, txn.pkMoveOriginUndo[0].existed)
		require.Equal(t, ridB, txn.pkMoveOriginUndo[0].prev)

		// An unrecorded key notes existed=false exactly once.
		txn.Lock()
		txn.pkMoveNoteOriginWriteLocked(ridB)
		txn.pkMoveNoteOriginWriteLocked(ridB)
		txn.Unlock()
		require.Len(t, txn.pkMoveOriginUndo, 2)
		require.False(t, txn.pkMoveOriginUndo[1].existed)
		require.Equal(t, ridB, txn.pkMoveOriginUndo[1].key)
	})

	t.Run("statement rollback retires the seen sets with the undo slices", func(t *testing.T) {
		txn := &Transaction{}
		bat := linkIDVecBatch(t, []byte("L3"))
		defer bat.Clean(mp)
		txn.Lock()
		txn.pkMoveNoteRetargetLocked(bat, 0, 0, ridA)
		txn.pkMoveNoteOriginWriteLocked(ridA)
		txn.rollbackPkMoveStmtStateLocked()
		txn.Unlock()
		require.Empty(t, txn.pkMoveRetargetUndo)
		require.Nil(t, txn.pkMoveRetargetUndoSeen)
		require.Empty(t, txn.pkMoveOriginUndo)
		require.Nil(t, txn.pkMoveOriginUndoSeen)
	})
}

// TestPkMoveS3LineageHeal proves the by-key lineage stewardship for
// S3-route continuity markers: an unresolved marker still records which
// durable lineage the key's row descends from, a later statement moving
// that row heals its recorded old identity back to the durable origin,
// intervening deletes retire the binding fail-closed, departures consume
// it so later generations never inherit a dead lineage, and statement
// rollback retires the bookkeeping with the statement.
func TestPkMoveS3LineageHeal(t *testing.T) {
	const userTable = uint64(42)
	acct := pkMoveAcctTbl{account: 7, table: userTable}

	t.Run("unresolved marker binds the durable origin and a later move heals", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x2 := types.BuildTestRowid(2, 2)
		y3 := types.BuildTestRowid(3, 3)

		txn := &Transaction{proc: proc}
		txn.statementID = 1

		// Statement 1: S3-route same-key edit at key 3 (payload edit);
		// the replacement never stages, so the marker stays unresolved.
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
		require.Equal(t, 0, marker.RowCount(), "markers are always consumed")
		stack := txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))]
		require.Len(t, stack, 1, "the unresolved marker binds the lineage by key")
		require.Equal(t, b0, stack[0].origin)
		require.Equal(t, 1, stack[0].stmt)
		require.False(t, stack[0].consumed)

		// Statement 2: move B 3->4. The producer names the S3-buffered
		// identity x2 it read, not the durable origin the committed stream
		// shows departing; the fill must heal the cell to b0.
		txn.statementID = 2
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&x2})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b0[:], move.Vecs[4].GetBytesAt(0),
			"the recorded old identity healed to the lineage's durable origin")
		require.True(t, move.Vecs[8].IsNull(0),
			"the replacement never staged: the new identity stays NULL")
		stack = txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))]
		require.Len(t, stack, 2)
		require.True(t, stack[1].consumed,
			"the recorded departure consumed the binding")
		require.Equal(t, 2, stack[1].stmt)

		// Statement 3: a NEW generation edits the row that re-arrived at
		// key 3. The consumed lineage must not leak into the binding: the
		// new marker's literal pre-image governs.
		txn.statementID = 3
		marker3 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&y3})
		defer marker3.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker3))
		stack = txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))]
		require.Len(t, stack, 3)
		require.Equal(t, y3, stack[2].origin,
			"a reincarnated key starts a fresh lineage literal to its pre-image")
		require.False(t, stack[2].consumed)
	})

	t.Run("workspace-stage pre-images anchor literally without a binding", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		w1 := pkMoveWorkspaceRowid(77)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&w1})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
		require.Equal(t, 0, marker.RowCount())
		require.Empty(t, txn.pkMoveKeyLineage,
			"workspace lineage rides the exact origin map, never a by-key binding")
	})

	t.Run("a staged delete of the key retires the binding without a heal", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x2 := types.BuildTestRowid(2, 2)
		dRid := types.BuildTestRowid(1, 2)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))

		// Statement 2: the row at key 3 is deleted outright (no move).
		txn.statementID = 2
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{catalog.Row_ID, "pk"})
		delBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(delBat.Vecs[0], dRid, false, mp))
		delBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(delBat.Vecs[1], int64(3), false, mp))
		delBat.SetRowCount(1)
		t.Cleanup(func() { delBat.Clean(mp) })
		txn.writes = append(txn.writes, Entry{
			typ:       DELETE,
			accountId: 7,
			tableId:   userTable,
			bat:       delBat,
		})

		_, ok := txn.lookupPkMoveKeyLineageLocked(acct, string(pkMoveEncodeInt64(t, 3)))
		require.False(t, ok, "a later staged delete of the key retired the lineage")

		// Statement 3: a link naming the deleted generation keeps its
		// literal identity (the consumer's evidence rules decide).
		txn.statementID = 3
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&x2})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, x2[:], move.Vecs[4].GetBytesAt(0),
			"no heal across a delete: identities stay literal, fail closed")
	})

	t.Run("binding composes through the exact origin map", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x1 := types.BuildTestRowid(2, 1)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		// An earlier healed lineage: x1 descends from durable b0.
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{x1: b0}
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&x1})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
		stack := txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))]
		require.Len(t, stack, 1)
		require.Equal(t, b0, stack[0].origin,
			"the binding names the composed durable origin, not the literal")
	})

	t.Run("statement rollback truncates bindings and drops the delete scan", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		marker1 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker1.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker1))

		txn.statementID = 2
		marker2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker2.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker2))
		require.Len(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))], 2)
		require.NotEmpty(t, txn.pkMoveDeleteScan,
			"the second bind's validity check folded the staged deletes")

		// Rolling back statement 2 retires only its own binding.
		txn.statementID = 1
		txn.rollbackPkMoveStmtStateLocked()
		require.Len(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))], 1)
		require.Nil(t, txn.pkMoveDeleteScan,
			"the folded cache can never serve rolled-back write entries")

		// Rolling back statement 1 drops the key and the table.
		txn.statementID = 0
		txn.rollbackPkMoveStmtStateLocked()
		require.Empty(t, txn.pkMoveKeyLineage)
	})

	t.Run("heal allocation failure fails the fill", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x2 := types.BuildTestRowid(2, 2)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))

		txn.statementID = 2
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&x2})
		defer move.Clean(mp)
		pkMoveSwapVecSeams(t, vector.AppendBytes,
			func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
				return errPkMoveInjected
			})
		err := txn.fillPkMoveLinkNewRowIDsLocked(7, move)
		require.ErrorIs(t, err, errPkMoveInjected,
			"the heal surfaces its allocation failure like every other pass")
		require.Equal(t, x2[:], move.Vecs[4].GetBytesAt(0),
			"the failed heal left the recorded identity untouched")
	})

	t.Run("identity already literal to the origin consumes the binding without a write", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))

		txn.statementID = 2
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&b0})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b0[:], move.Vecs[4].GetBytesAt(0))
		stack := txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 3))]
		require.True(t, stack[len(stack)-1].consumed,
			"a literal departure still counts as a use of the binding")
	})
}

// TestPkMoveDumpKeepsLinkOwnerTables proves the F5 continuity contract:
// a statement-boundary dump must leave the staged INSERT batches of every
// table a staged pk-move link references IN MEMORY. The link pins the
// moved row by its transaction-local workspace row id; converting that
// row into an object row mid-transaction would mint a durable object
// identity no recorded link names, and a later statement's move of the
// same lineage would leave a successor/old and predecessor/new pair the
// consumer's exact-identity pin cannot join — the walk would reach the
// unresolved-provenance abort instead of the native UPDATE fold. Only the
// commit dump (after the final prune) may flush these tables, and tables
// no staged link references dump exactly as before.
func TestPkMoveDumpKeepsPinnedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	newLinkEntry := func(t *testing.T, owner uint64, oldRows []*types.Rowid) Entry {
		return pkMoveLinkEntryFromBatch(t, mp,
			pkMoveMarkerOrMoveBatch(t, mp, owner,
				[][]byte{pkMoveEncodeInt64(t, 2)},
				[][]byte{pkMoveEncodeInt64(t, 3)},
				oldRows))
	}

	insertWithRowids := func(t *testing.T, rowids []types.Rowid, pks []int64) *batch.Batch {
		t.Helper()
		require.Len(t, pks, len(rowids))
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
		rowidVec := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidVec, rowids, nil, proc.Mp()))
		bat.Vecs[0] = rowidVec
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkVec, pks, nil, proc.Mp()))
		bat.Vecs[1] = pkVec
		bat.SetRowCount(len(pks))
		return bat
	}

	t.Run("pin set scans staged links, origins and heals", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		require.Empty(t, txn.pkMoveStagedLinkPinnedRowIDsLocked())

		ridOld := pkMoveWorkspaceRowid(101)
		txn.appendWorkspaceEntryLocked(newLinkEntry(t, userTable, []*types.Rowid{&ridOld}))
		got := txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.Contains(t, got, ridOld,
			"the row a staged link names stays pinned")
		require.NotContains(t, got, pkMoveWorkspaceRowid(102))

		// Origin keys no longer pin (r24 review F3): their rows
		// spill and the ledger keys ride the dump shadow onto the
		// derived object identities. Origin VALUES still pin while
		// they are workspace identities (a durable value can never
		// sit in a workspace INSERT batch).
		k, wsV := pkMoveWorkspaceRowid(120), pkMoveWorkspaceRowid(121)
		durable := types.BuildTestRowid(7, 7)
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{k: wsV}
		got = txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.NotContains(t, got, k,
			"the origin-keyed row spills; the ledger translates instead")
		require.Contains(t, got, wsV,
			"a workspace-staged origin value pins defensively")
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{k: durable}
		got = txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.NotContains(t, got, k)
		require.NotContains(t, got, durable,
			"a durable origin can never appear in a workspace batch")

		// Pending heals pin their workspace bindings.
		hRid := pkMoveWorkspaceRowid(160)
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{"L1": {rid: hRid}}
		got = txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.Contains(t, got, hRid)

		// Zero-row (fully consumed) link batches contribute nothing:
		// retired provenance no longer pins anything.
		ridZ := pkMoveWorkspaceRowid(180)
		zeroEntry := newLinkEntry(t, 43, []*types.Rowid{&ridZ})
		zeroEntry.bat.Shrink([]int64{}, false)
		zeroEntry.bat.SetRowCount(0)
		txn.appendWorkspaceEntryLocked(zeroEntry)
		got = txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.NotContains(t, got, ridZ)

		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	newDumpTxn := func(t *testing.T, links bool, origins bool) *Transaction {
		colexec.NewServer("")
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		catalogDB := &txnDatabase{op: txn.op, databaseId: catalog.MO_CATALOG_ID,
			databaseName: catalog.MO_CATALOG}
		txn.tableOps.addCreateTable(
			genTableKey(7, catalog.MO_DATA_BRANCH_UPDATE_LINK,
				catalog.MO_CATALOG_ID, catalog.MO_CATALOG),
			0,
			&txnTable{
				accountId: 7,
				tableId:   99,
				tableName: catalog.MO_DATA_BRANCH_UPDATE_LINK,
				db:        catalogDB,
				tableDef:  &pbplan.TableDef{Cols: []*pbplan.ColDef{{Name: "link_id"}}},
			},
		)
		tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
		require.NotNil(t, tbl)
		tbl.tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
		txn.tnStores = []DNStore{{}}
		txn.batchSelectList = make(map[*batch.Batch][]int64)
		txn.tablesInVain = make(map[uint64]int)
		txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
		txn.cnObjsSummary = make(map[types.Objectid]Summary)
		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		// Two INSERT batches of the SAME table: only the batch carrying
		// the pinned identity must stay.
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      userTable,
			databaseName: "db",
			tableName:    "tbl",
			pkCheckPos:   1,
			bat: insertWithRowids(t,
				[]types.Rowid{pkMoveWorkspaceRowid(1), pkMoveWorkspaceRowid(2)},
				[]int64{1, 2}),
		})
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      userTable,
			databaseName: "db",
			tableName:    "tbl",
			pkCheckPos:   1,
			bat: insertWithRowids(t,
				[]types.Rowid{pkMoveWorkspaceRowid(3), pkMoveWorkspaceRowid(4)},
				[]int64{3, 4}),
		})
		if links {
			pinned := pkMoveWorkspaceRowid(1)
			txn.appendWorkspaceEntryLocked(newLinkEntry(t, userTable, []*types.Rowid{&pinned}))
		}
		if origins {
			txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
				pkMoveWorkspaceRowid(1): types.BuildTestRowid(30, 1),
			}
		}
		return txn
	}

	run := func(t *testing.T, links bool, origins bool) (*Transaction, int) {
		txn := newDumpTxn(t, links, origins)
		fs, err := colexec.GetSharedFSFromProc(txn.proc)
		require.NoError(t, err)
		var pkCount int
		txn.Lock()
		err = txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false)
		txn.Unlock()
		require.NoError(t, err)
		return txn, pkCount
	}

	countKeptTableBatches := func(t *testing.T, txn *Transaction) int {
		kept := 0
		for i := range txn.writes {
			we := &txn.writes[i]
			if we.tableName == "tbl" && we.typ == INSERT && we.fileName == "" &&
				we.bat != nil && we.bat.RowCount() > 0 {
				kept++
			}
		}
		return kept
	}

	t.Run("link-pinned batch stays while the same-table sibling dumps", func(t *testing.T) {
		txn, pkCount := run(t, true, false)
		require.Equal(t, 2, pkCount,
			"the unpinned sibling batch of the owner table still spills")
		require.Equal(t, 1, countKeptTableBatches(t, txn),
			"exactly one batch holds a pinned row; whole-table retention is gone")
		require.NotZero(t, txn.approximateInMemInsertSize,
			"the kept batch keeps its workspace accounting")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("an unrelated spill wave retains no provenance state", func(t *testing.T) {
		// The r19 spill-side budget finding: the dump used to record the
		// row ids of every spilled batch whenever ANY link was staged in
		// the transaction, so one move plus arbitrarily many unrelated
		// INSERT waves grew uncharged heap until commit — for identities
		// the pin contract already guarantees can never leave the
		// workspace before the final prune, so the prune could never
		// consult them. The wave must now move bytes and nothing else.
		txn := newDumpTxn(t, true, true)
		bookBefore := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, bookBefore,
			"the origin bookkeeping is live when the wave runs")
		stagedBefore := txn.pkMoveLinkStagedBytes
		fs, err := colexec.GetSharedFSFromProc(txn.proc)
		require.NoError(t, err)
		var pkCount int
		txn.Lock()
		err = txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false)
		txn.Unlock()
		require.NoError(t, err)
		require.Equal(t, 2, pkCount,
			"the unpinned sibling wave spilled like before")
		require.Equal(t, 1, countKeptTableBatches(t, txn),
			"only the provenance-named batch stays behind")
		require.Equal(t, bookBefore, txn.pkMoveBookkeepBytesLocked(),
			"the spill wave retained zero new provenance bookkeeping")
		require.Equal(t, stagedBefore, txn.pkMoveLinkStagedBytes,
			"the spill wave staged no link bytes")
		// The deleted per-row spill track is asserted BY NAME through
		// reflection: the production fix removed pkMoveSpilledInserts,
		// so this guard must compile in a tree where the field does
		// not exist — while any reintroduction of a per-row spill
		// track under its name surfaces populated state here and
		// fails the wave outright.
		spillField := reflect.ValueOf(txn).Elem().FieldByName("pkMoveSpilledInserts")
		require.False(t, spillField.IsValid() && spillField.Len() > 0,
			"an unrelated spill wave retains no per-row spill identities")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}

		// A link-staged transaction without origin bookkeeping leaves
		// the measure at zero across the same wave: no shadow ledger
		// replaced the deleted spill track.
		txn2, pkCount2 := run(t, true, false)
		require.Equal(t, 2, pkCount2)
		require.Zero(t, txn2.pkMoveBookkeepBytesLocked(),
			"the wave leaves no provenance footprint at all")
		require.NoError(t, txn2.checkWorkspaceAccountingLocked())
		for i := range txn2.writes {
			txn2.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("origin-only batches dump and the ledger rides the shadow", func(t *testing.T) {
		// The r24 review's F3: origin keys used to pin their rows, so a
		// bulk non-key UPDATE's entire payload stayed in memory at every
		// statement-boundary dump beyond normal spill thresholds. The
		// pin is gone; the payload spills and the ledger keys translate
		// onto the derived object identities.
		txn := newDumpTxn(t, false, true)
		pinned := txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.NotContains(t, pinned, pkMoveWorkspaceRowid(1),
			"the origin map alone pins nothing")
		require.Zero(t, txn.pkMovePinnedRetainedBytesLocked(pinned),
			"origin bookkeeping carries no pin-retained payload charge")

		fs, err := colexec.GetSharedFSFromProc(txn.proc)
		require.NoError(t, err)
		var pkCount int
		txn.Lock()
		err = txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false)
		txn.Unlock()
		require.NoError(t, err)
		require.Equal(t, 4, pkCount,
			"the whole payload wave dumps like provenance-free writes")
		require.Equal(t, 0, countKeptTableBatches(t, txn),
			"no batch is pin-retained for origin bookkeeping alone")

		// The ledger survived the spill exactly: one entry, the same
		// durable origin value, re-keyed onto the derived object id.
		require.Len(t, txn.pkMoveRowidOrigins, 1)
		for k, v := range txn.pkMoveRowidOrigins {
			require.False(t, pkMoveIsWorkspaceStageRowid(&k),
				"the spilled origin key translated onto an object row id")
			require.NotEqual(t, pkMoveWorkspaceRowid(1), k)
			require.Equal(t, types.BuildTestRowid(30, 1), v,
				"the durable origin value is untouched by the translation")
		}
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("tables no provenance references dump as before", func(t *testing.T) {
		txn, pkCount := run(t, false, false)
		require.Equal(t, 4, pkCount)
		require.Equal(t, 0, countKeptTableBatches(t, txn),
			"an unreferenced table dumps to object storage normally")
		require.Zero(t, txn.approximateInMemInsertSize)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	// The r20 pin-retention budget finding: the keep predicate is
	// batch-wide while the provenance measure counted only link batches
	// and bookkeeping, so repeated single-row non-key updates of wide
	// rows retained every payload the origin map pinned — the dump never
	// spilled them and the budget never saw them.
	t.Run("the retained byte measure matches the keep predicate", func(t *testing.T) {
		txn := newDumpTxn(t, true, false)
		pinned := txn.pkMoveStagedLinkPinnedRowIDsLocked()
		require.NotEmpty(t, pinned)
		var want int64
		for i := range txn.writes {
			we := &txn.writes[i]
			if we.tableName == "tbl" && we.typ == INSERT && we.fileName == "" &&
				we.bat != nil && we.bat.RowCount() > 0 &&
				pkMoveBatchHasPinnedRowID(we.bat, pinned) {
				want += int64(we.bat.Size())
			}
		}
		require.Positive(t, want)
		require.Equal(t, want, txn.pkMovePinnedRetainedBytesLocked(pinned),
			"exactly the batches the dump keeps enter the charge")
		require.Zero(t, (&Transaction{proc: proc}).pkMovePinnedRetainedBytesLocked(pinned),
			"an empty workspace retains nothing")
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("retained pinned payload crosses the bound at the dump gate", func(t *testing.T) {
		txn := newDumpTxn(t, true, true)
		pinBytes := txn.pkMovePinnedRetainedBytesLocked(
			txn.pkMoveStagedLinkPinnedRowIDsLocked())
		require.Positive(t, pinBytes)
		retained := txn.pkMoveLinkStagedBytes + txn.pkMoveBookkeepBytesLocked() + pinBytes

		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = retained - 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		fs, err := colexec.GetSharedFSFromProc(txn.proc)
		require.NoError(t, err)
		var pkCount int
		txn.Lock()
		err = txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false)
		txn.Unlock()
		require.Error(t, err,
			"keeping the pinned payload would cross the budget: the statement fails")
		require.Contains(t, err.Error(), "pin-retained workspace bytes")
		require.Equal(t, 2, countKeptTableBatches(t, txn),
			"the rejected gate mutates nothing: every batch awaits the retry")
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("exact-fit retention passes and stays charged after the dump", func(t *testing.T) {
		txn := newDumpTxn(t, true, true)
		pinBytes := txn.pkMovePinnedRetainedBytesLocked(
			txn.pkMoveStagedLinkPinnedRowIDsLocked())
		require.Positive(t, pinBytes)
		retained := txn.pkMoveLinkStagedBytes + txn.pkMoveBookkeepBytesLocked() + pinBytes

		prevBudget := pkMoveLinkCoordinatorBudget
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })
		pkMoveLinkCoordinatorBudget = retained

		fs, err := colexec.GetSharedFSFromProc(txn.proc)
		require.NoError(t, err)
		var pkCount int
		txn.Lock()
		err = txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false)
		txn.Unlock()
		require.NoError(t, err)
		require.Equal(t, 2, pkCount, "the unpinned sibling still spills")
		require.Equal(t, 1, countKeptTableBatches(t, txn))
		require.Equal(t, pinBytes, txn.pkMovePinnedRetainedBytesLocked(
			txn.pkMoveStagedLinkPinnedRowIDsLocked()),
			"the kept payload stays charged until a later dump releases it")

		// A later wave's budget check sees the retained payload, not just
		// the link and bookkeeping measures.
		pkMoveLinkCoordinatorBudget = retained - 1
		require.Error(t, txn.checkPkMoveBookkeepBudgetLocked(),
			"post-dump checks observe the pin-retained bytes")
		pkMoveLinkCoordinatorBudget = 0
		require.NoError(t, txn.checkPkMoveBookkeepBudgetLocked(),
			"a zero budget disables the charge everywhere")
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	t.Run("a zero budget disables the dump-gate charge", func(t *testing.T) {
		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = 0
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })
		txn, pkCount := run(t, true, false)
		require.Equal(t, 2, pkCount)
		require.Equal(t, 1, countKeptTableBatches(t, txn),
			"the disabled charge never reshapes the keep decision")
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})

	// The r24 review's F3 determination: repeated same-key payload
	// updates must release their complete batches through statement-
	// boundary dumps while retained bookkeeping stays bounded (one
	// ledger entry per edited row), and a move arriving after the
	// spills must still anchor to the durable origin the committed
	// stream shows departing — byte-identical to the no-spill lineage.
	t.Run("multi-wave updates release payload; the later move anchors to the durable origin", func(t *testing.T) {
		txn := newDumpTxn(t, false, false)
		mp := proc.Mp()
		oldRowIdx, newRowIdx := 4, 8 // pkMovePruneLinkEntry's attr layout
		d0 := types.BuildTestRowid(30, 1)
		w1 := pkMoveWorkspaceRowid(1)
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{w1: d0}

		dump := func(t *testing.T) {
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			var pkCount int
			txn.Lock()
			defer txn.Unlock()
			require.NoError(t,
				txn.dumpInsertBatchLocked(context.Background(), fs, 0, &pkCount, false))
			require.Equal(t, 0, countKeptTableBatches(t, txn),
				"every wave releases its complete payload batch")
			require.NoError(t, txn.checkWorkspaceAccountingLocked())
		}

		// Wave 1: the whole edit wave spills; only the bounded ledger
		// entry persists, re-keyed onto the derived object identity.
		bookBefore := txn.pkMoveBookkeepBytesLocked()
		dump(t)
		require.Equal(t, bookBefore, txn.pkMoveBookkeepBytesLocked(),
			"the translation preserves the ledger size exactly")
		require.Len(t, txn.pkMoveRowidOrigins, 1)
		var d1 types.Rowid
		for k, v := range txn.pkMoveRowidOrigins {
			require.False(t, pkMoveIsWorkspaceStageRowid(&k),
				"the origin key now names an object row id")
			require.Equal(t, d0, v,
				"the durable origin value survives the spill untouched")
			d1 = k
		}
		require.NotEqual(t, w1, d1)

		// Wave 2: a same-key edit of the spilled row stages w5@pk1 and
		// its continuity marker composes the origin through the derived
		// identity exactly as production composes through a workspace
		// identity; the replacement spills again at the wave dump.
		w5 := pkMoveWorkspaceRowid(5)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      userTable,
			databaseName: "db",
			tableName:    "tbl",
			pkCheckPos:   1,
			bat:          insertWithRowids(t, []types.Rowid{w5}, []int64{1}),
		})
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 1)},
			[][]byte{pkMoveEncodeInt64(t, 1)},
			[]*types.Rowid{&d1})
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(1, marker))
		require.Equal(t, 0, marker.RowCount())
		marker.Clean(mp)
		require.Equal(t, d0, txn.pkMoveRowidOrigins[w5],
			"the second edit composes the same durable origin across the spill")

		bookBefore = txn.pkMoveBookkeepBytesLocked()
		dump(t)
		require.Equal(t, bookBefore, txn.pkMoveBookkeepBytesLocked(),
			"the second wave's translation preserves the ledger size too")
		require.Len(t, txn.pkMoveRowidOrigins, 1,
			"one bounded entry per LIVE lineage generation: the superseded "+
				"wave-1 entry retires under r26 review F5 / D112")
		var d5 types.Rowid
		for k, v := range txn.pkMoveRowidOrigins {
			require.False(t, pkMoveIsWorkspaceStageRowid(&k))
			require.Equal(t, d0, v)
			d5 = k
		}
		require.NotEqual(t, w5, d5)

		// The move: the workspace now presents the row by its wave-2
		// object identity, exactly what the delete-half scan would name.
		w9 := pkMoveWorkspaceRowid(9)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      userTable,
			databaseName: "db",
			tableName:    "tbl",
			pkCheckPos:   1,
			bat:          insertWithRowids(t, []types.Rowid{w9}, []int64{9}),
		})
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 1)},
			[][]byte{pkMoveEncodeInt64(t, 9)},
			[]*types.Rowid{&d5})
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(1, move))
		require.Equal(t, 1, move.RowCount())
		require.Equal(t, d0[:], move.Vecs[oldRowIdx].GetBytesAt(0),
			"the move still anchors to the durable origin, byte-identical to no-spill")
		require.Equal(t, w9[:], move.Vecs[newRowIdx].GetBytesAt(0))
		move.Clean(mp)

		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	})
}

// TestPkMoveDivertedDeleteRetiresLineageBinding proves the r18 review's
// diverted-delete finding: a delete that diverts to the per-block list
// (its row lived in an uncommitted CN object) never enters the
// staged-delete stream the binding validity check scans, so without
// retirement evidence an S3 same-key edit followed by DELETE, a spilled
// unrelated reinsert at the same key, and a PK move could heal the
// unrelated row onto the dead lineage — MERGE/PICK would then cascade
// the original parent's children onto it. The retirement ledger is
// recorded at the divert boundary even before the first move link
// exists; a later fill that would heal through the retired binding
// instead kills the link, the commit prune drops it, and the apply
// replays plain DELETE+INSERT — the delete semantics the committed
// history actually carries (ON UPDATE RESTRICT still fails loudly, ON
// DELETE CASCADE cleans the family), never a reparent.
func TestPkMoveDivertedDeleteRetiresLineageBinding(t *testing.T) {
	const userTable = uint64(42)
	acct := pkMoveAcctTbl{account: 7, table: userTable}
	pk3 := func(t *testing.T) string { return string(pkMoveEncodeInt64(t, 3)) }

	// divertedDelete drives one row of the [rowid, pk] delete layout
	// through the production divert/delete boundary.
	divertedDelete := func(t *testing.T, txn *Transaction, mp *mpool.MPool,
		tableID uint64, rid types.Rowid, pk int64) {
		t.Helper()
		del := batch.NewWithSize(2)
		del.SetAttributes([]string{catalog.Row_ID, "pk"})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], rid, false, mp))
		del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[1], pk, false, mp))
		del.SetRowCount(1)
		defer del.Clean(mp)
		require.NoError(t, txn.notePkMoveDivertedDeletes(tableID, del, []int64{0}))
	}

	// bindLineage stages an unresolved same-key marker: an S3-route edit
	// whose replacement never staged, binding the key's lineage to the
	// durable origin.
	bindLineage := func(t *testing.T, txn *Transaction, mp *mpool.MPool,
		origin types.Rowid, pk int64) {
		t.Helper()
		enc := pkMoveEncodeInt64(t, pk)
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{enc}, [][]byte{enc}, []*types.Rowid{&origin})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
	}

	t.Run("a diverted death before the first link kills the later heal", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)       // durable origin of pk 3
		x2 := types.BuildTestRowid(2, 2)       // the edit's uncommitted-object generation
		intruder := types.BuildTestRowid(3, 3) // the unrelated reinserted row

		txn := &Transaction{proc: proc}
		// Statement 1: an S3-route same-key edit at pk 3 binds the
		// lineage — the row at 3 descends from durable b0.
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 3)

		// Statement 2: the edited row is deleted outright; the delete
		// diverts (its object is uncommitted) BEFORE any move link
		// exists, so neither pkMoveLinkTables nor the staged-delete scan
		// ever sees it.
		txn.statementID = 2
		divertedDelete(t, txn, mp, userTable, x2, 3)
		require.NotEmpty(t, txn.pkMoveRetireDeaths[acct][pk3(t)],
			"the diverted death was recorded before the first move link")
		require.Empty(t, txn.pkMoveDeadPks,
			"no link table is registered yet: the death ledger stays empty")

		// Statement 3: a spilled reinsert produced an unrelated row at
		// pk 3 (an identity the committed stream never ties to b0), and
		// the transaction moves THAT row 3->4. The fill must refuse the
		// heal onto the murdered lineage and kill the link instead.
		txn.statementID = 3
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&intruder})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, intruder[:], move.Vecs[4].GetBytesAt(0),
			"a retired lineage never heals: the recorded identity stays literal")
		linkID := string(move.Vecs[1].GetBytesAt(0))
		require.Contains(t, txn.pkMoveKilledLinks, linkID,
			"the fill killed the link onto the lineage the delete retired")

		// The commit prune drops the killed link: MERGE/PICK replay the
		// move as DELETE+INSERT, so a declared ON UPDATE action never
		// re-parents the dead lineage's children onto the unrelated row.
		// (An explicit zero-selection keep entry drops every row of the
		// batch; an absent entry would keep it wholesale.)
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move))
		require.Equal(t, map[int][]int64{0: nil},
			pkMoveLinkRowsToKeep(txn.writes, txn.pkMoveLedgerForPruneLocked()),
			"the killed provenance never reaches the apply: plain DELETE+INSERT replay")
	})

	t.Run("a death explained by a staged departure keeps the binding", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x2 := types.BuildTestRowid(2, 2)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 3)

		// Statement 2: the row's real move stages; the fill records the
		// observed old identity x2 in the coverage set (before healing
		// the recorded cell to the lineage origin).
		txn.statementID = 2
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&x2})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b0[:], move.Vecs[4].GetBytesAt(0),
			"the valid binding healed the move's recorded old identity")

		// Statement 3: the moved row's own pre-image delete diverts. The
		// departed generation x2 IS a recorded old side (coverage), so
		// the death proves the move's provenance, not a murder.
		txn.statementID = 3
		divertedDelete(t, txn, mp, userTable, x2, 3)
		require.False(t, txn.pkMoveBindingRetiredLocked(acct, pk3(t), 1),
			"a staged departure explains the diverted death: the lineage legitimately left the key")
		require.Empty(t, txn.pkMoveKilledLinks,
			"no fill ever killed through the explained death")
	})

	t.Run("statement rollback revives the retired binding and drops the kill", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b0 := types.BuildTestRowid(1, 1)
		x2 := types.BuildTestRowid(2, 2)
		intruder := types.BuildTestRowid(3, 3)

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 3)
		txn.statementID = 2
		divertedDelete(t, txn, mp, userTable, x2, 3)
		txn.statementID = 3
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&intruder})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		linkID := string(move.Vecs[1].GetBytesAt(0))
		require.Contains(t, txn.pkMoveKilledLinks, linkID)

		// Rolling back statement 3 (the move) drops the kill; the
		// statement-2 death still stands.
		txn.statementID = 2
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.NotContains(t, txn.pkMoveKilledLinks, linkID,
			"the kill died with the statement whose fill made it")
		require.NotEmpty(t, txn.pkMoveRetireDeaths[acct][pk3(t)],
			"the earlier statement's diverted death survives")

		// Rolling back statement 2 (the DELETE) revives the binding: its
		// murdered generation never died in the surviving history.
		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Empty(t, txn.pkMoveRetireDeaths[acct][pk3(t)])
		origin, ok := txn.lookupPkMoveKeyLineageLocked(acct, pk3(t))
		require.True(t, ok, "the lineage binding revives with its murderer")
		require.Equal(t, b0, origin)
	})

	t.Run("a death older than the binding never retires it", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		b9 := types.BuildTestRowid(1, 9)
		dRid := types.BuildTestRowid(2, 5)
		b3 := types.BuildTestRowid(1, 3)
		x2 := types.BuildTestRowid(2, 2)

		txn := &Transaction{proc: proc}
		// Statement 1: a binding exists for pk 9, so the diverted death
		// of pk 3 lands in the retirement ledger even though pk 3 has no
		// binding of its own yet.
		txn.statementID = 1
		bindLineage(t, txn, mp, b9, 9)
		divertedDelete(t, txn, mp, userTable, dRid, 3)
		require.NotEmpty(t, txn.pkMoveRetireDeaths[acct][pk3(t)])

		// Statement 2: a NEW generation re-arrived at pk 3 and was
		// edited — the binding governs the reincarnated lineage, which
		// the old death never touched.
		txn.statementID = 2
		bindLineage(t, txn, mp, b3, 3)

		// Statement 3: moving the reincarnated row heals through the
		// fresh binding; the pre-binding death explained nothing about
		// this generation.
		txn.statementID = 3
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&x2})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b3[:], move.Vecs[4].GetBytesAt(0),
			"the reincarnated lineage's binding healed the move")
		require.Empty(t, txn.pkMoveKilledLinks,
			"the pre-binding death retired nothing")
	})
}

// TestPkMoveHarvestExcludesSelectedRows proves the r18 harvest-exclusion
// half of the workspace-to-S3 continuity finding: rows a same-key UPDATE
// selected out of their staged batch (batchSelectList — the logically
// deleted pre-images the commit shrinks away) must never resolve a
// replacement. Harvesting one re-anchors provenance on a generation the
// committed stream does not carry, and the commit prune then drops the
// genuine lineage: the apply reverts to DELETE+INSERT and ON DELETE
// CASCADE destroys children.
func TestPkMoveHarvestExcludesSelectedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	t.Run("selected rows never resolve and the fold rides the selection generation", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		w1 := pkMoveWorkspaceRowid(71)
		w2 := pkMoveWorkspaceRowid(72)
		entry := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{3, 4}, nil, []types.Rowid{w1, w2})
		txn.writes = []Entry{entry}

		harvest := txn.pkMoveHarvestNewRowIDsCachedLocked(7, userTable)
		require.Equal(t, w1, harvest[string(pkMoveEncodeInt64(t, 3))])
		require.Equal(t, w2, harvest[string(pkMoveEncodeInt64(t, 4))])

		// A same-key edit selects row 0 out of the staged batch: its
		// pre-image is logically deleted in place (this is exactly the
		// bump addBatchSelectionsLocked issues).
		txn.batchSelectList = map[*batch.Batch][]int64{entry.bat: {0}}
		txn.batchSelectListGen++
		excluded := txn.pkMoveHarvestNewRowIDsCachedLocked(7, userTable)
		_, found := excluded[string(pkMoveEncodeInt64(t, 3))]
		require.False(t, found,
			"a logically deleted staged row never resolves a replacement")
		require.Equal(t, w2, excluded[string(pkMoveEncodeInt64(t, 4))],
			"live rows of the same batch still resolve")

		// The uncached direct fold applies the same exclusion.
		folded, ok := pkMoveFoldHarvestEntries(txn.writes, 7, userTable, nil,
			func(bat *batch.Batch) []int64 { return txn.batchSelectList[bat] })
		require.True(t, ok)
		_, found = folded[string(pkMoveEncodeInt64(t, 3))]
		require.False(t, found)

		// Clearing the selection with another generation bump refolds
		// the row: the cache never serves contents folded under a
		// different selection state.
		delete(txn.batchSelectList, entry.bat)
		txn.batchSelectListGen++
		refolded := txn.pkMoveHarvestNewRowIDsCachedLocked(7, userTable)
		require.Equal(t, w1, refolded[string(pkMoveEncodeInt64(t, 3))],
			"the cache rebuilds with the selection state it folds")
	})
}

// TestPkMoveWorkspaceToS3RetargetToNull proves the r18 continuity half of
// the workspace-to-S3 finding: after a workspace-route pk move (2->3,
// replacement staged as w3), an S3-route same-key edit of the moved row
// resolves NO staged replacement (the pre-image is selected out, the
// replacement lives only in the S3 buffer), so the staged move link's
// workspace new-identity must switch to the S3 NULL identity. Without
// the switch the commit prune reads w3's consumption as a murder, drops
// the genuine link, and the apply reverts to DELETE+INSERT (the era-11
// binary destroyed the ON DELETE CASCADE child exactly this way). Every
// mutation rides the statement undo log: a rollback of the editing
// statement restores both the cell and the heal bindings.
func TestPkMoveWorkspaceToS3RetargetToNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	t.Run("the route switch nulls the staged identity and rollback restores it", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		b0 := types.BuildTestRowid(1, 1)
		w3 := pkMoveWorkspaceRowid(83)

		// Statement 1: the workspace-route move. The replacement stages
		// (pk 3 -> w3) and the link's new cell resolves to it.
		txn.statementID = 1
		ins := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{3}, nil, []types.Rowid{w3})
		txn.writes = append(txn.writes, ins)
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b0})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, w3[:], move.Vecs[8].GetBytesAt(0),
			"the replacement resolved to the staged workspace identity")
		txn.writes = append(txn.writes, pkMoveLinkEntryFromBatch(t, mp, move))

		// An earlier NULL-identity link carries a heal binding anchored
		// on w3: the switch must retire it (its link is already on the
		// NULL protocol) and the undo log must recreate it.
		healOwner := "heal-link-on-w3"
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{healOwner: {rid: w3, stmt: 1}}

		// Statement 2: an S3-route same-key edit of the moved row. Its
		// pre-image is the staged row itself, selected out of its batch
		// (logically deleted in place); the replacement lives only in
		// the S3 buffer, so the marker's harvest misses.
		txn.statementID = 2
		txn.batchSelectList = map[*batch.Batch][]int64{ins.bat: {0}}
		txn.batchSelectListGen++
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&w3})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
		require.Equal(t, 0, marker.RowCount(), "the marker is consumed")

		require.True(t, move.Vecs[8].IsNull(0),
			"the staged link switched to the S3 NULL identity across the route switch")
		require.NotContains(t, txn.pkMoveNullHeals, healOwner,
			"the heal binding anchored on the consumed identity retired")
		require.NotEmpty(t, txn.pkMoveRetargetUndo,
			"the switch rode the statement undo log")

		// Statement rollback restores the workspace identity AND the
		// heal binding.
		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.False(t, move.Vecs[8].IsNull(0))
		require.Equal(t, w3[:], move.Vecs[8].GetBytesAt(0),
			"rollback restored the retargeted workspace identity")
		require.Equal(t, pkMoveNullHeal{rid: w3, stmt: 1}, txn.pkMoveNullHeals[healOwner],
			"rollback recreated the heal binding with its birth statement")

		// The harvest cache folded nothing of the rolled-back edit: a
		// refill resolves the replacement exactly as before it. The
		// production caller clears batchSelectList wholesale on any
		// statement rollback (RollbackLastStatement), so mirror that.
		delete(txn.batchSelectList, ins.bat)
		txn.batchSelectListGen++
		require.Equal(t, w3, txn.pkMoveHarvestNewRowIDsCachedLocked(7, userTable)[string(pkMoveEncodeInt64(t, 3))])
	})
}

// TestPkMoveBookkeepBudget proves the transaction-level provenance
// bookkeeping rides the coordinator retention budget (the r18 review's
// marker-wave finding, re-scoped by the r22 review's F3): the by-key
// lineage stacks and statement ledgers are measured from the live maps,
// retention growth across statement boundaries is visible to the
// measure, and statement rollback shrinks the measure back under the
// budget. F3 re-scoped enforcement to durable growth: a marker-only wave
// in a transaction holding no staged links is consumed continuity
// evidence that dies with the transaction — refusing it rejected
// ordinary bulk non-key UPDATEs — so only fills that produced real link
// rows, fills in a transaction already holding live link rows, and
// admission of a real link batch refuse an over-budget wave. Before the
// measure existed, repeated marker-only statements (or a bulk non-key
// S3 update) accumulated unaccounted heap while the 64MB budget counted
// only retained link batches.
func TestPkMoveBookkeepBudget(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	writeWave := func(t *testing.T, txn *Transaction, pks []int64) {
		t.Helper()
		bat := pkMoveNewMarkerWriteBatch(t, mp, pks)
		rids, err := txn.WriteBatch(INSERT, "", 7, 1, 99,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK, bat, DNStore{})
		require.NoError(t, err)
		require.NotNil(t, rids)
		rids.Free(mp)
	}

	t.Run("the measure tracks retained bookkeeping with zero staged link bytes", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		writeWave(t, txn, []int64{11, 12, 13, 14})
		require.Zero(t, txn.pkMoveLinkStagedBytes,
			"a consumed marker wave stages no retained link batch")
		require.Empty(t, txn.writes)
		m1 := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, m1,
			"the lineage bookkeeping is measured even at zero retained link bytes")

		// A second wave in a later statement accumulates: S3-route bulk
		// edits keep their lineage evidence for the whole transaction.
		txn.statementID = 2
		writeWave(t, txn, []int64{21, 22, 23, 24})
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), m1,
			"bookkeeping accumulates across statement boundaries")
	})

	t.Run("a participation-free over-budget wave commits; only durable growth is refused", func(t *testing.T) {
		// Measure one wave's bookkeeping at the default budget, then
		// inject a budget one byte under it.
		probe := &Transaction{proc: proc}
		probe.statementID = 1
		writeWave(t, probe, []int64{31, 32, 33, 34})
		waveBytes := probe.pkMoveBookkeepBytesLocked()
		require.Positive(t, waveBytes)

		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = waveBytes - 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		// The r22 review's F3: a marker-only wave whose transaction holds
		// no staged link rows is consumed continuity evidence that dies
		// with the transaction — it must NOT be rejected, or ordinary
		// bulk non-key UPDATEs (one marker family per touched row, no
		// move anywhere) fail on provenance they never commit.
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		writeWave(t, txn, []int64{31, 32, 33, 34})
		require.Empty(t, txn.writes,
			"a consumed marker wave still stages nothing")
		require.Equal(t, waveBytes, txn.pkMoveBookkeepBytesLocked(),
			"the exempt wave's bookkeeping is measured, unrefused")
		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Zero(t, txn.pkMoveBookkeepBytesLocked(),
			"the rollback retired the exempt statement's bookkeeping")
		require.Empty(t, txn.pkMoveKeyLineage)

		// Once the transaction holds live staged link rows, the SAME
		// wave guards durable growth and fails statement-atomically:
		// the rejected fill stages nothing, and rollback clears exactly
		// what the failed statement recorded.
		txn2 := &Transaction{proc: proc}
		txn2.statementID = 1
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{nil},
			[]uint64{900})
		txn2.appendWorkspaceEntryLocked(linkEntry)
		defer txn2.releaseWorkspaceEntryBatchLocked(0)
		bat := pkMoveNewMarkerWriteBatch(t, mp, []int64{31, 32, 33, 34})
		rids, err := txn2.WriteBatch(INSERT, "", 7, 1, 99,
			catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK, bat, DNStore{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "coordinator retention budget")
		require.Nil(t, rids)
		require.NotZero(t, txn2.pkMoveBookkeepBytesLocked(),
			"the failed statement's evidence outlives the error until rollback")
		txn2.statementID = 0
		require.NoError(t, txn2.rollbackPkMoveStmtStateLocked())
		require.Zero(t, txn2.pkMoveBookkeepBytesLocked(),
			"the rollback retired the failed statement's bookkeeping")
		require.Empty(t, txn2.pkMoveKeyLineage)
	})

	t.Run("participation-free delete waves stay unrefused; a staged link re-arms them", func(t *testing.T) {
		// The r23 review's F3: the D95 gate exempted only the fill-end
		// check, so an ordinary bulk non-key UPDATE inside an explicit
		// transaction — one marker family per touched row, no move
		// anywhere, no staged link — was rejected the moment a following
		// DELETE crossed either delete boundary's unconditional budget
		// check. A participation-free wave's deaths and retirements are
		// evidence only the commit evaluation of a staged link consumes;
		// with no staged link they die with the transaction and must be
		// recorded but never charged. The same wave IS charged the
		// moment the transaction holds a live stage: durable provenance
		// then depends on the evidence.
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		writeWave(t, txn, []int64{31, 32, 33, 34})
		waveBytes := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, waveBytes)

		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = waveBytes - 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		key := pkMoveAcctTbl{account: 7, table: userTable}
		txn.appendPkMoveKeyLineageLocked(
			key, string(pkMoveEncodeInt64(t, 31)), types.BuildTestRowid(1, 31))

		newDelWave := func(ridBase int64) *batch.Batch {
			bat := batch.NewWithSize(2)
			bat.SetAttributes([]string{"rowid", "pk"})
			rowidVec := vector.NewVec(types.T_Rowid.ToType())
			for i := 0; i < 3; i++ {
				require.NoError(t, vector.AppendFixed(
					rowidVec, types.BuildTestRowid(9, ridBase+int64(i)), false, mp))
			}
			pkVec := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(
				pkVec, []int64{31, 9001, 9002}, nil, mp))
			bat.Vecs[0] = rowidVec
			bat.Vecs[1] = pkVec
			bat.SetRowCount(3)
			return bat
		}
		allThree := []int64{0, 1, 2}

		divWave := newDelWave(3100)
		defer divWave.Clean(mp)
		require.NoError(t, txn.notePkMoveDivertedDeletes(userTable, divWave, allThree),
			"no staged link anywhere: the over-budget delete wave is recorded, unrefused")
		require.Len(t, txn.pkMoveRetireDeaths[key], 3,
			"the exempt wave's evidence is still recorded for in-transaction chains")

		swalWave := newDelWave(3200)
		defer swalWave.Clean(mp)
		require.NoError(t, txn.notePkMoveSwallowedDeletes(userTable, swalWave, allThree),
			"the raw-workspace boundary shares the participation gate")
		require.Len(t, txn.pkMoveRetireDeaths[key], 3,
			"the second wave maps onto the same three dead keys")
		require.Len(t, txn.pkMoveRetireDeaths[key][string(pkMoveEncodeInt64(t, 31))], 2,
			"both boundaries recorded their statements' evidence")

		// Participation re-arms the identical wave: stage one live link
		// row and the same boundary check again enforces the budget,
		// statement-atomically.
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{nil},
			[]uint64{900})
		txn.appendWorkspaceEntryLocked(linkEntry)
		defer txn.releaseWorkspaceEntryBatchLocked(len(txn.writes) - 1)
		require.Error(t, txn.notePkMoveDivertedDeletes(userTable, divWave, allThree),
			"a live staged link makes the same wave durable-growth evidence")
	})

	t.Run("admission counts the bookkeeping against the same budget", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		writeWave(t, txn, []int64{41, 42, 43, 44})
		book := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, book)

		// A budget exactly equal to the retained bookkeeping admits no
		// further link batch, however small: admission sees the
		// bookkeeping, not just the staged link bytes.
		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = book
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{nil})
		defer move.Clean(mp)
		err := txn.admitPkMoveLinkBatchLocked(move)
		require.Error(t, err,
			"a budget the bookkeeping alone saturates admits no link batch")
		require.Contains(t, err.Error(), "coordinator retention budget")
	})

	t.Run("statement rollback shrinks the measure exactly", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		writeWave(t, txn, []int64{51, 52, 53, 54})
		m1 := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, m1)

		txn.statementID = 2
		writeWave(t, txn, []int64{55, 56, 57, 58})
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), m1)

		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Equal(t, m1, txn.pkMoveBookkeepBytesLocked(),
			"the rolled-back wave's entries retire with it")

		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Zero(t, txn.pkMoveBookkeepBytesLocked())
		require.Empty(t, txn.pkMoveKeyLineage)
		require.Empty(t, txn.pkMoveMarkerStmts)
	})
}

// TestPkMoveDivertedDeleteBudgetWave closes the r19 divert-side budget
// finding: a DELETE wave diverted after the transaction's final link
// fill kept growing the retirement and dead-pk ledgers with no budget
// check in sight — the fill-end and admission checks only observe
// fill-driven growth. The production divert boundary now enforces the
// same coordinator budget once per critical section, after the whole
// batch is noted, statement-atomically: the error fails the deleting
// statement and its rollback truncates exactly the entries the wave
// recorded, dropping the measure back.
func TestPkMoveDivertedDeleteBudgetWave(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	key := pkMoveAcctTbl{account: 7, table: userTable}

	newDelBatch := func(t *testing.T, pks []int64, ridBase int64) *batch.Batch {
		t.Helper()
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{"rowid", "pk"})
		rids := make([]types.Rowid, len(pks))
		for i := range pks {
			rids[i] = types.BuildTestRowid(9, ridBase+int64(i))
		}
		rowidVec := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidVec, rids, nil, mp))
		bat.Vecs[0] = rowidVec
		pkVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkVec, pks, nil, mp))
		bat.Vecs[1] = pkVec
		bat.SetRowCount(len(pks))
		return bat
	}
	allOffsets := func(n int) []int64 {
		offsets := make([]int64, n)
		for i := range offsets {
			offsets[i] = int64(i)
		}
		return offsets
	}

	t.Run("a retirement-death wave crosses the injected budget and rollback releases", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		// One live lineage binding: every diverted delete of its table
		// records a retirement death. The r23 participation gate charges
		// the wave once durable provenance is at stake, so stage one live
		// link row: then these deaths are exactly the post-fill growth
		// the fill-end checks never see.
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{nil},
			[]uint64{900})
		txn.appendWorkspaceEntryLocked(linkEntry)
		defer txn.releaseWorkspaceEntryBatchLocked(len(txn.writes) - 1)
		pk := pkMoveEncodeInt64(t, 100)
		txn.appendPkMoveKeyLineageLocked(key, string(pk), types.BuildTestRowid(1, 1))
		baseline := txn.pkMoveBookkeepBytesLocked()
		require.Positive(t, baseline)

		// One byte above the baseline: the first retention the wave
		// adds crosses the budget.
		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = baseline + 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		txn.statementID = 2
		del := newDelBatch(t, []int64{100, 101, 102}, 1000)
		defer del.Clean(mp)
		err := txn.notePkMoveDivertedDeletes(userTable, del, allOffsets(3))
		require.Error(t, err)
		require.Contains(t, err.Error(), "coordinator retention budget")
		require.Len(t, txn.pkMoveRetireDeaths[key], 3,
			"the failed wave's evidence outlives the error until rollback")
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), baseline,
			"the wave's growth is measured before the rollback")

		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Equal(t, baseline, txn.pkMoveBookkeepBytesLocked(),
			"the rollback released exactly the wave's retained bytes")
		require.Empty(t, txn.pkMoveRetireDeaths[key],
			"the rolled-back wave's deaths die with its statement")
	})

	t.Run("a dead-pk wave after the final fill crosses the budget and rollback releases", func(t *testing.T) {
		// The final fill already happened: the table owns staged link
		// rows, so the divert boundary gates on pkMoveLinkTables and
		// records into pkMoveDeadPks — the ledger behind the
		// NULL-identity survivors' KEEP evaluation. The r23 participation
		// gate reads the live workspace stage, so this txn stages the
		// link row synthetically just as the fill would have.
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{nil},
			[]uint64{900})
		txn.appendWorkspaceEntryLocked(linkEntry)
		defer txn.releaseWorkspaceEntryBatchLocked(len(txn.writes) - 1)
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}
		require.Zero(t, txn.pkMoveBookkeepBytesLocked())

		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		txn.statementID = 2
		del := newDelBatch(t, []int64{200, 201, 202}, 2000)
		defer del.Clean(mp)
		err := txn.notePkMoveDivertedDeletes(userTable, del, allOffsets(3))
		require.Error(t, err)
		require.Contains(t, err.Error(), "coordinator retention budget")
		require.Len(t, txn.pkMoveDeadPks[key], 3)
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), int64(0))

		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Zero(t, txn.pkMoveBookkeepBytesLocked(),
			"the rollback released the dead-pk wave's every byte")
		require.Empty(t, txn.pkMoveDeadPks)
	})

	t.Run("a zero budget never refuses a diverted wave", func(t *testing.T) {
		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = 0
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		txn := &Transaction{proc: proc}
		txn.statementID = 1
		txn.pkMoveLinkTables = map[pkMoveAcctTbl]struct{}{key: {}}
		del := newDelBatch(t, []int64{300, 301, 302}, 3000)
		defer del.Clean(mp)
		require.NoError(t, txn.notePkMoveDivertedDeletes(userTable, del, allOffsets(3)))
		require.Len(t, txn.pkMoveDeadPks[key], 3,
			"the disabled check records the wave wholesale")
	})
}

func TestPkMoveWorkspaceSwitchBindsDurableOrigin(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	// One transaction: a workspace-route same-key edit (durable row b0 ->
	// staged replacement w1), then an S3-route same-key edit of that row
	// (pre-image w1, logically deleted in place, replacement buffered), then
	// the PK move of the key. The switch must bind the key to the composed
	// durable origin, or the move keeps an uncommitted identity and the
	// branch apply replays DELETE+INSERT.
	stageSwitch := func(t *testing.T, txn *Transaction, w1 types.Rowid) *batch.Batch {
		txn.statementID = 1
		ins := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{2}, nil, []types.Rowid{w1})
		txn.writes = append(txn.writes, ins)
		return ins.bat
	}

	t.Run("workspace edit then S3 edit binds the composed durable origin", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		acct := pkMoveAcctTbl{account: 7, table: userTable}
		b0 := types.BuildTestRowid(1, 1)
		w1 := pkMoveWorkspaceRowid(81)
		// The statement-2 S3 edit lands as a persisted uncommitted
		// object: its rowid is NOT workspace-stage (the live RED's link
		// anchor carries this same ordinary-segment shape).
		w9 := types.BuildTestRowid(5, 9)

		insBat := stageSwitch(t, txn, w1)
		edit1 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&b0})
		defer edit1.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit1))
		require.Equal(t, b0, txn.pkMoveRowidOrigins[w1],
			"the workspace replacement carries the durable origin")

		// Statement 2: the S3-route edit consumes w1; the marker's harvest
		// misses (the row is logically deleted in place), so it is
		// unresolved and must bind the key through the exact origin map.
		txn.statementID = 2
		txn.batchSelectList = map[*batch.Batch][]int64{insBat: {0}}
		txn.batchSelectListGen++
		edit2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&w1})
		defer edit2.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit2))
		stack := txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 2))]
		require.Len(t, stack, 1,
			"the route switch must bind the key to the composed durable origin")
		require.Equal(t, b0, stack[0].origin,
			"the binding names the durable origin, not the workspace generation")
		require.Equal(t, 2, stack[0].stmt)

		// Statement 3: the move names the S3-buffered generation w9, which
		// never enters the committed stream; Pass 1b heals the old side
		// back to b0 through the binding.
		txn.statementID = 3
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&w9})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b0[:], move.Vecs[4].GetBytesAt(0),
			"the move's old side heals to the durable origin")
	})

	t.Run("an independently inserted pre-image stays unbound", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		acct := pkMoveAcctTbl{account: 7, table: userTable}
		w1 := pkMoveWorkspaceRowid(91)

		// The row at key 2 was INSERTED by this transaction: no durable
		// origin exists and none must be invented.
		insBat := stageSwitch(t, txn, w1)
		txn.statementID = 2
		txn.batchSelectList = map[*batch.Batch][]int64{insBat: {0}}
		txn.batchSelectListGen++
		edit2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&w1})
		defer edit2.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit2))
		require.Empty(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 2))],
			"a workspace generation with no recorded origin anchors literally")
	})

	t.Run("a murdered lineage does not resurrect through the switch", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		acct := pkMoveAcctTbl{account: 7, table: userTable}
		b0 := types.BuildTestRowid(1, 1)
		w1 := pkMoveWorkspaceRowid(81)
		wS2 := types.BuildTestRowid(5, 2)
		wAny := types.BuildTestRowid(5, 3)

		insBat := stageSwitch(t, txn, w1)
		edit1 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&b0})
		defer edit1.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit1))
		txn.statementID = 2
		txn.batchSelectList = map[*batch.Batch][]int64{insBat: {0}}
		txn.batchSelectListGen++
		edit2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&w1})
		defer edit2.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit2))
		require.Len(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 2))], 1)

		// Statement 3: the lineage's live generation is deleted outright.
		// The delete diverts (uncommitted row) and NO staged old side
		// explains it, so the binding retires; a re-arrival at the key
		// must not inherit the dead lineage.
		txn.statementID = 3
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{catalog.Row_ID, "pk"})
		delBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(delBat.Vecs[0], wS2, false, mp))
		delBat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(delBat.Vecs[1], int64(2), false, mp))
		delBat.SetRowCount(1)
		defer delBat.Clean(mp)
		txn.notePkMoveDivertedDeleteLocked(userTable, delBat, 0)

		txn.statementID = 4
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&wAny})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, wAny[:], move.Vecs[4].GetBytesAt(0),
			"a retired binding never heals: identities stay literal, fail closed")
		require.Contains(t, txn.pkMoveKilledLinks, string([]byte{0}),
			"the killed link rides the prune's drop list")
	})

	t.Run("statement rollback drops the switch binding", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		acct := pkMoveAcctTbl{account: 7, table: userTable}
		b0 := types.BuildTestRowid(1, 1)
		w1 := pkMoveWorkspaceRowid(81)

		insBat := stageSwitch(t, txn, w1)
		edit1 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&b0})
		defer edit1.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit1))
		txn.statementID = 2
		txn.batchSelectList = map[*batch.Batch][]int64{insBat: {0}}
		txn.batchSelectListGen++
		edit2 := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&w1})
		defer edit2.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, edit2))
		require.Len(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 2))], 1)

		txn.statementID = 1
		txn.rollbackPkMoveStmtStateLocked()
		require.Empty(t, txn.pkMoveKeyLineage[acct][string(pkMoveEncodeInt64(t, 2))],
			"rolling the switch statement back retires its binding")
	})
}

// TestPkMoveWorkspaceToS3RetargetToNullMultiRow extends the route-switch
// regression to a wave: one unresolved-marker batch whose pre-images name
// the staged replacements of MANY staged links must null every matching
// new_row_id cell, retire every heal binding anchored on the consumed
// identities, and ride the statement undo log — while links and bindings
// naming other identities stay byte-identical.
func TestPkMoveWorkspaceToS3RetargetToNullMultiRow(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	txn := &Transaction{proc: proc}
	txn.statementID = 1
	b0 := types.BuildTestRowid(1, 1)
	b1 := types.BuildTestRowid(1, 2)
	b2 := types.BuildTestRowid(1, 3)
	w1 := pkMoveWorkspaceRowid(71)
	w2 := pkMoveWorkspaceRowid(72)
	w3 := pkMoveWorkspaceRowid(73)
	// Row 3 is an earlier NULL-identity (S3-route) link healed onto the
	// same generation the second move staged: heal bindings live only on
	// NULL-identity links, never under a link that still names a
	// workspace replacement.
	linkEntry := pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{userTable, userTable, userTable, userTable},
		[]*types.Rowid{&b0, &b1, &b2, &b2},
		[]*types.Rowid{&w1, &w2, &w3, nil},
		[]uint64{700, 701, 702, 703})
	txn.writes = append(txn.writes, linkEntry)
	txn.pkMoveLinkBirth = map[string]int{
		string([]byte{0}): 1, string([]byte{1}): 1,
		string([]byte{2}): 1, string([]byte{3}): 1,
	}
	// A heal binding anchored on the second move's replacement, and one
	// anchored on an identity no marker consumes.
	txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
		string([]byte{3}): {rid: w2, stmt: 1},
		"other":           {rid: w3, stmt: 1},
	}

	// Statement 2: S3-route edits consume the first two replacements in
	// one wave.
	txn.statementID = 2
	markers := pkMoveMarkerOrMoveBatch(t, mp, userTable,
		[][]byte{pkMoveEncodeInt64(t, 11), pkMoveEncodeInt64(t, 12)},
		[][]byte{pkMoveEncodeInt64(t, 11), pkMoveEncodeInt64(t, 12)},
		[]*types.Rowid{&w1, &w2})
	defer markers.Clean(mp)
	require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, markers))

	require.True(t, linkEntry.bat.Vecs[8].IsNull(0), "row 0 switched to NULL")
	require.True(t, linkEntry.bat.Vecs[8].IsNull(1), "row 1 switched to NULL")
	require.Equal(t, w3[:], linkEntry.bat.Vecs[8].GetBytesAt(2),
		"a link no marker named stays byte-identical")
	require.True(t, linkEntry.bat.Vecs[8].IsNull(3),
		"an already-NULL link keeps its S3 identity")
	require.NotContains(t, txn.pkMoveNullHeals, string([]byte{3}),
		"the heal binding anchored on the consumed identity retired")
	require.Equal(t, pkMoveNullHeal{rid: w3, stmt: 1}, txn.pkMoveNullHeals["other"],
		"a binding anchored on an unconsumed identity survives")
	require.Len(t, txn.pkMoveRetargetUndo, 3,
		"two cell mutations plus one heal removal rode the undo log")

	// Rollback restores every cell and the removed binding.
	txn.statementID = 1
	require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
	require.False(t, linkEntry.bat.Vecs[8].IsNull(0))
	require.Equal(t, w1[:], linkEntry.bat.Vecs[8].GetBytesAt(0))
	require.False(t, linkEntry.bat.Vecs[8].IsNull(1))
	require.Equal(t, w2[:], linkEntry.bat.Vecs[8].GetBytesAt(1))
	require.Equal(t, pkMoveNullHeal{rid: w2, stmt: 1}, txn.pkMoveNullHeals[string([]byte{3})],
		"rollback recreated the retired heal binding")
	require.Equal(t, pkMoveNullHeal{rid: w3, stmt: 1}, txn.pkMoveNullHeals["other"])
}

// BenchmarkPkMoveWorkspaceToS3NullRetarget measures one fill pass whose M
// unresolved markers route-switch a workspace of L staged links. The
// indexed path pays O(L + M); the per-marker full staged-set scans it
// replaced paid O(L * M) while holding txn.Lock.
func BenchmarkPkMoveWorkspaceToS3NullRetarget(b *testing.B) {
	proc := testutil.NewProcess(b)
	mp := proc.Mp()
	must := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}
	encode := func(v int64) []byte {
		packer := types.NewPacker()
		defer packer.Close()
		packer.EncodeInt64(v)
		return append([]byte(nil), packer.Bytes()...)
	}
	linkVecs := func(bat *batch.Batch) {
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[5] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[6] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[7] = vector.NewVec(types.T_varbinary.ToType())
		bat.Vecs[8] = vector.NewVec(types.T_binary.ToType())
		bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[10] = vector.NewVec(types.T_binary.ToType())
	}
	linkAttrs := []string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPKSha,
		catalog.DataBranchUpdateLinkColOldRow,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPKSha,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColNewRow,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
	}
	for _, n := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("links=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				txn := &Transaction{proc: proc}
				txn.statementID = 2

				linkBat := batch.NewWithSize(11)
				linkBat.SetAttributes(linkAttrs)
				linkVecs(linkBat)
				txn.pkMoveLinkBirth = make(map[string]int, n)
				txn.pkMoveNullHeals = make(map[string]pkMoveNullHeal, n)
				ws := make([]types.Rowid, n)
				for r := 0; r < n; r++ {
					id := []byte{byte(r >> 8), byte(r)}
					w := pkMoveWorkspaceRowid(uint32(1000 + r))
					ws[r] = w
					d := types.BuildTestRowid(1, int64(r+1))
					must(vector.AppendFixed(linkBat.Vecs[0], types.BuildTestRowid(9, int64(r+1)), false, mp))
					must(vector.AppendBytes(linkBat.Vecs[1], id, false, mp))
					must(vector.AppendFixed(linkBat.Vecs[2], uint64(42), false, mp))
					must(vector.AppendBytes(linkBat.Vecs[3], id, false, mp))
					must(vector.AppendBytes(linkBat.Vecs[4], d[:], false, mp))
					must(vector.AppendBytes(linkBat.Vecs[5], encode(int64(r+1)), false, mp))
					must(vector.AppendBytes(linkBat.Vecs[6], id, false, mp))
					must(vector.AppendBytes(linkBat.Vecs[7], encode(int64(10000+r)), false, mp))
					must(vector.AppendBytes(linkBat.Vecs[8], w[:], false, mp))
					must(vector.AppendFixed(linkBat.Vecs[9], uint64(700+r), false, mp))
					must(vector.AppendBytes(linkBat.Vecs[10], testDisttaePkMoveTxnID(byte(r%255+1)), false, mp))
					txn.pkMoveLinkBirth[string(id)] = 1
					if r%2 == 0 {
						txn.pkMoveNullHeals[string(id)] = pkMoveNullHeal{rid: w, stmt: 1}
					}
				}
				linkBat.SetRowCount(n)
				txn.writes = append(txn.writes, Entry{
					typ:          INSERT,
					accountId:    7,
					databaseName: catalog.MO_CATALOG,
					tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
					bat:          linkBat,
				})

				markers := batch.NewWithSize(11)
				markers.SetAttributes(linkAttrs)
				linkVecs(markers)
				for r := 0; r < n; r++ {
					id := []byte{byte(r >> 8), byte(r)}
					must(vector.AppendFixed(markers.Vecs[0], types.BuildTestRowid(8, int64(r+1)), false, mp))
					must(vector.AppendBytes(markers.Vecs[1], id, false, mp))
					must(vector.AppendFixed(markers.Vecs[2], uint64(42), false, mp))
					must(vector.AppendBytes(markers.Vecs[3], id, false, mp))
					must(vector.AppendBytes(markers.Vecs[4], ws[r][:], false, mp))
					must(vector.AppendBytes(markers.Vecs[5], encode(int64(10000+r)), false, mp))
					must(vector.AppendBytes(markers.Vecs[6], id, false, mp))
					must(vector.AppendBytes(markers.Vecs[7], encode(int64(10000+r)), false, mp))
					must(vector.AppendBytes(markers.Vecs[8], nil, true, mp))
					must(vector.AppendFixed(markers.Vecs[9], uint64(900+r), false, mp))
					must(vector.AppendBytes(markers.Vecs[10], testDisttaePkMoveTxnID(byte(r%255+1)), false, mp))
				}
				markers.SetRowCount(n)
				b.StartTimer()
				must(txn.fillPkMoveLinkNewRowIDsLocked(7, markers))
				b.StopTimer()
				markers.Clean(mp)
				linkBat.Clean(mp)
			}
		})
	}
}

// TestPkMoveSwallowedDeletesRetireLineage drives the PRODUCTION raw-
// workspace delete boundary (Transaction.deleteBatch -> deleteTableWrites
// -> batchSelectList -> notePkMoveSwallowedDeletes): a delete whose row
// lives in an uncommitted INSERT batch vanishes from the outgoing delete
// batch without tombstone or diverted record. The boundary must record
// the death so a by-key lineage binding cannot outlive the generation it
// names, while same-statement staged departures keep legitimate edit
// chains alive and statement rollback revives exactly its own records.
func TestPkMoveSwallowedDeletesRetireLineage(t *testing.T) {
	const userTable = uint64(42)
	acct := pkMoveAcctTbl{account: 7, table: userTable}
	enc2 := func(t *testing.T) string { return string(pkMoveEncodeInt64(t, 2)) }

	newBoundaryTxn := func(t *testing.T) (*Transaction, *mpool.MPool) {
		t.Helper()
		rt := runtime.ServiceRuntime("")
		previous, existed := rt.GetGlobalVariables(runtime.ColexecServer)
		server := colexec.NewServer("")
		rt.SetGlobalVariables(runtime.ColexecServer, server)
		t.Cleanup(func() {
			if existed {
				rt.SetGlobalVariables(runtime.ColexecServer, previous)
			} else {
				rt.CompareAndDeleteGlobalVariables(runtime.ColexecServer, server)
			}
		})
		proc := testutil.NewProc(t)
		op := newTxnOperatorForTest(t)
		return &Transaction{proc: proc, op: op, engine: &Engine{},
			batchSelectList: make(map[*batch.Batch][]int64),
			deletedBlocks:   &deletedBlocks{offsets: make(map[types.Blockid][]int64)}}, proc.Mp()
	}

	// bindLineage stages an unresolved same-key marker: an S3-route edit
	// whose replacement never staged, binding the key's lineage to the
	// durable origin.
	bindLineage := func(t *testing.T, txn *Transaction, mp *mpool.MPool, origin types.Rowid, pk int64) {
		t.Helper()
		enc := pkMoveEncodeInt64(t, pk)
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{enc}, [][]byte{enc}, []*types.Rowid{&origin})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
	}

	// swallowedDelete drives one [rowid, pk] delete-batch row through the
	// PRODUCTION deleteBatch boundary: the row lives in a staged INSERT
	// entry, so deleteTableWrites excludes it from the workspace batch and
	// the outgoing delete batch shrinks to zero rows.
	swallowedDelete := func(t *testing.T, txn *Transaction, mp *mpool.MPool,
		tableID uint64, rid types.Rowid, pk int64) {
		t.Helper()
		insert := batch.NewWithSize(2)
		insert.SetAttributes([]string{catalog.Row_ID, "pk"})
		insert.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(insert.Vecs[0], rid, false, mp))
		insert.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(insert.Vecs[1], pk, false, mp))
		insert.SetRowCount(1)
		defer insert.Clean(mp)
		txn.writes = append(txn.writes, Entry{typ: INSERT, databaseId: 7,
			tableId: tableID, accountId: 7, bat: insert})

		del := batch.NewWithSize(2)
		del.SetAttributes([]string{catalog.Row_ID, "pk"})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], rid, false, mp))
		del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[1], pk, false, mp))
		del.SetRowCount(1)
		defer del.Clean(mp)
		out, err := txn.deleteBatch(del, 7, tableID)
		require.NoError(t, err)
		require.Zero(t, out.RowCount(),
			"the raw-workspace row vanished from the outgoing delete batch")
		require.Equal(t, []int64{0}, txn.batchSelectList[insert],
			"the workspace INSERT batch lost exactly the deleted row")
	}

	t.Run("a raw workspace death retires the binding it murdered", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		b0 := types.BuildTestRowid(1, 1) // durable origin at pk 2
		w2 := pkMoveWorkspaceRowid(2)    // the workspace generation the delete swallows

		// Statement 1: an S3-route same-key edit binds pk 2's lineage to b0.
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 2)
		org, ok := txn.lookupPkMoveKeyLineageLocked(acct, enc2(t))
		require.True(t, ok)
		require.Equal(t, b0, org)

		// Statement 2: a plain DELETE of the generation at pk 2 swallows the
		// workspace row: no tombstone, no diverted record, no staged DELETE.
		txn.statementID = 2
		swallowedDelete(t, txn, mp, userTable, w2, 2)
		require.NotEmpty(t, txn.pkMoveRetireDeaths[acct][enc2(t)],
			"the swallowed death must reach the retirement ledger")
		// The pk-level death ledger only serves staged links murdered
		// after their birth; this transaction has no surviving link rows
		// when the delete lands (the binding marker was consumed by its
		// own fill), so the participation gate correctly records nothing.
		require.Empty(t, txn.pkMoveDeadPks,
			"no surviving link rows: the pk-level death ledger stays empty")
		_, ok = txn.lookupPkMoveKeyLineageLocked(acct, enc2(t))
		require.False(t, ok, "the murdered lineage no longer governs the key")
		require.True(t, txn.pkMoveBindingRetiredLocked(acct, enc2(t), 1))

		// A statement later moving whatever re-arrived at pk 2 must not heal
		// onto the dead lineage: the fill sees the retired binding and keeps
		// the moved row's identity literal (never b0).
		txn.statementID = 3
		intruder := types.BuildTestRowid(3, 3)
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&intruder})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, intruder[:], move.Vecs[4].GetBytesAt(0),
			"a retired lineage never heals: the recorded identity stays literal")
	})

	t.Run("a swallowed death on a participating table reaches the pk-level death ledger", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		enc9 := string(pkMoveEncodeInt64(t, 9))

		// Statement 1: a resolved move keeps a link row, so the table
		// participates and the pk-level death gate opens.
		txn.statementID = 1
		b2 := types.BuildTestRowid(1, 1)
		b3 := types.BuildTestRowid(2, 2)
		repl := batch.NewWithSize(2)
		repl.SetAttributes([]string{catalog.Row_ID, "pk"})
		repl.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(repl.Vecs[0], b3, false, mp))
		repl.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(repl.Vecs[1], int64(3), false, mp))
		repl.SetRowCount(1)
		defer repl.Clean(mp)
		// pkCheckPos is the harvest's pk-column position: production
		// WriteBatch prepends the rowid and records it, so a resolvable
		// replacement always carries it.
		txn.writes = append(txn.writes, Entry{typ: INSERT, databaseId: 7,
			tableId: userTable, accountId: 7, bat: repl, pkCheckPos: 1})
		move := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 3)},
			[]*types.Rowid{&b2})
		defer move.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, move))
		require.Equal(t, b3[:], move.Vecs[8].GetBytesAt(0),
			"the staged replacement resolved the link's new identity")
		require.Contains(t, txn.pkMoveLinkTables, acct,
			"the resolved move survives, so the table participates")

		// Statement 2: a plain delete swallows the row at key 9. The
		// participating table records the death in the pk-level ledger;
		// with no by-key lineage binding anywhere, the retirement ledger
		// stays empty.
		txn.statementID = 2
		w9 := pkMoveWorkspaceRowid(9)
		swallowedDelete(t, txn, mp, userTable, w9, 9)
		require.Contains(t, txn.pkMoveDeadPks[acct][enc9], 2,
			"the swallowed death reaches the pk-level death ledger")
		require.Empty(t, txn.pkMoveRetireDeaths,
			"no lineage bindings: the retirement ledger stays empty")
	})

	t.Run("a same-statement staged departure explains the swallowed death", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		b0 := types.BuildTestRowid(1, 1)
		w2 := pkMoveWorkspaceRowid(2)

		// Statement 1 binds the lineage.
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 2)

		// Statement 2: a same-key edit's marker names w2 as its old side
		// (the fill observes w2 as a recorded departure), and the edit's own
		// delete swallows that row: the death carries the continuation's
		// provenance, so the lineage survives.
		txn.statementID = 2
		marker := pkMoveMarkerOrMoveBatch(t, mp, userTable,
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[][]byte{pkMoveEncodeInt64(t, 2)},
			[]*types.Rowid{&w2})
		defer marker.Clean(mp)
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(7, marker))
		swallowedDelete(t, txn, mp, userTable, w2, 2)
		org, ok := txn.lookupPkMoveKeyLineageLocked(acct, enc2(t))
		require.True(t, ok, "the explained death keeps the legitimate continuation")
		require.Equal(t, b0, org)
		require.False(t, txn.pkMoveBindingRetiredLocked(acct, enc2(t), 1))
	})

	t.Run("statement rollback revives the murdered binding", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		b0 := types.BuildTestRowid(1, 1)
		w2 := pkMoveWorkspaceRowid(2)

		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 2)
		endStmt1 := len(txn.writes)

		txn.statementID = 2
		swallowedDelete(t, txn, mp, userTable, w2, 2)
		require.True(t, txn.pkMoveBindingRetiredLocked(acct, enc2(t), 1))

		// Roll statement 2 back exactly like RollbackLastStatement does:
		// truncate first, step the id back, then the provenance undo.
		txn.writes = txn.writes[:endStmt1]
		txn.statementID = 1
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		org, ok := txn.lookupPkMoveKeyLineageLocked(acct, enc2(t))
		require.True(t, ok, "the rolled-back delete revives the lineage binding")
		require.Equal(t, b0, org)
		require.Empty(t, txn.pkMoveRetireDeaths[acct][enc2(t)],
			"the statement's retirement records die with it")
		require.Empty(t, txn.pkMoveDeadPks[acct][enc2(t)],
			"the statement's pk-death records die with it")
	})

	t.Run("deletes without provenance record nothing", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		w2 := pkMoveWorkspaceRowid(2)
		txn.statementID = 1
		swallowedDelete(t, txn, mp, userTable, w2, 2)
		require.Empty(t, txn.pkMoveRetireDeaths)
		require.Empty(t, txn.pkMoveDeadPks)
	})

	t.Run("the coordinator budget bounds the swallowed wave atomically", func(t *testing.T) {
		txn, mp := newBoundaryTxn(t)
		b0 := types.BuildTestRowid(1, 1)
		w2 := pkMoveWorkspaceRowid(2)
		txn.statementID = 1
		bindLineage(t, txn, mp, b0, 2)
		// The r23 participation gate charges the wave once the
		// transaction holds durability-bound provenance: stage one live
		// link row so the boundary below is charged exactly as the
		// post-fill production shape is.
		linkEntry := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable},
			[]*types.Rowid{nil},
			[]*types.Rowid{nil},
			[]uint64{900})
		txn.appendWorkspaceEntryLocked(linkEntry)
		defer txn.releaseWorkspaceEntryBatchLocked(len(txn.writes) - 1)

		prevBudget := pkMoveLinkCoordinatorBudget
		pkMoveLinkCoordinatorBudget = 1
		t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prevBudget })

		insert := batch.NewWithSize(2)
		insert.SetAttributes([]string{catalog.Row_ID, "pk"})
		insert.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(insert.Vecs[0], w2, false, mp))
		insert.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(insert.Vecs[1], int64(2), false, mp))
		insert.SetRowCount(1)
		defer insert.Clean(mp)
		txn.writes = append(txn.writes, Entry{typ: INSERT, databaseId: 7,
			tableId: userTable, accountId: 7, bat: insert})
		del := batch.NewWithSize(2)
		del.SetAttributes([]string{catalog.Row_ID, "pk"})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], w2, false, mp))
		del.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[1], int64(2), false, mp))
		del.SetRowCount(1)
		defer del.Clean(mp)
		_, err := txn.deleteBatch(del, 7, userTable)
		require.Error(t, err, "the wave over the budget fails the deleting statement")

		pkMoveLinkCoordinatorBudget = 0
		insert2 := batch.NewWithSize(2)
		insert2.SetAttributes([]string{catalog.Row_ID, "pk"})
		insert2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(insert2.Vecs[0], w2, false, mp))
		insert2.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(insert2.Vecs[1], int64(2), false, mp))
		insert2.SetRowCount(1)
		defer insert2.Clean(mp)
		txn.writes = append(txn.writes, Entry{typ: INSERT, databaseId: 7,
			tableId: userTable, accountId: 7, bat: insert2})
		del2 := batch.NewWithSize(2)
		del2.SetAttributes([]string{catalog.Row_ID, "pk"})
		del2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del2.Vecs[0], w2, false, mp))
		del2.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(del2.Vecs[1], int64(2), false, mp))
		del2.SetRowCount(1)
		defer del2.Clean(mp)
		_, err = txn.deleteBatch(del2, 7, userTable)
		require.NoError(t, err, "a zero budget disables the check")
	})
}

// TestPkMoveRewriteCarriesProvenance drives the r25 review F2 seam through
// the REAL production pieces: a statement-boundary dirty-object rewrite
// (compactDeletionOnObjsLocked -> rewriteObjectByDeletion) re-mints the
// physical identity of every row SURVIVING the rewrite, so bookkeeping
// that named the pre-rewrite identity — a staged link's old-cell, the
// durable-origin ledger, a null-heal binding — would dangle, and the
// later move of the surviving row would replay as DELETE/INSERT. The
// original object is written and rewritten on the UT's shared memory FS
// by the same writer and reader the boundary uses; the shadow/resolve
// pair must join by pk value, and carryPkMoveProvenanceThroughRekeyLocked
// must re-key every structure atomically.
func TestPkMoveRewriteCarriesProvenance(t *testing.T) {
	ctx := context.Background()

	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	proc := txn.proc
	mp := proc.Mp()
	if proc.Ctx == nil {
		proc.Ctx = ctx
	}

	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	require.NotNil(t, tbl)
	// Production table defs carry the trailing rowid column;
	// ensureSeqnumsAndTypesExpectRowid derives [pk] from it.
	tbl.tableDef.Cols = []*pbplan.ColDef{
		{Name: "pk", Typ: pbplan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
		{Name: catalog.Row_ID, Typ: pbplan.Type{Id: int32(types.T_Rowid)}, Seqnum: 1},
	}
	tbl.proc.Store(proc)
	sharedFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	txn.engine.fs = sharedFS
	tbl.ensureSeqnumsAndTypesExpectRowid()

	// writeObj commits one object with the given (already ascending) pks
	// through the same CNS3 writer the boundary's rewrite uses.
	writeObj := func(t *testing.T, pks []int64) []objectio.ObjectStats {
		t.Helper()
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], pks, nil, mp))
		bat.SetRowCount(len(pks))
		w := colexec.NewCNS3DataWriterForService(
			proc.GetService(), mp, sharedFS, tbl.tableDef, -1, false)
		require.NoError(t, w.Write(ctx, bat))
		stats, wErr := w.Sync(ctx)
		require.NoError(t, wErr)
		w.Close()
		bat.Clean(mp)
		return stats
	}

	t.Run("the surviving row's provenance re-keys onto the rewritten identity", func(t *testing.T) {
		origStats := writeObj(t, []int64{1, 2, 3})
		require.Len(t, origStats, 1)
		origBlk := objectio.NewBlockidWithObjectID(
			origStats[0].ObjectName().ObjectId(), 0)
		dA := types.NewRowid(&origBlk, 0) // pk 1 — the statement deletes it
		dB := types.NewRowid(&origBlk, 1) // pk 2 — survives; provenance names it
		dC := types.NewRowid(&origBlk, 2) // pk 3 — survives unnamed

		d0A := types.BuildTestRowid(30, 1)
		d0B := types.BuildTestRowid(30, 2)
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{dA: d0A, dB: d0B}
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{"L1": {rid: dB}}
		// The earlier move staged pre-boundary names the survivor's
		// pre-rewrite identity as its old side (new cell pending fill).
		txn.writes = []Entry{pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{uint64(42)},
			[]*types.Rowid{&dB},
			[]*types.Rowid{nil},
			[]uint64{810})}
		txn.accountWorkspaceEntryLocked(&txn.writes[0])

		relevant := txn.pkMoveRewriteMigrationRelevantLocked()
		require.Contains(t, relevant, dA, "origin keys are relevant")
		require.Contains(t, relevant, dB, "link cells, origin keys and heals are relevant")
		require.Contains(t, relevant, d0A, "origin values are relevant")
		require.Contains(t, relevant, d0B, "origin values are relevant")
		require.NotContains(t, relevant, dC,
			"an unnamed row must not enter the shadow set")

		deletions := map[objectio.Blockid][]int64{origBlk: {0}}
		shadow, err := recordPkMoveRewriteShadow(
			ctx, mp, txn.engine.fs, origStats[0], tbl.tableDef, deletions, relevant)
		require.NoError(t, err)
		require.Len(t, shadow, 1,
			"only the named survivor shadows: the deleted dA is skipped and dC costs one read at most")
		require.Equal(t, dB, shadow[0].ws)

		// The REAL rewrite: survivors re-minted into a fresh object.
		_, newStats, _, _, err := tbl.rewriteObjectByDeletion(ctx, origStats[0], deletions)
		require.NoError(t, err)
		require.Len(t, newStats, 1)

		mapping, err := resolvePkMoveDumpShadow(
			ctx, mp, sharedFS, newStats, tbl.tableDef, shadow)
		require.NoError(t, err)
		require.Len(t, mapping, 1)
		dB2, ok := mapping[dB]
		require.True(t, ok, "the shadow joins the rewritten object by pk value")
		newBlk := objectio.NewBlockidWithObjectID(
			newStats[0].ObjectName().ObjectId(), 0)
		require.Equal(t, types.NewRowid(&newBlk, 0), dB2,
			"pk 2 sorts first among the survivors [2,3] of the rewritten object")

		genBefore := txn.pkMoveHarvestGen
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{dB}, []types.Rowid{dB2}))
		require.Equal(t, genBefore+1, txn.pkMoveHarvestGen,
			"one epoch bump covers the whole wave")

		require.Equal(t, d0A, txn.pkMoveRowidOrigins[dA],
			"the deleted row's bygone lineage stays keyed by its old identity")
		require.Equal(t, d0B, txn.pkMoveRowidOrigins[dB2],
			"the survivor's durable origin follows the rewritten identity")
		_, stale := txn.pkMoveRowidOrigins[dB]
		require.False(t, stale, "no stale origin key survives the carry")
		require.Equal(t, dB2, txn.pkMoveNullHeals["L1"].rid,
			"the null-heal binding follows the rewritten identity")
		bat := txn.writes[0].bat
		require.Equal(t, dB2[:], bat.Vecs[4].GetBytesAt(0),
			"the staged link's old side rides the same re-key (prune layout oldRowIdx=4)")
		require.True(t, bat.Vecs[8].IsNull(0),
			"the pending new cell stays NULL")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})

	t.Run("no provenance keeps the zero-read fast path", func(t *testing.T) {
		bare := &Transaction{proc: proc}
		require.Nil(t, bare.pkMoveRewriteMigrationRelevantLocked(),
			"ordinary transactions keep the nil fast path: zero object reads")
		genBefore := bare.pkMoveHarvestGen
		require.NoError(t, bare.carryPkMoveProvenanceThroughRekeyLocked(nil, nil))
		require.Equal(t, genBefore, bare.pkMoveHarvestGen,
			"an empty wave costs no epoch bump")
	})
}

// TestPkMoveTransferTombstonesCarryProvenance pins the r25 review F3 seam:
// the RC tombstone transfer re-mints a pending delete's row id onto the
// storage generation the commit actually publishes
// (batchTransferToTombstones). The pk-move bookkeeping naming the
// pre-transfer identity — staged link cells, the origin ledger (keys AND
// composed values), null-heal bindings — must ride the SAME re-key in one
// shot, or the committed tombstone and its link diverge and the apply
// side replays the move as DELETE/INSERT, breaking ON UPDATE CASCADE.
func TestPkMoveTransferTombstonesCarryProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)

	t.Run("the transferred identity re-keys tombstone and provenance atomically", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		oldTomb := pkMoveWorkspaceRowid(301)
		newTomb := types.BuildTestRowid(30, 301)
		durable := types.BuildTestRowid(31, 7)
		other := pkMoveWorkspaceRowid(302)
		other2K := pkMoveWorkspaceRowid(303)
		bystander := types.BuildTestRowid(33, 9)

		del := pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{oldTomb})
		link := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable, userTable},
			[]*types.Rowid{&oldTomb, nil},
			[]*types.Rowid{nil, &oldTomb},
			[]uint64{820, 821})
		txn.writes = []Entry{del, link}
		txn.accountWorkspaceEntryLocked(&txn.writes[0])
		txn.accountWorkspaceEntryLocked(&txn.writes[1])
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
			oldTomb: durable,   // the tombstone id as an edited row's current identity
			other:   oldTomb,   // ... and as a composed generation's value
			other2K: bystander, // an untouched lineage
		}
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{"L1": {rid: oldTomb}}

		genBefore := txn.pkMoveHarvestGen
		txn.Lock()
		require.NoError(t, transferTombstoneRowidsAndCarryPkMove(
			txn, txn.writes, []int32{0}, []int32{0}, []types.Rowid{newTomb}))
		txn.Unlock()
		require.Equal(t, genBefore+1, txn.pkMoveHarvestGen,
			"one epoch bump covers the whole transfer wave")

		require.Equal(t, newTomb,
			vector.MustFixedColNoTypeCheck[types.Rowid](txn.writes[0].bat.Vecs[0])[0],
			"the pending tombstone now names the transferred generation")
		bat := txn.writes[1].bat
		require.Equal(t, newTomb[:], bat.Vecs[4].GetBytesAt(0),
			"the link's old_row_id cell rides the transfer (prune layout oldRowIdx=4)")
		require.True(t, bat.Vecs[8].IsNull(0))
		require.True(t, bat.Vecs[4].IsNull(1))
		require.Equal(t, newTomb[:], bat.Vecs[8].GetBytesAt(1),
			"the link's new_row_id cell rides the transfer (prune layout newRowIdx=8)")
		require.Equal(t, durable, txn.pkMoveRowidOrigins[newTomb],
			"the origin key follows the tombstone")
		require.Equal(t, newTomb, txn.pkMoveRowidOrigins[other],
			"a composed origin value follows the tombstone")
		require.Equal(t, bystander, txn.pkMoveRowidOrigins[other2K],
			"an unnamed lineage stays verbatim")
		require.Equal(t, newTomb, txn.pkMoveNullHeals["L1"].rid,
			"the null-heal binding follows the tombstone")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})

	t.Run("an unchanged identity costs no remap and no epoch", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		rid := types.BuildTestRowid(30, 555)
		del := pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{rid})
		txn.writes = []Entry{del}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{rid: types.BuildTestRowid(31, 1)}

		genBefore := txn.pkMoveHarvestGen
		txn.Lock()
		require.NoError(t, transferTombstoneRowidsAndCarryPkMove(
			txn, txn.writes, []int32{0}, []int32{0}, []types.Rowid{rid}))
		txn.Unlock()
		require.Equal(t, genBefore, txn.pkMoveHarvestGen,
			"identity-equal transfers never enter the carry")
		require.Equal(t, types.BuildTestRowid(31, 1), txn.pkMoveRowidOrigins[rid])
	})

	t.Run("an empty target set is a no-op", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.Lock()
		require.NoError(t, transferTombstoneRowidsAndCarryPkMove(
			txn, nil, nil, nil, nil))
		txn.Unlock()
	})
}

// --- r26 review F3 residual (D113): transfer-retired entries never publish ---

// pkMoveFileDeleteEntry builds one file-backed DELETE workspace entry in the
// AllocCNS3ResultBat(true) layout: a single ObjectMeta_ObjectStats varlena
// column holding the marshaled input stats.
func pkMoveFileDeleteEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	dbID, tableID uint64,
	stats ...objectio.ObjectStats,
) Entry {
	t.Helper()
	bat := colexec.AllocCNS3ResultBat(true)
	for i := range stats {
		require.NoError(t, vector.AppendBytes(
			bat.Vecs[0], stats[i].Marshal(), false, mp))
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	name := ""
	if len(stats) != 0 {
		name = stats[0].ObjectName().String()
	}
	return Entry{
		typ:                DELETE,
		accountId:          accountID,
		databaseId:         dbID,
		tableId:            tableID,
		databaseName:       "user_db",
		tableName:          "user_t",
		fileName:           name,
		bat:                bat,
		pkCheckPos:         1,
		pkCheckReady:       true,
		pkMovePkPos:        2,
		autoIncrEpochKnown: true,
	}
}

func pkMoveTestObjectStats(t *testing.T) objectio.ObjectStats {
	t.Helper()
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
	return *stats
}

func stashOf(txn *Transaction, ss objectio.ObjectStats) bool {
	found := false
	want := ss.ObjectName()
	txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(k, v any) bool {
		ks := k.(objectio.ObjectStats)
		if ks.ObjectName().Equal(want) {
			found = true
		}
		return !found
	})
	return found
}

// TestPkMoveTransferRetiresSupersededEntries drives the D113 retirement seam:
// a pass-through object-route transfer published replacements carrying every
// input row, so the superseded source entries must leave the commit
// serialization — fully covered slots release in place (nil-bat convention),
// partially covered ones shrink, and the retired stats land in one statement-
// tagged record. Statement rollback resurrects byte-identical entries whose
// replacement publications the tail cut discarded; the committed outcome
// clears the ledger for close-time GC instead.
func TestPkMoveTransferRetiresSupersededEntries(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const (
		dbID          = uint64(7)
		userTable     = uint64(42)
		otherTable    = uint64(43)
		accountID     = uint32(9)
		retireStmt    = 2
		laterRollback = 3
	)

	statA := pkMoveTestObjectStats(t)
	statB := pkMoveTestObjectStats(t)
	statC := pkMoveTestObjectStats(t)
	statD := pkMoveTestObjectStats(t)

	newTxn := func() *Transaction {
		return &Transaction{
			proc:            proc,
			engine:          &Engine{},
			writes:          make([]Entry, 0, 4),
			offsets:         []int{0, 0},
			batchSelectList: make(map[*batch.Batch][]int64),
			cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
			statementID: retireStmt,
			tnStores:    []DNStore{{}},
		}
	}
	tbl := &txnTable{tableId: userTable, db: &txnDatabase{databaseId: dbID}}

	seed := func(txn *Transaction) {
		txn.writes = append(txn.writes,
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, userTable, statA, statB),
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, userTable, statC),
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, otherTable, statD))
		for i := range txn.writes {
			txn.accountWorkspaceEntryLocked(&txn.writes[i])
		}
		for _, ss := range []objectio.ObjectStats{statA, statB, statC, statD} {
			txn.StashFlushedTombstones(ss)
		}
	}

	retire := func(txn *Transaction) {
		txn.Lock()
		require.NoError(t, txn.retireTransferredTombstoneStatsLocked(tbl,
			[]objectio.ObjectStats{statA, statC}))
		txn.Unlock()
	}

	t.Run("retirement shrinks and releases without touching other tables", func(t *testing.T) {
		txn := newTxn()
		seed(txn)
		retire(txn)

		require.Len(t, txn.writes, 3, "slice indices never shift: statement offsets stay valid")
		e1 := &txn.writes[0]
		require.Equal(t, 1, e1.bat.RowCount(), "the partial slot keeps its live stats only")
		require.Equal(t, statB.Marshal(),
			e1.bat.Vecs[0].GetBytesAt(0))
		require.Nil(t, txn.writes[1].bat,
			"the fully covered slot releases in place — genWriteReqs skips it")
		require.Equal(t, 1, txn.writes[2].bat.RowCount(),
			"other tables' entries are never matched")

		require.False(t, stashOf(txn, statA), "retired stats leave the read-visible stash")
		require.False(t, stashOf(txn, statC))
		require.True(t, stashOf(txn, statB))
		require.True(t, stashOf(txn, statD))

		require.Len(t, txn.retiredTransfers, 1)
		rec := txn.retiredTransfers[0]
		require.Equal(t, retireStmt, rec.statementSeq)
		require.Len(t, rec.entries, 2)
		require.Equal(t, retireStatNames(statA), retireStatNames(rec.entries[0].stats...))
		require.Equal(t, retireStatNames(statC), retireStatNames(rec.entries[1].stats...))
		require.Equal(t, userTable, rec.entries[0].tableId)
		require.True(t, rec.entries[0].pkCheckReady, "publication identity preserved verbatim")
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})

	t.Run("resurrection restores byte-identical entries for rolled statements", func(t *testing.T) {
		txn := newTxn()
		seed(txn)
		retire(txn)

		txn.Lock()
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(laterRollback))
		txn.Unlock()
		require.Len(t, txn.retiredTransfers, 1,
			"retirements of SURVIVING statements keep their replacements published")

		txn.Lock()
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(retireStmt))
		txn.Unlock()
		require.Empty(t, txn.retiredTransfers)
		require.Len(t, txn.writes, 5, "two resurrected entries re-appended at the tail")

		res1, res2 := txn.writes[3], txn.writes[4]
		require.Equal(t, retireStatNames(statA), retireStatNames(readEntryStats(t, res1)...))
		require.Equal(t, retireStatNames(statC), retireStatNames(readEntryStats(t, res2)...))
		require.Equal(t, userTable, res1.tableId)
		require.Equal(t, int(1), res1.pkCheckPos)
		require.True(t, res1.autoIncrEpochKnown)
		require.True(t, stashOf(txn, statA), "resurrected stats re-enter the stash")
		require.True(t, stashOf(txn, statC))
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})

	t.Run("committed close clears the ledger exactly once", func(t *testing.T) {
		txn := newTxn()
		seed(txn)
		retire(txn)

		txn.gcRetiredTransferFiles()
		require.Nil(t, txn.retiredTransfers,
			"close-time gc consumes the ledger — a second close is a no-op")
		txn.gcRetiredTransferFiles()
		require.Len(t, txn.writes, 3)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})
}

// retireStatNames renders object stats as comparable short names.
func retireStatNames(stats ...objectio.ObjectStats) []string {
	names := make([]string, 0, len(stats))
	for i := range stats {
		names = append(names, stats[i].ObjectName().ObjectId().ShortStringEx())
	}
	return names
}

// readEntryStats decodes a file-backed DELETE entry's stats column.
func readEntryStats(t *testing.T, e Entry) []objectio.ObjectStats {
	t.Helper()
	require.NotNil(t, e.bat)
	col, area := vector.MustVarlenaRawData(e.bat.Vecs[0])
	out := make([]objectio.ObjectStats, 0, len(col))
	for _, c := range col {
		out = append(out, objectio.ObjectStats(c.GetByteSlice(area)))
	}
	return out
}

// --- r26 review F1: compaction worker failures must fail the boundary ---

// TestPkMoveCompactionPropagatesShadowFailure drives the REAL
// compactDeletionOnObjsLocked boundary with one dirty flushed object whose
// provenance-named survivors need a shadow, then breaks the shadow/read-back
// step. r26 review F1: the ants workers converted that failure into a panic
// the pool's default recovery swallowed (ants v2.12.0 worker.go:57-66), so
// the boundary carried nothing, reclaimed the original objects and returned
// success — discarding surviving user rows' rewrite while reporting nil.
// The boundary must instead fail the enclosing statement/commit and leave
// the original objects unreclaimed.
//
// r27 review F2/F5 (D115/D118) strengthen this to the failure-atomicity
// contract the review demanded: a failed compaction wave must be a pure
// no-op against everything outside the current statement's tail.
// Specifically the boundary must
//   - keep EVERY scanned INSERT entry's batch registered (releasing the
//     originals only after the complete wave succeeded), so an OLDER
//     statement's spilled replacement is still intact afterwards;
//   - publish NOTHING (no copy/replacement registrations survive), so the
//     ordinary RollbackLastStatement tail-cut restores the pre-boundary
//     state exactly once;
//   - reclaim every replacement object file the workers persisted but
//     could not publish (post-Sync shadow/readback/publication failures),
//     leaving the shared-FS file inventory untouched.
//
// The explicit-transaction subtest frames the staged flush as an older
// statement, drives the injected failure, runs the real
// RollbackLastStatement, and then lifts the injection and re-drives the
// boundary — the repair must succeed on the SAME staged data, proving the
// failed wave destroyed nothing.
func TestPkMoveCompactionPropagatesShadowFailure(t *testing.T) {
	ctx := context.Background()

	colexec.NewServer("")
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	if proc.Ctx == nil {
		proc.Ctx = ctx
	}

	cols := []*pbplan.ColDef{
		{Name: "pk", Typ: pbplan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
		{Name: catalog.Row_ID, Typ: pbplan.Type{Id: int32(types.T_Rowid)}, Seqnum: 1},
	}
	newTxnTbl := func(t *testing.T) (*Transaction, *txnTable) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		txn.proc = proc
		tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
		require.NotNil(t, tbl)
		tbl.tableDef.Cols = cols
		tbl.proc.Store(proc)
		sharedFS, err := colexec.GetSharedFSFromProc(proc)
		require.NoError(t, err)
		txn.engine.fs = sharedFS
		tbl.ensureSeqnumsAndTypesExpectRowid()
		return txn, tbl
	}
	sharedFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	writeObj := func(t *testing.T, tbl *txnTable, pks []int64) []objectio.ObjectStats {
		t.Helper()
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], pks, nil, mp))
		bat.SetRowCount(len(pks))
		w := colexec.NewCNS3DataWriterForService(
			proc.GetService(), mp, sharedFS, tbl.tableDef, -1, false)
		require.NoError(t, w.Write(ctx, bat))
		stats, wErr := w.Sync(ctx)
		require.NoError(t, wErr)
		w.Close()
		bat.Clean(mp)
		return stats
	}

	// stageDirtyWave registers one flushed INSERT entry carrying origStats
	// as a dirty object (row 0 deleted), names the row-1 survivor in the
	// origin ledger, and severs the table's pk definition: the CNS3 writer
	// still writes (a missing pk name only loses the sort key), but the
	// provenance shadow hard-errors "pk column missing" inside the worker.
	stageDirtyWave := func(t *testing.T, txn *Transaction, tbl *txnTable,
		origStats []objectio.ObjectStats) (types.Blockid, func()) {
		t.Helper()
		origBlk := objectio.NewBlockidWithObjectID(
			origStats[0].ObjectName().ObjectId(), 0)
		dB := types.NewRowid(&origBlk, 1) // row 1 survives; provenance names it
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
			dB: types.BuildTestRowid(30, 2),
		}

		// The flushed INSERT entry in the exact BlockMeta layout the
		// boundary scans (txn.go:2693-2755).
		blkBat := colexec.AllocCNS3ResultBat(false)
		blkInfoBuf := new(bytes.Buffer)
		_, infoErr := (&objectio.BlockInfo{BlockID: origBlk}).MarshalWithBuf(blkInfoBuf)
		require.NoError(t, infoErr)
		require.NoError(t, vector.AppendBytes(blkBat.Vecs[0], blkInfoBuf.Bytes(), false, mp))
		require.NoError(t, vector.AppendBytes(
			blkBat.Vecs[1], origStats[0].Marshal(), false, mp))
		blkBat.SetRowCount(1)
		txn.writes = append(txn.writes, Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   7,
			tableId:      42,
			databaseName: "db",
			tableName:    "tbl",
			bat:          blkBat,
		})
		txn.accountWorkspaceEntryLocked(&txn.writes[len(txn.writes)-1])

		objId := *origStats[0].ObjectName().ObjectId()
		txn.cnObjsSummary = map[types.Objectid]Summary{
			objId: {
				objBat:     blkBat,
				accountId:  1,
				databaseId: 7,
				tableId:    42,
				tbName:     "tbl",
				dbName:     "db",
			},
		}
		txn.deletedBlocks = &deletedBlocks{
			offsets: map[types.Blockid][]int64{origBlk: {0}},
		}

		savedName := tbl.tableDef.Pkey.PkeyColName
		tbl.tableDef.Pkey.PkeyColName = "no_such_column"
		restore := func() { tbl.tableDef.Pkey.PkeyColName = savedName }
		t.Cleanup(restore)

		require.NotNil(t, txn.pkMoveRewriteMigrationRelevantLocked(),
			"the provenance-named survivor must make this wave provenance-relevant")
		return origBlk, restore
	}

	// listSharedFiles walks the whole shared FS and returns every file
	// path it holds — the inventory oracle for the r27 F5 orphan check.
	listSharedFiles := func(t *testing.T) map[string]struct{} {
		t.Helper()
		names := make(map[string]struct{})
		var walk func(dir string)
		walk = func(dir string) {
			for entry, lerr := range sharedFS.List(ctx, dir) {
				require.NoError(t, lerr)
				name := entry.Name
				if dir != "" {
					name = dir + "/" + entry.Name
				}
				if entry.IsDir {
					walk(name)
					continue
				}
				names[name] = struct{}{}
			}
		}
		walk("")
		return names
	}

	t.Run("a shadow failure inside the rewrite worker fails the boundary", func(t *testing.T) {
		txn, tbl := newTxnTbl(t)
		origStats := writeObj(t, tbl, []int64{1, 2, 3})
		require.Len(t, origStats, 1)
		_, _ = stageDirtyWave(t, txn, tbl, origStats)
		before := listSharedFiles(t)

		txn.Lock()
		err := txn.compactDeletionOnObjsLocked(ctx)
		txn.Unlock()
		require.Error(t, err,
			"a failed provenance read must fail the enclosing statement, not report success")
		require.Contains(t, err.Error(), "pk-move rewrite shadow",
			"the propagated failure must name its provenance origin, got %v", err)

		// r27 review F2: the failure must not have destructively modified
		// the staged data — the scanned INSERT entry keeps its batch and
		// nothing was published beside it.
		require.Len(t, txn.writes, 1,
			"a failed wave must not publish replacement registrations")
		// r27 review F5: no persisted replacement may be left orphaned in
		// the shared FS, and the original object must not be reclaimed.
		// (Checked first: on the unfixed boundary the worker's panic path
		// leaves exactly that orphan behind.)
		require.Equal(t, before, listSharedFiles(t),
			"a failed wave must leave the shared-FS inventory untouched (r27 F5)")
		require.NotNil(t, txn.writes[0].bat,
			"the original INSERT batch must survive a failed wave (r27 F2)")
	})

	t.Run("the failure propagates out of the workspace merge boundary", func(t *testing.T) {
		txn, tbl := newTxnTbl(t)
		origStats := writeObj(t, tbl, []int64{10, 20})
		require.Len(t, origStats, 1)
		_, _ = stageDirtyWave(t, txn, tbl, origStats)

		txn.Lock()
		err := txn.mergeTxnWorkspaceLocked(ctx)
		txn.Unlock()
		require.Error(t, err,
			"the statement-boundary merge must die with the provenance failure (txn.go:2323)")
	})

	t.Run("an older statement's spill survives the failed boundary through rollback and retry", func(t *testing.T) {
		txn, tbl := newTxnTbl(t)
		// The retry half reaches the registration path.
		txn.tnStores = []DNStore{{}}
		origStats := writeObj(t, tbl, []int64{1, 2, 3})
		require.Len(t, origStats, 1)
		_, restore := stageDirtyWave(t, txn, tbl, origStats)
		before := listSharedFiles(t)

		// Frame the staged flush as statement 0 — IncrStatementID's tail
		// would record exactly this offset pair.
		txn.offsets = append(txn.offsets, len(txn.writes))
		txn.statementID = 1

		txn.Lock()
		err := txn.compactDeletionOnObjsLocked(ctx)
		txn.Unlock()
		require.Error(t, err)
		require.Contains(t, err.Error(), "pk-move rewrite shadow")

		// The older statement's data must be byte-stable after the failed
		// maintenance boundary (r27 F2).
		require.Len(t, txn.writes, 1)
		require.NotNil(t, txn.writes[0].bat,
			"an older statement's INSERT batch must survive a failed boundary")
		require.Equal(t, before, listSharedFiles(t),
			"the failed boundary must leave no orphaned replacement file (r27 F5)")

		// The service reacts to the boundary error with a statement
		// rollback (default mo_rollback_txn_on_error=0). The real
		// RollbackLastStatement must cut nothing and restore nothing — the
		// failed boundary published nothing — and the older statement must
		// still be intact afterwards.
		mockOp, ok := txn.op.(*mock_frontend.MockTxnOperator)
		require.True(t, ok, "the harness operator is the gomock operator")
		mockOp.EXPECT().EnterRollbackStmt().AnyTimes()
		mockOp.EXPECT().ExitRollbackStmt().AnyTimes()
		txn.tableCache = new(sync.Map)
		require.NoError(t, txn.RollbackLastStatement(ctx))
		require.Len(t, txn.writes, 1)
		require.NotNil(t, txn.writes[0].bat,
			"statement rollback must not destroy the older statement's data")
		require.Equal(t, before, listSharedFiles(t))

		// Lift the injection and re-drive the boundary on the SAME staged
		// data: the repair must now succeed, proving the failed wave lost
		// nothing. The deletion marks are NOT re-armed (r29 review F2,
		// D124): the failed wave never consumed them and the statement
		// rollback dropped none of them, so the retry consumes exactly the
		// staged ledger. The successful direct call stands in for a whole
		// boundary, so its staged finalization is applied the way
		// IncrStatementID's adoption applies it (r29 review F1, D123).
		restore()
		txn.Lock()
		err = txn.compactDeletionOnObjsLocked(ctx)
		txn.applyPendingBoundaryFinalizationLocked()
		txn.Unlock()
		require.NoError(t, err,
			"the untouched staged data must compact once the provenance read is healthy")
		require.Len(t, txn.writes, 2,
			"the retried wave publishes exactly the replacement registration")
		require.Nil(t, txn.writes[0].bat,
			"a fully successful wave releases the original batch (nil-slot lifecycle)")
		rep := txn.writes[1]
		require.Equal(t, INSERT, rep.typ)
		require.NotNil(t, rep.bat)
		require.NotEmpty(t, rep.fileName)
		after := listSharedFiles(t)
		delete(after, singleNewName(t, before, after))
		require.Equal(t, before, after,
			"the successful retry adds exactly the published replacement file")
	})
}

// singleNewName returns the single name present in after but not in before,
// failing the test unless exactly one such name exists.
func singleNewName(t *testing.T, before, after map[string]struct{}) string {
	t.Helper()
	var extra []string
	for name := range after {
		if _, ok := before[name]; !ok {
			extra = append(extra, name)
		}
	}
	require.Len(t, extra, 1, "exactly one new file must appear, got %v", extra)
	return extra[0]
}

// --- r26 review F4: re-key durable origins retained in lineage bindings ---

// TestPkMoveRemapCarriesLineageBindings pins r26 review F4: the shared
// identity migration (remapPkMoveLinkRowIDsLocked) re-keys the origin
// ledger, the null heals and the staged link cells, but left the
// pkMoveKeyLineage binding origins stale. A lineage bound to a workspace
// generation that spilled to S3 heals a later move onto that durable
// identity; when the RC transfer re-mints exactly that identity, the heal
// must follow, or the resulting link names an identity absent from the
// committed tombstones and the apply replays DELETE+INSERT.
func TestPkMoveRemapCarriesLineageBindings(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("the transfer re-keys binding origins, preserving statement tags", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		oldRid := types.BuildTestRowid(30, 300) // the bound durable origin the transfer re-mints
		newRid := types.BuildTestRowid(31, 301) // ... onto this published identity
		keep := types.BuildTestRowid(32, 302)   // an untouched binding
		acctTbl := pkMoveAcctTbl{account: 1, table: 42}
		txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
			acctTbl: {
				"K": {
					{origin: keep, wm: 1, stmt: 1},
					{origin: oldRid, wm: 3, stmt: 2},
				},
			},
		}

		txn.Lock()
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{oldRid}, []types.Rowid{newRid}))
		txn.Unlock()

		stack := txn.pkMoveKeyLineage[acctTbl]["K"]
		require.Len(t, stack, 2, "a mapped binding must not be dropped")
		require.Equal(t, keep, stack[0].origin, "the unrelated binding stays verbatim")
		require.Equal(t, 1, stack[0].wm)
		require.Equal(t, 1, stack[0].stmt)
		require.Equal(t, newRid, stack[1].origin,
			"the bound durable origin rides the same re-key as heals and link cells")
		require.Equal(t, 3, stack[1].wm, "the writes watermark survives for statement rollback")
		require.Equal(t, 2, stack[1].stmt, "the statement tag survives for the rollback tail-cut")

		origin, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, "K")
		require.True(t, ok)
		require.Equal(t, newRid, origin,
			"a later move must heal onto the transferred identity, not the stale one")
	})

	t.Run("a binding whose origin the rewrite retired dies with it", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		replaced := types.BuildTestRowid(30, 310)
		fresh := types.BuildTestRowid(31, 311)
		victim := types.BuildTestRowid(30, 312) // the shrink removed this survivor's neighbor
		acctTbl := pkMoveAcctTbl{account: 1, table: 42}
		txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
			acctTbl: {
				"K": {
					{origin: victim, wm: 1, stmt: 1},
					{origin: replaced, wm: 3, stmt: 2},
				},
			},
		}

		txn.Lock()
		require.NoError(t, txn.remapPkMoveLinkRowIDsLocked(
			[]types.Rowid{replaced}, []types.Rowid{fresh},
			map[types.Rowid]struct{}{victim: {}}))
		txn.Unlock()

		stack := txn.pkMoveKeyLineage[acctTbl]["K"]
		require.Len(t, stack, 1,
			"a dead origin's rowid value is re-minted for a survivor — the stale binding must die")
		require.Equal(t, fresh, stack[0].origin)
	})
}

// --- r26 review F5: bound continuity bookkeeping for ordinary updates ---

// TestPkMoveContinuityRetiresSupersededOrigins pins r26 review F5: every
// ordinary same-key update emits one continuity marker, and consuming it
// propagated origins[preImage] to origins[fresh] without RETIRING the
// superseded generation — so a bulk of repeated updates retained one
// origin map entry per UPDATE forever (the budget bypass at the fill end
// explicitly tolerates marker-only waves because nothing durable depends
// on them yet). Retention must stay O(live rows): each propagation's
// pre-image is dead the moment staged links and heals re-point, which the
// fill already does before the origin handoff, so the entry can be
// retired statement-rollback-safely (journaled like any origin write).
func TestPkMoveContinuityRetiresSupersededOrigins(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const tableID = uint64(42)
	const accountID = uint32(7)

	advanceStatement := func(txn *Transaction) {
		// Mirror types.go's IncrStatementID provenance block: the finished
		// statement's undo records sit beyond rollback reach, and its
		// marker-only continuity ledger entries retire (D112).
		txn.pkMoveRetargetUndo = nil
		txn.pkMoveOriginUndo = nil
		txn.pkMoveRetargetUndoSeen = nil
		txn.pkMoveOriginUndoSeen = nil
		txn.retirePkMoveFinishedMarkerStmtsLocked(txn.statementID)
		txn.statementID++
	}

	t.Run("repeated same-key updates keep one origin per live key", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 1) // the committed generation
		preImage := durable
		baseBytes := int64(0)
		for wave := 0; wave < 3; wave++ {
			fresh := types.BuildTestRowid(40, int64(100+wave))
			entry := pkMoveStagedInsertEntry(t, mp, accountID, tableID,
				[]int64{3}, nil, []types.Rowid{fresh})
			txn.writes = append(txn.writes, entry)
			require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID,
				pkMoveMarkerBatch(t, mp, tableID, 3, preImage)))
			require.Len(t, txn.pkMoveRowidOrigins, 1,
				"wave %d: only the live generation may retain an origin entry", wave+1)
			require.Equal(t, durable, txn.pkMoveRowidOrigins[fresh],
				"wave %d: the live generation keeps the durable origin", wave+1)
			preImage = fresh
			advanceStatement(txn)
			if wave == 0 {
				baseBytes = txn.pkMoveBookkeepBytesLocked()
			} else {
				require.Equal(t, baseBytes, txn.pkMoveBookkeepBytesLocked(),
					"wave %d: retained bookkeeping must stay flat across updates of the same key", wave+1)
			}
		}
	})

	t.Run("statement rollback restores the retired generation", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 5)
		preImage := durable
		var rid1, rid2 types.Rowid
		for wave := 0; wave < 2; wave++ {
			fresh := types.BuildTestRowid(40, int64(200+wave))
			entry := pkMoveStagedInsertEntry(t, mp, accountID, tableID,
				[]int64{3}, nil, []types.Rowid{fresh})
			txn.writes = append(txn.writes, entry)
			require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID,
				pkMoveMarkerBatch(t, mp, tableID, 3, preImage)))
			if wave == 0 {
				rid1 = fresh
			} else {
				rid2 = fresh
			}
			preImage = fresh
			if wave == 0 {
				advanceStatement(txn)
			}
		}
		require.Equal(t, map[types.Rowid]types.Rowid{rid2: durable},
			txn.pkMoveRowidOrigins, "precondition: the superseded head is retired")

		// Roll back statement 1 (the second wave): the statement's own
		// origin write dies and the retired generation returns.
		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		require.Equal(t, map[types.Rowid]types.Rowid{rid1: durable},
			txn.pkMoveRowidOrigins,
			"a rolled-back update revives exactly the generation its rows carry")
	})

	t.Run("a genuine move after many edits still anchors to the durable origin", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 9)
		preImage := durable
		var head types.Rowid
		for wave := 0; wave < 3; wave++ {
			fresh := types.BuildTestRowid(40, int64(300+wave))
			entry := pkMoveStagedInsertEntry(t, mp, accountID, tableID,
				[]int64{3}, nil, []types.Rowid{fresh})
			txn.writes = append(txn.writes, entry)
			require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID,
				pkMoveMarkerBatch(t, mp, tableID, 3, preImage)))
			head = fresh
			preImage = fresh
			advanceStatement(txn)
		}

		move := pkMoveMarkerOrMoveBatch(t, mp, tableID,
			[][]byte{pkMoveEncodeInt64(t, 3)}, [][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&head})
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID, move))
		bat := move
		require.Equal(t, durable[:], bat.Vecs[4].GetBytesAt(0),
			"the move's old side anchors to the durable origin, not a retired ghost (oldRowIdx=4)")
	})
}

// --- r26 review F2: harvest composite-key identities ---

// TestPkMoveHarvestCompositeKeyEntries pins r26 review F2 / D109 through
// the PRODUCTION WriteBatch path: a composite-keyed table's batches
// materialize the composed __mo_cpkey column, and the provenance readers
// must harvest it even though duplicate checks deliberately stay blind to
// composite batches. Before D109 the harvest resolved the pk vector only
// through pkCheckPos, which txn.go assigns -1 to composite keys BEFORE
// scanning attrs — so an edit-then-composite-move transaction lost the
// first replacement's workspace-to-durable origin and the later move
// replayed DELETE+INSERT with a foreign-key violation (live RED banked at
// /tmp/e20_red_live_f2.log).
func TestPkMoveHarvestCompositeKeyEntries(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	ctx := context.Background()
	const accountId = uint32(1)
	const tableId = uint64(42)

	newCompositeTxn := func(t *testing.T) *Transaction {
		return newTransactionWithActivePKTableForTest(t, catalog.CPrimaryKeyColName)
	}
	newCpBatch := func(t *testing.T, pks [][]byte) *batch.Batch {
		t.Helper()
		bat := batch.NewWithSize(1)
		bat.SetAttributes([]string{catalog.CPrimaryKeyColName})
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		for _, pk := range pks {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], pk, false, mp))
		}
		bat.SetRowCount(len(pks))
		return bat
	}

	t.Run("composite inserts resolve the provenance pk vector, not the check one", func(t *testing.T) {
		txn := newCompositeTxn(t)
		bat := newCpBatch(t, [][]byte{[]byte("c1"), []byte("c2")})
		rids, err := txn.WriteBatch(INSERT, "", accountId, 7, tableId, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.NotNil(t, rids)
		// The returned rowid vector IS the staged batch's vec 0: freeing it
		// here would empty the workspace entry under the harvest.

		require.Len(t, txn.writes, 1)
		we := &txn.writes[0]
		require.Equal(t, -1, we.pkCheckPos,
			"duplicate checks must keep rejecting composite batches (pre-r26 semantics)")
		require.Equal(t, 1, we.pkMovePkPos,
			"the rowid prepend shifts the composed pk column from attr 0 to vec 1")

		harvested, ok := pkMoveFoldHarvestEntries(txn.writes, accountId, tableId, nil, nil)
		require.True(t, ok)
		require.Len(t, harvested, 2)
		packer := types.NewPacker()
		defer packer.Close()
		for i, cp := range [][]byte{[]byte("c1"), []byte("c2")} {
			packer.Reset()
			packer.EncodeStringType(cp)
			rid, hit := harvested[string(packer.Bytes())]
			require.True(t, hit, "composite pk %q must be harvestable", cp)
			require.Equal(t,
				vector.GetFixedAtNoTypeCheck[types.Rowid](we.bat.Vecs[0], i), rid)
		}
	})

	t.Run("the duplicate-check resolver keeps its exact pre-r26 answers", func(t *testing.T) {
		txn := newCompositeTxn(t)
		pos, ready, err := txn.resolvePKCheckPosForWrite(
			ctx, INSERT, accountId, "db", "tbl", tableId,
			newCpBatch(t, [][]byte{[]byte("c1")}))
		require.NoError(t, err)
		require.True(t, ready)
		require.Equal(t, -1, pos, "composite resolution stays rejected for duplicate checks")

		del := batch.NewWithSize(2)
		del.SetAttributes([]string{objectio.PhysicalAddr_Attr, catalog.CPrimaryKeyColName})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], types.BuildTestRowid(50, 1), false, mp))
		del.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(del.Vecs[1], []byte("c1"), false, mp))
		del.SetRowCount(1)
		pos, ready, err = txn.resolvePKCheckPosForWrite(
			ctx, DELETE, accountId, "db", "tbl", tableId, del)
		require.NoError(t, err)
		require.True(t, ready)
		require.Equal(t, -1, pos, "composite deletes stay check-blind")
		del.Clean(mp)
	})

	t.Run("composite deletes fold through the provenance position", func(t *testing.T) {
		txn := newCompositeTxn(t)
		del := batch.NewWithSize(2)
		del.SetAttributes([]string{objectio.PhysicalAddr_Attr, catalog.CPrimaryKeyColName})
		del.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixed(del.Vecs[0], types.BuildTestRowid(51, 1), false, mp))
		del.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(del.Vecs[1], []byte("c9"), false, mp))
		del.SetRowCount(1)
		_, err := txn.WriteBatch(DELETE, "", accountId, 7, tableId, "db", "tbl", del, DNStore{})
		require.NoError(t, err)

		require.Len(t, txn.writes, 1)
		we := &txn.writes[0]
		require.Equal(t, -1, we.pkCheckPos)
		require.Equal(t, 1, we.pkMovePkPos,
			"the delete layout [rowid, pk] resolves the composed pk at vec 1")

		folded, ok := pkMoveFoldDeleteEntries(txn.writes, accountId, tableId, 0, nil)
		require.True(t, ok)
		require.Len(t, folded, 1)
		packer := types.NewPacker()
		defer packer.Close()
		packer.Reset()
		packer.EncodeStringType([]byte("c9"))
		idx, hit := folded[string(packer.Bytes())]
		require.True(t, hit, "the composite delete must fold by its composed pk")
		require.Equal(t, 0, idx)
	})

	t.Run("the dump shadow reads composite batches by their provenance position", func(t *testing.T) {
		txn := newCompositeTxn(t)
		bat := newCpBatch(t, [][]byte{[]byte("c1"), []byte("c2")})
		rids, err := txn.WriteBatch(INSERT, "", accountId, 7, tableId, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.NotNil(t, rids)
		// The returned rowid vector IS the staged batch's vec 0 (owned by
		// the workspace entry): reading it here, never freeing it twice.

		we := &txn.writes[0]
		head := vector.GetFixedAtNoTypeCheck[types.Rowid](we.bat.Vecs[0], 0)
		durable := types.BuildTestRowid(31, 7)
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{head: durable}

		key := workspaceTableKey{tableKey: genTableKey(accountId, "tbl", 7, "db")}
		txn.Lock()
		shadows, shErr := txn.recordPkMoveDumpShadowLocked(nil, key, we)
		txn.Unlock()
		require.NoError(t, shErr,
			"a composite batch with the provenance position must shadow instead of hard-erroring")
		require.Len(t, shadows[key], 1)
		require.Equal(t, head, shadows[key][0].ws)

		packer := types.NewPacker()
		defer packer.Close()
		packer.Reset()
		packer.EncodeStringType([]byte("c1"))
		require.Equal(t, string(packer.Bytes()), shadows[key][0].pk,
			"the shadow records the composed pk, never a payload column")
	})
}

// --- r26 review F3: provenance through persisted tombstone transfers ---

// TestPkMoveTransferRekeyPairs pins the r26 review F3 / D110 mapping
// helper: doTransferRowids emits the transferred batch in object READ
// order with freshly minted row ids, so the old->new mapping must join by
// pk VALUE, carry only genuinely changed identities, and fail closed on
// any disagreement between the staged intents and the read-back object.
// Before D110 the staged batch was orphaned without carrying the mapping
// (live RED banked at /tmp/e20_red_live_f34.log).
func TestPkMoveTransferRekeyPairs(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	newBatch := func(t *testing.T, ids []types.Rowid, pks []int64) *batch.Batch {
		t.Helper()
		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], ids, nil, mp))
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[1], pks, nil, mp))
		bat.SetRowCount(len(ids))
		return bat
	}

	t.Run("the read-order result joins back by pk value", func(t *testing.T) {
		oldA := types.BuildTestRowid(30, 1)
		oldB := types.BuildTestRowid(30, 2)
		oldC := types.BuildTestRowid(30, 3)
		newA := types.BuildTestRowid(40, 1)
		newB := types.BuildTestRowid(40, 2)
		newC := types.BuildTestRowid(40, 3)
		staged := newBatch(t,
			[]types.Rowid{oldA, oldB, oldC}, []int64{10, 20, 30})
		defer staged.Clean(mp)

		result := newBatch(t,
			[]types.Rowid{newC, newA, newB}, []int64{30, 10, 20})
		defer result.Clean(mp)

		oldIds, newIds, err := pkMoveTransferRekeyPairs(staged, result)
		require.NoError(t, err)
		require.Len(t, oldIds, 3)
		got := make(map[types.Rowid]types.Rowid, 3)
		for i := range oldIds {
			got[oldIds[i]] = newIds[i]
		}
		require.Equal(t, map[types.Rowid]types.Rowid{
			oldA: newA, oldB: newB, oldC: newC,
		}, got, "every old identity maps to the new identity of ITS pk, not its position")

		t.Run("the pairs carry provenance through the shared remap", func(t *testing.T) {
			txn := &Transaction{proc: proc}
			durable := types.BuildTestRowid(60, 9)
			txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{oldA: durable}
			txn.Lock()
			require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(oldIds, newIds))
			txn.Unlock()
			require.Equal(t, durable, txn.pkMoveRowidOrigins[newA],
				"the transferred identity keeps its durable origin under the new rowid")
			_, stale := txn.pkMoveRowidOrigins[oldA]
			require.False(t, stale, "the old transferred identity retires")
		})
	})

	t.Run("unchanged identities pair nothing", func(t *testing.T) {
		idA := types.BuildTestRowid(30, 11)
		staged := newBatch(t, []types.Rowid{idA}, []int64{10})
		defer staged.Clean(mp)
		result := newBatch(t, []types.Rowid{idA}, []int64{10})
		defer result.Clean(mp)

		oldIds, newIds, err := pkMoveTransferRekeyPairs(staged, result)
		require.NoError(t, err)
		require.Empty(t, oldIds)
		require.Empty(t, newIds)
	})

	t.Run("a row-count disagreement fails the transfer", func(t *testing.T) {
		staged := newBatch(t,
			[]types.Rowid{types.BuildTestRowid(30, 21), types.BuildTestRowid(30, 22)},
			[]int64{10, 20})
		defer staged.Clean(mp)
		result := newBatch(t, []types.Rowid{types.BuildTestRowid(40, 21)}, []int64{10})
		defer result.Clean(mp)

		_, _, err := pkMoveTransferRekeyPairs(staged, result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read back as")
	})

	t.Run("a staged pk missing from the object fails closed", func(t *testing.T) {
		staged := newBatch(t,
			[]types.Rowid{types.BuildTestRowid(30, 31), types.BuildTestRowid(30, 32)},
			[]int64{10, 20})
		defer staged.Clean(mp)
		result := newBatch(t,
			[]types.Rowid{types.BuildTestRowid(40, 31), types.BuildTestRowid(40, 32)},
			[]int64{10, 99})
		defer result.Clean(mp)

		_, _, err := pkMoveTransferRekeyPairs(staged, result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing from the read-back object")
	})

	t.Run("an ambiguous read-back pk fails closed", func(t *testing.T) {
		staged := newBatch(t,
			[]types.Rowid{types.BuildTestRowid(30, 41), types.BuildTestRowid(30, 42)},
			[]int64{10, 20})
		defer staged.Clean(mp)
		result := newBatch(t,
			[]types.Rowid{types.BuildTestRowid(40, 41), types.BuildTestRowid(40, 42)},
			[]int64{10, 10})
		defer result.Clean(mp)

		_, _, err := pkMoveTransferRekeyPairs(staged, result)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ambiguous")
	})
}

// --- r27 review F3 (D116): partial retirement records only completed
// detachments ---

// TestPkMoveTransferRetirementShrinkFailureIsAtomic pins the r27 review F3
// contract: a partial retirement must remove stats from the read-visible
// stash and journal them into the rollback ledger ONLY after the retained
// batch actually exists. Recording first and allocating second left a
// mid-shrink failure with the original entry still holding the retired
// stats AND the ledger naming them, so the statement rollback resurrected
// them beside the intact original — two workspace publications of one
// tombstone identity.
func TestPkMoveTransferRetirementShrinkFailureIsAtomic(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const (
		dbID       = uint64(7)
		userTable  = uint64(42)
		accountID  = uint32(9)
		retireStmt = 2
	)

	statA := pkMoveTestObjectStats(t)
	statB := pkMoveTestObjectStats(t)
	statC := pkMoveTestObjectStats(t)

	newTxn := func() *Transaction {
		return &Transaction{
			proc:            proc,
			engine:          &Engine{},
			writes:          make([]Entry, 0, 4),
			offsets:         []int{0, 0},
			batchSelectList: make(map[*batch.Batch][]int64),
			cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
			statementID: retireStmt,
			tnStores:    []DNStore{{}},
		}
	}
	tbl := &txnTable{tableId: userTable, db: &txnDatabase{databaseId: dbID}}

	countStat := func(txn *Transaction, want objectio.ObjectStats) int {
		n := 0
		for i := range txn.writes {
			if txn.writes[i].bat == nil {
				continue
			}
			for _, ss := range readEntryStats(t, txn.writes[i]) {
				if ss.ObjectName().Equal(want.ObjectName()) {
					n++
				}
			}
		}
		return n
	}

	t.Run("failure during the first shrink mutates nothing and rollback adds nothing", func(t *testing.T) {
		txn := newTxn()
		txn.writes = append(txn.writes,
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, userTable, statA, statB))
		txn.accountWorkspaceEntryLocked(&txn.writes[0])
		txn.StashFlushedTombstones(statA)
		txn.StashFlushedTombstones(statB)

		pkMoveSwapVecSeams(t,
			func(*vector.Vector, []byte, bool, *mpool.MPool) error { return errPkMoveInjected },
			vector.SetBytesAt)

		txn.Lock()
		err := txn.retireTransferredTombstoneStatsLocked(tbl, []objectio.ObjectStats{statA})
		txn.Unlock()
		require.ErrorIs(t, err, errPkMoveInjected)

		e := &txn.writes[0]
		require.Equal(t, 2, e.bat.RowCount(),
			"the failed shrink never touched the original slot")
		require.Equal(t, retireStatNames(statA, statB),
			retireStatNames(readEntryStats(t, *e)...))
		require.True(t, stashOf(txn, statA), "stash untouched: nothing was detached")
		require.True(t, stashOf(txn, statB))
		require.Empty(t, txn.retiredTransfers,
			"no detachment completed -> no ledger record")

		// The statement-rollback resurrection hook must add nothing.
		txn.Lock()
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(retireStmt))
		txn.Unlock()
		require.Len(t, txn.writes, 1, "rollback resurrected nothing")
		require.Equal(t, 1, countStat(txn, statA))
		require.Equal(t, 1, countStat(txn, statB))
		require.True(t, stashOf(txn, statA))
		require.True(t, stashOf(txn, statB))
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})

	t.Run("failure in a later entry records only the completed detachment", func(t *testing.T) {
		txn := newTxn()
		txn.writes = append(txn.writes,
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, userTable, statA),
			pkMoveFileDeleteEntry(t, mp, accountID, dbID, userTable, statB, statC))
		for i := range txn.writes {
			txn.accountWorkspaceEntryLocked(&txn.writes[i])
		}
		txn.StashFlushedTombstones(statA)
		txn.StashFlushedTombstones(statB)
		txn.StashFlushedTombstones(statC)

		pkMoveSwapVecSeams(t,
			func(*vector.Vector, []byte, bool, *mpool.MPool) error { return errPkMoveInjected },
			vector.SetBytesAt)

		txn.Lock()
		err := txn.retireTransferredTombstoneStatsLocked(tbl,
			[]objectio.ObjectStats{statA, statB})
		txn.Unlock()
		require.ErrorIs(t, err, errPkMoveInjected)

		// entry0 was fully covered: released and recorded BEFORE the later
		// failure — a completed detachment whose file the close-time gc owns.
		require.Nil(t, txn.writes[0].bat)
		require.False(t, stashOf(txn, statA))
		// entry1 is untouched: its failed shrink detached nothing.
		require.Equal(t, retireStatNames(statB, statC),
			retireStatNames(readEntryStats(t, txn.writes[1])...))
		require.True(t, stashOf(txn, statB))
		require.True(t, stashOf(txn, statC))

		require.Len(t, txn.retiredTransfers, 1)
		require.Len(t, txn.retiredTransfers[0].entries, 1,
			"the ledger records only the COMPLETED detachment")
		require.Equal(t, retireStatNames(statA),
			retireStatNames(txn.retiredTransfers[0].entries[0].stats...))

		// Statement rollback resurrects exactly the completed detachment;
		// entry1's stats must not twin. Restore the allocation seam at
		// the phase boundary: the injection window pinned the RETIRE
		// burst, and the e27 seam coverage makes the rollback phase's
		// resurrection builds ride it too (the resurrection-failure
		// contract itself is pinned by the sibling r33 test
		// TestPkMoveResurrectionPartialFailurePreservesRetiredReservations).
		pkMoveRestoreVecSeams(t)
		txn.Lock()
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(retireStmt))
		txn.Unlock()
		require.Len(t, txn.writes, 3,
			"one resurrected tail entry for the completed detachment")
		require.Equal(t, 1, countStat(txn, statA))
		require.Equal(t, 1, countStat(txn, statB),
			"no twin publication of the failed shrink's stats")
		require.Equal(t, 1, countStat(txn, statC))
		require.True(t, stashOf(txn, statA))
		require.True(t, stashOf(txn, statB))
		require.True(t, stashOf(txn, statC))
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
	})
}

// --- r27 review F6 (D119): finished lineage-binding stacks collapse ---

// TestPkMoveContinuityCollapsesFinishedLineageBinds pins the r27 review F6
// contract: the unresolved-marker route (the S3 buffering path of a plain
// same-key UPDATE) appends one lineage binding per key per statement, the
// D112 resolved-generation retirement never sees it, and the finished-
// statement marker retirement only touched the pkMoveMarkerStmts ledger —
// so repeated edits retained O(rows x statements) of lineage history even
// though every re-bind composes the SAME durable origin out of the live
// top. The statement-finishing sweep must collapse each key's stack to its
// single reachable top (and retire permanently consumed keys), keeping
// retention O(live keys) while rollback, later moves, and consumption keep
// identical answers.
func TestPkMoveContinuityCollapsesFinishedLineageBinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const (
		accountID = uint32(1)
		tableID   = uint64(42)
	)
	acctTbl := pkMoveAcctTbl{account: accountID, table: tableID}
	enc3 := string(pkMoveEncodeInt64(t, 3))

	advanceStatement := func(txn *Transaction) {
		// Mirror types.go's IncrStatementID provenance block: the finished
		// statement's undo records sit beyond rollback reach, its
		// marker-only continuity entries retire (D112), and its continuity
		// history sweeps (r27 review F6, D119).
		txn.pkMoveRetargetUndo = nil
		txn.pkMoveOriginUndo = nil
		txn.pkMoveRetargetUndoSeen = nil
		txn.pkMoveOriginUndoSeen = nil
		txn.retirePkMoveFinishedMarkerStmtsLocked(txn.statementID)
		txn.sweepPkMoveFinishedContinuityLocked(txn.statementID)
		txn.statementID++
	}
	// An UNRESOLVED marker: the edit's replacement never stages (the S3
	// buffering route), so the fill only binds the lineage.
	bindWave := func(t *testing.T, txn *Transaction, preImage types.Rowid) {
		t.Helper()
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID,
			pkMoveMarkerBatch(t, mp, tableID, 3, preImage)))
	}

	t.Run("unresolved S3-route waves keep one binding per key at flat bytes", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 1)
		preImage := durable
		var baseBytes int64
		for wave := 0; wave < 4; wave++ {
			bindWave(t, txn, preImage)
			preImage = types.BuildTestRowid(30, int64(100+wave))
			advanceStatement(txn)
			stack := txn.pkMoveKeyLineage[acctTbl][enc3]
			require.Len(t, stack, 1,
				"wave %d: one binding per live key, not one per statement", wave+1)
			require.Equal(t, durable, stack[0].origin,
				"wave %d: the collapsed top still composes the durable origin", wave+1)
			require.Empty(t, txn.pkMoveStagedOldRids,
				"wave %d: superseded coverage retires — the edit that observed each old side consumed it", wave+1)
			if wave == 0 {
				baseBytes = txn.pkMoveBookkeepBytesLocked()
			} else {
				require.Equal(t, baseBytes, txn.pkMoveBookkeepBytesLocked(),
					"wave %d: retained continuity bytes stay flat across statements", wave+1)
			}
		}
	})

	t.Run("a recorded diverted death keeps its coverage explanation", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 31)
		bindWave(t, txn, durable)

		// A diverted death naming the consumed generation is already
		// recorded: its staged-old explanation must survive the sweep,
		// or the binding would read the explained departure as a murder.
		txn.pkMoveRetireDeaths = map[pkMoveAcctTbl]map[string][]pkMoveRetireDeath{
			acctTbl: {enc3: {{rid: durable, stmt: 0}}},
		}
		advanceStatement(txn)
		require.Contains(t, txn.pkMoveStagedOldRids, durable,
			"the recorded death keeps its explanation")
		require.Len(t, txn.pkMoveStagedOldRids, 1)
		require.NotNil(t, txn.pkMoveStagedOldRids)

		// Once the death itself retires (rollback tail cut), the coverage
		// follows at the next sweep.
		txn.pkMoveRetireDeaths = nil
		advanceStatement(txn)
		require.Empty(t, txn.pkMoveStagedOldRids,
			"no retained death names the rid any longer")
	})

	t.Run("statement rollback still restores the visible binding across a collapse", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 5)
		bindWave(t, txn, durable)                      // statement 0
		advanceStatement(txn)                          // collapse to the durable top
		bindWave(t, txn, types.BuildTestRowid(30, 51)) // statement 1 re-binds
		require.Len(t, txn.pkMoveKeyLineage[acctTbl][enc3], 2,
			"precondition: the un-collapsed current statement rides the stack top")

		// Roll back statement 1: the tail cut revives the collapsed floor.
		txn.statementID = 0
		require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
		stack := txn.pkMoveKeyLineage[acctTbl][enc3]
		require.Len(t, stack, 1)
		require.Equal(t, 0, stack[0].stmt, "the finished floor survives the cut")
		origin, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, enc3)
		require.True(t, ok)
		require.Equal(t, durable, origin)
	})

	t.Run("a later move anchors to the durable origin after collapses", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 9)
		preImage := durable
		var head types.Rowid
		for wave := 0; wave < 3; wave++ {
			bindWave(t, txn, preImage)
			head = types.BuildTestRowid(30, int64(200+wave))
			preImage = head
			advanceStatement(txn)
		}
		move := pkMoveMarkerOrMoveBatch(t, mp, tableID,
			[][]byte{pkMoveEncodeInt64(t, 3)}, [][]byte{pkMoveEncodeInt64(t, 4)},
			[]*types.Rowid{&head})
		require.NoError(t, txn.fillPkMoveLinkNewRowIDsLocked(accountID, move))
		require.Equal(t, durable[:], move.Vecs[4].GetBytesAt(0),
			"the move's old side anchors to the collapsed durable origin (oldRowIdx=4)")
	})

	t.Run("a binding consumed by a finished statement retires entirely", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		durable := types.BuildTestRowid(30, 21)
		bindWave(t, txn, durable)
		txn.consumePkMoveKeyLineageLocked(acctTbl, enc3)
		advanceStatement(txn)
		require.Empty(t, txn.pkMoveKeyLineage,
			"the finished consumption can never roll back: the dead lineage retires")
		_, ok := txn.lookupPkMoveKeyLineageLocked(acctTbl, enc3)
		require.False(t, ok, "a re-arriving key must not inherit the dead lineage")
	})
}

// TestPkMoveResurrectionReturnsReservationsAcrossCycles pins r33 review
// F2's release contract (D136) on the healthy terminal: a rollback
// resurrection returns the retirement's booked reservations for every
// unique file it brings back to workspace ownership, so repeated
// retirement/rollback cycles are budget-neutral. On the RED semantics
// each cycle leaves its two bookings behind, and the pinned hard
// ceiling (4) is crossed by cycle three against an EMPTY retained set
// — the r33 review's "exhaustion with healthy deletion" shape. The
// first cycle's settlement assert pins the leak on the RED tree.
func TestPkMoveResurrectionReturnsReservationsAcrossCycles(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	txn, tbl := fx.newTxnTbl(t)

	// Pinned ceiling 4 with healthy deletion: two bookings per cycle
	// must always book.
	pinOrphanGCBounds(t, 2, 4, 1)

	const retireStmt = 2
	statA := fx.writeObj(t, tbl, []int64{21})[0]
	statC := fx.writeObj(t, tbl, []int64{22})[0]
	before := fsInventory(t, base)

	// One file-backed DELETE entry per object, stash-visible exactly
	// like a flushed tombstone.
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.writes = append(txn.writes,
		pkMoveFileDeleteEntry(t, fx.mp, 1, 7, 42, statA),
		pkMoveFileDeleteEntry(t, fx.mp, 1, 7, 42, statC))
	for i := range txn.writes {
		txn.accountWorkspaceEntryLocked(&txn.writes[i])
	}
	txn.StashFlushedTombstones(statA)
	txn.StashFlushedTombstones(statC)
	txn.statementID = retireStmt

	for cycle := 1; cycle <= 3; cycle++ {
		txn.Lock()
		require.NoError(t, txn.retireTransferredTombstoneStatsLocked(tbl,
			[]objectio.ObjectStats{statA, statC}),
			"cycle %d: retirement books its fresh pair", cycle)
		_, _, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
		require.Equal(t, 2, reserved,
			"cycle %d: both unique retired names booked (inputs 2, slack 0 released)", cycle)
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(retireStmt))
		txn.Unlock()
		alive, _, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
		require.Equal(t, 0, reserved,
			"D136: cycle %d's resurrection returned both reservations", cycle)
		require.Empty(t, txn.retiredTransfers,
			"cycle %d: the whole ledger resurrected", cycle)
		if cycle == 3 {
			require.False(t, alive,
				"D136: every reservation returned — the registry released the shell")
		}
	}

	// Restored full admission capacity: the ceiling's whole booking
	// budget is free again.
	require.NoError(t, reserveDurableOrphanOwnership(txn.engine.fs, 4))
	releaseDurableOrphanReservation(txn.engine.fs, 4)
	require.Equal(t, before, fsInventory(t, base),
		"retire/resurrect cycles never reclaim a publishing file")
}

// TestPkMoveResurrectionPartialFailurePreservesRetiredReservations pins
// r33 review F2's partial-failure clause (D136): a mid-walk build
// failure inside the resurrection settles exactly the entries already
// back in the workspace while the still-retired record keeps its own
// reservation — which the close drain then converts and settles, with
// the filesystem inventory and the registry both reconciled at the
// end. The D136 seam rides the established pkMoveVecAppendBytes
// allocation seam, and the fixture's FileService is REAL (the r32-era
// resurrection tests ran on a nil one and never saw the accounting).
func TestPkMoveResurrectionPartialFailurePreservesRetiredReservations(t *testing.T) {
	proc, _, base := newR29SharedProc(t)
	fx := newR28Fixture(t, proc)
	txn, tbl := fx.newTxnTbl(t)

	pinOrphanGCBounds(t, 2, 8, 1)
	const retireStmt = 2

	before := fsInventory(t, base)
	statA := fx.writeObj(t, tbl, []int64{31})[0]
	statB := fx.writeObj(t, tbl, []int64{32})[0]
	statC := fx.writeObj(t, tbl, []int64{33})[0]

	// Entry 0 carries [statA, statB] — the retirement covers statA and
	// shrinks the entry to statB; entry 1 carries statC and releases in
	// place. Booking: 3 inputs - 1 slack = 2 outstanding.
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.writes = append(txn.writes,
		pkMoveFileDeleteEntry(t, fx.mp, 1, 7, 42, statA, statB),
		pkMoveFileDeleteEntry(t, fx.mp, 1, 7, 42, statC))
	for i := range txn.writes {
		txn.accountWorkspaceEntryLocked(&txn.writes[i])
	}
	for _, ss := range []objectio.ObjectStats{statA, statB, statC} {
		txn.StashFlushedTombstones(ss)
	}
	txn.statementID = retireStmt

	txn.Lock()
	require.NoError(t, txn.retireTransferredTombstoneStatsLocked(tbl,
		[]objectio.ObjectStats{statA, statC}))
	_, retained, _, reserved, _, _ := orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 0, retained, "retirement alone never registers")
	require.Equal(t, 2, reserved,
		"the two unique retired names stay booked after the slack release")
	txn.Unlock()

	// Fail the SECOND entry's batch build inside the resurrection: entry
	// 0 (statA) returns to the workspace first and must settle exactly
	// its name; entry 1 (statC) stays retired with its booking.
	appendCalls := 0
	pkMoveSwapVecSeams(t,
		func(vec *vector.Vector, data []byte, isNull bool, mp *mpool.MPool) error {
			appendCalls++
			if appendCalls == 2 {
				return errPkMoveInjected
			}
			return vector.AppendBytes(vec, data, isNull, mp)
		},
		pkMoveVecSetBytesAt)
	txn.Lock()
	err := txn.resurrectRetiredTransferEntriesLocked(retireStmt)
	txn.Unlock()
	require.ErrorIs(t, err, errPkMoveInjected,
		"the injected build failure surfaces from the resurrection")

	_, _, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 1, reserved,
		"D136: the resurrected entry's file settled exactly once; the still-retired record keeps its own")
	require.Len(t, txn.retiredTransfers, 1)
	require.Len(t, txn.retiredTransfers[0].entries, 1,
		"only the still-retired entry survives in the ledger")
	require.Equal(t, retireStatNames(statC),
		retireStatNames(txn.retiredTransfers[0].entries[0].stats...))
	require.True(t, stashOf(txn, statA),
		"the resurrected stat re-entered the read-visible stash")
	require.False(t, stashOf(txn, statC),
		"the still-retired stat stays out of the stash")

	// Final reconciliation: the close drain converts the surviving
	// reservation name-for-name and the healed sweep reclaims the file
	// to a byte-exact inventory with the registry released.
	txn.gcRetiredTransferFiles()
	_, retained, _, reserved, _, _ = orphanGCEntryState(txn.engine.fs)
	require.Equal(t, 1, retained, "the drain registered the retired file")
	require.Equal(t, 0, reserved,
		"D136: every reservation terminal reconciled — nothing leaks")
	retryDurableOrphanGC(txn.engine)
	require.Eventually(t, func() bool {
		a, _, _, _, _, _ := orphanGCEntryState(txn.engine.fs)
		return !a
	}, 10*time.Second, 5*time.Millisecond,
		"the confirmed deletion released the drained shell")
	waitFileGone(t, base, statC.ObjectName().String())
	final := fsInventory(t, base)
	require.Contains(t, final, statA.ObjectName().String(),
		"the resurrected file stays — it publishes with the restored entry")
	require.Contains(t, final, statB.ObjectName().String(),
		"the never-retired stat's file was never reclaimed")
	require.NotContains(t, final, statC.ObjectName().String())
	require.Len(t, final, len(before)+2,
		"only the still-retired file left the inventory")
}
