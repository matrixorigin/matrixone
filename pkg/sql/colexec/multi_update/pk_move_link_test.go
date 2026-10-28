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

package multi_update

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newTestPkMoveLinkCtx mirrors plan.buildUpdatePkMoveLinkTableDef (unexported
// in the plan package): the operator only needs the column layout to build
// link batches.
func newTestPkMoveLinkCtx(ownerIdx int) *MultiUpdateCtx {
	// Dense seqnums mirror the plan builder's stamping; the spill sinker
	// writes one object column-meta slot per seqnum and duplicate (zero)
	// seqnums corrupt the persisted extent accounting.
	linkCols := []*plan.ColDef{
		{Name: catalog.DataBranchUpdateLinkColLinkID, Typ: plan.Type{Id: int32(types.T_binary), Width: catalog.DataBranchUpdateLinkIDBytes}},
		{Name: catalog.DataBranchUpdateLinkColTableID, Typ: plan.Type{Id: int32(types.T_uint64)}},
		{Name: catalog.DataBranchUpdateLinkColOldPKSha, Typ: plan.Type{Id: int32(types.T_binary), Width: catalog.DataBranchUpdateLinkPKShaBytes}},
		{Name: catalog.DataBranchUpdateLinkColOldRow, Typ: plan.Type{Id: int32(types.T_binary), Width: 24}},
		{Name: catalog.DataBranchUpdateLinkColOldPK, Typ: plan.Type{Id: int32(types.T_varbinary), Width: 65535}},
		{Name: catalog.DataBranchUpdateLinkColNewPKSha, Typ: plan.Type{Id: int32(types.T_binary), Width: catalog.DataBranchUpdateLinkPKShaBytes}},
		{Name: catalog.DataBranchUpdateLinkColNewPK, Typ: plan.Type{Id: int32(types.T_varbinary), Width: 65535}},
		{Name: catalog.DataBranchUpdateLinkColNewRow, Typ: plan.Type{Id: int32(types.T_binary), Width: 24}},
		{Name: catalog.DataBranchUpdateLinkColWriteTS, Typ: plan.Type{Id: int32(types.T_uint64)}},
		{Name: catalog.DataBranchUpdateLinkColTxnID, Typ: plan.Type{Id: int32(types.T_binary), Width: catalog.DataBranchUpdateLinkTxnIDBytes}},
		{Name: catalog.Row_ID, Typ: rowIdTyp},
	}
	for i := range linkCols[:len(linkCols)-1] {
		linkCols[i].Seqnum = uint32(i)
	}
	return &MultiUpdateCtx{
		ObjRef: &plan.ObjectRef{
			SchemaName: catalog.MO_CATALOG,
			ObjName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		},
		TableDef: &plan.TableDef{
			Name: catalog.MO_DATA_BRANCH_UPDATE_LINK,
			Cols: linkCols,
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.DataBranchUpdateLinkColLinkID,
				Names:       []string{catalog.DataBranchUpdateLinkColLinkID},
			},
			TableType: catalog.SystemOrdinaryRel,
		},
		TargetUpdateCtxIdx: ownerIdx,
	}
}

// testPkMoveLinkWriteTS is the fixed snapshot-ns value UTs feed into the link
// batch builder.
const testPkMoveLinkWriteTS = uint64(42_000_000_000)

// testPkMoveLinkTxnID is the fabricated recording-transaction identity the
// writer persists into the txn_id column and salts into workspace link ids.
var testPkMoveLinkTxnID = []byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}

var testPkMoveLinkTxnIDOther = []byte{1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 1, 2, 3}

// expectedPkMoveLinkID mirrors the writer's link id layout:
// [8-byte big-endian table id][32-byte sha256 of the old pk encoding][row id].
func expectedPkMoveLinkID(tblID uint64, oldPKBytes []byte, rowid types.Rowid) []byte {
	id := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
	binary.BigEndian.PutUint64(id, tblID)
	sha := sha256.Sum256(oldPKBytes)
	copy(id[8:], sha[:])
	copy(id[8+catalog.DataBranchUpdateLinkPKShaBytes:], rowid[:])
	return id
}

func packerEncodeString(s string) []byte {
	p := types.NewPacker()
	defer p.Close()
	p.EncodeStringType([]byte(s))
	return append([]byte(nil), p.Bytes()...)
}

func packerEncodeInt32(v int32) []byte {
	p := types.NewPacker()
	defer p.Close()
	p.EncodeInt32(v)
	return append([]byte(nil), p.Bytes()...)
}

func TestCollectUpdatePkMoveLinkRowsInt32Pk(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const tblID = uint64(42)
	ownerCtx := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: tblID,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: "b", Typ: varcharTyp},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		// Insert positions follow table column order minus Row_ID.
		InsertCols: []int{0, 1},
		// [old rowid, old pk]
		DeleteCols: []int{2, 3},
	}
	linkCtx := newTestPkMoveLinkCtx(0)
	require.True(t, isUpdatePkMoveLinkCtx(linkCtx))

	r1 := types.BuildTestRowid(1, 1)
	r2 := types.BuildTestRowid(1, 2)
	r3 := types.BuildTestRowid(1, 3)
	r4 := types.BuildTestRowid(1, 4)

	contextBatch := batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.NewInt32Vector(4, types.T_int32.ToType(), mp, false, nil, []int32{1, 3, 5, 7})
	contextBatch.Vecs[1] = testutil.MakeVarcharVector([]string{"x", "y", "z", "w"}, nil, mp)
	contextBatch.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{r1, r2, r3, r4}, []uint64{3}, mp)
	contextBatch.Vecs[3] = testutil.NewInt32Vector(4, types.T_int32.ToType(), mp, false, nil, []int32{1, 2, 4, 6})
	contextBatch.SetRowCount(4)
	defer contextBatch.Clean(mp)

	linkBat, err := collectUpdatePkMoveLinkRows(proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, linkBat)
	defer linkBat.Clean(mp)

	// row3 has a null old rowid and is skipped. Row 0 is a same-key
	// replacement (1->1) of a durable row: it emits a continuity marker so
	// the workspace fill can record the durable lineage origin, and the
	// fill drops the marker before commit. Rows 1 and 2 are real moves.
	require.Equal(t, 3, linkBat.RowCount())
	require.Equal(t, []string{
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
	}, linkBat.Attrs)

	// The stored old/new pk bytes must equal the branch-hashmap key encoding
	// (vector.EncodeRowToPacker), so diff can do encoded-key lookups verbatim.
	golden := func(v int32) []byte {
		vec := testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{v})
		defer vec.Free(mp)
		p := types.NewPacker()
		defer p.Close()
		require.NoError(t, vector.EncodeRowToPacker(p, vec, 0))
		return append([]byte(nil), p.Bytes()...)
	}
	old2, old4 := golden(2), golden(4)
	new3, new5 := golden(3), golden(5)
	shaOf := func(b []byte) []byte {
		sha := sha256.Sum256(b)
		return sha[:]
	}

	golden1 := golden(1)
	require.Equal(t, expectedPkMoveLinkID(tblID, golden1, r1), linkBat.Vecs[0].GetBytesAt(0))
	require.Equal(t, expectedPkMoveLinkID(tblID, old2, r2), linkBat.Vecs[0].GetBytesAt(1))
	require.Equal(t, expectedPkMoveLinkID(tblID, old4, r3), linkBat.Vecs[0].GetBytesAt(2))
	require.Equal(t, []uint64{tblID, tblID, tblID}, vector.MustFixedColWithTypeCheck[uint64](linkBat.Vecs[1]))
	require.Equal(t, shaOf(golden1), linkBat.Vecs[2].GetBytesAt(0))
	require.Equal(t, shaOf(old2), linkBat.Vecs[2].GetBytesAt(1))
	require.Equal(t, shaOf(old4), linkBat.Vecs[2].GetBytesAt(2))
	require.Equal(t, r1[:], linkBat.Vecs[3].GetBytesAt(0))
	require.Equal(t, r2[:], linkBat.Vecs[3].GetBytesAt(1))
	require.Equal(t, r3[:], linkBat.Vecs[3].GetBytesAt(2))
	require.Equal(t, golden1, linkBat.Vecs[4].GetBytesAt(0))
	require.Equal(t, old2, linkBat.Vecs[4].GetBytesAt(1))
	require.Equal(t, old4, linkBat.Vecs[4].GetBytesAt(2))
	require.Equal(t, shaOf(golden1), linkBat.Vecs[5].GetBytesAt(0))
	require.Equal(t, shaOf(new3), linkBat.Vecs[5].GetBytesAt(1))
	require.Equal(t, shaOf(new5), linkBat.Vecs[5].GetBytesAt(2))
	require.Equal(t, golden1, linkBat.Vecs[6].GetBytesAt(0))
	require.Equal(t, new3, linkBat.Vecs[6].GetBytesAt(1))
	require.Equal(t, new5, linkBat.Vecs[6].GetBytesAt(2))
	// new_row_id is written NULL: the disttae workspace back-fills the
	// replacement row's identity when the link write is staged.
	require.True(t, linkBat.Vecs[7].IsNull(0))
	require.True(t, linkBat.Vecs[7].IsNull(1))
	require.True(t, linkBat.Vecs[7].IsNull(2))
	require.Equal(t, []uint64{testPkMoveLinkWriteTS, testPkMoveLinkWriteTS, testPkMoveLinkWriteTS},
		vector.MustFixedColWithTypeCheck[uint64](linkBat.Vecs[8]))
	// The recording transaction's durable identity rides every link row.
	for i := 0; i < linkBat.RowCount(); i++ {
		require.Equal(t, testPkMoveLinkTxnID, linkBat.Vecs[9].GetBytesAt(i))
	}
	// Cross-check the dispatch against the direct scalar encoding.
	require.Equal(t, packerEncodeInt32(2), old2)
}

func TestCollectUpdatePkMoveLinkRowsVarcharPk(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	const tblID = uint64(7)
	ownerCtx := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: tblID,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: "name", Typ: varcharTyp, Primary: true},
				{Name: "v", Typ: i32typ},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "name", Names: []string{"name"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0, 1},
		DeleteCols: []int{2, 3},
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	r1 := types.BuildTestRowid(2, 1)
	contextBatch := batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.MakeVarcharVector([]string{"bravo"}, nil, mp)
	contextBatch.Vecs[1] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{9})
	contextBatch.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{r1}, nil, mp)
	contextBatch.Vecs[3] = testutil.MakeVarcharVector([]string{"alpha"}, nil, mp)
	contextBatch.SetRowCount(1)
	defer contextBatch.Clean(mp)

	linkBat, err := collectUpdatePkMoveLinkRows(proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, linkBat)
	defer linkBat.Clean(mp)

	require.Equal(t, 1, linkBat.RowCount())
	require.Equal(t, packerEncodeString("alpha"), linkBat.Vecs[4].GetBytesAt(0))
	require.Equal(t, packerEncodeString("bravo"), linkBat.Vecs[6].GetBytesAt(0))
}

func TestCollectUpdatePkMoveLinkRowsCompositePk(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	// Composite pks surface through the hidden cpkey varbinary column; the
	// stored link bytes double-encode it exactly like branch-hashmap keys do.
	cpOld := packerEncodeString("a|1")
	cpNew := packerEncodeString("a|2")

	ownerCtx := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 9,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varbinary), Width: 65535}},
				{Name: "a", Typ: i32typ},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: catalog.CPrimaryKeyColName,
				Names:       []string{"a", "b"},
			},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0, 1},
		DeleteCols: []int{2, 3},
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	r1 := types.BuildTestRowid(3, 1)
	contextBatch := batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.MakeVarlenaVector([][]byte{cpNew}, nil, types.T_varbinary.ToType(), mp)
	contextBatch.Vecs[1] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
	contextBatch.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{r1}, nil, mp)
	contextBatch.Vecs[3] = testutil.MakeVarlenaVector([][]byte{cpOld}, nil, types.T_varbinary.ToType(), mp)
	contextBatch.SetRowCount(1)
	defer contextBatch.Clean(mp)

	linkBat, err := collectUpdatePkMoveLinkRows(proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, linkBat)
	defer linkBat.Clean(mp)

	require.Equal(t, 1, linkBat.RowCount())
	require.Equal(t, packerEncodeString(string(cpOld)), linkBat.Vecs[4].GetBytesAt(0))
	require.Equal(t, packerEncodeString(string(cpNew)), linkBat.Vecs[6].GetBytesAt(0))
}

func TestCollectUpdatePkMoveLinkRowsFailClosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	newOwner := func() *MultiUpdateCtx {
		return &MultiUpdateCtx{
			TableDef: &plan.TableDef{
				TblId: 1,
				Cols: []*plan.ColDef{
					{Name: "a", Typ: i32typ, Primary: true},
					{Name: catalog.Row_ID, Typ: rowIdTyp},
				},
				Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
				TableType: catalog.SystemOrdinaryRel,
			},
			InsertCols: []int{0},
			DeleteCols: []int{1, 2},
		}
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	r1 := types.BuildTestRowid(1, 1)
	newBatch := func() *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{5})
		bat.Vecs[1] = testutil.MakeRowIdVector([]types.Rowid{r1}, nil, mp)
		bat.Vecs[2] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{4})
		bat.SetRowCount(1)
		return bat
	}

	// nil and empty context batches produce nothing.
	bat, err := collectUpdatePkMoveLinkRows(proc, linkCtx, newOwner(), nil, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)

	empty := batch.NewWithSize(3)
	empty.Vecs[0] = testutil.NewInt32Vector(0, types.T_int32.ToType(), mp, false, nil, nil)
	empty.Vecs[1] = testutil.MakeRowIdVector(nil, nil, mp)
	empty.Vecs[2] = testutil.NewInt32Vector(0, types.T_int32.ToType(), mp, false, nil, nil)
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, newOwner(), empty, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)
	empty.Clean(mp)

	// owner without a pk: no link position can be derived.
	noPk := newOwner()
	noPk.TableDef.Pkey = nil
	input := newBatch()
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, noPk, input, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)
	input.Clean(mp)

	// truncated DeleteCols cannot provide old rowid + old pk.
	truncated := newOwner()
	truncated.DeleteCols = []int{1}
	input = newBatch()
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, truncated, input, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)
	input.Clean(mp)

	// out-of-range positions fail closed.
	outOfRange := newOwner()
	outOfRange.DeleteCols = []int{1, 9}
	input = newBatch()
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, outOfRange, input, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)
	input.Clean(mp)

	// a non-rowid vector at DeleteCols[0] is not an update context we trust.
	wrongType := newBatch()
	wrongType.Vecs[1].Free(mp)
	wrongType.Vecs[1] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, newOwner(), wrongType, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.Nil(t, bat)
	wrongType.Clean(mp)

	// unchanged pk with durable pre-images emits only continuity markers
	// (old pk == new pk, NULL new_row_id): the workspace fill consumes and
	// drops them, so nothing commits.
	same := batch.NewWithSize(3)
	same.Vecs[0] = testutil.NewInt32Vector(2, types.T_int32.ToType(), mp, false, nil, []int32{4, 4})
	same.Vecs[1] = testutil.MakeRowIdVector([]types.Rowid{r1, types.BuildTestRowid(1, 2)}, nil, mp)
	same.Vecs[2] = testutil.NewInt32Vector(2, types.T_int32.ToType(), mp, false, nil, []int32{4, 4})
	same.SetRowCount(2)
	defer same.Clean(mp)
	bat, err = collectUpdatePkMoveLinkRows(proc, linkCtx, newOwner(), same, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, bat)
	defer bat.Clean(mp)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, bat.Vecs[4].GetBytesAt(0), bat.Vecs[6].GetBytesAt(0))
	require.Equal(t, bat.Vecs[4].GetBytesAt(1), bat.Vecs[6].GetBytesAt(1))
	require.True(t, bat.Vecs[7].IsNull(0))
	require.True(t, bat.Vecs[7].IsNull(1))
}

func TestPkMoveLinkIDRowidPart(t *testing.T) {
	// Durable identities pass through verbatim: they are globally unique,
	// and keeping them byte-identical preserves the pre-fix id derivation
	// for links anchored on committed rows.
	durable := types.BuildTestRowid(3, 9)
	part := pkMoveLinkIDRowidPart(&durable, 100, testPkMoveLinkTxnID)
	require.Equal(t, durable[:], part)
	require.Equal(t, durable[:], pkMoveLinkIDRowidPart(&durable, 200, testPkMoveLinkTxnIDOther))

	// Workspace identities restart at the same values in every
	// transaction: the salt namespaces them by the transaction's create
	// timestamp, so two transactions mint distinct link ids for the same
	// raw identity, while one transaction stays deterministic.
	workspace := durable
	copy(workspace[0:16], colexec.TxnWorkspaceSegment[:])
	partA := pkMoveLinkIDRowidPart(&workspace, 100, testPkMoveLinkTxnID)
	require.Equal(t, partA, pkMoveLinkIDRowidPart(&workspace, 100, testPkMoveLinkTxnID))
	require.NotEqual(t, partA, pkMoveLinkIDRowidPart(&workspace, 101, testPkMoveLinkTxnID))
	// Two transactions created inside one physical instant (one shared
	// create timestamp) still mint distinct parts: the durable
	// transaction id, not the timestamp, is the namespace.
	require.NotEqual(t,
		pkMoveLinkIDRowidPart(&workspace, 100, testPkMoveLinkTxnID),
		pkMoveLinkIDRowidPart(&workspace, 100, testPkMoveLinkTxnIDOther))
	require.NotEqual(t, workspace[:], partA)
	require.Len(t, partA, types.RowidSize)
}

func TestCollectUpdatePkMoveLinkRowsNamespacesWorkspaceRowids(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	owner := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 1,
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0},
		DeleteCols: []int{1, 2},
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	wsRid := types.BuildTestRowid(1, 1)
	copy(wsRid[0:16], colexec.TxnWorkspaceSegment[:])
	newBatch := func() *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{7})
		bat.Vecs[1] = testutil.MakeRowIdVector([]types.Rowid{wsRid}, nil, mp)
		bat.Vecs[2] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{4})
		bat.SetRowCount(1)
		return bat
	}

	// The same transaction-local identity recorded by two different
	// transactions must mint two different catalog keys.
	in1 := newBatch()
	defer in1.Clean(mp)
	bat1, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in1, 100, testPkMoveLinkTxnID)
	require.NoError(t, err)
	defer bat1.Clean(mp)
	in2 := newBatch()
	defer in2.Clean(mp)
	bat2, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in2, 200, testPkMoveLinkTxnIDOther)
	require.NoError(t, err)
	defer bat2.Clean(mp)
	require.Equal(t, 1, bat1.RowCount())
	require.Equal(t, 1, bat2.RowCount())
	require.NotEqual(t,
		bat1.Vecs[0].GetBytesAt(0), bat2.Vecs[0].GetBytesAt(0),
		"workspace identities from different transactions collide on link_id",
	)
	// The raw identity column still records the verbatim old row id: only
	// the link id is namespaced.
	require.Equal(t, wsRid[:], bat1.Vecs[3].GetBytesAt(0))
}

// Two transactions whose create timestamps share one physical instant (the
// HLC advances the logical component instead of the physical one when the
// clock does not move, hlc.go) are still SEPARATE transactions. Because
// workspace row-id allocation restarts per transaction, two such
// transactions moving workspace-minted rows over the same table key mint
// byte-identical workspace identities; only the recording transaction's
// own identity may keep their catalog link ids apart.
func TestCollectUpdatePkMoveLinkRowsTxnNamespaceAtSharedCreateTS(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	owner := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 1,
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0},
		DeleteCols: []int{1, 2},
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	wsRid := types.BuildTestRowid(1, 1)
	copy(wsRid[0:16], colexec.TxnWorkspaceSegment[:])
	newBatch := func() *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{7})
		bat.Vecs[1] = testutil.MakeRowIdVector([]types.Rowid{wsRid}, nil, mp)
		bat.Vecs[2] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{4})
		bat.SetRowCount(1)
		return bat
	}

	in1 := newBatch()
	defer in1.Clean(mp)
	bat1, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in1, 100, testPkMoveLinkTxnID)
	require.NoError(t, err)
	defer bat1.Clean(mp)
	in2 := newBatch()
	defer in2.Clean(mp)
	bat2, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in2, 100, testPkMoveLinkTxnIDOther)
	require.NoError(t, err)
	defer bat2.Clean(mp)
	require.NotEqual(t,
		bat1.Vecs[0].GetBytesAt(0), bat2.Vecs[0].GetBytesAt(0),
		"a shared physical create timestamp is not a transaction namespace",
	)
}

func TestCollectUpdatePkMoveLinkRowsSameKeyMarkers(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	owner := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 1,
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0},
		DeleteCols: []int{1, 2},
	}
	linkCtx := newTestPkMoveLinkCtx(0)

	newBatch := func(rid types.Rowid) *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{4})
		bat.Vecs[1] = testutil.MakeRowIdVector([]types.Rowid{rid}, nil, mp)
		bat.Vecs[2] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{4})
		bat.SetRowCount(1)
		return bat
	}

	// A pk-unchanged replacement of a workspace-minted row emits one
	// continuity marker: old pk bytes == new pk bytes, the verbatim old
	// row id, NULL new_row_id.
	wsRid := types.BuildTestRowid(1, 1)
	copy(wsRid[0:16], colexec.TxnWorkspaceSegment[:])
	in := newBatch(wsRid)
	defer in.Clean(mp)
	bat, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, bat)
	defer bat.Clean(mp)
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, bat.Vecs[4].GetBytesAt(0), bat.Vecs[6].GetBytesAt(0),
		"a same-key marker must carry identical old and new pk encodings")
	require.Equal(t, wsRid[:], bat.Vecs[3].GetBytesAt(0))
	require.True(t, bat.Vecs[7].IsNull(0))
	// The marker still gets the transaction-namespaced link id (the old
	// identity is workspace-minted), so two transactions never collide.
	require.NotEqual(t,
		expectedPkMoveLinkID(1, bat.Vecs[4].GetBytesAt(0), wsRid),
		bat.Vecs[0].GetBytesAt(0))

	// A pk-unchanged replacement of a DURABLE row emits a marker too: the
	// workspace fill hook records the durable generation as the lineage
	// origin, so a later statement that moves the fresh row anchors its
	// link to the row id the committed stream shows departing. The
	// durable identity passes the link id rowid part verbatim (it is
	// globally unique and needs no transaction namespace).
	durable := types.BuildTestRowid(1, 1)
	in2 := newBatch(durable)
	defer in2.Clean(mp)
	bat2, err := collectUpdatePkMoveLinkRows(proc, linkCtx, owner, in2, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	require.NoError(t, err)
	require.NotNil(t, bat2)
	defer bat2.Clean(mp)
	require.Equal(t, 1, bat2.RowCount())
	require.Equal(t, bat2.Vecs[4].GetBytesAt(0), bat2.Vecs[6].GetBytesAt(0))
	require.Equal(t, durable[:], bat2.Vecs[3].GetBytesAt(0))
	require.True(t, bat2.Vecs[7].IsNull(0))
	require.Equal(t,
		expectedPkMoveLinkID(1, bat2.Vecs[4].GetBytesAt(0), durable),
		bat2.Vecs[0].GetBytesAt(0),
		"a durable old identity passes the link id rowid part verbatim")
}

func TestPkMoveLinkInsertPos(t *testing.T) {
	mkOwner := func(cols []*plan.ColDef, pk *plan.PrimaryKeyDef) *MultiUpdateCtx {
		return &MultiUpdateCtx{
			TableDef: &plan.TableDef{Cols: cols, Pkey: pk},
		}
	}

	pos, ok := pkMoveLinkInsertPos(mkOwner([]*plan.ColDef{
		{Name: "a", Typ: i32typ},
		{Name: catalog.Row_ID, Typ: rowIdTyp},
	}, &plan.PrimaryKeyDef{PkeyColName: "a"}))
	require.True(t, ok)
	require.Equal(t, 0, pos)

	// pk after another column keeps its insert position; Row_ID is skipped.
	pos, ok = pkMoveLinkInsertPos(mkOwner([]*plan.ColDef{
		{Name: "a", Typ: i32typ},
		{Name: "b", Typ: i32typ, Primary: true},
		{Name: catalog.Row_ID, Typ: rowIdTyp},
	}, &plan.PrimaryKeyDef{PkeyColName: "b"}))
	require.True(t, ok)
	require.Equal(t, 1, pos)

	require.False(t, func() bool { _, ok := pkMoveLinkInsertPos(nil); return ok }())
	require.False(t, func() bool {
		_, ok := pkMoveLinkInsertPos(mkOwner(nil, nil))
		return ok
	}())
	require.False(t, func() bool {
		_, ok := pkMoveLinkInsertPos(mkOwner([]*plan.ColDef{
			{Name: "a", Typ: i32typ},
			{Name: catalog.Row_ID, Typ: rowIdTyp},
		}, &plan.PrimaryKeyDef{PkeyColName: "missing"}))
		return ok
	}())
}

func TestIsUpdatePkMoveLinkCtx(t *testing.T) {
	require.False(t, isUpdatePkMoveLinkCtx(nil))

	ctx := newTestPkMoveLinkCtx(0)
	require.True(t, isUpdatePkMoveLinkCtx(ctx))

	// Any bound column makes it a real write ctx, not the synthetic marker.
	withCols := newTestPkMoveLinkCtx(0)
	withCols.InsertCols = []int{0}
	require.False(t, isUpdatePkMoveLinkCtx(withCols))

	// A same-named non-catalog object must not be treated as the link table.
	otherSchema := newTestPkMoveLinkCtx(0)
	otherSchema.ObjRef = &plan.ObjectRef{SchemaName: "test", ObjName: catalog.MO_DATA_BRANCH_UPDATE_LINK}
	require.False(t, isUpdatePkMoveLinkCtx(otherSchema))

	require.False(t, bytes.Equal(packerEncodeInt32(2), packerEncodeInt32(3)),
		"the fail-closed no-link decision relies on distinct pk encodings")
}

// Only the workspace-staging action records links through updateOneBatch:
// the buffering S3 operator records nothing there (its writer stages the
// links from the buffered batch, see s3WriterDelegate.append), and the
// flush operator never receives actionUpdate batches on current routes.
func TestPkMoveLinkWriteAction(t *testing.T) {
	require.True(t, pkMoveLinkWriteAction(UpdateWriteTable))
	require.False(t, pkMoveLinkWriteAction(UpdateFlushS3Info))
	require.False(t, pkMoveLinkWriteAction(UpdateWriteS3))
}

// The S3 writer hands the link ctx and the EXACT target-deduped batch it
// buffers to the link collector: the buffered batch is the last point where
// the old->new pairing exists, so provenance must be synthesized there.
// The batches are then FERRIED, never written producer-side: a remote
// producer's relation writes land in a mirror workspace the committing
// transaction never sees, so the tail output must carry them to
// UpdateFlushS3Info on the coordinator (actionPkMoveLink).
func TestS3WriterAppendRecordsPkMoveLinks(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	ctxs := prepareTestUpdateMultiUpdateCtx(false, false)
	mainCtx := ctxs[0]
	// Mirror the live plan: the synthetic link ctx carries its own
	// schema-only TableDef whose TblId stays 0 (a per-account virtual
	// catalog table has no plan table id). The writer never opens sinkers
	// or delete batches for it; the def only feeds the config walk.
	linkCtx := newTestPkMoveLinkCtx(0)
	require.True(t, isUpdatePkMoveLinkCtx(linkCtx))
	require.Zero(t, linkCtx.TableDef.TblId, "the live plan leaves the link ctx TblId unset")

	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	update.addAffectedRowsFunc = update.doAddAffectedRows

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.free(proc)) }()
	// newS3Writer binds the owning MultiUpdate's collector; the capture
	// closure replaces it to observe the wiring inside append.
	require.NotNil(t, writer.collectPkMoveLinkRows)

	type linkCall struct {
		link  *MultiUpdateCtx
		owner *MultiUpdateCtx
		rows  int
	}
	var calls []linkCall
	writer.collectPkMoveLinkRows = func(
		_ *process.Process,
		link, owner *MultiUpdateCtx, contextBatch *batch.Batch,
	) (*batch.Batch, error) {
		calls = append(calls, linkCall{link: link, owner: owner, rows: contextBatch.RowCount()})
		out := batch.NewWithSize(1)
		out.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(out.Vecs[0], uint64(contextBatch.RowCount()), false, proc.Mp()))
		out.SetAttributes([]string{"n"})
		out.SetRowCount(1)
		return out, nil
	}

	// The helper halves the last batch: two batches carry 8 and 4 rows,
	// and each buffered batch must produce exactly one link call with the
	// full per-batch row set.
	bats, _ := prepareUpdateTestBatchs(proc.GetMPool(), 2, 8, false, false)
	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-append-test")
	for _, bat := range bats {
		require.NoError(t, writer.append(proc, analyzer, bat))
	}

	require.Len(t, calls, 2)
	for i, want := range []int{8, 4} {
		require.Same(t, linkCtx, calls[i].link)
		require.Same(t, mainCtx, calls[i].owner)
		require.Equal(t, want, calls[i].rows)
	}

	// Buffered (not written): one link batch per buffered input batch is
	// held for the ferry, tagged with the link ctx's updateCtxs slot.
	require.Len(t, writer.pkMoveLinkBufs, 2)
	require.Equal(t, 1, writer.pkMoveLinkBufs[0].idx)
	require.Equal(t, 1, writer.pkMoveLinkBufs[1].idx)

	// The flush ferries both batches to the coordinator as actionPkMoveLink
	// rows carrying the OWNER table id (the link ctx's plan TblId is 0 by
	// design, so it cannot name the relation) plus the serialized batch.
	require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
	require.Nil(t, writer.pkMoveLinkBufs)
	require.NotNil(t, writer.outputBat)
	var ferryRows, ferryWithBatch int
	actions := vector.MustFixedColNoTypeCheck[uint8](writer.outputBat.Vecs[0])
	tables := vector.MustFixedColNoTypeCheck[uint64](writer.outputBat.Vecs[1])
	batData, batArea := vector.MustVarlenaRawData(writer.outputBat.Vecs[4])
	for i, action := range actions {
		if actionType(action) != actionPkMoveLink {
			continue
		}
		ferryRows++
		require.Equal(t, targetTableID(mainCtx), tables[i],
			"the ferry must name the owner table: the receiver resolves the link relation from it")
		if len(batData[i].GetByteSlice(batArea)) > 0 {
			ferryWithBatch++
			got := batch.NewOffHeapEmpty()
			require.NoError(t, got.UnmarshalBinaryWithAnyMp(batData[i].GetByteSlice(batArea), proc.Mp()))
			require.Equal(t, 1, got.RowCount())
			got.Clean(proc.Mp())
		}
	}
	require.Equal(t, 2, ferryRows)
	require.Equal(t, 2, ferryWithBatch)
}

// The coordinator side of the ferry: UpdateFlushS3Info must stage an
// actionPkMoveLink batch into THIS workspace (via the relation resolved
// for the ferried table id), untouched by affected-row accounting, and
// degrade instead of failing when the link table cannot be resolved.
func TestFlushS3InfoStagesFerriedPkMoveLinkBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	analyzer := process.NewAnalyzer(0, false, false, "flush-pkmove-test")

	// Live plan shape: the owner ctx has a real table id, the synthetic
	// link ctx carries a schema-only TableDef whose TblId stays 0.
	ownerCtx, _ := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
	ownerTableID := targetTableID(ownerCtx)
	linkCtx := newTestPkMoveLinkCtx(0)
	require.True(t, isUpdatePkMoveLinkCtx(linkCtx))
	require.Zero(t, linkCtx.TableDef.TblId, "the live plan leaves the link ctx TblId unset")

	linkBat := batch.NewWithSize(1)
	linkBat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	for _, v := range []uint64{11, 22} {
		require.NoError(t, vector.AppendFixed(linkBat.Vecs[0], v, false, proc.Mp()))
	}
	linkBat.SetAttributes([]string{"n"})
	linkBat.SetRowCount(2)
	defer linkBat.Clean(proc.Mp())

	// Encode exactly what the producer ferry emits: the action row names
	// the OWNER table (the link ctx's own TblId is 0 and unusable).
	writer := &s3WriterDelegate{
		updateCtxs: []*MultiUpdateCtx{ownerCtx, linkCtx},
		outputBat:  makeS3OutputBatch(),
	}
	ownerIdx := writer.updateCtxs[1].TargetUpdateCtxIdx
	require.NoError(t, writer.addBatchToOutput(proc.Mp(), actionPkMoveLink, ownerIdx, 2, "", linkBat))
	inputBat := writer.outputBat
	require.Equal(t, ownerTableID,
		vector.MustFixedColNoTypeCheck[uint64](inputBat.Vecs[1])[0])

	mockRel := mock_frontend.NewMockRelation(ctrl)
	var stagedRows int
	var stagedCol []uint64
	mockRel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b *batch.Batch) error {
			stagedRows = b.RowCount()
			stagedCol = append(stagedCol, vector.MustFixedColNoTypeCheck[uint64](b.Vecs[0])...)
			return nil
		}).Times(1)

	// The receiver resolves the link relation through its link ctx (lazy
	// per-account resolution), never through a by-id lookup of 0.
	flush := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
	flush.resetMultiUpdateCtxs()
	lookupUpdateCtxInfo(flush.ctr.updateCtxInfos, linkCtx).Source = mockRel
	flush.addAffectedRowsFunc = flush.doAddAffectedRows
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{inputBat})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	flush.AppendChild(child)
	_, err := flush.updateFlushS3Info(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, 2, stagedRows, "the ferried batch must stage in the coordinator workspace")
	require.Equal(t, []uint64{11, 22}, stagedCol)
	require.Zero(t, flush.GetAffectedRows(), "link writes must never inflate affected rows")

	// Degrade: a disabled link ctx (tenant predating the catalog table)
	// skips the batch instead of failing the flush.
	writer2 := &s3WriterDelegate{
		updateCtxs: []*MultiUpdateCtx{ownerCtx, linkCtx},
		outputBat:  makeS3OutputBatch(),
	}
	require.NoError(t, writer2.addBatchToOutput(proc.Mp(), actionPkMoveLink, ownerIdx, 2, "", linkBat))
	flush2 := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
	flush2.resetMultiUpdateCtxs()
	lookupUpdateCtxInfo(flush2.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled = true
	child2 := colexec.NewMockOperator().WithBatchs([]*batch.Batch{writer2.outputBat})
	t.Cleanup(func() { child2.Free(proc, false, nil) })
	flush2.AppendChild(child2)
	stagedRows = 0
	_, err = flush2.updateFlushS3Info(proc, analyzer)
	require.NoError(t, err, "a disabled ferried link ctx must degrade, not fail the flush")
	require.Zero(t, stagedRows)

	// Degrade: no link ctx matches the named owner table; the batch is
	// skipped instead of failing the flush.
	writer3 := &s3WriterDelegate{
		updateCtxs: []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 424242}}},
		outputBat:  makeS3OutputBatch(),
	}
	require.NoError(t, writer3.addBatchToOutput(proc.Mp(), actionPkMoveLink, 0, 2, "", linkBat))
	flush3 := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
	flush3.resetMultiUpdateCtxs()
	child3 := colexec.NewMockOperator().WithBatchs([]*batch.Batch{writer3.outputBat})
	t.Cleanup(func() { child3.Free(proc, false, nil) })
	flush3.AppendChild(child3)
	_, err = flush3.updateFlushS3Info(proc, analyzer)
	require.NoError(t, err, "an unmatched owner id must degrade, not fail the flush")
}

// newPkMoveLinkOwnerAndBatch builds an owner main-table ctx and one context
// batch carrying a single pk move 1 -> 3.
func newPkMoveLinkOwnerAndBatch(
	t *testing.T, mp *mpool.MPool,
) (ownerCtx *MultiUpdateCtx, contextBatch *batch.Batch) {
	ownerCtx = &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 42,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: "b", Typ: varcharTyp},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey:      &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
			TableType: catalog.SystemOrdinaryRel,
		},
		InsertCols: []int{0, 1},
		DeleteCols: []int{2, 3},
	}
	contextBatch = batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{3})
	contextBatch.Vecs[1] = testutil.MakeVarcharVector([]string{"x"}, nil, mp)
	contextBatch.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{types.BuildTestRowid(1, 1)}, nil, mp)
	contextBatch.Vecs[3] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
	contextBatch.SetRowCount(1)
	t.Cleanup(func() { contextBatch.Clean(mp) })
	return ownerCtx, contextBatch
}

// TestInsertUpdatePkMoveLinkRowsWritePath covers the transactional write
// seam: an injected link-write failure must abort the statement instead of
// silently dropping provenance, a degraded (table-missing) ctx must skip
// without writing, and a healthy write must reach the relation exactly
// once with new_row_id NULL for the workspace to back-fill.
func TestInsertUpdatePkMoveLinkRowsWritePath(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().CreateTS().Return(types.BuildTS(100, 0).ToTimestamp()).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{ID: testPkMoveLinkTxnID}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
	linkCtx := newTestPkMoveLinkCtx(0)

	t.Run("injected link-write failure aborts the statement", func(t *testing.T) {
		injected := moerr.NewInternalErrorNoCtx("pk-move link write failed")
		failRel := mock_frontend.NewMockRelation(ctrl)
		failRel.EXPECT().Write(gomock.Any(), gomock.Any()).Return(injected).Times(1)

		update := &MultiUpdate{}
		update.ctr.updateCtxInfos = map[string]*updateCtxInfo{
			updateCtxKey(linkCtx): {Source: failRel},
		}
		err := update.insertUpdatePkMoveLinkRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
			linkCtx, ownerCtx, contextBatch,
		)
		require.ErrorIs(t, err, injected)
	})

	t.Run("degraded link ctx skips the write", func(t *testing.T) {
		// No Write expectation: any attempt to reach the relation fails.
		unusedRel := mock_frontend.NewMockRelation(ctrl)
		update := &MultiUpdate{}
		update.ctr.updateCtxInfos = map[string]*updateCtxInfo{
			updateCtxKey(linkCtx): {Source: unusedRel, pkMoveLinkDisabled: true},
		}
		require.NoError(t, update.insertUpdatePkMoveLinkRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
			linkCtx, ownerCtx, contextBatch,
		))
	})

	t.Run("healthy write stages one NULL-identity link row", func(t *testing.T) {
		capRel := mock_frontend.NewMockRelation(ctrl)
		var gotRows int
		var newRowNull bool
		capRel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, bat *batch.Batch) error {
				gotRows = bat.RowCount()
				newRowNull = bat.Vecs[7].IsNull(0)
				return nil
			},
		).Times(1)

		update := &MultiUpdate{}
		update.ctr.updateCtxInfos = map[string]*updateCtxInfo{
			updateCtxKey(linkCtx): {Source: capRel},
		}
		require.NoError(t, update.insertUpdatePkMoveLinkRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
			linkCtx, ownerCtx, contextBatch,
		))
		require.Equal(t, 1, gotRows)
		require.True(t, newRowNull)
	})
}

// TestS3WriterSpillsPkMoveLinksOverBudget pins the F5 bounded-retention
// contract: synthesized link batches (same-key continuity markers
// included — lineage healing consumes them) must NOT grow with the whole
// statement. Once retention crosses pkMoveLinkMemBudget the writer spills
// every buffered batch into one object per link ctx and the final ferry
// names the object locations instead of carrying the rows. Two appends
// under a one-byte budget spill twice, the markers are all preserved in
// the ferried block-info, and the retention counter returns to zero.
func TestS3WriterSpillsPkMoveLinksOverBudget(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	ctxs := prepareTestUpdateMultiUpdateCtx(false, false)
	mainCtx := ctxs[0]
	linkCtx := newTestPkMoveLinkCtx(0)

	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	update.addAffectedRowsFunc = update.doAddAffectedRows

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.free(proc)) }()

	// Collect with the real collector at a fixed timestamp: the fixture's
	// old and new pk projections are the same vector, so every input row
	// yields one same-key continuity marker — exactly the row shape the
	// review flagged (bulk payload updates must spill markers too).
	writer.collectPkMoveLinkRows = func(
		p *process.Process,
		link, owner *MultiUpdateCtx, contextBatch *batch.Batch,
	) (*batch.Batch, error) {
		require.Same(t, linkCtx, link)
		require.Same(t, mainCtx, owner)
		return collectUpdatePkMoveLinkRows(
			p, link, owner, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	}

	bats, _ := prepareUpdateTestBatchs(proc.GetMPool(), 2, 8, false, false)
	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-spill-test")
	require.NoError(t, writer.append(proc, analyzer, bats[0]))
	// Append 1: 8 marker rows crossed the one-byte budget and spilled.
	require.Len(t, writer.pkMoveLinkBufs, 1)
	require.Nil(t, writer.pkMoveLinkBufs[0].bat)
	require.NotNil(t, writer.pkMoveLinkBufs[0].spilled)
	require.Equal(t, uint64(8), writer.pkMoveLinkBufs[0].rows)
	require.Zero(t, writer.pkMoveLinkBytes)

	require.NoError(t, writer.append(proc, analyzer, bats[1]))
	// Append 2 spills again: the new in-memory batch of 4 markers joins
	// the already-spilled entry without re-writing its rows.
	require.Len(t, writer.pkMoveLinkBufs, 2)
	for i, want := range []uint64{8, 4} {
		require.Nil(t, writer.pkMoveLinkBufs[i].bat)
		require.NotNil(t, writer.pkMoveLinkBufs[i].spilled)
		require.Equal(t, want, writer.pkMoveLinkBufs[i].rows)
	}
	for _, bat := range bats {
		bat.Clean(proc.GetMPool())
	}

	// The ferry sends one location row per spilled object: the action
	// names the owner table (TblId 0 by design on the link ctx), the
	// name column carries the spill marker, and the batch column carries
	// block-info (block locations + object stats), never link rows.
	require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
	require.Nil(t, writer.pkMoveLinkBufs)
	require.Zero(t, writer.pkMoveLinkBytes)

	actions := vector.MustFixedColNoTypeCheck[uint8](writer.outputBat.Vecs[0])
	tables := vector.MustFixedColNoTypeCheck[uint64](writer.outputBat.Vecs[1])
	nameData, nameArea := vector.MustVarlenaRawData(writer.outputBat.Vecs[3])
	batData, batArea := vector.MustVarlenaRawData(writer.outputBat.Vecs[4])
	var ferryRows int
	for i, action := range actions {
		if actionType(action) != actionPkMoveLink {
			continue
		}
		ferryRows++
		require.Equal(t, targetTableID(mainCtx), tables[i])
		require.Equal(t, pkMoveLinkSpillName, nameData[i].UnsafeGetString(nameArea))
		got := batch.NewOffHeapEmpty()
		require.NoError(t, got.UnmarshalBinaryWithAnyMp(batData[i].GetByteSlice(batArea), proc.Mp()))
		require.Equal(t, 3, got.VectorCount(), "spilled ferries carry block-info (blocks + object stats) plus the cleanup-marker column")
		require.Equal(t, pkMoveLinkSpillMarkerCol, got.Attrs[2],
			"the consumer locates its cleanup ownership by column name")
		require.Equal(t, types.T_varchar, got.Vecs[2].GetType().Oid)
		got.Clean(proc.Mp())
	}
	require.Equal(t, 2, ferryRows)
}

// TestSpilledPkMoveLinkBatchRoundTrip proves the receive half of the F5
// contract end to end: a spilled object's full row set (moves AND
// same-key continuity markers) reads back byte-identically and stages in
// the coordinator workspace exactly like an in-memory ferried batch.
func TestSpilledPkMoveLinkBatchRoundTrip(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	ctxs := prepareTestUpdateMultiUpdateCtx(false, false)
	mainCtx := ctxs[0]
	linkCtx := newTestPkMoveLinkCtx(0)

	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	update.addAffectedRowsFunc = update.doAddAffectedRows

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)

	// Craft link rows through the real collector with a dedicated owner
	// ctx: batch 1 moves keys 3->2, 5->4, 7->6 and marks the unchanged
	// key 1; batch 2 moves key 9->8 (2 moves + 2 markers + 1 move = 5
	// provenance rows total).
	ownerCtx := &MultiUpdateCtx{
		TableDef: &plan.TableDef{
			TblId: 42,
			Name:  "t",
			Cols: []*plan.ColDef{
				{Name: "a", Typ: i32typ, Primary: true},
				{Name: "b", Typ: varcharTyp},
				{Name: catalog.Row_ID, Typ: rowIdTyp},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}},
		},
		InsertCols: []int{0, 1},
		DeleteCols: []int{2, 3},
	}
	linkRows := func(rids []types.Rowid, newPKs, oldPKs []int32) *batch.Batch {
		cb := batch.NewWithSize(4)
		cb.Vecs[0] = testutil.NewInt32Vector(len(newPKs), types.T_int32.ToType(), proc.Mp(), false, nil, newPKs)
		cb.Vecs[1] = testutil.MakeVarcharVector([]string{"x", "y", "z", "w"}, nil, proc.Mp())
		cb.Vecs[2] = testutil.MakeRowIdVector(rids, nil, proc.Mp())
		cb.Vecs[3] = testutil.NewInt32Vector(len(oldPKs), types.T_int32.ToType(), proc.Mp(), false, nil, oldPKs)
		cb.SetRowCount(len(newPKs))
		defer cb.Clean(proc.Mp())
		out, err := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, cb, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, err)
		require.NotNil(t, out)
		return out
	}

	ridA := []types.Rowid{types.BuildTestRowid(1, 1), types.BuildTestRowid(1, 2), types.BuildTestRowid(1, 3), types.BuildTestRowid(1, 4)}
	links1 := linkRows(ridA, []int32{1, 2, 4, 6}, []int32{1, 3, 5, 7})
	require.Equal(t, 4, links1.RowCount())
	ridB := []types.Rowid{types.BuildTestRowid(2, 1)}
	links2 := linkRows(ridB, []int32{8}, []int32{9})
	require.Equal(t, 1, links2.RowCount())

	// Expected link ids (durable row ids pass through the id derivation
	// verbatim). The sinker writes the object pk-sorted by link id, so the
	// read-back order is sorted, not the spill order; the assertions below
	// compare the row SET.
	wantIDs := [][]byte{
		expectedPkMoveLinkID(42, packerEncodeInt32(1), ridA[0]),
		expectedPkMoveLinkID(42, packerEncodeInt32(3), ridA[1]),
		expectedPkMoveLinkID(42, packerEncodeInt32(5), ridA[2]),
		expectedPkMoveLinkID(42, packerEncodeInt32(7), ridA[3]),
		expectedPkMoveLinkID(42, packerEncodeInt32(9), ridB[0]),
	}

	writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
		pkMoveLinkBuf{idx: 1, bat: links1, rows: uint64(links1.RowCount())},
		pkMoveLinkBuf{idx: 1, bat: links2, rows: uint64(links2.RowCount())},
	)
	writer.pkMoveLinkBytes = int64(links1.Size() + links2.Size())

	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-roundtrip-test")
	require.NoError(t, writer.spillPkMoveLinkBufs(proc, analyzer))
	require.Len(t, writer.pkMoveLinkBufs, 1)
	require.Nil(t, writer.pkMoveLinkBufs[0].bat)
	require.NotNil(t, writer.pkMoveLinkBufs[0].spilled)
	require.Equal(t, uint64(5), writer.pkMoveLinkBufs[0].rows)

	require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))

	// Receive side: one location row stages every link row into the
	// coordinator relation.
	mockRel := mock_frontend.NewMockRelation(ctrl)
	var staged *batch.Batch
	mockRel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b *batch.Batch) error {
			staged = batch.NewWithSize(b.VectorCount())
			for i, v := range b.Vecs {
				staged.Vecs[i] = vector.NewVec(*v.GetType())
				require.NoError(t, staged.Vecs[i].UnionBatch(v, 0, v.Length(), nil, proc.Mp()))
			}
			staged.SetAttributes(b.Attrs)
			staged.SetRowCount(b.RowCount())
			return nil
		}).Times(1)

	flush := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	flush.resetMultiUpdateCtxs()
	lookupUpdateCtxInfo(flush.ctr.updateCtxInfos, linkCtx).Source = mockRel
	flush.addAffectedRowsFunc = flush.doAddAffectedRows
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{writer.outputBat})
	t.Cleanup(func() { child.Free(proc, false, nil) })
	flush.AppendChild(child)
	_, err = flush.updateFlushS3Info(proc, analyzer)
	require.NoError(t, err)

	require.NotNil(t, staged)
	require.Equal(t, len(wantIDs), staged.RowCount())
	// The sinker's object is pk-sorted by link id, so the read-back order
	// is the sorted link-id order, not the spill order: the round-trip
	// contract is the full row SET (plus NULL new_row_id and write_ts).
	idVec := staged.Vecs[0]
	wantSet := make(map[string]int, len(wantIDs))
	for _, want := range wantIDs {
		wantSet[string(want)]++
	}
	for i := 0; i < staged.RowCount(); i++ {
		id := string(idVec.GetBytesAt(i))
		require.Contains(t, wantSet, id, "row %d link id must survive the spill round trip", i)
		wantSet[id]--
		require.GreaterOrEqual(t, wantSet[id], 0, "row %d link id duplicated beyond the spilled set", i)
	}
	require.True(t, staged.Vecs[7].GetNulls().Count() == staged.RowCount(),
		"new_row_id stays NULL for the workspace to back-fill, across the spill path too")
	writeTSs := vector.MustFixedColNoTypeCheck[uint64](staged.Vecs[8])
	for _, ts := range writeTSs {
		require.Equal(t, testPkMoveLinkWriteTS, ts)
	}
	staged.Clean(proc.Mp())
}

// TestS3WriterSpillResetLifecycle: an aborted statement whose buffers
// already spilled must release every retained batch at reset, zero the
// retention counter, and let a fresh statement append + ferry freely.
func TestS3WriterSpillResetLifecycle(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	ctxs := prepareTestUpdateMultiUpdateCtx(false, false)
	mainCtx := ctxs[0]
	linkCtx := newTestPkMoveLinkCtx(0)

	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	update.addAffectedRowsFunc = update.doAddAffectedRows

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.free(proc)) }()
	writer.collectPkMoveLinkRows = func(
		p *process.Process,
		link, owner *MultiUpdateCtx, contextBatch *batch.Batch,
	) (*batch.Batch, error) {
		return collectUpdatePkMoveLinkRows(
			p, link, owner, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
	}

	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-reset-test")
	bats, _ := prepareUpdateTestBatchs(proc.GetMPool(), 1, 8, false, false)
	require.NoError(t, writer.append(proc, analyzer, bats[0]))
	require.Len(t, writer.pkMoveLinkBufs, 1)
	require.NotNil(t, writer.pkMoveLinkBufs[0].spilled)

	// Abort-style reset releases every retained batch (in-memory and
	// spilled) and zeroes the counter.
	require.NoError(t, writer.reset(proc))
	require.Nil(t, writer.pkMoveLinkBufs)
	require.Zero(t, writer.pkMoveLinkBytes)

	// The writer stays fully functional for the next statement (the
	// fixture halves the last — and only — batch: 8 rows in, 4 out).
	bats2, _ := prepareUpdateTestBatchs(proc.GetMPool(), 1, 8, false, false)
	require.NoError(t, writer.append(proc, analyzer, bats2[0]))
	require.Len(t, writer.pkMoveLinkBufs, 1)
	require.Equal(t, uint64(4), writer.pkMoveLinkBufs[0].rows)
	require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
	for _, bat := range bats {
		bat.Clean(proc.GetMPool())
	}
	for _, bat := range bats2 {
		bat.Clean(proc.GetMPool())
	}
}

// TestPkMoveLinkSpillObjectsHaveCleanupOwner pins the ownership contract
// for temporary provenance spill objects: every persisted spill object
// has exactly ONE owner at any moment — the producing writer from spill
// until the ferry PUBLISHES it (or until teardown for an unpublished
// one), then the committing consumer, guarded by a durable cleanup
// marker written before the object joins the ferry. Consumption of an
// UNPUBLISHED group only READS the objects (the receiver must never
// delete: an abort between spill and ferry staging may still name them);
// reset and free reclaim every unpublished group's objects and markers;
// a group whose ownership marker cannot be secured is deleted on the
// spot while the spill fails closed, and teardown still reclaims the
// ownership that WAS secured — no spill object ever outlives its
// statement without an owner. A process crash abandons object and
// marker together; the marker's protection window then expires and the
// unpublished-cleanup GC replay is the terminal owner. (The published
// half of the contract — producer teardown keeping its hands off, the
// consumer reclaiming after a successful stage — lives in
// TestPkMoveLinkSpillConsumerOwnership.)
func TestPkMoveLinkSpillObjectsHaveCleanupOwner(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	requireExists := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(proc.Ctx, path)
		require.NoError(t, serr, "expected %s to exist", path)
	}
	requireGone := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(proc.Ctx, path)
		require.True(t, moerr.IsMoErrCode(serr, moerr.ErrFileNotFound),
			"expected %s to be reclaimed, got %v", path, serr)
	}
	requireNoOrphanMarkers := func(t *testing.T) {
		t.Helper()
		markers, remaining, lerr := ioutil.ListUnpublishedObjectCleanup(proc.Ctx, fs, 128)
		require.NoError(t, lerr)
		require.False(t, remaining)
		require.Empty(t, markers, "spill markers must not outlive their writer")
	}

	newSpillWriter := func(t *testing.T, linkCtxs ...*MultiUpdateCtx) (*s3WriterDelegate, *MultiUpdate) {
		mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
		ctxs := append([]*MultiUpdateCtx{mainCtx}, linkCtxs...)
		update := &MultiUpdate{MultiUpdateCtx: ctxs}
		update.resetMultiUpdateCtxs()
		update.addAffectedRowsFunc = update.doAddAffectedRows
		writer, werr := newS3Writer(proc.GetService(), update)
		require.NoError(t, werr)
		writer.collectPkMoveLinkRows = func(
			p *process.Process, link, owner *MultiUpdateCtx, contextBatch *batch.Batch,
		) (*batch.Batch, error) {
			return collectUpdatePkMoveLinkRows(
				p, link, owner, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		}
		return writer, update
	}

	// appendAndSpill feeds one 8-row update batch under the one-byte
	// budget: the writer must persist exactly one spill object and claim
	// ownership of it.
	appendAndSpill := func(t *testing.T, writer *s3WriterDelegate) {
		bats, _ := prepareUpdateTestBatchs(proc.GetMPool(), 1, 8, false, false)
		defer bats[0].Clean(proc.GetMPool())
		require.NoError(t, writer.append(proc,
			process.NewAnalyzer(0, false, false, "pk-move-link-owner-test"), bats[0]))
		require.Len(t, writer.pkMoveLinkBufs, 1)
		require.NotNil(t, writer.pkMoveLinkBufs[0].spilled)
	}

	t.Run("successful consumption only reads and free reclaims objects and markers", func(t *testing.T) {
		linkCtx := newTestPkMoveLinkCtx(0)
		writer, update := newSpillWriter(t, linkCtx)
		appendAndSpill(t, writer)

		files := pkMoveLinkBlockInfoFiles(writer.pkMoveLinkBufs[0].spilled)
		require.NotEmpty(t, files)
		for _, file := range files {
			requireExists(t, file)
		}

		// Successful consumption of the (still unpublished) group READS
		// the spilled rows back through the receiver's read path.
		back, rerr := update.loadSpilledPkMoveLinkBatch(
			proc, writer.pkMoveLinkBufs[0].spilled, linkCtx)
		require.NoError(t, rerr)
		require.Equal(t, writer.pkMoveLinkBufs[0].rows, uint64(back.RowCount()))
		back.Clean(proc.Mp())

		// A read-back must not delete: unpublished objects may still be
		// named by a ferry an abort never staged.
		for _, file := range files {
			requireExists(t, file)
		}
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)
		for _, rec := range owned {
			requireExists(t, rec.marker)
		}

		// free is a terminal owner action: objects and markers are gone.
		require.NoError(t, writer.free(proc))
		for _, file := range files {
			requireGone(t, file)
		}
		for _, rec := range owned {
			requireGone(t, rec.marker)
		}
		require.Len(t, owned, len(files),
			"every spilled file carries exactly one recorded cleanup marker")
		require.Empty(t, writer.pkMoveLinkSpillFiles, "free drops the ownership ledger")
		requireNoOrphanMarkers(t)
	})

	t.Run("reset reclaims objects and markers of an aborted statement", func(t *testing.T) {
		linkCtx := newTestPkMoveLinkCtx(0)
		writer, _ := newSpillWriter(t, linkCtx)
		appendAndSpill(t, writer)

		files := pkMoveLinkBlockInfoFiles(writer.pkMoveLinkBufs[0].spilled)
		require.NotEmpty(t, files)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		require.NoError(t, writer.reset(proc))
		for _, file := range files {
			requireGone(t, file)
		}
		for _, rec := range owned {
			requireGone(t, rec.marker)
		}
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})

	t.Run("a group whose ownership marker fails is deleted inline and the spill fails closed", func(t *testing.T) {
		linkCtx1 := newTestPkMoveLinkCtx(0)
		linkCtx2 := newTestPkMoveLinkCtx(0)
		writer, _ := newSpillWriter(t, linkCtx1, linkCtx2)
		analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-owner-test")

		// Wave 1: one in-memory group spills with healthy markers.
		ownerCtx1, contextBatch1 := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links1, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx1, ownerCtx1, contextBatch1, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links1, rows: uint64(links1.RowCount())})
		writer.pkMoveLinkBytes = int64(links1.Size())
		require.NoError(t, writer.spillPkMoveLinkBufs(proc, analyzer))
		owned1 := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)
		require.NotEmpty(t, owned1)

		// Wave 2: the cleanup-intent write fails for the new group.
		injected := moerr.NewInternalErrorNoCtx("injected marker failure")
		prevRecorder := pkMoveLinkRecordSpillMarker
		var rejected []string
		pkMoveLinkRecordSpillMarker = func(
			_ context.Context, _ fileservice.FileService, obj ioutil.UnpublishedObject,
		) (string, error) {
			rejected = append(rejected, obj.File)
			return "", injected
		}
		t.Cleanup(func() { pkMoveLinkRecordSpillMarker = prevRecorder })

		ownerCtx2, contextBatch2 := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links2, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx2, ownerCtx2, contextBatch2, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 2, bat: links2, rows: uint64(links2.RowCount())})
		writer.pkMoveLinkBytes = int64(links2.Size())

		serr := writer.spillPkMoveLinkBufs(proc, analyzer)
		require.Error(t, serr,
			"a spill that cannot secure cleanup ownership must fail the statement")
		require.Contains(t, serr.Error(), injected.Error())

		// The failed group's objects were deleted inline before the error
		// surfaced; the first group's ownership survives.
		require.NotEmpty(t, rejected)
		for _, file := range rejected {
			requireGone(t, file)
		}
		for _, rec := range owned1 {
			requireExists(t, rec.file)
			requireExists(t, rec.marker)
		}

		// Teardown still reclaims the ownership that was secured.
		require.NoError(t, writer.reset(proc))
		for _, rec := range owned1 {
			requireGone(t, rec.file)
			requireGone(t, rec.marker)
		}
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})
}

// TestPkMoveLinkSpillTerminalCleanup pins the r17 terminal-cleanup contract
// (F6): pipeline teardown cancels proc.Ctx BEFORE operator reset, so a
// cleanup that ran under proc.Ctx would strand every spill for the whole
// crash-recovery window — the live cleanup runs under a fresh bounded
// context. And cleanup is failure-atomic in the right direction: a marker
// is released only AFTER its object is confirmed absent, so a failed
// object delete keeps crash-recovery ownership instead of orphaning the
// file; a failed marker release after a successful object delete is the
// self-healing case (the replay finds the object absent).
func TestPkMoveLinkSpillTerminalCleanup(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	// Probes run under a live context: the canceled-teardown subtest swaps
	// proc.Ctx for a canceled one, and the assertions themselves must not
	// depend on the context under test.
	probeCtx := context.Background()
	requireExists := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(probeCtx, path)
		require.NoError(t, serr, "expected %s to exist", path)
	}
	requireGone := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(probeCtx, path)
		require.True(t, moerr.IsMoErrCode(serr, moerr.ErrFileNotFound),
			"expected %s to be reclaimed, got %v", path, serr)
	}
	requireNoOrphanMarkers := func(t *testing.T) {
		t.Helper()
		markers, remaining, lerr := ioutil.ListUnpublishedObjectCleanup(probeCtx, fs, 128)
		require.NoError(t, lerr)
		require.False(t, remaining)
		require.Empty(t, markers, "spill markers must not outlive their writer")
	}

	newSpillOnce := func(t *testing.T) *s3WriterDelegate {
		mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
		linkCtx := newTestPkMoveLinkCtx(0)
		update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		update.addAffectedRowsFunc = update.doAddAffectedRows
		writer, werr := newS3Writer(proc.GetService(), update)
		require.NoError(t, werr)

		ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links, rows: uint64(links.RowCount())})
		writer.pkMoveLinkBytes = int64(links.Size())
		require.NoError(t, writer.spillPkMoveLinkBufs(proc,
			process.NewAnalyzer(0, false, false, "pk-move-link-cleanup-test")))
		require.NotEmpty(t, writer.pkMoveLinkSpillFiles,
			"the spill must carry recorded ownership")
		return writer
	}

	t.Run("a canceled pipeline context still reclaims objects and markers", func(t *testing.T) {
		writer := newSpillOnce(t)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		// Pipeline.Cleanup cancels proc.Ctx before operator reset runs.
		saved := proc.Ctx
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		proc.Ctx = canceled
		t.Cleanup(func() { proc.Ctx = saved })

		require.NoError(t, writer.reset(proc))
		for _, rec := range owned {
			requireGone(t, rec.file)
			requireGone(t, rec.marker)
		}
		require.Empty(t, writer.pkMoveLinkSpillFiles)
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})

	t.Run("a failed object delete keeps its marker and recovery ownership", func(t *testing.T) {
		writer := newSpillOnce(t)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		injected := moerr.NewInternalErrorNoCtx("injected object delete failure")
		prevDelete := pkMoveLinkDeleteSpillObjects
		pkMoveLinkDeleteSpillObjects = func(
			context.Context, fileservice.FileService, ...string,
		) (int, error) {
			return 0, injected
		}
		t.Cleanup(func() { pkMoveLinkDeleteSpillObjects = prevDelete })

		require.NoError(t, writer.reset(proc))
		for _, rec := range owned {
			// The survivor keeps its marker: crash recovery owns it.
			requireExists(t, rec.file)
			requireExists(t, rec.marker)
		}

		// Restore the seam and reclaim by hand so the fixture stays clean.
		pkMoveLinkDeleteSpillObjects = prevDelete
		for _, rec := range owned {
			_, derr := pkMoveLinkDeleteSpillObjects(proc.Ctx, fs, rec.file)
			require.NoError(t, derr)
			require.NoError(t, pkMoveLinkReleaseSpillMarker(proc.Ctx, fs, rec.marker))
		}
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})

	t.Run("a failed marker release after a confirmed delete self-heals", func(t *testing.T) {
		writer := newSpillOnce(t)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		injected := moerr.NewInternalErrorNoCtx("injected marker release failure")
		prevRelease := pkMoveLinkReleaseSpillMarker
		pkMoveLinkReleaseSpillMarker = func(
			context.Context, fileservice.FileService, string,
		) error {
			return injected
		}
		t.Cleanup(func() { pkMoveLinkReleaseSpillMarker = prevRelease })

		require.NoError(t, writer.reset(proc))
		for _, rec := range owned {
			// The object is gone; its stale marker is the self-healing
			// case — the replay finds the object absent and releases it.
			requireGone(t, rec.file)
			requireExists(t, rec.marker)
		}

		// Emulate the recovery replay's release so the fixture stays clean.
		pkMoveLinkReleaseSpillMarker = prevRelease
		for _, rec := range owned {
			require.NoError(t, pkMoveLinkReleaseSpillMarker(proc.Ctx, fs, rec.marker))
		}
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})
}

// TestPkMoveLinkSpillMarkerPrecedesObject pins the r17 ownership-ordering
// contract (F7): the sinker reserves an object's name BEFORE any byte of
// it persists, and the name-observer hook records the cleanup marker at
// exactly that point — so a process crash at ANY later moment leaves an
// object some owner can reclaim, and a marker write failure aborts the
// sync before the object exists at all.
func TestPkMoveLinkSpillMarkerPrecedesObject(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	requireNoOrphanMarkers := func(t *testing.T) {
		t.Helper()
		markers, remaining, lerr := ioutil.ListUnpublishedObjectCleanup(proc.Ctx, fs, 128)
		require.NoError(t, lerr)
		require.False(t, remaining)
		require.Empty(t, markers, "spill markers must not outlive their writer")
	}

	newSpillWriter := func(t *testing.T) (*s3WriterDelegate, *MultiUpdateCtx) {
		mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
		linkCtx := newTestPkMoveLinkCtx(0)
		update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		update.addAffectedRowsFunc = update.doAddAffectedRows
		writer, werr := newS3Writer(proc.GetService(), update)
		require.NoError(t, werr)
		return writer, linkCtx
	}

	stageOneGroup := func(t *testing.T, writer *s3WriterDelegate, linkCtx *MultiUpdateCtx) {
		ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links, rows: uint64(links.RowCount())})
		writer.pkMoveLinkBytes = int64(links.Size())
	}

	t.Run("every marker lands before any byte of its object", func(t *testing.T) {
		prevRecorder := pkMoveLinkRecordSpillMarker
		var recorded []string
		pkMoveLinkRecordSpillMarker = func(
			ctx context.Context, rfs fileservice.FileService, obj ioutil.UnpublishedObject,
		) (string, error) {
			// At marker time the object must not exist yet — this is the
			// name-reservation-before-persist ordering.
			_, serr := rfs.StatFile(ctx, obj.File)
			require.True(t, moerr.IsMoErrCode(serr, moerr.ErrFileNotFound),
				"cleanup marker for %s must precede every persisted byte", obj.File)
			recorded = append(recorded, obj.File)
			return prevRecorder(ctx, rfs, obj)
		}
		t.Cleanup(func() { pkMoveLinkRecordSpillMarker = prevRecorder })

		writer, linkCtx := newSpillWriter(t)
		stageOneGroup(t, writer, linkCtx)
		require.NoError(t, writer.spillPkMoveLinkBufs(proc,
			process.NewAnalyzer(0, false, false, "pk-move-link-order-test")))
		require.NotEmpty(t, recorded, "the sinker must observe every spill object name")

		// The recorded ownership names exactly the files the ferry will
		// name: block-info files and tracked ownership coincide as sets.
		files := pkMoveLinkBlockInfoFiles(writer.pkMoveLinkBufs[0].spilled)
		require.NotEmpty(t, files)
		tracked := make(map[string]string, len(writer.pkMoveLinkSpillFiles))
		for _, rec := range writer.pkMoveLinkSpillFiles {
			tracked[rec.file] = rec.marker
		}
		require.Len(t, tracked, len(files),
			"every block-info file carries exactly one cleanup marker")
		for _, file := range files {
			marker, ok := tracked[file]
			require.True(t, ok, "block-info file %s has no recorded marker", file)
			_, serr := fs.StatFile(proc.Ctx, marker)
			require.NoError(t, serr, "marker %s must exist", marker)
		}
		require.ElementsMatch(t, recorded, files)

		require.NoError(t, writer.reset(proc))
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.free(proc))
	})

	t.Run("a marker failure persists nothing and fails the spill closed", func(t *testing.T) {
		injected := moerr.NewInternalErrorNoCtx("injected marker failure")
		prevRecorder := pkMoveLinkRecordSpillMarker
		var attempted []string
		pkMoveLinkRecordSpillMarker = func(
			_ context.Context, _ fileservice.FileService, obj ioutil.UnpublishedObject,
		) (string, error) {
			attempted = append(attempted, obj.File)
			return "", injected
		}
		t.Cleanup(func() { pkMoveLinkRecordSpillMarker = prevRecorder })

		writer, linkCtx := newSpillWriter(t)
		stageOneGroup(t, writer, linkCtx)
		serr := writer.spillPkMoveLinkBufs(proc,
			process.NewAnalyzer(0, false, false, "pk-move-link-order-test"))
		require.Error(t, serr)
		require.Contains(t, serr.Error(), injected.Error())

		require.NotEmpty(t, attempted, "the sync attempted to secure ownership")
		for _, file := range attempted {
			// The marker preceded persistence, so the failed group left no
			// object at all — nothing for recovery to discover.
			_, statErr := fs.StatFile(proc.Ctx, file)
			require.True(t, moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound),
				"a group whose marker failed must persist nothing, found %s", file)
		}
		require.Empty(t, writer.pkMoveLinkSpillFiles,
			"a failed group records no ownership")
		require.Empty(t, writer.pkMoveLinkSpillSeen)
		requireNoOrphanMarkers(t)
		require.NoError(t, writer.reset(proc))
		require.NoError(t, writer.free(proc))
	})
}

// TestResolvePkMoveLinkSourceErrorClassification pins the resolution
// contract: only the pre-upgrade MISSING TABLE condition may degrade to
// legacy delete+insert behaviour. Every other resolution failure is
// fatal to the statement — the resolver (getRelationByObjRef) also
// propagates arbitrary Engine.Database / Database.Relation failures, and
// silently degrading on a transient catalog error would commit the
// user's rows WITHOUT their provenance, which a later apply reads as
// proven absence and replays as plain DELETE+INSERT.
func TestResolvePkMoveLinkSourceErrorClassification(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	newUpdate := func(eng engine.Engine) (*MultiUpdate, *MultiUpdateCtx) {
		ownerCtx, _ := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		linkCtx := newTestPkMoveLinkCtx(0)
		update := &MultiUpdate{Action: UpdateWriteTable, Engine: eng,
			MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		return update, linkCtx
	}

	t.Run("the missing-table condition degrades and latches", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		calls := 0
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, string, client.TxnOperator) (engine.Database, error) {
				calls++
				return nil, moerr.NewNoSuchTableNoCtx(catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK)
			}).AnyTimes()
		update, linkCtx := newUpdate(eng)

		rel, err := update.resolvePkMoveLinkSource(proc, linkCtx)
		require.NoError(t, err)
		require.Nil(t, rel)
		require.True(t, lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled)

		// The sticky disable avoids re-resolution: a second resolve must
		// not consult the engine again.
		rel, err = update.resolvePkMoveLinkSource(proc, linkCtx)
		require.NoError(t, err)
		require.Nil(t, rel)
		require.Equal(t, 1, calls)
	})

	t.Run("a transient catalog error is fatal and never latches", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		calls := 0
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, string, client.TxnOperator) (engine.Database, error) {
				calls++
				if calls == 1 {
					return nil, moerr.NewInternalErrorNoCtx("transient catalog failure")
				}
				return database, nil
			}).AnyTimes()
		update, linkCtx := newUpdate(eng)

		rel, err := update.resolvePkMoveLinkSource(proc, linkCtx)
		require.Error(t, err, "a non-missing-table resolution failure must abort the statement")
		require.Nil(t, rel)
		require.False(t, lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled,
			"a transient failure must never latch the legacy degrade: user rows would commit without provenance")

		// The retried statement resolves cleanly and records provenance.
		rel, err = update.resolvePkMoveLinkSource(proc, linkCtx)
		require.NoError(t, err)
		require.NotNil(t, rel)
	})

	t.Run("a relation-level failure is fatal unless it is the missing table", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		database := mock_frontend.NewMockDatabase(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

		// Generic relation failure: fatal.
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			nil, moerr.NewInternalErrorNoCtx("relation scan failed")).Times(1)
		update, linkCtx := newUpdate(eng)
		rel, err := update.resolvePkMoveLinkSource(proc, linkCtx)
		require.Error(t, err)
		require.Nil(t, rel)
		require.False(t, lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled)

		// The same lookup answering NewNoSuchTable: the supported
		// pre-upgrade degrade.
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			nil, moerr.NewNoSuchTableNoCtx(catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK)).Times(1)
		update2, linkCtx2 := newUpdate(eng)
		rel, err = update2.resolvePkMoveLinkSource(proc, linkCtx2)
		require.NoError(t, err)
		require.Nil(t, rel)
		require.True(t, lookupUpdateCtxInfo(update2.ctr.updateCtxInfos, linkCtx2).pkMoveLinkDisabled)
	})

	t.Run("a missing database is never the missing-table degrade", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(
			nil, moerr.NewBadDBNoCtx(catalog.MO_CATALOG)).Times(1)
		update, linkCtx := newUpdate(eng)
		rel, err := update.resolvePkMoveLinkSource(proc, linkCtx)
		require.Error(t, err)
		require.Nil(t, rel)
		require.False(t, lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled)
	})
}

// TestPreparePropagatesPkMoveLinkResolutionErrors drives the eager
// Prepare resolution: a catalog failure there must abort the statement
// (Prepare returns the error, so the user's update and its provenance
// never stage), while the missing-table condition degrades with the
// sticky disable installed.
func TestPreparePropagatesPkMoveLinkResolutionErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ownerObjRef := &plan.ObjectRef{Schema: 1, Obj: 1, SchemaName: "test", ObjName: "t"}
	newPrepareUpdate := func(eng engine.Engine) (*MultiUpdate, *MultiUpdateCtx) {
		ownerCtx, _ := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		ownerCtx.ObjRef = ownerObjRef
		linkCtx := newTestPkMoveLinkCtx(0)
		update := &MultiUpdate{Action: UpdateWriteTable, Engine: eng,
			MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
		return update, linkCtx
	}

	t.Run("a generic catalog failure aborts Prepare", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(relation, nil).AnyTimes()
		database.EXPECT().Relation(gomock.Any(), catalog.MO_DATA_BRANCH_UPDATE_LINK, gomock.Any()).Return(
			nil, moerr.NewInternalErrorNoCtx("catalog scan failed")).Times(1)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

		update, _ := newPrepareUpdate(eng)
		err := update.Prepare(proc)
		require.Error(t, err, "Prepare must propagate the catalog failure: the statement aborts, user rows and provenance together")
	})

	t.Run("the missing link table degrades Prepare with the sticky disable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(relation, nil).AnyTimes()
		database.EXPECT().Relation(gomock.Any(), catalog.MO_DATA_BRANCH_UPDATE_LINK, gomock.Any()).Return(
			nil, moerr.NewNoSuchTableNoCtx(catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_UPDATE_LINK)).Times(1)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

		update, linkCtx := newPrepareUpdate(eng)
		require.NoError(t, update.Prepare(proc))
		require.True(t, lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx).pkMoveLinkDisabled)
	})
}

// TestPkMoveLinkSpillConsumerOwnership proves the r18 review's
// producer/consumer ownership finding with a deterministic barrier: once
// the ferry PUBLISHES a spill group's block-info, the producer's full
// teardown (reset AND free) runs FIRST, while the committing consumer's
// read-back is still pending — exactly the pipeline window where
// Connector.SendBatch has handed the metadata downstream but the
// receiver fragment has not read the objects yet. The objects and their
// markers must survive the teardown (producer hands ownership off at
// publication), the paused consumer's read-back must then succeed, and
// the consumer's successful stage is what reclaims objects and markers.
// A consumer-side reclaim failure keeps the marker — and with it the
// crash-recovery ownership — without touching the committed statement.
func TestPkMoveLinkSpillConsumerOwnership(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	probeCtx := context.Background()
	requireExists := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(probeCtx, path)
		require.NoError(t, serr, "expected %s to exist", path)
	}
	requireGone := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(probeCtx, path)
		require.True(t, moerr.IsMoErrCode(serr, moerr.ErrFileNotFound),
			"expected %s to be reclaimed, got %v", path, serr)
	}
	requireNoOrphanMarkers := func(t *testing.T) {
		t.Helper()
		markers, remaining, lerr := ioutil.ListUnpublishedObjectCleanup(probeCtx, fs, 128)
		require.NoError(t, lerr)
		require.False(t, remaining)
		require.Empty(t, markers, "spill markers must not outlive their owners")
	}

	mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
	linkCtx := newTestPkMoveLinkCtx(0)

	// newProducerWriter stages one link group and spills it through the
	// production path: one durable spill group with recorded ownership.
	newProducerWriter := func(t *testing.T) *s3WriterDelegate {
		update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		update.addAffectedRowsFunc = update.doAddAffectedRows
		writer, werr := newS3Writer(proc.GetService(), update)
		require.NoError(t, werr)

		ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links, rows: uint64(links.RowCount())})
		writer.pkMoveLinkBytes = int64(links.Size())
		require.NoError(t, writer.spillPkMoveLinkBufs(proc,
			process.NewAnalyzer(0, false, false, "pk-move-link-consumer-test")))
		require.NotEmpty(t, writer.pkMoveLinkSpillFiles,
			"the spill must carry recorded ownership")
		return writer
	}

	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-consumer-test")

	t.Run("teardown before read-back: objects survive, consumption reclaims", func(t *testing.T) {
		writer := newProducerWriter(t)
		rows := writer.pkMoveLinkBufs[0].rows
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		// The ferry publishes the spill group and hands the wire image
		// downstream; the producer holds no further ownership.
		require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
		for _, rec := range writer.pkMoveLinkSpillFiles {
			require.True(t, rec.published,
				"the ferry published the spill group's ownership")
		}
		wire, merr := writer.outputBat.MarshalBinary()
		require.NoError(t, merr)

		// BARRIER: full producer teardown runs BEFORE the consumer reads
		// a single object byte. Published objects and markers survive.
		require.NoError(t, writer.reset(proc))
		require.NoError(t, writer.free(proc))
		for _, rec := range owned {
			requireExists(t, rec.file)
			requireExists(t, rec.marker)
		}

		// The paused consumer now runs: unmarshal the wire image exactly
		// like the receiver fragment, read the objects back (they must
		// still exist), stage the links, then reclaim.
		raw := batch.NewOffHeapEmpty()
		defer raw.Clean(proc.Mp())
		require.NoError(t, raw.UnmarshalBinaryWithAnyMp(wire, proc.Mp()))

		var staged *batch.Batch
		mockRel := mock_frontend.NewMockRelation(ctrl)
		mockRel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, b *batch.Batch) error {
				staged = batch.NewWithSize(b.VectorCount())
				for i, v := range b.Vecs {
					staged.Vecs[i] = vector.NewVec(*v.GetType())
					require.NoError(t, staged.Vecs[i].UnionBatch(v, 0, v.Length(), nil, proc.Mp()))
				}
				staged.SetAttributes(b.Attrs)
				staged.SetRowCount(b.RowCount())
				return nil
			}).Times(1)
		flush := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		flush.resetMultiUpdateCtxs()
		lookupUpdateCtxInfo(flush.ctr.updateCtxInfos, linkCtx).Source = mockRel
		flush.addAffectedRowsFunc = flush.doAddAffectedRows
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{raw})
		defer child.Free(proc, false, nil)
		flush.AppendChild(child)

		_, ferr := flush.updateFlushS3Info(proc, analyzer)
		require.NoError(t, ferr, "the read-back and staging succeed after producer teardown")
		require.NotNil(t, staged, "the consumer staged the spilled links")
		require.Equal(t, int(rows), staged.RowCount(),
			"every spilled link row arrived (their objects outlived the producer)")
		staged.Clean(proc.Mp())

		// The successful stage transferred the last ownership: objects
		// deleted, markers released, no orphan left for the GC replay.
		for _, rec := range owned {
			requireGone(t, rec.file)
			requireGone(t, rec.marker)
		}
		requireNoOrphanMarkers(t)
	})

	t.Run("a failed consumer reclaim keeps crash-recovery ownership", func(t *testing.T) {
		writer := newProducerWriter(t)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)

		require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
		wire, merr := writer.outputBat.MarshalBinary()
		require.NoError(t, merr)
		require.NoError(t, writer.reset(proc))
		require.NoError(t, writer.free(proc))

		injected := moerr.NewInternalErrorNoCtx("injected consumer delete failure")
		prevDelete := pkMoveLinkDeleteSpillObjects
		pkMoveLinkDeleteSpillObjects = func(
			context.Context, fileservice.FileService, ...string,
		) (int, error) {
			return 0, injected
		}
		t.Cleanup(func() { pkMoveLinkDeleteSpillObjects = prevDelete })

		raw := batch.NewOffHeapEmpty()
		defer raw.Clean(proc.Mp())
		require.NoError(t, raw.UnmarshalBinaryWithAnyMp(wire, proc.Mp()))

		mockRel := mock_frontend.NewMockRelation(ctrl)
		mockRel.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		flush := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		flush.resetMultiUpdateCtxs()
		lookupUpdateCtxInfo(flush.ctr.updateCtxInfos, linkCtx).Source = mockRel
		flush.addAffectedRowsFunc = flush.doAddAffectedRows
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{raw})
		defer child.Free(proc, false, nil)
		flush.AppendChild(child)

		// The statement itself commits: the reclaim failure is a cleanup
		// concern, never a staging one.
		_, ferr := flush.updateFlushS3Info(proc, analyzer)
		require.NoError(t, ferr, "a reclaim failure must never fail the committed statement")
		for _, rec := range owned {
			// A failed object delete keeps the marker: recovery keeps ownership.
			requireExists(t, rec.file)
			requireExists(t, rec.marker)
		}

		// Restore the seam and reclaim the way the recovery replay would.
		pkMoveLinkDeleteSpillObjects = prevDelete
		for _, rec := range owned {
			_, derr := pkMoveLinkDeleteSpillObjects(probeCtx, fs, rec.file)
			require.NoError(t, derr)
			require.NoError(t, pkMoveLinkReleaseSpillMarker(probeCtx, fs, rec.marker))
		}
		requireNoOrphanMarkers(t)
	})
}

// TestS3WriterSpillOwnershipResetsBetweenExecutions pins the lifecycle of
// one REUSED s3 writer's spill bookkeeping: Prepare keeps the writer
// across pipeline executions, so reset must shed the transferred (already
// published) records WITHOUT deleting their objects or markers — a paused
// consumer still owns their read-back — and must arm a fresh protection
// job and deadline per execution, or a later execution's spill inherits
// the first execution's expiry and crash-recovery can reclaim a live
// unconsumed object early.
func TestS3WriterSpillOwnershipResetsBetweenExecutions(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer ctrl.Finish()
	defer proc.Free()

	fs, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)

	prevBudget := pkMoveLinkMemBudget
	pkMoveLinkMemBudget = 1
	t.Cleanup(func() { pkMoveLinkMemBudget = prevBudget })

	requireExists := func(t *testing.T, path string) {
		t.Helper()
		_, serr := fs.StatFile(context.Background(), path)
		require.NoError(t, serr, "expected %s to exist", path)
	}

	mainCtx := prepareTestUpdateMultiUpdateCtx(false, false)[0]
	linkCtx := newTestPkMoveLinkCtx(0)
	analyzer := process.NewAnalyzer(0, false, false, "pk-move-link-spill-reset-test")

	newProducerWriter := func(t *testing.T) *s3WriterDelegate {
		update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{mainCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		update.addAffectedRowsFunc = update.doAddAffectedRows
		writer, werr := newS3Writer(proc.GetService(), update)
		require.NoError(t, werr)

		ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links, rows: uint64(links.RowCount())})
		writer.pkMoveLinkBytes = int64(links.Size())
		require.NoError(t, writer.spillPkMoveLinkBufs(proc, analyzer))
		require.NotEmpty(t, writer.pkMoveLinkSpillFiles,
			"the spill must carry recorded ownership")
		return writer
	}

	t.Run("reset drops transferred records without deleting their files", func(t *testing.T) {
		writer := newProducerWriter(t)
		owned := append([]pkMoveLinkSpillFile(nil), writer.pkMoveLinkSpillFiles...)
		firstJob := writer.pkMoveLinkSpillJob
		require.NotEmpty(t, firstJob, "the first execution's spill armed a protection job")

		// Execution 1 publishes its spill group through the real ferry.
		require.NoError(t, writer.flushTailAndWriteToOutput(proc, analyzer))
		for _, rec := range writer.pkMoveLinkSpillFiles {
			require.True(t, rec.published,
				"the ferry published the spill group's ownership")
		}

		// The writer is reused for the next execution: reset must shed the
		// transferred ledger and arm a fresh lease, while a paused consumer
		// still owns the transferred objects and markers.
		require.NoError(t, writer.reset(proc))
		require.Empty(t, writer.pkMoveLinkSpillFiles,
			"transferred records leave the producer ledger at the execution boundary")
		require.Empty(t, writer.pkMoveLinkSpillSeen,
			"the name ledger resets between executions")
		require.Equal(t, "", writer.pkMoveLinkSpillJob,
			"a fresh protection job is armed per execution")
		require.Zero(t, writer.pkMoveLinkSpillValid,
			"the first execution's deadline must not carry over")
		for _, rec := range owned {
			requireExists(t, rec.file)
			requireExists(t, rec.marker)
		}

		// Execution 2 actually spills: a NEW job and a FRESH deadline.
		ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		links, lerr := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch, testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, lerr)
		writer.pkMoveLinkBufs = append(writer.pkMoveLinkBufs,
			pkMoveLinkBuf{idx: 1, bat: links, rows: uint64(links.RowCount())})
		writer.pkMoveLinkBytes = int64(links.Size())
		require.NoError(t, writer.spillPkMoveLinkBufs(proc, analyzer))
		require.NotEmpty(t, writer.pkMoveLinkSpillJob)
		require.NotEqual(t, firstJob, writer.pkMoveLinkSpillJob,
			"the second execution never inherits the first execution's job")
		require.Greater(t, writer.pkMoveLinkSpillValid, time.Now().UnixNano(),
			"the second execution's deadline is freshly minted")
		require.NoError(t, writer.reset(proc))
		require.NoError(t, writer.free(proc))
	})

	t.Run("an expired inherited lease re-mints on the next spill", func(t *testing.T) {
		writer := newProducerWriter(t)
		defer func() {
			require.NoError(t, writer.reset(proc))
			require.NoError(t, writer.free(proc))
		}()
		// Inject a prior execution's EXPIRED lease that a boundary failed
		// to clear: the next track must re-mint instead of extending
		// coverage under an already-expired deadline.
		writer.pkMoveLinkSpillJob = "pk-move-link-spill-stale"
		writer.pkMoveLinkSpillValid = time.Now().Add(-time.Hour).UnixNano()
		before := len(writer.pkMoveLinkSpillFiles)
		fake := writer.pkMoveLinkSpillFiles[0].file + ".second"
		require.NoError(t, writer.trackPkMoveLinkSpillObject(fs, context.Background(), fake))
		require.Len(t, writer.pkMoveLinkSpillFiles, before+1)
		stray := writer.pkMoveLinkSpillFiles[before]
		t.Cleanup(func() {
			_ = fs.Delete(context.Background(), stray.marker)
			_ = fs.Delete(context.Background(), stray.file)
		})
		writer.pkMoveLinkSpillFiles = writer.pkMoveLinkSpillFiles[:before]
		require.NotEqual(t, "pk-move-link-spill-stale", writer.pkMoveLinkSpillJob,
			"an expired lease re-mints on the next spill")
		require.Greater(t, writer.pkMoveLinkSpillValid, time.Now().UnixNano(),
			"the re-minted deadline covers the new spill")
	})
}

// newTestPkMoveRetentionDef mirrors the wide seven-column retention DDL
// (row_kind, table_id, database_name, table_name, obj_id, created_ts,
// logical_id) plus the materialized composite primary key and the rowid
// column the layout walker skips; the production definition comes from the
// catalog loader, so the fabric version fixes the attribute ORDER here.
func newTestPkMoveRetentionDef() *plan.TableDef {
	return &plan.TableDef{
		TblId:  9001,
		DbName: catalog.MO_CATALOG,
		Name:   catalog.MO_DATA_BRANCH_LINK_RETENTION,
		Cols: []*plan.ColDef{
			{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: catalog.DataBranchLinkRetentionColRowKind, Typ: plan.Type{Id: int32(types.T_int8)}},
			{Name: catalog.DataBranchLinkRetentionColTableID, Typ: plan.Type{Id: int32(types.T_uint64)}},
			{Name: catalog.DataBranchLinkRetentionColDatabaseName, Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: catalog.DataBranchLinkRetentionColTableName, Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535}},
			{Name: catalog.DataBranchLinkRetentionColObjID, Typ: plan.Type{Id: int32(types.T_uint64)}},
			{Name: catalog.DataBranchLinkRetentionColCreatedTS, Typ: plan.Type{Id: int32(types.T_uint64)}},
			{Name: catalog.DataBranchLinkRetentionColLogicalID, Typ: plan.Type{Id: int32(types.T_uint64)}},
			{Name: catalog.Row_ID, Typ: rowIdTyp},
		},
	}
}

// newTestPkMoveRetentionOwnerCtx builds the owner descriptor a retention
// identity row is minted from: ObjRef supplies the catalog names, TableDef
// the durable logical id.
func newTestPkMoveRetentionOwnerCtx() *MultiUpdateCtx {
	return &MultiUpdateCtx{
		ObjRef: &plan.ObjectRef{SchemaName: "db1", ObjName: "parent_t"},
		TableDef: &plan.TableDef{
			TblId:     4242,
			DbName:    "db1",
			Name:      "parent_t",
			LogicalId: 7777,
			Cols: []*plan.ColDef{
				{Name: "id", Typ: i32typ, Primary: true},
			},
		},
	}
}

// expectedPkMoveRetentionCpkey composes the composite primary key exactly
// the way the producer must: serial(row_kind, table_id).
func expectedPkMoveRetentionCpkey(t *testing.T, mp *mpool.MPool, rowKind int8, tableID uint64) []byte {
	t.Helper()
	kindVec := vector.NewVec(types.T_int8.ToType())
	defer kindVec.Free(mp)
	require.NoError(t, vector.AppendFixed(kindVec, rowKind, false, mp))
	tableVec := vector.NewVec(types.T_uint64.ToType())
	defer tableVec.Free(mp)
	require.NoError(t, vector.AppendFixed(tableVec, tableID, false, mp))
	packer := types.NewPacker()
	defer packer.Close()
	encKind, err := planfunction.NewSerialValueEncoder(kindVec)
	require.NoError(t, err)
	encTable, err := planfunction.NewSerialValueEncoder(tableVec)
	require.NoError(t, err)
	encKind(kindVec, 0, packer)
	encTable(tableVec, 0, packer)
	return append([]byte(nil), packer.Bytes()...)
}

// TestNewPkMoveRetentionBatch pins the identity-row composition: def-order
// layout with the rowid column skipped, the serial composite key, the
// owner-supplied names, and the durable logical id.
func TestNewPkMoveRetentionBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	newSource := func(def *plan.TableDef) engine.Relation {
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		return rel
	}

	t.Run("composes the def-order identity row with the serial cpkey", func(t *testing.T) {
		source := newSource(newTestPkMoveRetentionDef())
		ownerCtx := newTestPkMoveRetentionOwnerCtx()
		bat, err := newPkMoveRetentionBatch(proc, source, ownerCtx, 4242, 999)
		require.NoError(t, err)
		defer bat.Clean(mp)

		require.Equal(t, 1, bat.RowCount())
		require.Equal(t, []string{
			catalog.CPrimaryKeyColName,
			catalog.DataBranchLinkRetentionColRowKind,
			catalog.DataBranchLinkRetentionColTableID,
			catalog.DataBranchLinkRetentionColDatabaseName,
			catalog.DataBranchLinkRetentionColTableName,
			catalog.DataBranchLinkRetentionColObjID,
			catalog.DataBranchLinkRetentionColCreatedTS,
			catalog.DataBranchLinkRetentionColLogicalID,
		}, []string(bat.Attrs))
		require.Equal(t, expectedPkMoveRetentionCpkey(t, mp, 0, 4242),
			bat.Vecs[0].GetBytesAt(0))
		require.Equal(t, int8(catalog.DataBranchLinkRetentionRowKindReal),
			vector.MustFixedColNoTypeCheck[int8](bat.Vecs[1])[0])
		require.Equal(t, uint64(4242), vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])[0])
		require.Equal(t, "db1", string(bat.Vecs[3].GetBytesAt(0)))
		require.Equal(t, "parent_t", string(bat.Vecs[4].GetBytesAt(0)))
		require.Equal(t, uint64(4242), vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[5])[0])
		require.Equal(t, uint64(999), vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[6])[0])
		require.Equal(t, uint64(7777), vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[7])[0],
			"the owner descriptor's durable logical id rides the row")
	})

	t.Run("a nil owner descriptor records logical id 0 for engine back-fill", func(t *testing.T) {
		source := newSource(newTestPkMoveRetentionDef())
		ownerCtx := newTestPkMoveRetentionOwnerCtx()
		ownerCtx.TableDef = nil
		bat, err := newPkMoveRetentionBatch(proc, source, ownerCtx, 4242, 999)
		require.NoError(t, err)
		defer bat.Clean(mp)
		require.Equal(t, uint64(0), vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[7])[0])
	})

	t.Run("missing definitions fail instead of composing junk", func(t *testing.T) {
		nilRel := mock_frontend.NewMockRelation(ctrl)
		nilRel.EXPECT().GetTableDef(gomock.Any()).Return(nil).AnyTimes()
		_, err := newPkMoveRetentionBatch(proc, nilRel, newTestPkMoveRetentionOwnerCtx(), 4242, 999)
		require.Error(t, err)

		source := newSource(newTestPkMoveRetentionDef())
		ownerCtx := newTestPkMoveRetentionOwnerCtx()
		ownerCtx.ObjRef = nil
		_, err = newPkMoveRetentionBatch(proc, source, ownerCtx, 4242, 999)
		require.Error(t, err)
	})

	t.Run("an unexpected retention column fails loudly", func(t *testing.T) {
		def := newTestPkMoveRetentionDef()
		def.Cols = append(def.Cols, &plan.ColDef{Name: "mystery", Typ: i32typ})
		source := newSource(def)
		_, err := newPkMoveRetentionBatch(proc, source, newTestPkMoveRetentionOwnerCtx(), 4242, 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected retention column")
	})
}

// TestStagePkMoveRetentionRows drives the producer staging seam: the marked
// genesis write carries the composed identity row with the recording
// transaction's create timestamp, disabled lanes write nothing, and an
// injected write failure propagates verbatim.
func TestStagePkMoveRetentionRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().CreateTS().Return(types.BuildTS(100, 0).ToTimestamp()).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ownerCtx := newTestPkMoveRetentionOwnerCtx()
	linkCtx := newTestPkMoveLinkCtx(0)

	newUpdate := func() (*MultiUpdate, *updateCtxInfo) {
		update := &MultiUpdate{Action: UpdateWriteTable,
			MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
		require.NotNil(t, info)
		return update, info
	}

	t.Run("healthy staging marks the write and carries the create timestamp", func(t *testing.T) {
		update, info := newUpdate()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(newTestPkMoveRetentionDef()).AnyTimes()
		info.retentionSource = rel

		prevFn := pkMoveGenesisWriteFn
		t.Cleanup(func() { pkMoveGenesisWriteFn = prevFn })
		var (
			gotMarked bool
			gotRows   int
			gotTs     uint64
			gotTable  uint64
			gotLog    uint64
		)
		pkMoveGenesisWriteFn = func(
			source engine.Relation, ctx context.Context, bat *batch.Batch,
		) error {
			require.Same(t, rel, source)
			gotMarked = disttae.PkMoveGenesisWriteMarked(ctx)
			gotRows = bat.RowCount()
			gotTs = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[6])[0]
			gotTable = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[2])[0]
			gotLog = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[7])[0]
			return nil
		}
		require.NoError(t, update.stagePkMoveRetentionRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"), linkCtx, ownerCtx))
		require.True(t, gotMarked,
			"the workspace admission must distinguish producer genesis writes from funnel upserts")
		require.Equal(t, 1, gotRows)
		require.Equal(t, uint64(100), gotTs,
			"created_ts is the recording transaction's create timestamp")
		require.Equal(t, uint64(4242), gotTable)
		require.Equal(t, uint64(7777), gotLog)
	})

	t.Run("disabled lanes stage nothing", func(t *testing.T) {
		update, info := newUpdate()
		info.pkMoveLinkDisabled = true
		prevFn := pkMoveGenesisWriteFn
		t.Cleanup(func() { pkMoveGenesisWriteFn = prevFn })
		pkMoveGenesisWriteFn = func(
			engine.Relation, context.Context, *batch.Batch,
		) error {
			t.Fatal("a disabled lane must never reach the write seam")
			return nil
		}
		require.NoError(t, update.stagePkMoveRetentionRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"), linkCtx, ownerCtx))

		update2, info2 := newUpdate()
		info2.pkMoveRetentionDisabled = true
		require.NoError(t, update2.stagePkMoveRetentionRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"), linkCtx, ownerCtx))
	})

	t.Run("an injected write failure propagates verbatim", func(t *testing.T) {
		update, info := newUpdate()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(newTestPkMoveRetentionDef()).AnyTimes()
		info.retentionSource = rel

		injected := moerr.NewInternalErrorNoCtx("retention write blew up")
		prevFn := pkMoveGenesisWriteFn
		t.Cleanup(func() { pkMoveGenesisWriteFn = prevFn })
		pkMoveGenesisWriteFn = func(
			engine.Relation, context.Context, *batch.Batch,
		) error {
			return injected
		}
		err := update.stagePkMoveRetentionRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"), linkCtx, ownerCtx)
		require.ErrorIs(t, err, injected)
	})
}

// TestInsertUpdatePkMoveLinkRowsRetentionFailureIsAtomic pins the write
// ORDER at the seam: retention genesis stages first, so an injected genesis
// failure fails the statement BEFORE the link write — the positional
// workspace cut then removes both, never committing provenance without its
// identity.
func TestInsertUpdatePkMoveLinkRowsRetentionFailureIsAtomic(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().CreateTS().Return(types.BuildTS(100, 0).ToTimestamp()).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{ID: testPkMoveLinkTxnID}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ownerCtx, contextBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
	ownerCtx.ObjRef = &plan.ObjectRef{SchemaName: "db1", ObjName: "t"}
	ownerCtx.TableDef.LogicalId = 8888
	linkCtx := newTestPkMoveLinkCtx(0)

	// The link relation carries NO Write expectation: any attempted link
	// write fails the controller, proving staging precedes it.
	linkRel := mock_frontend.NewMockRelation(ctrl)
	retRel := mock_frontend.NewMockRelation(ctrl)
	retRel.EXPECT().GetTableDef(gomock.Any()).Return(newTestPkMoveRetentionDef()).AnyTimes()

	update := &MultiUpdate{Action: UpdateWriteTable,
		MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	require.NotNil(t, info)
	info.Source = linkRel
	info.retentionSource = retRel

	injected := moerr.NewInternalErrorNoCtx("retention genesis refused")
	prevFn := pkMoveGenesisWriteFn
	t.Cleanup(func() { pkMoveGenesisWriteFn = prevFn })
	pkMoveGenesisWriteFn = func(
		engine.Relation, context.Context, *batch.Batch,
	) error {
		return injected
	}

	err := update.insertUpdatePkMoveLinkRows(
		proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
		linkCtx, ownerCtx, contextBatch)
	require.ErrorIs(t, err, injected)
}

// ---------------------------------------------------------------------------
// D218(C.1) THE ACTUAL-MOVE ADMISSION GATE PINS. A continuity-only
// (all-marker) link batch invokes NO retention genesis on EITHER staging
// route — the genesis write seam and the engine-side probe spy both count
// ZERO, no staged seal — while the marker rows themselves keep flowing to
// the workspace fill hook; a batch carrying one real moved-address row
// stages genesis exactly once. The direct predicate cells pin the same
// equality the fill hook's marker consumption uses, plus the fail-closed
// shape for an unlocatable column pair.
// ---------------------------------------------------------------------------

// newGateSameKeyBatch builds the owner context batch for a same-key
// replacement (new pk bytes == old pk bytes): the link collector emits one
// continuity marker and no move.
func newGateSameKeyBatch(mp *mpool.MPool) *batch.Batch {
	contextBatch := batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
	contextBatch.Vecs[1] = testutil.MakeVarcharVector([]string{"p"}, nil, mp)
	contextBatch.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{types.BuildTestRowid(1, 1)}, nil, mp)
	contextBatch.Vecs[3] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
	contextBatch.SetRowCount(1)
	return contextBatch
}

// newGateMixedBatch carries one same-key marker row (pk 1 -> 1) ahead of
// one real moved-address row (pk 2 -> 5).
func newGateMixedBatch(mp *mpool.MPool) *batch.Batch {
	contextBatch := batch.NewWithSize(4)
	contextBatch.Vecs[0] = testutil.NewInt32Vector(2, types.T_int32.ToType(), mp, false, nil, []int32{1, 5})
	contextBatch.Vecs[1] = testutil.MakeVarcharVector([]string{"p", "q"}, nil, mp)
	contextBatch.Vecs[2] = testutil.MakeRowIdVector(
		[]types.Rowid{types.BuildTestRowid(1, 1), types.BuildTestRowid(1, 2)}, nil, mp)
	contextBatch.Vecs[3] = testutil.NewInt32Vector(2, types.T_int32.ToType(), mp, false, nil, []int32{1, 2})
	contextBatch.SetRowCount(2)
	return contextBatch
}

func TestPkMoveLinkBatchHasRealMove(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ownerCtx, moveBatch := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
	linkCtx := newTestPkMoveLinkCtx(0)

	collect := func(t *testing.T, contextBatch *batch.Batch) *batch.Batch {
		bat, err := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch,
			testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, err)
		require.NotNil(t, bat)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		return bat
	}

	t.Run("a real moved-address row admits", func(t *testing.T) {
		require.True(t, pkMoveLinkBatchHasRealMove(collect(t, moveBatch)))
	})
	t.Run("a marker-only batch admits nothing", func(t *testing.T) {
		same := newGateSameKeyBatch(proc.Mp())
		defer same.Clean(proc.Mp())
		require.False(t, pkMoveLinkBatchHasRealMove(collect(t, same)))
	})
	t.Run("a mixed batch admits on its one real move", func(t *testing.T) {
		mixed := newGateMixedBatch(proc.Mp())
		defer mixed.Clean(proc.Mp())
		require.True(t, pkMoveLinkBatchHasRealMove(collect(t, mixed)))
	})
	t.Run("empty and nil batches admit nothing", func(t *testing.T) {
		require.False(t, pkMoveLinkBatchHasRealMove(nil))
		empty := newPkMoveLinkBatch(proc, linkCtx)
		defer empty.Clean(proc.Mp())
		empty.SetRowCount(0)
		require.False(t, pkMoveLinkBatchHasRealMove(empty))
	})
	t.Run("an unlocatable column pair fails closed", func(t *testing.T) {
		drift := batch.NewWithSize(1)
		drift.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), proc.Mp(), false, nil, []int32{7})
		drift.SetAttributes([]string{"mystery"})
		drift.SetRowCount(1)
		defer drift.Clean(proc.Mp())
		require.True(t, pkMoveLinkBatchHasRealMove(drift),
			"provenance must never commit without identity on a drifted layout")
	})
}

func TestPkMoveRetentionGenesisAdmissionGate(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().CreateTS().Return(types.BuildTS(100, 0).ToTimestamp()).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{ID: testPkMoveLinkTxnID}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ownerCtx, _ := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
	ownerCtx.ObjRef = &plan.ObjectRef{SchemaName: "db1", ObjName: "t"}
	ownerCtx.TableDef.LogicalId = 8888
	linkCtx := newTestPkMoveLinkCtx(0)

	// The genesis write seam and the engine-side probe both stay reachable
	// by identity: a continuity-only batch must never reach either. The
	// probe fires engine-side behind a real workspace admission, so the
	// fabric relation route leaves it at zero on every arm — the move arms'
	// live probe witness lives in the two-session cells (D218(C.2)).
	var genesisWrites int
	prevFn := pkMoveGenesisWriteFn
	pkMoveGenesisWriteFn = func(engine.Relation, context.Context, *batch.Batch) error {
		genesisWrites++
		return nil
	}
	var probeCalls int
	prevProbe := disttae.PkMoveGenesisProbe
	disttae.PkMoveGenesisProbe = func(
		context.Context, *process.Process, engine.Engine, uint32, uint64, string, bool,
	) (bool, bool, uint64, error) {
		probeCalls++
		return false, false, 0, nil
	}
	t.Cleanup(func() {
		pkMoveGenesisWriteFn = prevFn
		disttae.PkMoveGenesisProbe = prevProbe
	})

	var linkWrites, linkRows int
	linkRel := mock_frontend.NewMockRelation(ctrl)
	linkRel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, bat *batch.Batch) error {
			linkWrites++
			linkRows += bat.RowCount()
			return nil
		}).AnyTimes()
	retRel := mock_frontend.NewMockRelation(ctrl)
	retRel.EXPECT().GetTableDef(gomock.Any()).Return(newTestPkMoveRetentionDef()).AnyTimes()

	update := &MultiUpdate{Action: UpdateWriteTable,
		MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
	update.resetMultiUpdateCtxs()
	info := lookupUpdateCtxInfo(update.ctr.updateCtxInfos, linkCtx)
	require.NotNil(t, info)
	info.Source = linkRel
	info.retentionSource = retRel

	runWorkspace := func(t *testing.T, contextBatch *batch.Batch) {
		require.NoError(t, update.insertUpdatePkMoveLinkRows(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
			linkCtx, ownerCtx, contextBatch))
	}
	runFerry := func(t *testing.T, contextBatch *batch.Batch) {
		linkBat, err := collectUpdatePkMoveLinkRows(
			proc, linkCtx, ownerCtx, contextBatch,
			testPkMoveLinkWriteTS, testPkMoveLinkTxnID)
		require.NoError(t, err)
		require.NotNil(t, linkBat)
		defer linkBat.Clean(proc.Mp())
		data, err := linkBat.MarshalBinary()
		require.NoError(t, err)
		require.NoError(t, update.flushPkMoveLinkBatch(
			proc, process.NewAnalyzer(0, false, false, "MultiUpdate"),
			42, "", data))
	}

	for _, tc := range []struct {
		name         string
		contextBatch func(mp *mpool.MPool) *batch.Batch
		wantGenesis  int
		wantRows     int
	}{
		{"a continuity-only batch", func(mp *mpool.MPool) *batch.Batch { return newGateSameKeyBatch(mp) }, 0, 1},
		{"a moved-address batch", func(mp *mpool.MPool) *batch.Batch {
			b := batch.NewWithSize(4)
			b.Vecs[0] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{3})
			b.Vecs[1] = testutil.MakeVarcharVector([]string{"x"}, nil, mp)
			b.Vecs[2] = testutil.MakeRowIdVector([]types.Rowid{types.BuildTestRowid(1, 1)}, nil, mp)
			b.Vecs[3] = testutil.NewInt32Vector(1, types.T_int32.ToType(), mp, false, nil, []int32{1})
			b.SetRowCount(1)
			return b
		}, 1, 1},
		{"a mixed marker+move batch", func(mp *mpool.MPool) *batch.Batch { return newGateMixedBatch(mp) }, 1, 2},
	} {
		contextBatch := tc.contextBatch(proc.Mp())
		for _, route := range []struct {
			name string
			run  func(t *testing.T, contextBatch *batch.Batch)
		}{
			{"workspace", runWorkspace},
			{"ferry", runFerry},
		} {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				beforeWrites, beforeLinks, beforeRows, beforeProbes :=
					genesisWrites, linkWrites, linkRows, probeCalls
				route.run(t, contextBatch)
				require.Equal(t, tc.wantGenesis, genesisWrites-beforeWrites,
					"genesis admission follows the batch's real moved-address rows only")
				require.Equal(t, 1, linkWrites-beforeLinks,
					"the batch itself always stages: markers reach the fill hook")
				require.Equal(t, tc.wantRows, linkRows-beforeRows)
				require.Equal(t, 0, probeCalls-beforeProbes,
					"no fabric-route probe: gating the write gates every downstream admission cost")
			})
		}
		contextBatch.Clean(proc.Mp())
	}
	require.Equal(t, 4, genesisWrites,
		"exactly the move-carrying batches admitted genesis, once per route per shape")
}

// TestResolvePkMoveRetentionSourceErrorClassification mirrors the link
// resolver's contract for the retention relation: only the pre-upgrade
// missing-table condition degrades (and latches), every other catalog
// failure is fatal and never latches.
func TestResolvePkMoveRetentionSourceErrorClassification(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	newUpdate := func(eng engine.Engine) (*MultiUpdate, *MultiUpdateCtx) {
		ownerCtx, _ := newPkMoveLinkOwnerAndBatch(t, proc.Mp())
		linkCtx := newTestPkMoveLinkCtx(0)
		update := &MultiUpdate{Action: UpdateWriteTable, Engine: eng,
			MultiUpdateCtx: []*MultiUpdateCtx{ownerCtx, linkCtx}}
		update.resetMultiUpdateCtxs()
		return update, linkCtx
	}

	t.Run("the missing-table condition degrades and latches", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		calls := 0
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, string, client.TxnOperator) (engine.Database, error) {
				calls++
				return nil, moerr.NewNoSuchTableNoCtx(
					catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION)
			}).AnyTimes()
		update, linkCtx := newUpdate(eng)

		rel, err := update.resolvePkMoveRetentionSource(proc, linkCtx)
		require.NoError(t, err)
		require.Nil(t, rel)
		require.True(t, lookupUpdateCtxInfo(
			update.ctr.updateCtxInfos, linkCtx).pkMoveRetentionDisabled)

		rel, err = update.resolvePkMoveRetentionSource(proc, linkCtx)
		require.NoError(t, err)
		require.Nil(t, rel)
		require.Equal(t, 1, calls, "the sticky disable avoids re-resolution")
	})

	t.Run("a transient catalog error is fatal and never latches", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		calls := 0
		database := mock_frontend.NewMockDatabase(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, string, client.TxnOperator) (engine.Database, error) {
				calls++
				if calls == 1 {
					return nil, moerr.NewInternalErrorNoCtx("transient catalog failure")
				}
				return database, nil
			}).AnyTimes()
		update, linkCtx := newUpdate(eng)

		rel, err := update.resolvePkMoveRetentionSource(proc, linkCtx)
		require.Error(t, err)
		require.Nil(t, rel)
		require.False(t, lookupUpdateCtxInfo(
			update.ctr.updateCtxInfos, linkCtx).pkMoveRetentionDisabled,
			"a transient failure must never latch: link rows would commit without identity")

		rel, err = update.resolvePkMoveRetentionSource(proc, linkCtx)
		require.NoError(t, err)
		require.NotNil(t, rel)
	})
}
