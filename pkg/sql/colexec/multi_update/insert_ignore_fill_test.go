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

package multi_update

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbPlan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type multiUpdateIgnoreWarningSession struct {
	total    uint64
	codes    []uint16
	messages []string
}

func (*multiUpdateIgnoreWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*multiUpdateIgnoreWarningSession) AddTempTable(string, string, string)        {}
func (*multiUpdateIgnoreWarningSession) RemoveTempTable(string, string)             {}
func (*multiUpdateIgnoreWarningSession) RemoveTempTableByRealName(string)           {}
func (*multiUpdateIgnoreWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}

func (s *multiUpdateIgnoreWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.codes = append(s.codes, codes[i])
		s.messages = append(s.messages, messages[i])
	}
}

func newIgnoreProfiledProc(t *testing.T) (context.Context, *gomock.Controller, *process.Process, *multiUpdateIgnoreWarningSession) {
	t.Helper()
	ctx, ctrl, proc := prepareTestCtx(t, false)
	session := &multiUpdateIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	return ctx, ctrl, proc, session
}

func newIgnoreFillMainHarness(
	t *testing.T,
	ctrl *gomock.Controller,
	withClusterByHelper bool,
) (*MultiUpdate, *MultiUpdateCtx, *[]*batch.Batch) {
	t.Helper()

	cols := []*pbPlan.ColDef{
		{ColId: 0, Name: "a", Typ: i32typ, Default: &pbPlan.Default{NullAbility: true}},
		{ColId: 1, Name: "nn", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
	}
	insertCols := []int{0, 1}
	if withClusterByHelper {
		cols = append(cols, &pbPlan.ColDef{
			ColId: 2, Name: catalog.PrefixCBColName + "helper",
			Typ: i32typ, Default: &pbPlan.Default{NullAbility: false},
		})
		insertCols = append(insertCols, 2)
	}

	tableDef := &pbPlan.TableDef{Name: "t_ig_fill", Cols: cols, Pkey: &pbPlan.PrimaryKeyDef{}}
	updateCtx := &MultiUpdateCtx{TableDef: tableDef, InsertCols: insertCols}

	written := make([]*batch.Batch, 0, 1)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			written = append(written, b)
			return nil
		}).AnyTimes()

	op := &MultiUpdate{
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{
				tableDef.Name: {
					Source:       rel,
					tableType:    UpdateMainTable,
					isContiguous: isContiguousMapping(updateCtx.InsertCols),
				},
			},
			insertBuf: make([]*batch.Batch, 1),
		},
	}
	op.addAffectedRowsFunc = op.doAddAffectedRows
	return op, updateCtx, &written
}

func makeIgnoreFillInputBatch(mp *mpool.MPool, withClusterByHelper bool) *batch.Batch {
	attrs := []string{"a", "nn"}
	vecs := []*vector.Vector{
		testutil.MakeInt32Vector([]int32{10, 11, 12}, nil, mp),
		testutil.MakeInt32Vector([]int32{10, 7, 12}, []uint64{1}, mp),
	}
	if withClusterByHelper {
		attrs = append(attrs, catalog.PrefixCBColName+"helper")
		vecs = append(vecs,
			testutil.MakeInt32Vector([]int32{4, 5, 6}, []uint64{0, 1, 2}, mp))
	}
	bat := batch.New(attrs)
	bat.Vecs = vecs
	bat.SetRowCount(3)
	return bat
}

// TestInsertMainTableIgnoreFillsInsteadOfAborting is the multi_update-path
// red/green pair for issue #30906: a batch with one NULL NOT NULL cell aborts
// the statement without IGNORE, while statement IGNORE adjusts the cell to
// the implicit default, keeps every row and publishes one warning.
func TestInsertMainTableIgnoreFillsInsteadOfAborting(t *testing.T) {
	// Red leg: without the IGNORE profile the constraint violation stays fatal.
	{
		_, ctrl, proc := prepareTestCtx(t, false)
		defer ctrl.Finish()
		defer proc.Free()

		op, _, written := newIgnoreFillMainHarness(t, ctrl, false)
		input := makeIgnoreFillInputBatch(proc.GetMPool(), false)
		defer input.Clean(proc.GetMPool())

		err := op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
		require.Empty(t, *written)
		op.Free(proc, true, err)
	}

	// Green leg: statement IGNORE adjusts the NULL cell and keeps the row.
	{
		_, ctrl, proc, session := newIgnoreProfiledProc(t)
		defer ctrl.Finish()
		defer proc.Free()

		op, _, written := newIgnoreFillMainHarness(t, ctrl, false)
		input := makeIgnoreFillInputBatch(proc.GetMPool(), false)
		defer input.Clean(proc.GetMPool())

		require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
		require.Len(t, *written, 1)
		got := (*written)[0]
		require.Equal(t, 3, got.RowCount())
		nn := vector.MustFixedColNoTypeCheck[int32](got.Vecs[1])
		require.Equal(t, []int32{10, 0, 12}, nn,
			"the NULL cell was adjusted to the implicit default")
		require.False(t, got.Vecs[1].GetNulls().Contains(1))
		require.Equal(t, uint64(1), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.Equal(t, []string{"Column 'nn' cannot be null"}, session.messages)
		op.Free(proc, false, nil)
	}
}

// TestInsertMainTableIgnoreFillsAfterPkNullFilter pins the replace-shape leg:
// rows with a NULL primary key are dropped by the filter first, and the NOT
// NULL fill applies only to the surviving rows, so dropped rows never
// publish adjustment warnings.
func TestInsertMainTableIgnoreFillsAfterPkNullFilter(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	op, _, written := newMainTablePkSecondHarness(t, ctrl)
	input := makePkSecondInputBatch(mp, []int32{5, 6}, nil,
		[]int64{0, 2}, []uint64{0}, []int32{0, 0}, []uint64{0, 1})
	defer input.Clean(mp)

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Len(t, *written, 1)
	got := (*written)[0]
	require.Equal(t, 1, got.RowCount(), "the NULL PK row was filtered out")
	require.Equal(t, int32(6), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[0], 0))
	ids := vector.MustFixedColNoTypeCheck[int64](got.Vecs[1])
	require.Equal(t, int64(2), ids[0])
	require.False(t, got.Vecs[2].IsNull(0), "the surviving NULL cell was adjusted")
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[2], 0))
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
	require.Equal(t, []string{"Column 'b' cannot be null"}, session.messages)
	op.Free(proc, false, nil)
}

// TestInsertMainTableIgnoreSkipsClusterByHelperColumns keeps the strict-check
// parity: the composite cluster-by helper column is excluded from both the
// strict NOT NULL check and the IGNORE adjustment, and its NULL passes
// through without any warning.
func TestInsertMainTableIgnoreSkipsClusterByHelperColumns(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()

	op, _, written := newIgnoreFillMainHarness(t, ctrl, true)
	input := makeIgnoreFillInputBatch(proc.GetMPool(), true)
	defer input.Clean(proc.GetMPool())

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Len(t, *written, 1)
	got := (*written)[0]
	require.Equal(t, 3, got.RowCount())
	nn := vector.MustFixedColNoTypeCheck[int32](got.Vecs[1])
	require.Equal(t, []int32{10, 0, 12}, nn)
	require.True(t, got.Vecs[2].GetNulls().Contains(1),
		"the cluster-by helper NULL passes through untouched")
	require.Equal(t, uint64(1), session.total,
		"only the NOT NULL column published an adjustment warning")
	require.Equal(t, []string{"Column 'nn' cannot be null"}, session.messages)
	op.Free(proc, false, nil)
}

// newIgnoreFillPkDropHarness mirrors newMainTablePkSecondHarness without the
// replace-shaped SkipInsertOnNullPk flag, so the only null-primary-key
// handling left is the statement-IGNORE row drop. The primary key is the
// SECOND column and InsertPkColIdx deliberately stays unset: ordinary INSERT
// plans never populate that REPLACE-only field (it defaults to column 0), so
// the resolver must find the key through TableDef.Pkey by name or the rule
// silently moves to the wrong column for every key-second table.
func newIgnoreFillPkDropHarness(
	t *testing.T,
	ctrl *gomock.Controller,
) (*MultiUpdate, *MultiUpdateCtx, *[]*batch.Batch) {
	t.Helper()

	tableDef := &pbPlan.TableDef{
		Name: "t_ig_pkdrop",
		Cols: []*pbPlan.ColDef{
			{ColId: 0, Name: "a", Typ: i32typ, Default: &pbPlan.Default{NullAbility: true}},
			{ColId: 1, Name: "id", Typ: i64typ, NotNull: true, Primary: true, Default: &pbPlan.Default{NullAbility: false}},
			{ColId: 2, Name: "b", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
		},
		Pkey: &pbPlan.PrimaryKeyDef{
			Cols:        []uint64{1},
			PkeyColId:   1,
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	updateCtx := &MultiUpdateCtx{
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2},
		// InsertPkColIdx intentionally omitted exactly like an ordinary
		// INSERT plan: the resolver must work from TableDef.Pkey alone.
	}

	written := make([]*batch.Batch, 0, 1)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			written = append(written, b)
			return nil
		}).AnyTimes()

	op := &MultiUpdate{
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{
				tableDef.Name: {
					Source:       rel,
					tableType:    UpdateMainTable,
					isContiguous: isContiguousMapping(updateCtx.InsertCols),
				},
			},
			insertBuf: make([]*batch.Batch, 1),
		},
	}
	op.addAffectedRowsFunc = op.doAddAffectedRows
	return op, updateCtx, &written
}

func makeIgnorePkDropInputBatch(mp *mpool.MPool) *batch.Batch {
	// rows: (a=5, id=NULL, b=0), (a=6, id=2, b=NULL), (a=NULL, id=NULL, b=1)
	attrs := []string{"a", "id", "b"}
	bat := batch.New(attrs)
	bat.Vecs = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{5, 6, 0}, []uint64{2}, mp),
		testutil.MakeInt64Vector([]int64{0, 2, 0}, []uint64{0, 2}, mp),
		testutil.MakeInt32Vector([]int32{0, 9, 1}, []uint64{1}, mp),
	}
	bat.SetRowCount(3)
	return bat
}

// TestInsertMainTableIgnoreDropsNullPkRowsAndWarns pins the primary-key
// invariant of the IGNORE fill: a NULL single-column key would be adjusted
// after the duplicate pipeline already matched the original values, so the
// row is skipped with one ER_BAD_NULL_ERROR warning instead, while surviving
// rows keep the ordinary NOT NULL adjustment.
func TestInsertMainTableIgnoreDropsNullPkRowsAndWarns(t *testing.T) {
	// Strict leg: without IGNORE the NULL primary key stays a fatal
	// constraint violation.
	{
		_, ctrl, proc := prepareTestCtx(t, false)
		defer ctrl.Finish()
		defer proc.Free()

		op, _, written := newIgnoreFillPkDropHarness(t, ctrl)
		input := makeIgnorePkDropInputBatch(proc.GetMPool())
		defer input.Clean(proc.GetMPool())

		err := op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
		require.Empty(t, *written)
		op.Free(proc, true, err)
	}

	// IGNORE leg: NULL-key rows are dropped and warned once each, and the
	// NULL cell of a surviving row is still adjusted.
	{
		_, ctrl, proc, session := newIgnoreProfiledProc(t)
		defer ctrl.Finish()
		defer proc.Free()

		op, _, written := newIgnoreFillPkDropHarness(t, ctrl)
		input := makeIgnorePkDropInputBatch(proc.GetMPool())
		defer input.Clean(proc.GetMPool())

		require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
		require.Len(t, *written, 1)
		got := (*written)[0]
		require.Equal(t, 1, got.RowCount(), "both NULL-key rows were dropped")
		require.Equal(t, int32(6), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[0], 0))
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[1], 0))
		require.False(t, got.Vecs[2].IsNull(0), "the surviving NULL cell was adjusted")
		require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[2], 0))
		// The fill runs before the drop: the surviving row's b adjustment
		// publishes first, then the two dropped key rows.
		require.Equal(t, uint64(3), session.total)
		require.Equal(t, []uint16{
			moerr.ER_BAD_NULL_ERROR,
			moerr.ER_BAD_NULL_ERROR,
			moerr.ER_BAD_NULL_ERROR,
		}, session.codes)
		require.Equal(t, []string{
			"Column 'b' cannot be null",
			"Column 'id' cannot be null",
			"Column 'id' cannot be null",
		}, session.messages)
		op.Free(proc, false, nil)
	}
}

// TestInsertMainTableIgnoreDropsAllNullPkBatch verifies the empty remainder:
// when every row carries a NULL key, no batch is written at all and the
// dropped rows are still reported.
func TestInsertMainTableIgnoreDropsAllNullPkBatch(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	op, _, written := newIgnoreFillPkDropHarness(t, ctrl)
	attrs := []string{"a", "id", "b"}
	input := batch.New(attrs)
	input.Vecs = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{5, 6}, []uint64{1}, mp),
		testutil.MakeInt64Vector([]int64{0, 0}, []uint64{0, 1}, mp),
		testutil.MakeInt32Vector([]int32{9, 9}, nil, mp),
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Empty(t, *written, "no row survives the drop, nothing is written")
	require.Equal(t, uint64(2), session.total)
	require.Equal(t, []string{
		"Column 'id' cannot be null",
		"Column 'id' cannot be null",
	}, session.messages)
	op.Free(proc, false, nil)
}

// TestInsertMainTableIgnoreKeepsNullablePseudoPkColumn pins the regression the
// unique-key table exposed: when the resolved primary-key slot points at a
// nullable column, several NULL keys are legal, exactly like a nullable
// unique key accepting any number of NULLs, so no row is dropped and no
// pseudo-key warning is published; only the NOT NULL adjustment applies.
func TestInsertMainTableIgnoreKeepsNullablePseudoPkColumn(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	tableDef := &pbPlan.TableDef{
		Name: "t_ig_nullable_slot",
		Cols: []*pbPlan.ColDef{
			{ColId: 0, Name: "a", Typ: i32typ, Default: &pbPlan.Default{NullAbility: true}},
			{ColId: 1, Name: "c1", Typ: i64typ, Default: &pbPlan.Default{NullAbility: true}},
			{ColId: 2, Name: "b", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
		},
		Pkey: &pbPlan.PrimaryKeyDef{},
	}
	updateCtx := &MultiUpdateCtx{
		TableDef:       tableDef,
		InsertCols:     []int{0, 1, 2},
		InsertPkColIdx: 1,
	}
	written := make([]*batch.Batch, 0, 1)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			written = append(written, b)
			return nil
		}).AnyTimes()
	op := &MultiUpdate{
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{
				tableDef.Name: {
					Source:       rel,
					tableType:    UpdateMainTable,
					isContiguous: isContiguousMapping(updateCtx.InsertCols),
				},
			},
			insertBuf: make([]*batch.Batch, 1),
		},
	}
	op.addAffectedRowsFunc = op.doAddAffectedRows

	attrs := []string{"a", "c1", "b"}
	input := batch.New(attrs)
	input.Vecs = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{5, 0}, []uint64{1}, mp),
		testutil.MakeInt64Vector([]int64{0, 0}, []uint64{0, 1}, mp),
		testutil.MakeInt32Vector([]int32{9, 0}, []uint64{1}, mp),
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Len(t, written, 1)
	got := written[0]
	require.Equal(t, 2, got.RowCount(), "both nullable-slot rows survive")
	require.True(t, got.Vecs[1].GetNulls().Contains(0), "the first NULL key passes through")
	require.True(t, got.Vecs[1].GetNulls().Contains(1), "the second NULL key passes through")
	require.False(t, got.Vecs[2].GetNulls().Contains(1), "the NOT NULL cell was adjusted")
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[2], 1))
	require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[2], 0))
	require.Equal(t, uint64(1), session.total, "only the NOT NULL adjustment warned")
	require.Equal(t, []string{"Column 'b' cannot be null"}, session.messages)
	op.Free(proc, false, nil)
}

// TestInsertNotNullFillOwnedTracksStatementShape pins the INSERT-owned gate:
// pipelines whose main context only inserts (INSERT, LOAD, insert-shaped
// ON DUPLICATE KEY statements) own the IGNORE NOT NULL adjustment, while
// UPDATE and REPLACE pipelines pair their insert legs with delete legs and
// must keep the strict NOT NULL rejection.
func TestInsertNotNullFillOwnedTracksStatementShape(t *testing.T) {
	tableDef := &pbPlan.TableDef{Name: "t_ig_owned", Cols: []*pbPlan.ColDef{
		{ColId: 0, Name: "a", Typ: i32typ, Default: &pbPlan.Default{NullAbility: true}},
	}}
	insertOnly := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{
		{TableDef: tableDef, InsertCols: []int{0}},
	}}
	require.True(t, insertOnly.insertNotNullFillOwned(),
		"an INSERT-shaped pipeline owns the fill")

	withDeletes := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{
		{TableDef: tableDef, InsertCols: []int{0}, DeleteCols: []int{0}},
	}}
	require.False(t, withDeletes.insertNotNullFillOwned(),
		"an UPDATE-shaped pipeline never owns the fill")

	require.False(t, (&MultiUpdate{}).insertNotNullFillOwned(),
		"a contextless operator never owns the fill")
}

// TestFillMainInsertNotNullForIgnoreSkipsUpdateLegs pins the INSERT-owned
// gate at the write side: under statement IGNORE the same shared code that
// INSERT legs run must be a no-op for the update legs, so a dynamic NULL
// keeps the fatal constraint violation that the update pipeline has always
// raised and no NULL cell is silently adjusted anywhere in the batch.
func TestFillMainInsertNotNullForIgnoreSkipsUpdateLegs(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	op, ctx, _ := newIgnoreFillPkDropHarness(t, ctrl)
	input := makeIgnorePkDropInputBatch(mp)
	defer input.Clean(mp)
	filled, dropped, err := fillMainInsertNotNullForIgnore(proc, ctx, input, false, false)
	require.NoError(t, err)
	require.False(t, filled)
	require.False(t, dropped)

	// Nothing changed: the NULL NOT NULL cells and the NULL key cells are
	// all still NULL, and no warning was published.
	require.True(t, input.Vecs[1].GetNulls().Contains(0))
	require.True(t, input.Vecs[1].GetNulls().Contains(2))
	require.True(t, input.Vecs[2].GetNulls().Contains(1))
	require.Equal(t, uint64(0), session.total)
	op.Free(proc, false, nil)
}

// TestInsertMainTableIgnoreDropsNullPkRowsOffColumnZero pins the name-based
// key resolution for the position the planner never encoded: with the
// single-column NOT NULL primary key in the THIRD slot and a NOT NULL
// non-key column in the first, the drop must remove the rows whose key cell
// is NULL and adjust the survivor, never the other way around.
func TestInsertMainTableIgnoreDropsNullPkRowsOffColumnZero(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	tableDef := &pbPlan.TableDef{
		Name: "t_ig_pk_third",
		Cols: []*pbPlan.ColDef{
			{ColId: 0, Name: "nn", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
			{ColId: 1, Name: "a", Typ: i32typ, Default: &pbPlan.Default{NullAbility: true}},
			{ColId: 2, Name: "id", Typ: i64typ, NotNull: true, Primary: true, Default: &pbPlan.Default{NullAbility: false}},
		},
		Pkey: &pbPlan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	// InsertPkColIdx intentionally omitted: ordinary INSERT never sets it and
	// the zero value points at the first column, which here enforces NOT
	// NULL but is not the key.
	updateCtx := &MultiUpdateCtx{TableDef: tableDef, InsertCols: []int{0, 1, 2}}
	written := make([]*batch.Batch, 0, 1)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			written = append(written, b)
			return nil
		}).AnyTimes()
	op := &MultiUpdate{
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{
				tableDef.Name: {
					Source:       rel,
					tableType:    UpdateMainTable,
					isContiguous: isContiguousMapping(updateCtx.InsertCols),
				},
			},
			insertBuf: make([]*batch.Batch, 1),
		},
	}
	op.addAffectedRowsFunc = op.doAddAffectedRows

	// rows: (nn=1, a=NULL, id=NULL) and (nn=NULL, a=5, id=7)
	attrs := []string{"nn", "a", "id"}
	input := batch.New(attrs)
	input.Vecs = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{1, 0}, []uint64{1}, mp),
		testutil.MakeInt32Vector([]int32{0, 5}, []uint64{0}, mp),
		testutil.MakeInt64Vector([]int64{0, 7}, []uint64{0}, mp),
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Len(t, written, 1)
	got := written[0]
	require.Equal(t, 1, got.RowCount(), "only the NULL-key row was dropped")
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](got.Vecs[2], 0))
	require.False(t, got.Vecs[0].GetNulls().Contains(0), "the survivor was adjusted")
	require.Equal(t, []string{
		"Column 'nn' cannot be null",
		"Column 'id' cannot be null",
	}, session.messages)
	op.Free(proc, false, nil)
}

// TestInsertMainTableIgnoreCompositePkMembersFollowFill pins that composite
// keys keep the fill path: every member column is adjusted with its own
// per-cell warning and no single-column row drop engages, because a
// composite key owns no single cell to resolve.
func TestInsertMainTableIgnoreCompositePkMembersFollowFill(t *testing.T) {
	_, ctrl, proc, session := newIgnoreProfiledProc(t)
	defer ctrl.Finish()
	defer proc.Free()
	mp := proc.GetMPool()

	tableDef := &pbPlan.TableDef{
		Name: "t_ig_comp_pk",
		Cols: []*pbPlan.ColDef{
			{ColId: 0, Name: "k1", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
			{ColId: 1, Name: "k2", Typ: i32typ, NotNull: true, Default: &pbPlan.Default{NullAbility: false}},
		},
		Pkey: &pbPlan.PrimaryKeyDef{
			PkeyColName: "k1-k2",
			Names:       []string{"k1", "k2"},
		},
	}
	updateCtx := &MultiUpdateCtx{TableDef: tableDef, InsertCols: []int{0, 1}}
	written := make([]*batch.Batch, 0, 1)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().
		Write(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *batch.Batch) error {
			written = append(written, b)
			return nil
		}).AnyTimes()
	op := &MultiUpdate{
		MultiUpdateCtx: []*MultiUpdateCtx{updateCtx},
		ctr: container{
			updateCtxInfos: map[string]*updateCtxInfo{
				tableDef.Name: {
					Source:       rel,
					tableType:    UpdateMainTable,
					isContiguous: isContiguousMapping(updateCtx.InsertCols),
				},
			},
			insertBuf: make([]*batch.Batch, 1),
		},
	}
	op.addAffectedRowsFunc = op.doAddAffectedRows

	attrs := []string{"k1", "k2"}
	input := batch.New(attrs)
	input.Vecs = []*vector.Vector{
		testutil.MakeInt32Vector([]int32{0, 3}, []uint64{0}, mp),
		testutil.MakeInt32Vector([]int32{2, 0}, []uint64{1}, mp),
	}
	input.SetRowCount(2)
	defer input.Clean(mp)

	require.NoError(t, op.insert_main_table(proc, process.NewTempAnalyzer(), 0, input))
	require.Len(t, written, 1)
	got := written[0]
	require.Equal(t, 2, got.RowCount(), "composite keys keep every row")
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[0], 0))
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](got.Vecs[1], 1))
	require.Equal(t, []string{
		"Column 'k1' cannot be null",
		"Column 'k2' cannot be null",
	}, session.messages)
	op.Free(proc, false, nil)
}
