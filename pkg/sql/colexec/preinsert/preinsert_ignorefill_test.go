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

package preinsert

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type preInsertIgnoreWarningSession struct {
	total    uint64
	codes    []uint16
	messages []string
}

func (*preInsertIgnoreWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*preInsertIgnoreWarningSession) AddTempTable(string, string, string)        {}
func (*preInsertIgnoreWarningSession) RemoveTempTable(string, string)             {}
func (*preInsertIgnoreWarningSession) RemoveTempTableByRealName(string)           {}
func (*preInsertIgnoreWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *preInsertIgnoreWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.codes = append(s.codes, codes[i])
		s.messages = append(s.messages, messages[i])
	}
}

func ignoreFillNotNullCol(name string, oid types.T) *plan.ColDef {
	return &plan.ColDef{
		Name: name,
		Typ:  plan.Type{Id: int32(oid)},
		Default: &plan.Default{
			NullAbility: false,
		},
	}
}

func resetChildWithBatch(arg *PreInsert, bat *batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

// TestPreInsertIgnoreStatementAdjustsInsteadOfAborting is the operator-level
// red/green pair for issue #30906: the same mixed-validity batch that aborts
// under strict mode succeeds under statement IGNORE with its NULL cell
// adjusted to the implicit default and one ER_BAD_NULL_ERROR warning.
func TestPreInsertIgnoreStatementAdjustsInsteadOfAborting(t *testing.T) {
	buildBatch := func(mp *mpool.MPool) *batch.Batch {
		aVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(aVec, []int64{10, 11, 12}, nil, mp))
		nnVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(nnVec, []int64{10, 7, 12}, nil, mp))
		nnVec.GetNulls().Add(1)
		bat := batch.New([]string{"a", "nn"})
		bat.Vecs = []*vector.Vector{aVec, nnVec}
		bat.SetRowCount(3)
		return bat
	}

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a", Typ: i64typ},
			ignoreFillNotNullCol("nn", types.T_int64),
		},
		Pkey: &plan.PrimaryKeyDef{},
	}

	// Red leg: without IGNORE the batch aborts with the constraint violation.
	{
		proc := testutil.NewProc(t)
		defer proc.Free()
		arg := &PreInsert{
			ctr: container{
				canFreeVecIdx: make(map[int]bool),
			},
			TableDef: tableDef,
			Attrs:    []string{"a", "nn"},
		}
		resetChildWithBatch(arg, buildBatch(proc.Mp()))
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
		arg.Free(proc, false, err)
	}

	// Green leg: statement IGNORE keeps every row and adjusts the NULL cell.
	{
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		arg := &PreInsert{
			ctr: container{
				canFreeVecIdx: make(map[int]bool),
			},
			TableDef: tableDef,
			Attrs:    []string{"a", "nn"},
		}
		resetChildWithBatch(arg, buildBatch(proc.Mp()))
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		out := arg.ctr.buf
		require.Equal(t, 3, out.RowCount())
		nn := out.Vecs[1]
		require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](nn, 0))
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](nn, 1),
			"the NULL cell was adjusted to the implicit default")
		require.Equal(t, int64(12), vector.GetFixedAtNoTypeCheck[int64](nn, 2))
		require.Equal(t, uint64(1), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.Equal(t, []string{"Column 'nn' cannot be null"}, session.messages)
		arg.Free(proc, false, nil)
	}
}

// TestPreInsertIgnoreSelectorMirrorsAdjustedColumns pins the UPDATE IGNORE
// selector branch: fills applied to the shrunk clone must be mirrored back
// into the full-width output batch for exactly the selected rows, and every
// non-selected placeholder row must keep its NULL.
func TestPreInsertIgnoreSelectorMirrorsAdjustedColumns(t *testing.T) {
	proc := testutil.NewProc(t)
	session := &preInsertIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Update", "DML", true)
	defer proc.Free()

	nnVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(nnVec, []int64{7, 5, 9}, nil, proc.Mp()))
	nnVec.GetNulls().Add(0)
	nnVec.GetNulls().Add(2)
	rowNumberVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(rowNumberVec, []int64{1, 1, 1}, nil, proc.Mp()))
	rowNumberVec.GetNulls().Add(2)
	activeVec := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(activeVec, []bool{true, false, true}, nil, proc.Mp()))
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowIDVec,
		[]types.Rowid{{1}, {2}, {3}}, nil, proc.Mp()))

	bat := batch.New([]string{"nn", "sel_row_number", "sel_active", "sel_rowid"})
	bat.Vecs = []*vector.Vector{nnVec, rowNumberVec, activeVec, rowIDVec}
	bat.SetRowCount(3)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{ignoreFillNotNullCol("nn", types.T_int64)},
		Pkey: &plan.PrimaryKeyDef{},
	}
	arg := &PreInsert{
		ctr: container{
			canFreeVecIdx: make(map[int]bool),
		},
		TableDef:           tableDef,
		Attrs:              []string{"nn"},
		ColOffset:          0,
		HasTargetSelector:  true,
		TargetRowNumberCol: 1,
		TargetActiveCol:    2,
		TargetRowIDCol:     3,
	}
	resetChildWithBatch(arg, bat)
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.NoError(t, err)

	out := arg.ctr.buf
	require.Equal(t, 3, out.Vecs[0].Length())
	nnOut := out.Vecs[0]
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](nnOut, 0),
		"the selected NULL row was aligned with the implicit default")
	require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](nnOut, 1),
		"the non-selected row keeps its value")
	require.True(t, nnOut.GetNulls().Contains(2),
		"the unmatched placeholder row keeps its NULL")
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
	require.Equal(t, []string{"Column 'nn' cannot be null"}, session.messages)
	arg.Free(proc, false, nil)
}

// TestPreInsertIgnoreFillDropsSingleColumnPkBeforeConsumers pins the pinned
// primary-key semantics inside PRE_INSERT: the adjustment leaves a resolvable
// single-column NOT NULL primary key NULL and the operator then drops that
// row from its own output with one warning per dropped row, so CHECK
// filtering, unique-key arbitration, the foreign-key check, the parent-key
// lock and the write never evaluate a row whose key cell would be adjusted
// into a colliding implicit default. The dropped row's other supplied-NULL
// NOT NULL cells publish their per-cell warnings during the fill, before the
// key warning. Composite keys follow the regular fill, keeping every row.
func TestPreInsertIgnoreFillDropsSingleColumnPkBeforeConsumers(t *testing.T) {
	t.Run("single-column key is dropped before the consumers", func(t *testing.T) {
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		mp := proc.Mp()
		idVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(idVec, []int64{0, 2, 0}, nil, mp))
		idVec.GetNulls().Add(0)
		nnVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(nnVec, []int64{20, 0, 400}, nil, mp))
		nnVec.GetNulls().Add(1)
		bat := batch.New([]string{"id", "nn"})
		bat.Vecs = []*vector.Vector{idVec, nnVec}
		bat.SetRowCount(3)

		tableDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				ignoreFillNotNullCol("id", types.T_int64),
				ignoreFillNotNullCol("nn", types.T_int64),
			},
			Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
		}
		arg := &PreInsert{
			ctr:      container{canFreeVecIdx: make(map[int]bool)},
			TableDef: tableDef,
			Attrs:    []string{"id", "nn"},
		}
		resetChildWithBatch(arg, bat)
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		out := arg.ctr.buf
		require.Equal(t, 2, out.RowCount(), "the NULL-key row is gone before any consumer")
		require.False(t, out.Vecs[0].IsNull(1), "the zero-valued key row survives untouched")
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0))
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 1))
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 0),
			"the surviving row's NULL cell was adjusted")
		require.Equal(t, int64(400), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 1))
		// The fill publishes its per-cell warning before the key drop's.
		require.Equal(t, uint64(2), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR, moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.Equal(t, []string{
			"Column 'nn' cannot be null",
			"Column 'id' cannot be null",
		}, session.messages)
		arg.Free(proc, false, nil)
	})
	t.Run("composite keys follow the regular fill", func(t *testing.T) {
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		mp := proc.Mp()
		idVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(idVec, []int64{0, 2, 0}, nil, mp))
		idVec.GetNulls().Add(0)
		nnVec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(nnVec, []int64{20, 0, 400}, nil, mp))
		nnVec.GetNulls().Add(1)
		bat := batch.New([]string{"id", "nn"})
		bat.Vecs = []*vector.Vector{idVec, nnVec}
		bat.SetRowCount(3)

		tableDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				ignoreFillNotNullCol("id", types.T_int64),
				ignoreFillNotNullCol("nn", types.T_int64),
			},
			Pkey: &plan.PrimaryKeyDef{Names: []string{"nn", "id"}},
		}
		arg := &PreInsert{
			ctr:      container{canFreeVecIdx: make(map[int]bool)},
			TableDef: tableDef,
			Attrs:    []string{"id", "nn"},
		}
		resetChildWithBatch(arg, bat)
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		out := arg.ctr.buf
		require.Equal(t, 3, out.RowCount(), "composite keys keep every row")
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0),
			"the composite key member was adjusted")
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 1))
		require.Equal(t, uint64(2), session.total)
		require.Equal(t, []string{
			"Column 'id' cannot be null",
			"Column 'nn' cannot be null",
		}, session.messages)
		arg.Free(proc, false, nil)
	})
}

// TestPreInsertIgnoreNewUpdateKeepsStrictRejection pins that the pre-insert
// IGNORE machinery never applies to UPDATE-owned batches: even with the
// statement IGNORE profile set, a pre-insert serving the insert legs of an
// UPDATE keeps the historical strict behavior and raises the constraint
// violation instead of adjusting cells or dropping rows.
func TestPreInsertIgnoreNewUpdateKeepsStrictRejection(t *testing.T) {
	proc := testutil.NewProc(t)
	session := &preInsertIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	defer proc.Free()

	mp := proc.Mp()
	nnVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(nnVec, []int64{0, 5}, nil, mp))
	nnVec.GetNulls().Add(1)
	bat := batch.New([]string{"nn"})
	bat.Vecs = []*vector.Vector{nnVec}
	bat.SetRowCount(2)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{ignoreFillNotNullCol("nn", types.T_int64)},
		Pkey: &plan.PrimaryKeyDef{},
	}
	arg := &PreInsert{
		ctr:         container{canFreeVecIdx: make(map[int]bool)},
		TableDef:    tableDef,
		Attrs:       []string{"nn"},
		IsNewUpdate: true,
	}
	resetChildWithBatch(arg, bat)
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation),
		"UPDATE-owned rows keep the strict NOT NULL rejection")
	require.Equal(t, uint64(0), session.total, "no adjustment warning leaks out")
	arg.Free(proc, true, err)
}
