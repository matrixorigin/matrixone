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

func ignoreFillNullableCol(name string, oid types.T) *plan.ColDef {
	return &plan.ColDef{
		Name: name,
		Typ:  plan.Type{Id: int32(oid)},
		Default: &plan.Default{
			NullAbility: true,
		},
	}
}

// TestPreInsertIgnoreDetachAliasedSourceVectors pins that every destination
// column owns independent writable storage before the IGNORE adjustment
// runs: an INSERT IGNORE ... SELECT whose select list repeats one source
// column shares one physical vector between two destinations (the
// same-type assignment cast bypass hands the projection both column
// references over the same input slot), and the first column's adjustment
// must not rewrite its sibling's value nor clear a shared null bitmap the
// second column still needs for its own warning.
func TestPreInsertIgnoreDetachAliasedSourceVectors(t *testing.T) {
	t.Run("NOT NULL and nullable destinations share one source", func(t *testing.T) {
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		shared := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(shared, []int64{10, 0, 12}, nil, proc.Mp()))
		shared.GetNulls().Add(1)
		bat := batch.New([]string{"a", "b"})
		bat.Vecs = []*vector.Vector{shared, shared}
		bat.SetRowCount(3)

		tableDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				ignoreFillNotNullCol("a", types.T_int64),
				ignoreFillNullableCol("b", types.T_int64),
			},
			Pkey: &plan.PrimaryKeyDef{},
		}
		arg := &PreInsert{
			ctr:      container{canFreeVecIdx: make(map[int]bool)},
			TableDef: tableDef,
			Attrs:    []string{"a", "b"},
		}
		resetChildWithBatch(arg, bat)
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)

		out := arg.ctr.buf
		require.NotSame(t, out.Vecs[0], out.Vecs[1],
			"repeated source columns must not share one physical vector in the output")
		aOut := out.Vecs[0]
		require.False(t, aOut.IsNull(1))
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](aOut, 1),
			"the NOT NULL destination was adjusted to the implicit default")
		require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](aOut, 0))
		require.Equal(t, int64(12), vector.GetFixedAtNoTypeCheck[int64](aOut, 2))
		bOut := out.Vecs[1]
		require.True(t, bOut.IsNull(1),
			"the nullable destination keeps the source's NULL, not its sibling's adjustment")
		require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](bOut, 0))
		require.Equal(t, int64(12), vector.GetFixedAtNoTypeCheck[int64](bOut, 2))
		require.Equal(t, uint64(1), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.Equal(t, []string{"Column 'a' cannot be null"}, session.messages)
		arg.Free(proc, false, nil)
	})

	t.Run("two NOT NULL destinations share one source", func(t *testing.T) {
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		shared := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(shared, []int64{10, 0, 12}, nil, proc.Mp()))
		shared.GetNulls().Add(1)
		bat := batch.New([]string{"a", "b"})
		bat.Vecs = []*vector.Vector{shared, shared}
		bat.SetRowCount(3)

		tableDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				ignoreFillNotNullCol("a", types.T_int64),
				ignoreFillNotNullCol("b", types.T_int64),
			},
			Pkey: &plan.PrimaryKeyDef{},
		}
		arg := &PreInsert{
			ctr:      container{canFreeVecIdx: make(map[int]bool)},
			TableDef: tableDef,
			Attrs:    []string{"a", "b"},
		}
		resetChildWithBatch(arg, bat)
		require.NoError(t, arg.Prepare(proc))
		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)

		out := arg.ctr.buf
		require.NotSame(t, out.Vecs[0], out.Vecs[1],
			"repeated source columns must not share one physical vector in the output")
		for _, col := range out.Vecs {
			require.False(t, col.IsNull(1))
			require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](col, 1),
				"each destination carries its own adjusted cell")
			require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](col, 0))
			require.Equal(t, int64(12), vector.GetFixedAtNoTypeCheck[int64](col, 2))
		}
		// One warning per adjusted destination: the second column's NULL is
		// taken from the original bitmap of an independent vector, not from
		// a bitmap the first column's adjustment already cleared.
		require.Equal(t, uint64(2), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR, moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.ElementsMatch(t, []string{
			"Column 'a' cannot be null",
			"Column 'b' cannot be null",
		}, session.messages)
		arg.Free(proc, false, nil)
	})

	t.Run("detached storage is reused cleanly across batches", func(t *testing.T) {
		proc := testutil.NewProc(t)
		session := &preInsertIgnoreWarningSession{}
		proc.Session = session
		proc.SetStmtProfile(&process.StmtProfile{})
		proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
		defer proc.Free()

		buildShared := func(vals []int64, nullRow int) *vector.Vector {
			v := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(v, vals, nil, proc.Mp()))
			if nullRow >= 0 {
				v.GetNulls().Add(uint64(nullRow))
			}
			return v
		}
		bat1 := batch.New([]string{"a", "b"})
		bat1.Vecs = []*vector.Vector{buildShared([]int64{0, 7}, 0), nil}
		bat1.Vecs[1] = bat1.Vecs[0]
		bat1.SetRowCount(2)
		bat2 := batch.New([]string{"a", "b"})
		bat2.Vecs = []*vector.Vector{buildShared([]int64{9, 4}, 1), nil}
		bat2.Vecs[1] = bat2.Vecs[0]
		bat2.SetRowCount(2)

		tableDef := &plan.TableDef{
			Cols: []*plan.ColDef{
				ignoreFillNotNullCol("a", types.T_int64),
				ignoreFillNullableCol("b", types.T_int64),
			},
			Pkey: &plan.PrimaryKeyDef{},
		}
		arg := &PreInsert{
			ctr:      container{canFreeVecIdx: make(map[int]bool)},
			TableDef: tableDef,
			Attrs:    []string{"a", "b"},
		}
		op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat2})
		arg.Children = nil
		arg.AppendChild(op)
		require.NoError(t, arg.Prepare(proc))

		_, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		out1 := arg.ctr.buf
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[0], 0))
		require.True(t, out1.Vecs[1].IsNull(0),
			"first batch: the nullable destination keeps the shared source's NULL")
		require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](out1.Vecs[1], 1))

		_, err = vm.Exec(arg, proc)
		require.NoError(t, err)
		out2 := arg.ctr.buf
		require.NotSame(t, out2.Vecs[0], out2.Vecs[1])
		require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[0], 0),
			"second batch: recomputed, not stale first-batch data")
		require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[0], 1),
			"second batch: the NOT NULL destination was adjusted again")
		require.True(t, out2.Vecs[1].IsNull(1),
			"second batch: the nullable destination keeps its own NULL")
		require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](out2.Vecs[1], 0),
			"second batch: the nullable destination keeps the shared source's first value")
		require.Equal(t, uint64(2), session.total)
		require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR, moerr.ER_BAD_NULL_ERROR}, session.codes)
		require.Equal(t, []string{
			"Column 'a' cannot be null",
			"Column 'a' cannot be null",
		}, session.messages)
		arg.Free(proc, false, nil)
	})
}

// TestPreInsertIgnoreDropSurvivesAliasedSourceVectors pins the aliased
// shape of the pinned single-column NULL primary-key drop: a NULL-PK row
// ahead of survivors, with the surviving rows' columns sharing one physical
// source vector, must compact once per physical vector, keep the aliased
// destinations value-consistent and never panic the shrink.
func TestPreInsertIgnoreDropSurvivesAliasedSourceVectors(t *testing.T) {
	proc := testutil.NewProc(t)
	session := &preInsertIgnoreWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	defer proc.Free()

	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(idVec, []int64{0, 1, 2}, nil, proc.Mp()))
	idVec.GetNulls().Add(0)
	shared := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(shared, []int64{9, 10, 20}, nil, proc.Mp()))
	bat := batch.New([]string{"id", "a", "b"})
	bat.Vecs = []*vector.Vector{idVec, shared, shared}
	bat.SetRowCount(3)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			ignoreFillNotNullCol("id", types.T_int64),
			ignoreFillNullableCol("a", types.T_int64),
			ignoreFillNullableCol("b", types.T_int64),
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
	}
	arg := &PreInsert{
		ctr:      container{canFreeVecIdx: make(map[int]bool)},
		TableDef: tableDef,
		Attrs:    []string{"id", "a", "b"},
	}
	resetChildWithBatch(arg, bat)
	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.NoError(t, err)

	out := arg.ctr.buf
	require.Equal(t, 2, out.RowCount(), "only the NULL-key row is gone")
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0))
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 1))
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 0),
		"the aliased first destination keeps the surviving rows")
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[1], 1))
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[2], 0),
		"the aliased second destination keeps the same surviving rows")
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[2], 1))
	require.Equal(t, uint64(1), session.total)
	require.Equal(t, []uint16{moerr.ER_BAD_NULL_ERROR}, session.codes)
	require.Equal(t, []string{"Column 'id' cannot be null"}, session.messages)
	arg.Free(proc, false, nil)
}
