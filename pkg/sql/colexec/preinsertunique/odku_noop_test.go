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
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/odkueval"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func directNoopUpdate(c *plan.OdkuActionCtx) {
	c.UpdateExprs = []*plan.Expr{{Typ: c.SourceColumns[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}}}}
}

// Instrument only after preparation has verified the real concrete executor.
// This wrapper forwards the same call and never changes the input frame.
type countedNoopColumn struct {
	colexec.ExpressionExecutor
	calls, failAt int
	err           error
}

func (e *countedNoopColumn) Eval(p *process.Process, inputs []*batch.Batch, sels []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls == e.failAt {
		return nil, e.err
	}
	return e.ExpressionExecutor.Eval(p, inputs, sels)
}

func noopNotNull(t *testing.T, p *process.Process, column int32, typ plan.Type) *plan.Expr {
	t.Helper()
	notNull, err := function.GetFunctionByName(p.Ctx, "isnotnull", []types.Type{odkuType(typ)})
	require.NoError(t, err)
	assert, err := function.GetFunctionByName(p.Ctx, "_check_constraint_assert", []types.Type{types.T_bool.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	boolean := plan.Type{Id: int32(types.T_bool)}
	return &plan.Expr{Typ: boolean, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: assert.GetEncodedOverloadID(), ObjName: "_check_constraint_assert"},
		Args: []*plan.Expr{
			{Typ: boolean, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: notNull.GetEncodedOverloadID(), ObjName: "isnotnull"},
				Args: []*plan.Expr{{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: column}}}},
			}}},
			{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "native 100% message"}}}},
		},
	}}}
}

func TestODKUDirectNoopMatchesNormalActions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     [][9]int64
		want     uint64
		nullable bool
	}{
		{"insert then repeats and changes", [][9]int64{{1, 10, 5, 1}, {1, 10, 5, 2}, {1, 10, 6, 3}, {1, 10, 6, 4}, {1, 10, 5, 5}}, 5, false},
		{"snapshot changes then reverts", [][9]int64{{99, 10, 5, 1, 0, 7, 7, 10, 5}, {99, 10, 5, 2, 0, 7, 7, 10, 5}, {99, 10, 6, 3, 0, 7, 7, 10, 5}, {99, 10, 6, 4, 0, 7, 7, 10, 5}, {99, 10, 5, 5, 0, 7, 7, 10, 5}}, 4, false},
		{"insert then snapshot no-op and change", [][9]int64{{1, 10, 5, 1}, {99, 20, 5, 2, 0, 7, 7, 20, 5}, {99, 20, 5, 3, 0, 7, 7, 20, 5}, {99, 20, 6, 4, 0, 7, 7, 20, 5}}, 3, false},
		{"multiple owners across batches", [][9]int64{{1, 10, 5, 1}, {2, 20, 8, 2}, {1, 10, 5, 3}, {2, 20, 8, 4}, {1, 10, 6, 5}, {2, 20, 8, 6}}, 4, false},
		{"nullable nonzero owner", [][9]int64{{1, 10, 5, 1}, {2, 20, 7, 2}, {2, 20, 7, 3}, {2, 20, 7, 4}}, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reference []byte
			for _, fast := range []bool{false, true} {
				arg, proc, child, account := odkuActionFixture(t, 16<<20)
				c := arg.PreInsertCtx.OdkuActions
				directNoopUpdate(c)
				if !tc.nullable {
					c.UpdateChecks = []*plan.Expr{noopNotNull(t, proc, 2, c.OldColumns[2].Typ)}
				}
				for attempt := range 2 {
					first := odkuActionInput(t, proc, tc.rows[:2])
					second := odkuActionInput(t, proc, tc.rows[2:])
					if tc.nullable {
						for _, source := range []*batch.Batch{first, second} {
							for row := range source.RowCount() {
								if vector.GetFixedAtNoTypeCheck[int64](source.Vecs[0], row) == 2 {
									source.Vecs[2].GetNulls().Add(uint64(row))
								}
							}
						}
					}
					second.Vecs[3].GetGrouping().Add(uint64(second.RowCount() - 1))
					child.WithBatchs([]*batch.Batch{first, second})
					require.NoError(t, arg.Prepare(proc))
					require.Equal(t, int32(3), arg.ctr.actions.noopOrdinal)
					if !fast {
						arg.ctr.actions.noopOrdinal = -1
					}
					counter := &countedNoopColumn{ExpressionExecutor: arg.ctr.actions.assignments[0]}
					arg.ctr.actions.assignments[0] = counter
					published, publications := uint64(0), 0
					arg.PublishAffectedRows = func(n uint64) error { published = n; publications++; return nil }
					var encoded bytes.Buffer
					for {
						out, err := arg.Call(proc)
						require.NoError(t, err)
						if out.Batch != nil {
							require.NoError(t, out.Batch.MarshalBinaryWithGroupingTo(&encoded))
						}
						if out.Status == vm.ExecStop {
							break
						}
					}
					require.Equal(t, tc.want, published)
					require.Equal(t, 1, publications)
					inserts := 1
					if tc.name == "snapshot changes then reverts" {
						inserts = 0
					}
					if tc.name == "multiple owners across batches" || tc.nullable {
						inserts = 2
					}
					require.Equal(t, len(tc.rows)-inserts, counter.calls, "every duplicate RHS runs exactly once, including failed probes")
					// Unchanged snapshot rows need not be retained. Compare writer
					// output, logical counts and exact-once RHS evaluation instead.
					if !fast && attempt == 0 {
						reference = append([]byte(nil), encoded.Bytes()...)
					} else {
						require.Equal(t, reference, encoded.Bytes())
					}
					arg.Reset(proc, false, nil)
					child.Free(proc, false, nil)
					require.Zero(t, account.Snapshot().Used)
				}
			}
		})
	}
}

func TestODKUDirectNoopEligibilityRejectsUnknownShapes(t *testing.T) {
	arg, proc, _, _ := odkuActionFixture(t, 16<<20)
	c := arg.PreInsertCtx.OdkuActions
	originalRHS := proto.Clone(c.UpdateExprs[0]).(*plan.Expr)
	directNoopUpdate(c)
	require.NoError(t, arg.Prepare(proc))
	for name, change := range map[string]func(*plan.OdkuActionCtx){
		"current-image RHS": func(p *plan.OdkuActionCtx) { p.UpdateExprs[0].GetCol().RelPos = 0 },
		"function RHS":      func(p *plan.OdkuActionCtx) { p.UpdateExprs[0] = originalRHS },
		"literal RHS": func(p *plan.OdkuActionCtx) {
			p.UpdateExprs[0] = &plan.Expr{Typ: p.UpdateExprs[0].Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}}}
		},
		"automatic phase":         func(p *plan.OdkuActionCtx) { p.AutoUpdateAssignmentCount = 1 },
		"generated suffix":        func(p *plan.OdkuActionCtx) { p.ChangeCheckAssignmentCount = 0 },
		"key assignment":          func(p *plan.OdkuActionCtx) { p.UpdateColumns[0] = 0 },
		"uncovered comparison":    func(p *plan.OdkuActionCtx) { p.ChangeCheckColumns = append(p.ChangeCheckColumns, 1) },
		"unmapped target":         func(p *plan.OdkuActionCtx) { p.InputColumns[2] = -1 },
		"duplicate table mapping": func(p *plan.OdkuActionCtx) { p.InputColumns[1] = 0 },
		"extra metadata":          func(p *plan.OdkuActionCtx) { p.SourceColumns = append(p.SourceColumns, p.SourceColumns[3]) },
		"missing ordinal":         func(p *plan.OdkuActionCtx) { p.SourceOrdinal.GetCol().ColPos = 123 },
		"ordinal mapped to table": func(p *plan.OdkuActionCtx) { p.InputColumns[2] = 3 },
		"deferred physical flag":  func(p *plan.OdkuActionCtx) { p.PhysicalChangedColumn = 4 },
		"final selector":          func(p *plan.OdkuActionCtx) { p.ActionFinalColumn = 3 },
		"prior view":              func(p *plan.OdkuActionCtx) { p.PriorData = &plan.OdkuPriorData{} },
		"FK":                      func(p *plan.OdkuActionCtx) { p.ForeignKeyChecks = []*plan.OdkuForeignKeyCheck{{}} },
	} {
		t.Run(name, func(t *testing.T) {
			p := proto.Clone(c).(*plan.OdkuActionCtx)
			change(p)
			a := *arg.ctr.actions
			// Malformed/extra metadata is a shape-only control, not permission to execute it.
			if name == "key assignment" || name == "duplicate table mapping" || name == "final selector" || name == "prior view" || name == "FK" {
				a.staticKeys = false
			}
			require.Equal(t, int32(-1), directNoopOrdinal(p, &a))
		})
	}
	a := *arg.ctr.actions
	a.assignments = []colexec.ExpressionExecutor{&countedNoopColumn{ExpressionExecutor: a.assignments[0]}}
	require.Equal(t, int32(-1), directNoopOrdinal(c, &a), "only a verified concrete column executor qualifies")
}

// Stage a registered owner through the same production phases, stopping before
// the second action so its terminal publication can be controlled precisely.
func stageDirectNoop(t *testing.T, fresh bool, configure ...func(*plan.OdkuActionCtx, *batch.Batch, *process.Process)) (*PreInsertUnique, *process.Process, *mpool.AllocationAccount) {
	t.Helper()
	arg, proc, _, account := odkuActionFixture(t, 16<<20)
	c := arg.PreInsertCtx.OdkuActions
	directNoopUpdate(c)
	source := odkuActionInput(t, proc, [][9]int64{{99, 10, 5, 1, 0, 7, 7, 10, 5}, {99, 10, 5, 2, 0, 7, 7, 10, 5}})
	source.Vecs[3].GetGrouping().Add(1)
	for _, change := range configure {
		change(c, source, proc)
	}
	require.NoError(t, arg.Prepare(proc))
	a := arg.ctr.actions
	a.source = source
	for i, key := range c.Constraints {
		a.keys.Vecs[i] = source.Vecs[key.InputKey.GetCol().ColPos]
	}
	require.NoError(t, arg.initODKUActionRows(proc, a.incoming, a.keys, a.old, c.RowIdColumn, odkuMutableColumns(c)))
	require.NoError(t, arg.prepareODKUTarget(proc))
	if !fresh {
		// Construct an existing owner through the ordinary action path;
		// the direct proof can leave an unchanged snapshot unregistered.
		ordinal := a.noopOrdinal
		a.noopOrdinal = -1
		require.NoError(t, arg.applyODKUAction(proc))
		a.noopOrdinal = ordinal
		a.sourceRow = 1
		a.targetReady = false
		require.NoError(t, arg.prepareODKUTarget(proc))
	}
	return arg, proc, account
}

func TestODKUDirectNoopPublicationFailureAndCancellation(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		for _, terminal := range []string{"RHS", "native check", "cancel", "publication allocation"} {
			t.Run(fmt.Sprintf("fresh=%t/%s", fresh, terminal), func(t *testing.T) {
				arg, proc, account := stageDirectNoop(t, fresh)
				a, c := arg.ctr.actions, arg.PreInsertCtx.OdkuActions
				cause := moerr.NewInternalErrorNoCtx("stop no-op action")
				var filler []byte
				defer func() {
					if filler != nil {
						proc.Mp().Free(filler)
					}
				}()
				switch terminal {
				case "RHS":
					a.assignments[0] = &countedNoopColumn{ExpressionExecutor: a.assignments[0], failAt: 1, err: cause}
				case "native check":
					checks, err := odkueval.CompileChecks([]*plan.Expr{noopNotNull(t, proc, 2, c.OldColumns[2].Typ)}, []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_Rowid.ToType()}, []types.Type{}, proc, account)
					require.NoError(t, err)
					a.updateChecks = checks
					image := arg.ctr.actionRows.images
					if fresh {
						image = a.proposed
					}
					require.NoError(t, image.Vecs[2].PreExtendNulls(1, proc.Mp()))
					require.NoError(t, a.incoming.Vecs[2].PreExtendNulls(1, proc.Mp()))
					image.Vecs[2].GetNulls().Add(0)
					a.incoming.Vecs[2].GetNulls().Add(0)
				case "cancel":
					ctx, cancel := context.WithCancelCause(proc.Ctx)
					defer cancel(nil)
					proc.Ctx = ctx
					cancel(cause)
				case "publication allocation":
					var err error
					filler, err = proc.Mp().AllocAccounted(int(account.Snapshot().Limit-account.Snapshot().Used), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
					require.NoError(t, err)
				}
				done, err := arg.tryDirectNoop(proc, a.target)
				require.True(t, done)
				if fresh && terminal == "publication allocation" {
					require.NoError(t, err, "an unchanged snapshot needs no retained allocation")
					require.Equal(t, int64(1), arg.ctr.lastOrdinal)
					require.Zero(t, arg.ctr.actionRows.images.RowCount())
					require.Zero(t, a.logicalAffected)
					return
				} else if terminal == "native check" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
					require.ErrorContains(t, err, "native 100% message")
				} else if terminal == "publication allocation" {
					require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				} else {
					require.Same(t, cause, err)
				}
				if fresh {
					require.Zero(t, arg.ctr.lastOrdinal)
					require.Zero(t, arg.ctr.actionRows.images.RowCount())
				} else {
					require.Equal(t, int64(1), arg.ctr.lastOrdinal)
					require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](arg.ctr.actionRows.images.Vecs[3], 0))
				}
				require.Zero(t, a.logicalAffected)
			})
		}
	}
}

func TestODKUDirectNoopInputCheckPrecedesRHS(t *testing.T) {
	arg, proc, _ := stageDirectNoop(t, false)
	a, c := arg.ctr.actions, arg.PreInsertCtx.OdkuActions
	require.Equal(t, int32(3), a.noopOrdinal)
	// A generic explicit-input CHECK fails, although the update itself is a
	// proven no-op. It must win over even an unexpected RHS error.
	check := noopNotNull(t, proc, 0, c.OldColumns[0].Typ)
	condition := check.GetF().Args[0].GetF()
	f, err := function.GetFunctionByName(proc.Ctx, "isnull", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	condition.Func = &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: "isnull"}
	c.InsertChecks, c.InsertCheckConstraintCount = []*plan.Expr{check}, 1
	a.insertChecks, err = odkueval.CompileChecks(c.InsertChecks, []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_Rowid.ToType()}, nil, proc, arg.allocationAccount)
	require.NoError(t, err)
	counter := &countedNoopColumn{ExpressionExecutor: a.assignments[0], failAt: 1, err: moerr.NewInternalErrorNoCtx("RHS must not run")}
	a.assignments[0] = counter
	err = arg.applyODKUAction(proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.ErrorContains(t, err, "native 100% message")
	require.Zero(t, counter.calls)
	require.Equal(t, int64(1), arg.ctr.lastOrdinal)
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](arg.ctr.actionRows.images.Vecs[3], 0))
	require.Zero(t, a.logicalAffected)
}

func TestODKUDirectNoopProbeDeclinesBeforeRHS(t *testing.T) {
	for _, unsupported := range []bool{false, true} {
		t.Run(fmt.Sprint(unsupported), func(t *testing.T) {
			arg, proc, _ := stageDirectNoop(t, false, func(c *plan.OdkuActionCtx, b *batch.Batch, p *process.Process) {
				if !unsupported {
					vector.SetFixedAtNoTypeCheck(b.Vecs[2], 1, int64(6))
					return
				}
				typ := plan.Type{Id: int32(types.T_float64)}
				c.SourceColumns[2].Typ, c.OldColumns[2].Typ, c.UpdateExprs[0].Typ = typ, typ, typ
				for _, column := range []int{2, 8} {
					b.Vecs[column].Free(p.Mp())
					b.Vecs[column] = vector.NewVec(types.T_float64.ToType())
					for range b.RowCount() {
						require.NoError(t, vector.AppendFixed(b.Vecs[column], math.Float64frombits(0x7ff8000000000001), false, p.Mp()))
					}
				}
			})
			a := arg.ctr.actions
			require.Equal(t, int32(3), a.noopOrdinal, "the runtime value proof must make this decision")
			counter := &countedNoopColumn{ExpressionExecutor: a.assignments[0]}
			a.assignments[0] = counter
			done, err := arg.tryDirectNoop(proc, a.target)
			require.NoError(t, err)
			require.False(t, done)
			require.Zero(t, counter.calls)
			before := a.logicalAffected
			require.NoError(t, arg.applyODKUAction(proc))
			require.Equal(t, 1, counter.calls)
			require.Equal(t, before+2, a.logicalAffected, "NaN with identical bits is SQL-unequal")
			require.Equal(t, int64(2), arg.ctr.lastOrdinal)
		})
	}
}

func TestODKUDirectNoopRepeatedAssignmentProbe(t *testing.T) {
	for _, different := range []bool{false, true} {
		t.Run(fmt.Sprint(different), func(t *testing.T) {
			arg, proc, _ := stageDirectNoop(t, false, func(c *plan.OdkuActionCtx, b *batch.Batch, _ *process.Process) {
				c.UpdateExprs = append(c.UpdateExprs, proto.Clone(c.UpdateExprs[0]).(*plan.Expr))
				if different {
					c.UpdateExprs[1].GetCol().ColPos = 0
				}
				c.UpdateColumns = append(c.UpdateColumns, 2)
				c.ChangeCheckAssignmentCount = 2
				for row := range b.RowCount() {
					vector.SetFixedAtNoTypeCheck(b.Vecs[0], row, int64(7))
					vector.SetFixedAtNoTypeCheck(b.Vecs[2], row, int64(7))
					vector.SetFixedAtNoTypeCheck(b.Vecs[8], row, int64(7))
				}
				if different {
					vector.SetFixedAtNoTypeCheck(b.Vecs[0], 1, int64(8))
				}
			})
			a := arg.ctr.actions
			counters := make([]*countedNoopColumn, len(a.assignments))
			for i, exec := range a.assignments {
				counters[i] = &countedNoopColumn{ExpressionExecutor: exec}
				a.assignments[i] = counters[i]
			}
			done, err := arg.tryDirectNoop(proc, a.target)
			require.NoError(t, err)
			require.Equal(t, !different, done)
			if different {
				for _, counter := range counters {
					require.Zero(t, counter.calls)
				}
				require.NoError(t, arg.applyODKUAction(proc))
			}
			for _, counter := range counters {
				require.Equal(t, 1, counter.calls)
			}
			require.Equal(t, int64(2), arg.ctr.lastOrdinal)
		})
	}
}
