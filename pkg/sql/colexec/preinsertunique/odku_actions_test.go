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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Source: incoming(id,u,n,ordinal), priorPK, priorUK, original(id,u,n,rowid).
// The fixture has no CHECK/FK plans; SQL tests own those observable contracts.
func odkuActionFixture(t *testing.T, limit uint64) (*PreInsertUnique, *process.Process, *colexec.MockOperator, *mpool.AllocationAccount) {
	t.Helper()
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	child := colexec.NewMockOperator()
	arg := &PreInsertUnique{}
	arg.AppendChild(child)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	t.Cleanup(func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
		require.NoError(t, arg.ClearAllocationAccount(account))
	})
	ref := func(rel, pos int32, typ types.T) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
	}
	f, err := function.GetFunctionByName(proc.Ctx, "+", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	add := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: "+"}, Args: []*plan.Expr{ref(0, 2, types.T_int64), ref(1, 2, types.T_int64)},
	}}}
	c := &plan.OdkuActionCtx{
		InputColumns: []int32{0, 1, 2, -1}, PkColumn: 0, RowIdColumn: 3, ActionFinalColumn: -1,
		ChangeCheckAssignmentCount: 1, ChangeCheckColumns: []int32{2},
		SourceOrdinal: ref(0, 3, types.T_int64), UpdateColumns: []int32{2}, UpdateExprs: []*plan.Expr{add},
	}
	for i := range 4 {
		c.SourceColumns = append(c.SourceColumns, ref(0, int32(i), types.T_int64))
		typ := types.T_int64
		if i == 3 {
			typ = types.T_Rowid
		}
		c.OldColumns = append(c.OldColumns, ref(0, int32(i+6), typ))
	}
	for i, name := range []string{"id", "u"} {
		c.Constraints = append(c.Constraints, &plan.OdkuConstraint{
			Name: name, Types: []plan.Type{{Id: int32(types.T_int64)}}, TargetEligible: true,
			InputKey: ref(0, int32(i), types.T_int64), PriorTarget: ref(0, int32(i+4), types.T_int64), CurrentKey: ref(0, int32(i), types.T_int64),
		})
	}
	arg.PreInsertCtx = &plan.PreInsertUkCtx{OrderedOdku: true, OdkuActions: c}
	return arg, proc, child, account
}

func odkuActionInput(t *testing.T, proc *process.Process, rows [][9]int64) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(10)
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	for i := range b.Vecs {
		typ := types.T_int64
		if i == 9 {
			typ = types.T_Rowid
		}
		b.Vecs[i] = vector.NewVec(typ.ToType())
	}
	for _, row := range rows {
		for i, n := range row {
			require.NoError(t, vector.AppendFixed(b.Vecs[i], n, i >= 4 && n == 0, proc.Mp()))
		}
		require.NoError(t, vector.AppendFixed(b.Vecs[9], types.Rowid{0: 3}, row[6] == 0, proc.Mp()))
	}
	b.SetRowCount(len(rows))
	return b
}

func TestODKUActionsOrderPriorityAndReset(t *testing.T) {
	a, proc, child, account := odkuActionFixture(t, 16<<20)
	for attempt := range 2 {
		first := odkuActionInput(t, proc, [][9]int64{{1, 10, 3, 1}, {99, 10, 5, 2}})
		second := odkuActionInput(t, proc, [][9]int64{{3, 30, 11, 3, 3, 3, 3, 30, 7}, {1, 30, 2, 4, 0, 3, 3, 30, 7}})
		child.WithBatchs([]*batch.Batch{first, second})
		require.NoError(t, a.Prepare(proc))
		out, err := a.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, out.Batch)
		require.Len(t, out.Batch.Vecs, len(a.PreInsertCtx.OdkuActions.SourceColumns)+len(a.PreInsertCtx.OdkuActions.OldColumns))
		require.Equal(t, 2, out.Batch.RowCount())
		require.Equal(t, []int64{1, 3}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[0]))
		require.Equal(t, []int64{10, 30}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[1]))
		require.Equal(t, []int64{10, 18}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[2]))
		require.Equal(t, []int64{4, 3}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[3]), "non-table source values follow the last action")
		require.True(t, out.Batch.Vecs[4].IsNull(0))
		require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[4], 1))
		require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[6], 1))
		require.True(t, out.Batch.Vecs[7].IsNull(0))
		require.Equal(t, types.Rowid{0: 3}, vector.GetFixedAtNoTypeCheck[types.Rowid](out.Batch.Vecs[7], 1))
		out, err = a.Call(proc)
		require.NoError(t, err)
		require.Nil(t, out.Batch)
		a.Reset(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used, "attempt %d", attempt)
	}
}

func TestODKUStaticClaimsPreserveGroupedInsertSemantics(t *testing.T) {
	var native []byte
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			a, proc, child, _ := odkuActionFixture(t, 16<<20)
			first := odkuActionInput(t, proc, [][9]int64{{1, 10, 2, 1}})
			second := odkuActionInput(t, proc, [][9]int64{{2, 20, 3, 2}, {2, 0, 4, 3}})
			// A later batch must still upgrade the native integer key map and
			// preserve its grouping grammar for inserted claims and repeated hits.
			// ROLLUP marks the rolled-up NULL, not an ordinary non-NULL value.
			second.Vecs[1].GetNulls().Add(1)
			second.Vecs[1].GetGrouping().Add(1)
			if !static {
				// A scalar key follows the real generic preparation path.
				key := a.PreInsertCtx.OdkuActions.Constraints[1]
				key.CurrentKey = &plan.Expr{Typ: key.CurrentKey.Typ, Expr: &plan.Expr_F{F: &plan.Function{
					Func: a.PreInsertCtx.OdkuActions.UpdateExprs[0].GetF().Func,
					Args: []*plan.Expr{key.CurrentKey, {Typ: key.CurrentKey.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}},
				}}}
			}
			child.WithBatchs([]*batch.Batch{first, second})
			require.NoError(t, a.Prepare(proc))
			require.Equal(t, static, a.ctr.actions.staticKeys)
			out, err := a.Call(proc)
			require.NoError(t, err)
			require.NotNil(t, out.Batch)
			require.Nil(t, a.ctr.acceptedKeys[1].fixedKeys, "the later grouped batch selected the generic key encoder")
			require.Equal(t, []int64{2, 7}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[2]))
			var encoded bytes.Buffer
			require.NoError(t, out.Batch.MarshalBinaryWithGroupingTo(&encoded))
			if !static {
				native = append([]byte(nil), encoded.Bytes()...)
			} else {
				require.Equal(t, native, encoded.Bytes(), "values, original identities and grouping match native transitions")
				require.Nil(t, a.ctr.actionRows.keys, "the current image is the only retained static-key value")
			}
		})
	}
}

func TestODKUActionsCheckFailureThenReset(t *testing.T) {
	a, proc, child, account := odkuActionFixture(t, 16<<20)
	boolType := plan.Type{Id: int32(types.T_bool)}
	positive, err := function.GetFunctionByName(proc.Ctx, ">", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	condition := &plan.Expr{Typ: boolType, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: positive.GetEncodedOverloadID(), ObjName: ">"},
		Args: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}},
		},
	}}}
	assertion, err := function.GetFunctionByName(proc.Ctx, "_check_constraint_assert", []types.Type{types.T_bool.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	c := a.PreInsertCtx.OdkuActions
	c.InsertChecks = []*plan.Expr{{Typ: boolType, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: assertion.GetEncodedOverloadID(), ObjName: "_check_constraint_assert"},
		Args: []*plan.Expr{condition, {Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "positive n"}}}}},
	}}}}
	c.InsertCheckConstraintCount = 1
	for _, value := range []int64{-1, 3} {
		child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, [][9]int64{{1, 10, value, 1}})})
		require.NoError(t, a.Prepare(proc))
		out, err := a.Call(proc)
		if value < 0 {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation), "%v", err)
			require.Nil(t, out.Batch)
			require.Zero(t, a.ctr.actionRows.images.RowCount(), "checks precede key claims and image publication")
			_, err = a.Call(proc)
			require.ErrorContains(t, err, "require Reset")
		} else {
			require.NoError(t, err)
			require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
		}
		a.Reset(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
	}
}

func TestODKUActionsFailureAndCleanup(t *testing.T) {
	for _, name := range []string{"ordinal", "key type", "short old vector", "canceled", "budget-1", "budget-4096"} {
		t.Run(name, func(t *testing.T) {
			limit := uint64(16 << 20)
			if name == "budget-1" {
				limit = 1
			}
			if name == "budget-4096" {
				limit = 4096
			}
			a, proc, child, _ := odkuActionFixture(t, limit)
			b := odkuActionInput(t, proc, [][9]int64{{1, 10, 3, 1}})
			if name == "ordinal" {
				vector.SetFixedAtNoTypeCheck(b.Vecs[3], 0, int64(0))
			}
			if name == "key type" {
				a.PreInsertCtx.OdkuActions.Constraints[0].InputKey.GetCol().ColPos = 9
			}
			if name == "short old vector" {
				b.Vecs[8].CleanOnlyData()
			}
			child.WithBatchs([]*batch.Batch{b})
			err := a.Prepare(proc)
			prepared := err == nil
			if name == "canceled" {
				ctx, cancel := context.WithCancelCause(proc.Ctx)
				proc.Ctx = ctx
				cause := moerr.NewInternalErrorNoCtx("action source stopped")
				cancel(cause)
			}
			if err == nil {
				_, err = a.Call(proc)
			}
			require.Error(t, err)
			if prepared {
				_, again := a.Call(proc)
				require.ErrorContains(t, again, "require Reset")
			}
			if name == "canceled" {
				require.Same(t, context.Cause(proc.Ctx), err)
			}
			if name == "ordinal" {
				require.ErrorContains(t, err, "strictly increasing")
			}
			if name == "key type" || name == "short old vector" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
			}
			if name == "budget-1" || name == "budget-4096" {
				require.True(t, mpool.AllocationFailureReasonOf(err) == mpool.AllocationFailureCapacity || moerr.IsMoErrCode(err, moerr.ErrOOM), "%v", err)
			}
		})
	}
}

type odkuTestPriorView struct {
	lookup func(uint64) (*batch.Batch, error)
}

func (v odkuTestPriorView) Lookup(_ context.Context, table uint64, _ *vector.Vector, _ int) (*batch.Batch, error) {
	return v.lookup(table)
}
func (odkuTestPriorView) Validate(context.Context) error { return nil }
func (odkuTestPriorView) Close() error                   { return nil }

func TestODKUActionsValidatePriorRowsAtBoundary(t *testing.T) {
	for _, shape := range []string{"short layout", "nil vector", "short vector", "wrong type", "row count", "valid", "extra column", "unique short layout", "unique nil vector", "unique short vector", "unique wrong type"} {
		t.Run(shape, func(t *testing.T) {
			a, proc, child, account := odkuActionFixture(t, 16<<20)
			c := a.PreInsertCtx.OdkuActions
			c.PriorData = &plan.OdkuPriorData{Ref: &plan.ObjectRef{Obj: 1}, TableDef: &plan.TableDef{}}
			for _, col := range c.OldColumns {
				c.PriorData.TableDef.Cols = append(c.PriorData.TableDef.Cols, &plan.ColDef{Typ: col.Typ})
			}
			for _, key := range c.Constraints {
				key.LookupRef = c.PriorData.Ref
			}
			if strings.HasPrefix(shape, "unique ") {
				c.Constraints[0].LookupRef = &plan.ObjectRef{Obj: 2}
			}
			a.LockKeys = func(*process.Process, int, *vector.Vector) error { return nil }
			malformed := true
			a.PriorDataView = odkuTestPriorView{lookup: func(table uint64) (*batch.Batch, error) {
				if table == 2 {
					b := batch.NewWithSize(1)
					b.Attrs = []string{catalog.IndexTablePrimaryColName}
					b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(1), false, proc.Mp()))
					b.SetRowCount(1)
					if malformed {
						switch shape {
						case "unique short layout":
							b.Attrs = append([]string{"key"}, b.Attrs...)
						case "unique nil vector":
							b.Vecs[0].Free(proc.Mp())
							b.Vecs[0] = nil
						case "unique short vector":
							b.Vecs[0].CleanOnlyData()
						case "unique wrong type":
							b.Vecs[0].Free(proc.Mp())
							b.Vecs[0] = vector.NewConstNull(types.T_int32.ToType(), 1, proc.Mp())
						}
					}
					return b, nil
				}
				b := batch.NewWithSize(4)
				for i, value := range []int64{1, 10, 7} {
					b.Vecs[i] = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixed(b.Vecs[i], value, false, proc.Mp()))
				}
				b.Vecs[3] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[3], types.Rowid{0: 3}, false, proc.Mp()))
				b.SetRowCount(1)
				if malformed {
					switch shape {
					case "short layout":
						b.Vecs[3].Free(proc.Mp())
						b.Vecs = b.Vecs[:3]
					case "nil vector":
						b.Vecs[2].Free(proc.Mp())
						b.Vecs[2] = nil
					case "short vector":
						b.Vecs[2].CleanOnlyData()
					case "wrong type":
						b.Vecs[2].Free(proc.Mp())
						b.Vecs[2] = vector.NewVec(types.T_int32.ToType())
						require.NoError(t, vector.AppendFixed(b.Vecs[2], int32(7), false, proc.Mp()))
					case "row count":
						b.SetRowCount(0)
					case "extra column":
						b.Vecs = append(b.Vecs, vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp()))
					}
				}
				return b, nil
			}}
			valid := shape == "valid" || shape == "extra column"
			for attempt := range 2 {
				child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, [][9]int64{{1, 10, 3, 1}})})
				require.NoError(t, a.Prepare(proc))
				out, err := a.Call(proc)
				if attempt == 0 && !valid {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
					require.Nil(t, out.Batch)
					_, err = a.Call(proc)
					require.ErrorContains(t, err, "require Reset")
				} else {
					require.NoError(t, err)
					require.NotNil(t, out.Batch)
					require.Equal(t, 1, out.Batch.RowCount())
					require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
				}
				a.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				require.Zero(t, account.Snapshot().Used)
				malformed = false
			}
		})
	}
}

func TestODKUMutableActionsRetainDistinctTransientLocks(t *testing.T) {
	a, proc, child, account := odkuActionFixture(t, 16<<20)
	c := a.PreInsertCtx.OdkuActions
	c.PriorData = &plan.OdkuPriorData{Ref: &plan.ObjectRef{Obj: 1}, TableDef: &plan.TableDef{}}
	for _, col := range c.OldColumns {
		c.PriorData.TableDef.Cols = append(c.PriorData.TableDef.Cols, &plan.ColDef{Typ: col.Typ})
	}
	for i, key := range c.Constraints {
		key.LookupRef = &plan.ObjectRef{Obj: int64(i + 1)}
	}
	// n=VALUES(n), followed by the generated UNIQUE key u=n. The same
	// inserted owner moves A -> B -> A -> C, crossing an already vacated key.
	incoming := &plan.Expr{Typ: c.SourceColumns[2].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}}}
	c.UpdateColumns = []int32{2, 1}
	c.UpdateExprs = []*plan.Expr{incoming, c.SourceColumns[2]}
	c.ChangeCheckAssignmentCount = 2
	c.ChangeCheckColumns = []int32{1, 2}
	a.PriorDataView = odkuTestPriorView{lookup: func(uint64) (*batch.Batch, error) { return nil, nil }}
	for range 2 {
		locked := make(map[int][]int64)
		a.LockKeys = func(_ *process.Process, slot int, keys *vector.Vector) error {
			require.NotContains(t, locked, slot, "validate each constraint once per attempt")
			locked[slot] = append([]int64(nil), vector.MustFixedColNoTypeCheck[int64](keys)...)
			return nil
		}
		child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, [][9]int64{
			{1, 10, 10, 1}, {1, 20, 20, 2}, {1, 10, 10, 3}, {1, 30, 30, 4},
		})})
		require.NoError(t, a.Prepare(proc))
		out, err := a.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, out.Batch)
		require.Equal(t, 1, out.Batch.RowCount())
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[0], 0))
		require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[1], 0))
		require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
		require.Equal(t, map[int][]int64{0: {1}, 1: {10, 20, 30}}, locked)
		out, err = a.Call(proc)
		require.NoError(t, err)
		require.Nil(t, out.Batch)
		a.Reset(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
	}
}

func TestODKUActionsInvalidMetadata(t *testing.T) {
	for name, change := range map[string]func(*plan.OdkuActionCtx){
		"missing ordinal":           func(c *plan.OdkuActionCtx) { c.SourceOrdinal = nil },
		"ordinal relation":          func(c *plan.OdkuActionCtx) { c.SourceOrdinal.GetCol().RelPos = 1 },
		"negative ordinal position": func(c *plan.OdkuActionCtx) { c.SourceOrdinal.GetCol().ColPos = -1 },
		"ordinal type":              func(c *plan.OdkuActionCtx) { c.SourceOrdinal.Typ.Id = int32(types.T_uint64) },
		"missing original":          func(c *plan.OdkuActionCtx) { c.OldColumns[0] = nil },
		"wrong rowid":               func(c *plan.OdkuActionCtx) { c.OldColumns[3].Typ.Id = int32(types.T_int64) },
		"assignment image":          func(c *plan.OdkuActionCtx) { c.UpdateColumns[0] = 4 },
		"check prefix":              func(c *plan.OdkuActionCtx) { c.InsertCheckConstraintCount = 1 },
		"update phase overflow":     func(c *plan.OdkuActionCtx) { c.ChangeCheckAssignmentCount = ^uint32(0) },
		"automatic phase overflow":  func(c *plan.OdkuActionCtx) { c.AutoUpdateAssignmentCount = 2 },
		"invalid change column":     func(c *plan.OdkuActionCtx) { c.ChangeCheckColumns[0] = 9 },
		"physical flag layout":      func(c *plan.OdkuActionCtx) { c.PhysicalChangedColumn = 99 },
		"unmaterialized key":        func(c *plan.OdkuActionCtx) { c.Constraints[0].InputKey.GetCol().RelPos = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			a, proc, child, _ := odkuActionFixture(t, 16<<20)
			read := false
			child.WithEndOfDataCallback(func() { read = true })
			change(a.PreInsertCtx.OdkuActions)
			err := a.Prepare(proc)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
			require.False(t, read, "invalid metadata must fail before source execution")
		})
	}
}

func TestODKUActionsAfterForcedSortSpill(t *testing.T) {
	a, proc, child, _ := odkuActionFixture(t, 16<<20)
	expr := a.PreInsertCtx.OdkuActions.SourceOrdinal
	specs := []*plan.OrderBySpec{{Expr: expr}}
	local := &order.Order{OrderBySpec: specs}
	local.AppendChild(child)
	t.Cleanup(func() { local.Free(proc, false, nil) })
	global := &mergeorder.MergeOrder{OrderBySpecs: specs, SpillThreshold: 1}
	global.AppendChild(local)
	t.Cleanup(func() { global.Free(proc, false, nil) })
	a.GetOperatorBase().Children[0] = global
	t.Cleanup(func() { a.Reset(proc, false, nil) })
	b := odkuActionInput(t, proc, [][9]int64{{30, 10, 30, 3}, {10, 10, 10, 1}, {20, 10, 20, 2}})
	child.WithBatchs([]*batch.Batch{b})
	require.NoError(t, local.Prepare(proc))
	require.NoError(t, global.Prepare(proc))
	require.NoError(t, a.Prepare(proc))
	out, err := a.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, out.Batch)
	require.Equal(t, 1, out.Batch.RowCount())
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[0], 0))
	require.Equal(t, int64(60), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
	require.Positive(t, global.OpAnalyzer.GetOpStats().SpillSize, "restoration must really spill")
}

func TestODKUActionsValidateSupersededFKImages(t *testing.T) {
	// 8193 actions cross the output quota while one logical action is pending.
	// The final image must appear once, after all superseded images, and a quota
	// suspension must not apply the next assignment twice.
	for _, tc := range []struct {
		count int
		stop  bool
	}{{3, false}, {colexec.DefaultBatchSize + 1, false}, {colexec.DefaultBatchSize + 1, true}} {
		t.Run(fmt.Sprintf("%d/cancel=%t", tc.count, tc.stop), func(t *testing.T) {
			count := tc.count
			a, proc, child, _ := odkuActionFixture(t, 16<<20)
			c := a.PreInsertCtx.OdkuActions
			boolRef := func(rel, pos int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
			}
			c.SourceColumns = append(c.SourceColumns, boolRef(0, 10), boolRef(0, 11))
			c.ActionFinalColumn = 5
			c.ForeignKeyChecks = []*plan.OdkuForeignKeyCheck{{Columns: []plan.ColRef{{RelPos: 0, ColPos: 2}}, Eligibility: boolRef(1, 4), CheckUpdates: true}}
			input := make([][9]int64, count)
			for i := range input {
				input[i] = [9]int64{1, 10, 1, int64(i + 1)}
			}
			b := odkuActionInput(t, proc, input)
			for range 2 {
				v := vector.NewVec(types.T_bool.ToType())
				b.Vecs = append(b.Vecs, v)
				for range count {
					require.NoError(t, vector.AppendFixed(v, false, false, proc.Mp()))
				}
			}
			child.WithBatchs([]*batch.Batch{b})
			require.NoError(t, a.Prepare(proc))
			total, final := 0, 0
			var stopped error
			for {
				out, err := a.Call(proc)
				if stopped != nil {
					require.Same(t, stopped, err)
					require.Nil(t, out.Batch)
					break
				}
				require.NoError(t, err)
				if out.Batch == nil {
					break
				}
				for row := range out.Batch.RowCount() {
					total++
					require.Equal(t, int64(total), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], row))
					require.True(t, vector.GetFixedAtNoTypeCheck[bool](out.Batch.Vecs[4], row))
					if vector.GetFixedAtNoTypeCheck[bool](out.Batch.Vecs[5], row) {
						final++
						require.Equal(t, count, total, "only the last image uses locked final FK validation")
					}
				}
				if tc.stop {
					require.Equal(t, colexec.DefaultBatchSize, total)
					ctx, cancel := context.WithCancelCause(proc.Ctx)
					proc.Ctx = ctx
					stopped = moerr.NewInternalErrorNoCtx("stop while FK output is pending")
					cancel(stopped)
				}
			}
			if tc.stop {
				require.Zero(t, final, "cancellation cannot publish the pending final image")
			} else {
				require.Equal(t, count, total)
				require.Equal(t, 1, final)
			}
		})
	}
}

func TestODKUDeferredOriginalPreservesImagesAndRepeatedActions(t *testing.T) {
	for _, onlyNoop := range []bool{false, true} {
		t.Run(fmt.Sprintf("noop_only=%t", onlyNoop), func(t *testing.T) {
			a, proc, child, account := odkuActionFixture(t, 16<<20)
			for range 2 { // Prepared reuse owns no prior attempt's scratch or marker.
				firstRows := [][9]int64{{3, 30, 0, 1, 3, 3, 3, 30, 7}}
				if !onlyNoop {
					firstRows = append(firstRows, [9]int64{4, 40, 6, 2, 4, 4, 4, 40, 11})
				}
				first := odkuActionInput(t, proc, firstRows)
				inputs := []*batch.Batch{first}
				if !onlyNoop {
					second := odkuActionInput(t, proc, [][9]int64{{99, 30, 5, 3, 0, 3, 3, 30, 7}, {99, 40, 0, 4, 0, 4, 4, 40, 11}})
					for _, input := range []*batch.Batch{first, second} {
						vector.SetFixedAtNoTypeCheck(input.Vecs[9], 1, types.Rowid{0: 4})
					}
					inputs = []*batch.Batch{first, second}
				}
				child.WithBatchs(inputs)
				require.NoError(t, a.Prepare(proc))
				require.True(t, a.ctr.actions.staticKeys)
				out, err := a.Call(proc)
				require.NoError(t, err)
				if onlyNoop {
					require.Nil(t, out.Batch, "a physical no-op needs no writer row")
					require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](a.ctr.actionRows.images.Vecs[2], 0))
					require.Zero(t, a.ctr.actions.logicalAffected)
				} else {
					require.NotNil(t, out.Batch)
					require.Equal(t, []int64{12, 17}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[2]))
					require.Equal(t, []int64{3, 4}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[4]))
					require.Equal(t, []int64{7, 11}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[6]))
					require.Equal(t, []types.Rowid{{0: 3}, {0: 4}}, vector.MustFixedColWithTypeCheck[types.Rowid](out.Batch.Vecs[7]))
				}
				out, err = a.Call(proc)
				require.NoError(t, err)
				require.Nil(t, out.Batch)
				a.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestODKUAutomaticAssignmentFollowsEffectiveChange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		inputs   []int64
		want     []int64
		affected uint64
	}{
		{"noop", []int64{0}, nil, 0},
		{"change then noop", []int64{1, 0}, []int64{99, 8}, 2},
		{"change then revert", []int64{1, -1}, []int64{99, 7}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, proc, child, _ := odkuActionFixture(t, 16<<20)
			c := a.PreInsertCtx.OdkuActions
			c.Constraints = c.Constraints[:1] // Column u is ordinary in this fixture.
			c.UpdateColumns = append(c.UpdateColumns, 1)
			c.UpdateExprs = append(c.UpdateExprs, &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 99}}}})
			c.AutoUpdateAssignmentCount = 1
			inputs := make([][9]int64, len(tc.inputs))
			for i, value := range tc.inputs {
				inputs[i] = [9]int64{3, 30, value, int64(i + 1), 3, 3, 3, 30, 7}
			}
			child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, inputs)})
			var count uint64
			publications := 0
			a.PublishAffectedRows = func(rows uint64) error { count = rows; publications++; return nil }
			require.NoError(t, a.Prepare(proc))
			out, err := a.Call(proc)
			require.NoError(t, err)
			if tc.want == nil {
				require.Nil(t, out.Batch)
			} else {
				require.NotNil(t, out.Batch)
				require.Equal(t, tc.want[0], vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[1], 0))
				require.Equal(t, tc.want[1], vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
			}
			out, err = a.Call(proc)
			require.NoError(t, err)
			require.Nil(t, out.Batch)
			require.Equal(t, tc.affected, count)
			require.Equal(t, 1, publications)
		})
	}
}

func TestODKUFixedOutputSkipsUnchangedOwners(t *testing.T) {
	a, proc, child, _ := odkuActionFixture(t, 16<<20)
	input := odkuActionInput(t, proc, [][9]int64{
		{3, 30, 1, 1, 3, 3, 3, 30, 7},
		{4, 40, 0, 2, 4, 4, 4, 40, 11},
		{5, 50, 2, 3, 5, 5, 5, 50, 15},
	})
	for row := range 3 {
		vector.SetFixedAtNoTypeCheck(input.Vecs[9], row, types.Rowid{0: byte(row + 3)})
	}
	child.WithBatchs([]*batch.Batch{input})
	var count uint64
	a.PublishAffectedRows = func(rows uint64) error { count = rows; return nil }
	require.NoError(t, a.Prepare(proc))
	out, err := a.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, out.Batch)
	require.Equal(t, []int64{3, 5}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[0]), "the unchanged middle owner never reaches physical writers")
	require.Equal(t, []int64{8, 17}, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[2]))
	out, err = a.Call(proc)
	require.NoError(t, err)
	require.Nil(t, out.Batch)
	require.Equal(t, uint64(4), count)
}

func TestODKUEmptyChangePhaseRetainsAutomaticAndGeneratedImage(t *testing.T) {
	a, proc, child, _ := odkuActionFixture(t, 16<<20)
	c := a.PreInsertCtx.OdkuActions
	c.Constraints = c.Constraints[:1]
	c.ChangeCheckAssignmentCount, c.AutoUpdateAssignmentCount = 0, 1
	c.ChangeCheckColumns = nil
	c.UpdateColumns = []int32{1, 2}
	c.UpdateExprs = []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 99}}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
	}
	child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, [][9]int64{{3, 30, 999, 1, 3, 3, 3, 30, 30}})})
	require.NoError(t, a.Prepare(proc))
	out, err := a.Call(proc)
	require.NoError(t, err)
	require.Nil(t, out.Batch)
	require.Zero(t, a.ctr.actions.logicalAffected)
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](a.ctr.actionRows.images.Vecs[1], 0))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](a.ctr.actionRows.images.Vecs[2], 0))
}

func TestODKUScratchPreservesSelectedMetadata(t *testing.T) {
	a, proc, _, _ := odkuActionFixture(t, 16<<20)
	dst, src := batch.NewWithSize(2), batch.NewWithSize(2)
	for _, b := range []*batch.Batch{dst, src} {
		b.Vecs[0], b.Vecs[1] = vector.NewVec(types.T_int64.ToType()), vector.NewVec(types.T_varchar.ToType())
		defer b.Clean(proc.Mp())
	}
	for round := range 4 {
		src.CleanOnlyData()
		for row := range 2 {
			null := (row+round)%2 == 0
			require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(round*10+row), null, proc.Mp()))
			require.NoError(t, vector.AppendBytes(src.Vecs[1], []byte(strings.Repeat("v", 128+round)), null, proc.Mp()))
		}
		src.SetRowCount(2)
		if round%2 == 1 { // ROLLUP NULL; grouping survives repeated scratch reuse.
			src.Vecs[0].GetGrouping().Set(1)
			src.Vecs[1].GetGrouping().Set(1)
		}
		for _, row := range []int{1, 0, 1} {
			dst.Vecs[0].SetSorted(true)
			require.NoError(t, a.copyODKURow(proc, dst, src, row, nil))
			require.False(t, dst.Vecs[0].GetSorted())
			for col := range dst.Vecs {
				require.Equal(t, src.Vecs[col].IsNull(uint64(row)), dst.Vecs[col].IsNull(0))
				require.Equal(t, src.Vecs[col].GetGrouping().Contains(uint64(row)), dst.Vecs[col].GetGrouping().Contains(0))
				if !dst.Vecs[col].IsNull(0) {
					require.Equal(t, src.Vecs[col].GetRawBytesAt(row), dst.Vecs[col].GetRawBytesAt(0))
				}
			}
			require.LessOrEqual(t, len(dst.Vecs[1].GetArea()), 131, "reused varlen scratch retains only the live row")
		}
	}
	// Constants share a physical value; grouping still selects the logical row.
	src.Vecs[0].Free(proc.Mp())
	src.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 2, proc.Mp())
	src.Vecs[0].GetGrouping().Set(0)
	require.NoError(t, a.copyODKURow(proc, dst, src, 1, nil))
	require.True(t, dst.Vecs[0].IsNull(0))
	require.False(t, dst.Vecs[0].GetGrouping().Contains(0))
	require.NoError(t, a.copyODKURow(proc, dst, src, 0, nil))
	require.True(t, dst.Vecs[0].GetGrouping().Contains(0))
	// The varlen primitive selects grouping before normalizing a constant's
	// value slot. Repeated-copy elision must preserve that selected metadata.
	src.Vecs[1].Free(proc.Mp())
	var err error
	src.Vecs[1], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("constant", 32)), 2, proc.Mp())
	require.NoError(t, err)
	src.Vecs[1].GetGrouping().Set(0)
	for _, row := range []int{1, 1, 0, 0, 1} {
		require.NoError(t, a.copyODKURow(proc, dst, src, row, nil))
		require.Equal(t, row == 0, dst.Vecs[1].GetGrouping().Contains(0))
		require.Equal(t, src.Vecs[1].GetBytesAt(row), dst.Vecs[1].GetBytesAt(0))
	}
	for _, b := range []*batch.Batch{dst, src} {
		b.Vecs[0].Free(proc.Mp())
		b.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	}
	require.NoError(t, vector.AppendFixedList(src.Vecs[0], []float64{math.Copysign(0, -1), 0}, nil, proc.Mp()))
	for _, row := range []int{0, 0, 1, 1, 0} {
		require.NoError(t, a.copyODKURow(proc, dst, src, row, nil))
		require.Equal(t, math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](src.Vecs[0], row)), math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](dst.Vecs[0], 0)), "scratch equality preserves signed-zero bits")
	}
}

func TestODKUActionsFinalOutputBoundaries(t *testing.T) {
	for _, count := range []int{8191, 8192, 8193, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			a, proc, child, account := odkuActionFixture(t, 64<<20)
			input := make([][9]int64, count)
			for row := range input {
				input[row] = [9]int64{int64(row + 1), int64(row + 1), int64(row + 11), int64(row + 1)}
			}
			b := odkuActionInput(t, proc, input)
			b.Vecs[2].GetNulls().Add(0)
			b.Vecs[2].GetGrouping().Set(0)
			wide := count == 5
			if wide {
				v := vector.NewVec(types.New(types.T_varchar, 0, 0))
				b.Vecs = append(b.Vecs, v)
				for row := range count {
					require.NoError(t, vector.AppendBytes(v, []byte(strings.Repeat("x", 1<<19)), row == 1, proc.Mp()))
				}
				v.GetGrouping().Set(1)
				// Replacing one retained payload creates dead input area. It
				// must not enter either output values or the byte quota.
				replacement := vector.NewVec(types.T_varchar.ToType())
				defer replacement.Free(proc.Mp())
				require.NoError(t, vector.AppendBytes(replacement, []byte(strings.Repeat("y", 1<<19)), false, proc.Mp()))
				require.NoError(t, v.Copy(replacement, 0, 0, proc.Mp()))
				a.PreInsertCtx.OdkuActions.SourceColumns = append(a.PreInsertCtx.OdkuActions.SourceColumns, &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 10}}})
			}
			for range 2 {
				child.WithBatchs([]*batch.Batch{b})
				require.NoError(t, a.Prepare(proc))
				total, batches := 0, []int{}
				for {
					out, err := a.Call(proc)
					require.NoError(t, err)
					if out.Batch == nil {
						break
					}
					batches = append(batches, out.Batch.RowCount())
					for row := range out.Batch.RowCount() {
						require.Equal(t, int64(total+1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[0], row))
						require.Equal(t, total == 0, out.Batch.Vecs[2].IsNull(uint64(row)))
						require.Equal(t, total == 0, out.Batch.Vecs[2].GetGrouping().Contains(uint64(row)))
						if wide {
							require.Equal(t, b.Vecs[10].IsNull(uint64(total)), out.Batch.Vecs[4].IsNull(uint64(row)))
							require.Equal(t, total == 1, out.Batch.Vecs[4].GetGrouping().Contains(uint64(row)))
							if total != 1 {
								require.Equal(t, b.Vecs[10].GetBytesAt(total), out.Batch.Vecs[4].GetBytesAt(row))
							}
						}
						total++
					}
				}
				require.Equal(t, count, total)
				if wide {
					require.Equal(t, []int{3, 2}, batches)
				} else if count <= 8192 {
					require.Equal(t, []int{count}, batches)
				} else {
					require.Equal(t, []int{8192, 1}, batches)
				}
				a.Reset(proc, false, nil)
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestODKUFinalOutputFailureNeverPublishesPartialColumns(t *testing.T) {
	const limit = uint64(16 << 20)
	a, proc, child, account := odkuActionFixture(t, limit)
	input := make([][9]int64, 32)
	for row := range input {
		input[row] = [9]int64{int64(row + 1), int64(row + 1), 1, int64(row + 1)}
	}
	child.WithBatchs([]*batch.Batch{odkuActionInput(t, proc, input)})
	var filler []byte
	t.Cleanup(func() {
		if filler != nil {
			proc.Mp().Free(filler)
		}
	})
	child.WithEndOfDataCallback(func() {
		// All actions have finished, but final output has not begun. Admit
		// its first column and deny the next, using the real native budget.
		require.NoError(t, a.ctr.buf.Vecs[0].PreExtend(len(input), proc.Mp()))
		used := account.Snapshot().Used
		var err error
		filler, err = proc.Mp().AllocAccounted(int(limit-used-64), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
		require.NoError(t, err)
	})
	require.NoError(t, a.Prepare(proc))
	out, err := a.Call(proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), "%v", err)
	require.Nil(t, out.Batch)
	require.Equal(t, len(input), a.ctr.buf.Vecs[0].Length(), "the failure is after a successful column")
	require.Zero(t, a.ctr.buf.RowCount(), "publication occurs only after every column succeeds")
	require.Zero(t, a.ctr.actions.finalRow)
	_, err = a.Call(proc)
	require.ErrorContains(t, err, "require Reset")
	proc.Mp().Free(filler)
	filler = nil
	a.Reset(proc, false, nil)
	child.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	child.WithEndOfDataCallback(nil).WithBatchs([]*batch.Batch{odkuActionInput(t, proc, input)})
	require.NoError(t, a.Prepare(proc))
	out, err = a.Call(proc)
	require.NoError(t, err)
	require.Equal(t, len(input), out.Batch.RowCount(), "reset permits a clean successful attempt")
}

func TestODKUOutputBitmapAdmissionUsesSelectedRows(t *testing.T) {
	const (
		limit = uint64(16 << 20)
		first = 8192
	)
	for _, tc := range []struct {
		name       string
		headroom   uint64
		failSecond bool
	}{
		{name: "both batches fit", headroom: 768},
		{name: "selected null still needs a bitmap", headroom: 16, failSecond: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, proc, child, account := odkuActionFixture(t, limit)
			input := make([][9]int64, first+1)
			wantIDs, wantValues := make([]int64, first), make([]int64, first)
			for i := 0; i < first; i++ {
				id := int64(i + 1)
				input[i] = [9]int64{id, id * 10, 1, id, id, id, id, id * 10, 3}
				wantIDs[i], wantValues[i] = id, 4
			}
			input[first] = [9]int64{1000001, 10000010, 1, first + 1}
			source := odkuActionInput(t, proc, input)
			for i := 0; i < first; i++ {
				rowID := types.Rowid{0: 3, 1: byte(i), 2: byte(i >> 8)}
				require.NoError(t, vector.SetFixedAtNoTypeCheck(source.Vecs[9], i, rowID))
			}
			child.WithBatchs([]*batch.Batch{source})
			var filler []byte
			t.Cleanup(func() {
				if filler != nil {
					proc.Mp().Free(filler)
				}
			})
			child.WithEndOfDataCallback(func() {
				// Isolate bitmap admission after actual actions have retained all
				// originals and the final insert. Only the latter has NULL origins.
				require.Equal(t, first+1, a.ctr.actionRows.images.RowCount())
				for _, v := range a.ctr.actionRows.origins.Vecs {
					require.False(t, v.IsNull(first-1))
					require.True(t, v.IsNull(first))
				}
				for _, v := range a.ctr.buf.Vecs {
					require.NoError(t, v.PreExtend(first, proc.Mp()))
				}
				var err error
				filler, err = proc.Mp().AllocAccounted(int(limit-account.Snapshot().Used-tc.headroom), account, mpool.AllocationOwnerMin, preInsertUniqueAllocationSiteRetainedData)
				require.NoError(t, err)
			})
			require.NoError(t, a.Prepare(proc))
			out, err := a.Call(proc)
			require.NoError(t, err)
			require.NotNil(t, out.Batch)
			require.Equal(t, first, out.Batch.RowCount())
			require.Equal(t, wantIDs, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[0]))
			require.Equal(t, wantIDs, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[4]))
			require.Equal(t, wantValues, vector.MustFixedColWithTypeCheck[int64](out.Batch.Vecs[2]))

			out, err = a.Call(proc)
			if tc.failSecond {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), "%v", err)
				require.Nil(t, out.Batch)
				require.Zero(t, a.ctr.buf.RowCount(), "the failed batch is not published")
				require.Equal(t, first, a.ctr.actions.finalRow)
				_, err = a.Call(proc)
				require.ErrorContains(t, err, "require Reset")
			} else {
				require.NoError(t, err)
				require.NotNil(t, out.Batch)
				require.Equal(t, 1, out.Batch.RowCount())
				require.Equal(t, int64(1000001), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[0], 0))
				for _, v := range out.Batch.Vecs[4:] {
					require.True(t, v.IsNull(0))
				}
			}
			proc.Mp().Free(filler)
			filler = nil
			a.Reset(proc, false, nil)
			child.Free(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
		})
	}
}
