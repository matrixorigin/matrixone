// Copyright 2021 - 2026 Matrix Origin
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

package readutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioutil "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestCompileFilterExprUnaryNull(t *testing.T) {
	table := &plan.TableDef{
		Name: "t", Name2ColIndex: map[string]int32{"id": 0, "v": 1},
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "v", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
	}
	for _, name := range []string{"isnull", "is_null", "isnotnull", "is_not_null"} {
		t.Run(name, func(t *testing.T) {
			isNull := name == "isnull" || name == "is_null"
			canonical := "isnotnull"
			if isNull {
				canonical = "isnull"
			}
			col := MakeColExprForTest(1, types.T_varchar, "v")
			makeExpr := func(args ...*plan.Expr) *plan.Expr {
				expr := MakeFunctionExprForTest(canonical, []*plan.Expr{col})
				expr.GetF().Func.ObjName = name
				expr.GetF().Args = args
				return expr
			}
			fast, load, object, block, seek, compiled, _ := CompileFilterExpr(makeExpr(col), table, nil)
			require.True(t, compiled, "NULL predicates have one column argument, not a column and a constant")
			require.Nil(t, fast)
			require.NotNil(t, load)
			require.NotNil(t, object)
			require.NotNil(t, block)
			require.Nil(t, seek)

			for _, counts := range []struct{ rows, nulls uint32 }{{0, 0}, {2, 0}, {2, 1}, {2, 2}} {
				t.Run(fmt.Sprintf("rows=%d/nulls=%d", counts.rows, counts.nulls), func(t *testing.T) {
					seqnums := objectio.NewSeqnums([]uint16{0, 1})
					meta := objectio.NewBlock(seqnums)
					meta.BlockHeader().SetRows(counts.rows)
					// The other column deliberately has the opposite null distribution.
					meta.ColumnMeta(0).SetNullCnt(counts.rows - counts.nulls)
					meta.ColumnMeta(1).SetNullCnt(counts.nulls)
					stop, keep, err := block(0, meta, nil)
					require.NoError(t, err)
					require.False(t, stop, "null counts cannot terminate a sorted scan")
					want := counts.nulls < counts.rows
					if isNull {
						want = counts.nulls > 0
					}
					require.Equal(t, want, keep)
				})
			}

			for caseName, args := range map[string][]*plan.Expr{
				"no_argument": nil,
				"constant":    {plan2.MakePlan2StringConstExprWithType("x")},
				"two_columns": {col, col},
				"binary":      {col, plan2.MakePlan2StringConstExprWithType("x")},
			} {
				t.Run(caseName, func(t *testing.T) {
					_, _, _, _, _, compiled, _ := CompileFilterExpr(makeExpr(args...), table, nil)
					require.False(t, compiled)
				})
			}
		})
	}
}

func TestCompileFilterExprNullColumnIdentity(t *testing.T) {
	newTable := func() *plan.TableDef {
		return &plan.TableDef{Name: "t", Name2ColIndex: map[string]int32{"id": 0, "v.part": 1, "v": 2}, Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Primary: true},
			{Name: "v.part", Seqnum: 1},
			{Name: "v", Seqnum: 2},
		}}
	}
	for _, name := range []string{"isnull", "isnotnull"} {
		t.Run(name, func(t *testing.T) {
			for _, column := range []string{"v", "x.v", "x.y.v", "v.part", "x.y.v.part"} {
				t.Run(column, func(t *testing.T) {
					// Scan-local ColPos is deliberately not the full-schema ordinal.
					expr := MakeFunctionExprForTest(name, []*plan.Expr{MakeColExprForTest(0, types.T_varchar, column)})
					_, _, _, block, _, compiled, _ := CompileFilterExpr(expr, newTable(), nil)
					require.True(t, compiled)
					meta := objectio.NewBlock(objectio.NewSeqnums([]uint16{0, 1, 2}))
					meta.BlockHeader().SetRows(1)
					meta.ColumnMeta(1).SetNullCnt(1)
					meta.ColumnMeta(2).SetNullCnt(1)
					stop, keep, err := block(0, meta, nil)
					require.NoError(t, err)
					require.False(t, stop)
					require.Equal(t, name == "isnull", keep, "must not use the non-NULL primary key's metadata")
				})
			}
			for _, invalid := range []string{"unknown", "empty_name", "missing_map", "negative_index", "out_of_range", "nil_column", "wrong_name", "ambiguous_suffix", "nil_table"} {
				t.Run(invalid, func(t *testing.T) {
					table := newTable()
					column := "x.y.v"
					switch invalid {
					case "unknown":
						column = "x.y.unknown"
					case "empty_name":
						column = ""
					case "missing_map":
						table.Name2ColIndex = nil
					case "negative_index":
						table.Name2ColIndex["v"] = -1
					case "out_of_range":
						table.Name2ColIndex["v"] = 99
					case "nil_column":
						table.Cols[2] = nil
					case "wrong_name":
						table.Name2ColIndex["v"] = 0
					case "ambiguous_suffix":
						column = "x.y.v.part"
						table.Name2ColIndex["part"] = 2
						table.Cols[2].Name = "part"
					case "nil_table":
						table = nil
					}
					expr := MakeFunctionExprForTest(name, []*plan.Expr{MakeColExprForTest(0, types.T_varchar, column)})
					fast, load, object, block, seek, compiled, _ := CompileFilterExpr(expr, table, nil)
					require.False(t, compiled, "unproven column identity must retain residual row evaluation")
					require.Nil(t, fast)
					require.Nil(t, load)
					require.Nil(t, object)
					require.Nil(t, block)
					require.Nil(t, seek)
				})
			}
		})
	}
}

func TestCompileFilterExprAllColumnsChecked(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	table := &plan.TableDef{Name: "t", Name2ColIndex: map[string]int32{"k": 0, "id": 1, "v": 2}, Cols: []*plan.ColDef{
		{Name: "k", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "id", Seqnum: 1, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "v", Seqnum: 2, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}}
	for _, op := range []string{"=", "<", "<=", ">", ">=", "between", "in_range", "in", "prefix_eq", "prefix_between", "prefix_in_range", "prefix_in"} {
		t.Run(op, func(t *testing.T) {
			makePredicate := func(column string) *plan.Expr {
				col := MakeColExprForTest(0, types.T_varchar, column)
				if op == "in" || op == "prefix_in" {
					expr := MakeInExprForTest(col, [][]byte{[]byte("1"), []byte("2")}, types.T_varchar, proc.Mp())
					if op == "prefix_in" {
						expr.GetF().Func.ObjName = "prefix_in"
					}
					return expr
				}
				value := "1"
				if op == "<" {
					value = "3"
				} else if op == ">" {
					value = "0"
				}
				args := []*plan.Expr{col, plan2.MakePlan2StringConstExprWithType(value)}
				if op == "between" || op == "in_range" || op == "prefix_between" || op == "prefix_in_range" {
					args = append(args, plan2.MakePlan2StringConstExprWithType("2"))
				}
				if op == "in_range" || op == "prefix_in_range" {
					args = append(args, plan2.MakePlan2Uint8ConstExprWithType(0))
				}
				expr := MakeFunctionExprForTest(op, args)
				_, err := plan2.ReplaceFoldExpr(proc, expr, &executors)
				require.NoError(t, err)
				require.NoError(t, plan2.EvalFoldExpr(proc, expr, &executors))
				return expr
			}
			nullExpr := MakeFunctionExprForTest("isnull", []*plan.Expr{MakeColExprForTest(2, types.T_varchar, "v")})
			predicate := makePredicate("x.y.id")
			pkFast, _, _, _, _, canCompile, _ := CompileFilterExpr(predicate, table, nil)
			require.True(t, canCompile)
			require.NotNil(t, pkFast, "the resolved column is a non-leading primary key, not k")
			fast, _, _, _, _, compiled, _ := CompileFilterExprs([]*plan.Expr{predicate, nullExpr}, table, nil)
			require.True(t, compiled)
			require.NotNil(t, fast)
			zm := index.NewZM(types.T_varchar, 0)
			index.UpdateZM(zm, []byte("1"))
			index.UpdateZM(zm, []byte("2"))
			stats := objectio.NewObjectStats()
			require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
			keep, err := fast(stats)
			require.NoError(t, err)
			require.True(t, keep, "the first column k must not replace the non-leading primary key")
			absent := "9"
			if op == ">" || op == ">=" {
				absent = "0"
			}
			nonmatching := index.NewZM(types.T_varchar, 0)
			index.UpdateZM(nonmatching, []byte(absent))
			require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, nonmatching))
			keep, err = fast(stats)
			require.NoError(t, err)
			require.False(t, keep, "known, unrelated PK ranges should still be pruned")

			for _, badName := range []string{"x.y.unknown", "x.y.v.id"} {
				badTable := *table
				if badName == "x.y.v.id" {
					badTable.Name2ColIndex = map[string]int32{"k": 0, "id": 1, "v": 2, "v.id": 0}
					badTable.Cols = append([]*plan.ColDef(nil), table.Cols...)
					badTable.Cols[0] = &plan.ColDef{Name: "v.id", Seqnum: 0}
				}
				bad := makePredicate(badName)
				for _, shape := range []string{"list", "and", "or", "list_expr"} {
					t.Run(badName+"/"+shape, func(t *testing.T) {
						exprs := []*plan.Expr{nullExpr, bad}
						if shape == "list_expr" {
							// Internal admission must inspect list children even when the
							// later compiler would reject this synthetic list shape.
							exprs = []*plan.Expr{{Expr: &plan.Expr_List{List: &plan.ExprList{List: exprs}}}}
						} else if shape != "list" {
							exprs = []*plan.Expr{MakeFunctionExprForTest(shape, exprs)}
						}
						fast, load, object, block, seek, compiled, hint := CompileFilterExprs(exprs, &badTable, nil)
						require.False(t, compiled, "any unproven reference must reject the complete filter")
						require.False(t, hint)
						require.Nil(t, fast)
						require.Nil(t, load)
						require.Nil(t, object)
						require.Nil(t, block)
						require.Nil(t, seek)
					})
				}
			}
		})
	}
}

func TestCompileFilterExprNullObjectMetadata(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("null-filter", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	table := &plan.TableDef{Name: "t", Name2ColIndex: map[string]int32{"v": 1}, Cols: []*plan.ColDef{
		{Name: "id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "v", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}}
	// Four two-row objects cover no NULLs, mixed/all NULLs, and pre-ADD-COLUMN data.
	for _, nulls := range []int{0, 1, 2, -1} {
		t.Run(fmt.Sprintf("nulls=%d", nulls), func(t *testing.T) {
			seqnums := []uint16{0, 1}
			if nulls == -1 {
				seqnums = seqnums[:1]
			}
			bat := batch.NewWithSize(len(seqnums))
			t.Cleanup(func() { bat.Clean(mp) })
			for i := range seqnums {
				bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
				for row := 0; row < 2; row++ {
					require.NoError(t, vector.AppendBytes(bat.Vecs[i], []byte("x"), i == 1 && row < nulls, mp))
				}
			}
			bat.SetRowCount(2)
			writer := objectioutil.ConstructWriter(0, seqnums, -1, false, false, fs)
			_, err := writer.WriteBatch(bat)
			require.NoError(t, err)
			blocks, _, err := writer.Sync(ctx)
			require.NoError(t, err)
			require.Len(t, blocks, 1)
			location := objectio.BuildLocation(writer.GetName(), blocks[0].GetExtent(), 2, blocks[0].GetID())
			meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
			require.NoError(t, err)
			for _, name := range []string{"isnull", "isnotnull"} {
				expr := MakeFunctionExprForTest(name, []*plan.Expr{MakeColExprForTest(0, types.T_varchar, "x.y.v")})
				_, _, object, block, _, compiled, _ := CompileFilterExpr(expr, table, fs)
				require.True(t, compiled)
				keep, err := object(meta, nil)
				require.NoError(t, err)
				want := nulls < 2
				if name == "isnull" {
					want = nulls > 0
				}
				if nulls == -1 {
					want = true // No metadata proof: residual row evaluation owns the result.
				}
				require.Equal(t, want, keep, name)
				stop, keep, err := block(0, meta.MustDataMeta().GetBlockMeta(0), nil)
				require.NoError(t, err)
				require.False(t, stop)
				require.Equal(t, want, keep, name)
			}
		})
	}
}

func TestCompileFilterExprsNullPreservesPKPruning(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	table := &plan.TableDef{
		Name: "t", Name2ColIndex: map[string]int32{"id": 0, "group_id": 1, "v": 2},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "group_id", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "v", Seqnum: 2, Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
	}
	in := MakeInExprForTest(MakeColExprForTest(0, types.T_varchar, "id"), [][]byte{[]byte("g-1"), []byte("g-2")}, types.T_varchar, proc.Mp())
	equal := MakeFunctionExprForTest("=", []*plan.Expr{
		MakeColExprForTest(1, types.T_varchar, "group_id"), plan2.MakePlan2StringConstExprWithType("g"),
	})
	_, err := plan2.ReplaceFoldExpr(proc, equal, &executors)
	require.NoError(t, err)
	require.NoError(t, plan2.EvalFoldExpr(proc, equal, &executors))
	require.True(t, plan2.HasFoldExprForList([]*plan.Expr{equal}))

	for _, name := range []string{"isnull", "isnotnull"} {
		t.Run(name, func(t *testing.T) {
			nullExpr := MakeFunctionExprForTest(name, []*plan.Expr{MakeColExprForTest(2, types.T_varchar, "v")})
			for _, nested := range []bool{false, true} {
				t.Run(fmt.Sprintf("nested=%v", nested), func(t *testing.T) {
					exprs := []*plan.Expr{in, equal, nullExpr}
					if nested {
						exprs = []*plan.Expr{MakeFunctionExprForTest("and", []*plan.Expr{in,
							MakeFunctionExprForTest("and", []*plan.Expr{equal, nullExpr})})}
					}
					fast, load, object, block, seek, compiled, hint := CompileFilterExprs(exprs, table, nil)
					require.True(t, compiled, "an IS [NOT] NULL conjunct must not disable PK pruning")
					require.True(t, hint)
					for _, key := range []string{"a", "g-1", "g-2", "z"} {
						zm := index.NewZM(types.T_varchar, 0)
						index.UpdateZM(zm, []byte(key))
						stats := objectio.NewObjectStats()
						require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
						require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
						keep, err := fast(stats)
						require.NoError(t, err)
						require.Equal(t, key == "g-1" || key == "g-2", keep)
						if !keep {
							var blocks objectio.BlockInfoSlice
							// A nil file service is intentional: rejected objects must not enter metadata/data I/O.
							_, loads, _, _, _, _, _, hits, err := FilterObjects(context.Background(), engine.RangesParam{},
								fast, load, object, block, seek, nil, []objectio.ObjectStats{*stats}, nil, &blocks, hint, nil, nil)
							require.NoError(t, err)
							require.Zero(t, loads)
							require.Equal(t, 1, hits)
							require.Zero(t, blocks.Len())
						}
					}
				})
			}
		})
	}
}
