// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func makeColExprForTest(idx int32, typ types.T) *plan.Expr {
	schema := []string{"a", "b", "c", "d"}
	containerType := typ.ToType()
	exprType := plan2.MakePlan2Type(&containerType)

	return &plan.Expr{
		Typ: exprType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: idx,
				Name:   schema[idx],
			},
		},
	}
}

func makeFunctionExprForTest(name string, args []*plan.Expr) *plan.Expr {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = plan2.MakeTypeByPlan2Expr(arg)
	}

	funId, returnType, _, _ := function.GetFunctionByName(context.TODO(), name, argTypes)

	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&returnType),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     funId,
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func makeZonemapForTest(typ types.T, min any, max any) [64]byte {
	zm := index.NewZoneMap(typ.ToType())
	_ = zm.Update(min)
	_ = zm.Update(max)
	buf, _ := zm.Marshal()

	var zmBuf [64]byte
	copy(zmBuf[:], buf[:])
	return zmBuf
}

func makeBlockMetaForTest() BlockMeta {
	return BlockMeta{
		Zonemap: [][64]byte{
			makeZonemapForTest(types.T_int64, int64(10), int64(100)),
			makeZonemapForTest(types.T_int64, int64(20), int64(200)),
			makeZonemapForTest(types.T_int64, int64(30), int64(300)),
			makeZonemapForTest(types.T_int64, int64(1), int64(8)),
		},
	}
}

func getTableDefBySchemaAndType(name string, columns []string, schema []string, types []types.Type) *plan.TableDef {
	columnsMap := make(map[string]struct{})
	for _, col := range columns {
		columnsMap[col] = struct{}{}
	}

	var cols []*plan.ColDef
	nameToIndex := make(map[string]int32)

	for i, col := range schema {
		if _, ok := columnsMap[col]; ok {
			cols = append(cols, &plan.ColDef{
				Name: col,
				Typ:  plan2.MakePlan2Type(&types[i]),
			})
		}
		nameToIndex[col] = int32(i)
	}

	return &plan.TableDef{
		Name:          name,
		Cols:          cols,
		Name2ColIndex: nameToIndex,
	}
}

func makeTableDefForTest(columns []string) *plan.TableDef {
	schema := []string{"a", "b", "c", "d"}
	types := []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}
	return getTableDefBySchemaAndType("t1", columns, schema, types)
}

func TestBlockMetaMarshal(t *testing.T) {
	location := []byte("test")
	meta := BlockMeta{
		Info: catalog.BlockInfo{},
		Zonemap: [][64]byte{
			makeZonemapForTest(types.T_int64, int64(10), int64(100)),
			makeZonemapForTest(types.T_blob, []byte("a"), []byte("h")),
			// makeZonemapForTest(types.T_varchar, "a", "h"),
		},
	}
	meta.Info.SetMetaLocation(location)
	data := blockMarshal(meta)
	meta0 := blockUnmarshal(data)
	require.Equal(t, meta, meta0)
}

func TestCheckExprIsMonotonic(t *testing.T) {
	type asserts = struct {
		result bool
		expr   *plan.Expr
	}
	testCases := []asserts{
		// a > 1  -> true
		{true, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		})},
		// a >= b -> true
		{true, makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		})},
		// abs(a) -> false
		{false, makeFunctionExprForTest("abs", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
		})},
	}

	t.Run("test checkExprIsMonotonic", func(t *testing.T) {
		for i, testCase := range testCases {
			isMonotonic := plan2.CheckExprIsMonotonic(context.TODO(), testCase.expr)
			if isMonotonic != testCase.result {
				t.Fatalf("checkExprIsMonotonic testExprs[%d] is different with expected", i)
			}
		}
	})
}

// delete this if TestNeedRead is not skipped anymore
func TestMakeBlockMeta(t *testing.T) {
	_ = makeBlockMetaForTest()
}

func TestNeedRead(t *testing.T) {
	t.Skip("NeedRead always returns true fot start cn-dn with flushing")
	type asserts = struct {
		result  bool
		columns []string
		expr    *plan.Expr
	}
	blockMeta := makeBlockMetaForTest()

	testCases := []asserts{
		{true, []string{"a"}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		})},
		{false, []string{"a"}, makeFunctionExprForTest("<", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(-1),
		})},
		{false, []string{"a"}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(3000000),
		})},
		{false, []string{"a", "d"}, makeFunctionExprForTest("<", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		})},
		{true, []string{"a", "d"}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		})},
		// c > (a + d) => true
		{true, []string{"c", "a", "d"}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("+", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				makeColExprForTest(2, types.T_int64),
			}),
		})},
		// (a > b) and (c > d) => true
		{true, []string{"a", "b", "c", "d"}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				makeColExprForTest(1, types.T_int64),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(2, types.T_int64),
				makeColExprForTest(3, types.T_int64),
			}),
		})},
		{true, []string{"a"}, makeFunctionExprForTest("abs", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
		})},
	}

	t.Run("test needRead", func(t *testing.T) {
		for i, testCase := range testCases {
			columnMap, columns, maxCol := plan2.GetColumnsByExpr(testCase.expr, makeTableDefForTest(testCase.columns))
			result := needRead(context.Background(), testCase.expr, blockMeta, makeTableDefForTest(testCase.columns), columnMap, columns, maxCol, testutil.NewProc())
			if result != testCase.result {
				t.Fatalf("test needRead at cases[%d], get result is different with expected", i)
			}
		}
	})
}

func TestGetNonIntPkValueByExpr(t *testing.T) {
	type asserts = struct {
		result bool
		data   any
		expr   *plan.Expr
		typ    types.T
	}

	testCases := []asserts{
		// a > "a"  false   only 'and', '=' function is supported
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2StringConstExprWithType("a"),
		}), types.T_int64},
		// a = 100  true
		{true, int64(100),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(100),
			}), types.T_int64},
		// b > 10 and a = "abc"  true
		{true, []byte("abc"),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
			}), types.T_char},
	}

	t.Run("test getPkValueByExpr", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := getPkValueByExpr(testCase.expr, "a", testCase.typ)
			if result != testCase.result {
				t.Fatalf("test getPkValueByExpr at cases[%d], get result is different with expected", i)
			}
			if result {
				if a, ok := data.([]byte); ok {
					b := testCase.data.([]byte)
					if !bytes.Equal(a, b) {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				} else {
					if data != testCase.data {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				}
			}
		}
	})
}

func TestComputeRangeByNonIntPk(t *testing.T) {
	type asserts = struct {
		result bool
		data   uint64
		expr   *plan.Expr
	}

	getHash := func(e *plan.Expr) uint64 {
		_, ret := getConstantExprHashValue(context.TODO(), e, testutil.NewProc())
		return ret
	}

	testCases := []asserts{
		// a > "a"  false   only 'and', '=' function is supported
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2StringConstExprWithType("a"),
		})},
		// a > coalesce("a")  false,  the second arg must be constant
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("coalesce", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2StringConstExprWithType("a"),
			}),
		})},
		// a = "abc"  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2StringConstExprWithType("abc"),
			})},
		// a = "abc" and b > 10  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
		// b > 10 and a = "abc"  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
			})},
		// a = "abc" or b > 10  false
		{false, 0,
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
		// a = "abc" or a > 10  false
		{false, 0,
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
	}

	t.Run("test computeRangeByNonIntPk", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := computeRangeByNonIntPk(context.TODO(), testCase.expr, "a", testutil.NewProc())
			if result != testCase.result {
				t.Fatalf("test computeRangeByNonIntPk at cases[%d], get result is different with expected", i)
			}
			if result {
				if data != testCase.data {
					t.Fatalf("test computeRangeByNonIntPk at cases[%d], data is not match", i)
				}
			}
		}
	})
}

func TestComputeRangeByIntPk(t *testing.T) {
	type asserts = struct {
		result bool
		items  []int64
		expr   *plan.Expr
	}

	testCases := []asserts{
		// a > abs(20)   not support now
		{false, []int64{21}, makeFunctionExprForTest("like", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("abs", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
		})},
		// a > 20
		{true, []int64{}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		})},
		// a > 20 and b < 1  is equal a > 20
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
		})},
		// 1 < b and a > 20   is equal a > 20
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(1),
				makeColExprForTest(1, types.T_int64),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
		})},
		// a > 20 or b < 1  false.
		{false, []int64{}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
		})},
		// a = 20
		{true, []int64{20}, makeFunctionExprForTest("=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		})},
		// a > 20 and a < =25
		{true, []int64{21, 22, 23, 24, 25}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(25),
			}),
		})},
		// a > 20 and a <=25 and b > 100   todo： unsupport now。  when compute a <=25 and b > 10, we get items too much.
		{false, []int64{21, 22, 23, 24, 25}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(25),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(100),
				}),
			}),
		})},
		// a > 20 and a < 10  => empty
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		})},
		// a < 20 or 100 < a
		{false, []int64{}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(100),
				makeColExprForTest(0, types.T_int64),
			}),
		})},
		// a =1 or a = 2 or a=30
		{true, []int64{2, 1, 30}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(2),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
		})},
		// (a >5 or a=1) and (a < 8 or a =11) => 1,6,7,11  todo,  now can't compute now
		{false, []int64{6, 7, 11, 1}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(5),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(8),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(11),
				}),
			}),
		})},
	}

	t.Run("test computeRangeByIntPk", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := computeRangeByIntPk(testCase.expr, "a", "")
			if result != testCase.result {
				t.Fatalf("test computeRangeByIntPk at cases[%d], get result is different with expected", i)
			}
			if result {
				if len(data.items) != len(testCase.items) {
					t.Fatalf("test computeRangeByIntPk at cases[%d], data length is not match", i)
				}
				for j, val := range testCase.items {
					if data.items[j] != val {
						t.Fatalf("test computeRangeByIntPk at cases[%d], data[%d] is not match", i, j)
					}
				}
			}
		}
	})
}

// func TestGetListByRange(t *testing.T) {
// 	type asserts = struct {
// 		result []DNStore
// 		list   []DNStore
// 		r      [][2]int64
// 	}

// 	testCases := []asserts{
// 		{[]DNStore{{UUID: "1"}, {UUID: "2"}}, []DNStore{{UUID: "1"}, {UUID: "2"}}, [][2]int64{{14, 32324234234234}}},
// 		{[]DNStore{{UUID: "1"}}, []DNStore{{UUID: "1"}, {UUID: "2"}}, [][2]int64{{14, 14}}},
// 	}

// 	t.Run("test getListByRange", func(t *testing.T) {
// 		for i, testCase := range testCases {
// 			result := getListByRange(testCase.list, testCase.r)
// 			if len(result) != len(testCase.result) {
// 				t.Fatalf("test getListByRange at cases[%d], data length is not match", i)
// 			}
// 			/*
// 				for j, r := range testCase.result {
// 					if r.UUID != result[j].UUID {
// 						t.Fatalf("test getListByRange at cases[%d], result[%d] is not match", i, j)
// 					}
// 				}
// 			*/
// 		}
// 	})
// }
