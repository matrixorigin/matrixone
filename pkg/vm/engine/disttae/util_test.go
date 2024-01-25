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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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

	finfo, err := function.GetFunctionByName(context.TODO(), name, argTypes)
	if err != nil {
		panic(err)
	}

	retTyp := finfo.GetReturnType()

	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&retTyp),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     finfo.GetEncodedOverloadID(),
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func TestBlockMetaMarshal(t *testing.T) {
	location := []byte("test")
	var info catalog.BlockInfo
	info.SetMetaLocation(location)
	data := catalog.EncodeBlockInfo(info)
	info2 := catalog.DecodeBlockInfo(data)
	require.Equal(t, info, *info2)
}

func TestCheckExprIsZonemappable(t *testing.T) {
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

	t.Run("test checkExprIsZonemappable", func(t *testing.T) {
		for i, testCase := range testCases {
			zonemappable := plan2.ExprIsZonemappable(context.TODO(), testCase.expr)
			if zonemappable != testCase.result {
				t.Fatalf("checkExprIsZonemappable testExprs[%d] is different with expected", i)
			}
		}
	})
}

func TestEvalZonemapFilter(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)
	type myCase = struct {
		exprs  []*plan.Expr
		meta   objectio.BlockObject
		desc   []string
		expect []bool
	}

	zm0 := index.NewZM(types.T_float64, 0)
	zm0.Update(float64(-10))
	zm0.Update(float64(20))
	zm1 := index.NewZM(types.T_float64, 0)
	zm1.Update(float64(5))
	zm1.Update(float64(25))
	zm2 := index.NewZM(types.T_varchar, 0)
	zm2.Update([]byte("abc"))
	zm2.Update([]byte("opq"))
	zm3 := index.NewZM(types.T_varchar, 0)
	zm3.Update([]byte("efg"))
	zm3.Update(index.MaxBytesValue)
	cases := []myCase{
		{
			desc: []string{
				"a>10", "a>30", "a<=-10", "a<-10", "a+b>60", "a+b<-5", "a-b<-34", "a-b<-35", "a-b<=-35", "a>b",
				"a>b+15", "a>=b+15", "a>100 or b>10", "a>100 and b<0", "d>xyz", "d<=efg", "d<efg", "c>d", "c<d",
			},
			exprs: []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(60),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-5),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-34),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeColExprForTest(1, types.T_float64),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				makeFunctionExprForTest(">=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
				}),
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					makeFunctionExprForTest("<", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(0),
					}),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("xyz"),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(2, types.T_varchar),
					makeColExprForTest(3, types.T_varchar),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(2, types.T_varchar),
					makeColExprForTest(3, types.T_varchar),
				}),
			},
			meta: func() objectio.BlockObject {
				objDataMeta := objectio.BuildMetaData(1, 4)
				meta := objDataMeta.GetBlockMeta(0)
				meta.MustGetColumn(0).SetZoneMap(zm0)
				meta.MustGetColumn(1).SetZoneMap(zm1)
				meta.MustGetColumn(2).SetZoneMap(zm2)
				meta.MustGetColumn(3).SetZoneMap(zm3)
				return meta
			}(),
			expect: []bool{
				true, false, true, false, false, false, true, false, true, true,
				false, true, true, false, true, true, false, true, true,
			},
		},
	}

	columnMap := map[int]int{0: 0, 1: 1, 2: 2, 3: 3}

	for _, tc := range cases {
		for i, expr := range tc.exprs {
			cnt := plan2.AssignAuxIdForExpr(expr, 0)
			zms := make([]objectio.ZoneMap, cnt)
			vecs := make([]*vector.Vector, cnt)
			zm := colexec.EvaluateFilterByZoneMap(context.Background(), proc, expr, tc.meta, columnMap, zms, vecs)
			require.Equal(t, tc.expect[i], zm, tc.desc[i])
		}
	}
	require.Zero(t, m.CurrNB())
}

func TestGetCompositePkValueByExpr(t *testing.T) {
	type myCase struct {
		desc    []string
		exprs   []*plan.Expr
		expect  []int
		hasNull []bool
	}
	// a, b, c, d are columns of table t1
	// d,c,b are composite primary key
	tc := myCase{
		desc: []string{
			"a=10", "a=20 and b=10", "a=20 and d=10", "b=20 and c=10",
			"b=10 and d=20", "b=10 and c=20 and d=30",
			"c=10 and d=20", "d=10 or a=10", "d=10 or c=20 and a=30",
			"d=10 or c=20 and d=30", "d=10 and c=20 or d=30",
			"d=null", "c=null", "b=null", "null=b", "c=null and a=10",
		},
		hasNull: []bool{
			false, false, false, false, false, false, false, false, false, false, false,
			true, true, true, true, true,
		},
		expect: []int{
			0, 0, 1, 0, 1, 3, 2, 0, 0, 1, 0,
			0, 0, 0, 0, 0,
		},
		exprs: []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_float64),
				plan2.MakePlan2Float64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(3, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(2, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(3, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(2, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(20),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(3, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(30),
					}),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(2, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(3, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(2, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(20),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(0),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(3, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(2, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(3, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(2, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(3, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(3, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(2, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(3, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
			}),
		},
	}
	pks := []string{"d", "c", "b"}
	for i, expr := range tc.exprs {
		vals := make([]*plan.Literal, len(pks))
		ok, hasNull := getCompositPKVals(expr, pks, vals, nil)
		cnt := 0
		require.Equal(t, tc.hasNull[i], hasNull)
		if hasNull {
			require.False(t, ok)
			continue
		}
		if ok {
			for _, val := range vals {
				t.Logf("val: %v", val)
			}
			cnt = getValidCompositePKCnt(vals)
		}
		require.Equal(t, tc.expect[i], cnt)
	}
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
			result, _, data := getPkValueByExpr(testCase.expr, "a", testCase.typ, nil)
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

func mockStatsList(t *testing.T, statsCnt int) (statsList []objectio.ObjectStats) {
	for idx := 0; idx < statsCnt; idx++ {
		stats := objectio.NewObjectStats()
		blkCnt := rand.Uint32()%100 + 1
		require.Nil(t, objectio.SetObjectStatsBlkCnt(stats, blkCnt))
		require.Nil(t, objectio.SetObjectStatsRowCnt(stats, options.DefaultBlockMaxRows*(blkCnt-1)+options.DefaultBlockMaxRows*6/10))
		require.Nil(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectName(objectio.NewSegmentid(), uint16(blkCnt))))
		require.Nil(t, objectio.SetObjectStatsExtent(stats, objectio.NewExtent(0, 0, 0, 0)))
		require.Nil(t, objectio.SetObjectStatsSortKeyZoneMap(stats, index.NewZM(types.T_bool, 1)))

		statsList = append(statsList, *stats)
	}

	return
}

func TestNewStatsBlkIter(t *testing.T) {
	stats := mockStatsList(t, 1)[0]
	blks := UnfoldBlkInfoFromObjStats(&stats)

	iter := NewStatsBlkIter(&stats)
	for iter.Next() {
		actual := iter.Entry()
		id := actual.BlockID.Sequence()
		require.Equal(t, blks[id].BlockID, actual.BlockID)

		loc1 := objectio.Location(blks[id].MetaLoc[:])
		loc2 := objectio.Location(actual.MetaLoc[:])
		require.Equal(t, loc1.Name(), loc2.Name())
		require.Equal(t, loc1.Extent(), loc2.Extent())
		require.Equal(t, loc1.ID(), loc2.ID())
		require.Equal(t, loc1.Rows(), loc2.Rows())
	}
}

func TestForeachBlkInObjStatsList(t *testing.T) {
	statsList := mockStatsList(t, 100)

	count := 0
	ForeachBlkInObjStatsList(false, func(blk *catalog.BlockInfo) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, 1)

	count = 0
	ForeachBlkInObjStatsList(true, func(blk *catalog.BlockInfo) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, len(statsList))

	count = 0
	ForeachBlkInObjStatsList(true, func(blk *catalog.BlockInfo) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)

	count = 0
	ForeachBlkInObjStatsList(false, func(blk *catalog.BlockInfo) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)
}
