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
	"github.com/stretchr/testify/assert"
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

func makeInExprForTest[T any](arg0 *plan.Expr, vals []T, oid types.T, mp *mpool.MPool) *plan.Expr {
	vec := vector.NewVec(oid.ToType())
	for _, val := range vals {
		_ = vector.AppendAny(vec, val, false, mp)
	}
	data, _ := vec.MarshalBinary()
	vec.Free(mp)
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.InFunctionEncodedID,
					ObjName: function.InFunctionName,
				},
				Args: []*plan.Expr{
					arg0,
					{
						Typ: plan2.MakePlan2Type(vec.GetType()),
						Expr: &plan.Expr_Vec{
							Vec: &plan.LiteralVec{
								Len:  int32(len(vals)),
								Data: data,
							},
						},
					},
				},
			},
		},
	}
}

func TestBlockMetaMarshal(t *testing.T) {
	location := []byte("test")
	var info objectio.BlockInfo
	info.SetMetaLocation(location)
	data := objectio.EncodeBlockInfo(info)
	info2 := objectio.DecodeBlockInfo(data)
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
			result, _, _, data := getPkValueByExpr(testCase.expr, "a", testCase.typ, true, nil)
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

func TestForeachBlkInObjStatsList(t *testing.T) {
	statsList := mockStatsList(t, 100)

	count := 0
	ForeachBlkInObjStatsList(false, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, 1)

	count = 0
	ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, len(statsList))

	count = 0
	ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)

	count = 0
	ForeachBlkInObjStatsList(false, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)
}

func TestGetPKExpr(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)
	type myCase struct {
		desc     []string
		exprs    []*plan.Expr
		valExprs []*plan.Expr
	}
	tc := myCase{
		desc: []string{
			"a=10",
			"a=20 and a=10",
			"30=a and 20=a",
			"a in (1,2)",
			"b=40 and a=50",
			"a=60 or b=70",
			"b=80 and c=90",
			"a=60 or a=70",
			"a=60 or (a in (70,80))",
			"(a=10 or b=20) or a=30",
			"(a=10 or b=20) and a=30",
			"(b=10 and a=20) or a=30",
		},
		exprs: []*plan.Expr{
			// a=10
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			// a=20 and a=10
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			}),
			// 30=a and 20=a
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					plan2.MakePlan2Int64ConstExprWithType(30),
					makeColExprForTest(0, types.T_int64),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					plan2.MakePlan2Int64ConstExprWithType(20),
					makeColExprForTest(0, types.T_int64),
				}),
			}),
			// a in (1,2)
			makeInExprForTest[int64](
				makeColExprForTest(0, types.T_int64),
				[]int64{1, 2},
				types.T_int64,
				m,
			),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(40),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(50),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(70),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(80),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(2, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(90),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(70),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(60),
				}),
				makeInExprForTest[int64](
					makeColExprForTest(0, types.T_int64),
					[]int64{70, 80},
					types.T_int64,
					m,
				),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(1, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(10),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(20),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
		},
		valExprs: []*plan.Expr{
			plan2.MakePlan2Int64ConstExprWithType(10),
			plan2.MakePlan2Int64ConstExprWithType(20),
			plan2.MakePlan2Int64ConstExprWithType(30),
			plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(2)),
			plan2.MakePlan2Int64ConstExprWithType(50),
			nil,
			nil,
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(60),
							plan2.MakePlan2Int64ConstExprWithType(70),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(60),
							plan2.MakePlan2Int64VecExprWithType(m, int64(70), int64(80)),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
			nil,
			plan2.MakePlan2Int64ConstExprWithType(30),
			{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							plan2.MakePlan2Int64ConstExprWithType(20),
							plan2.MakePlan2Int64ConstExprWithType(30),
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_int64),
					NotNullable: true,
				},
			},
		},
	}
	pkName := "a"
	for i, expr := range tc.exprs {
		rExpr := getPkExpr(expr, pkName, proc)
		// if rExpr != nil {
		// 	t.Logf("%s||||%s||||%s", plan2.FormatExpr(expr), plan2.FormatExpr(rExpr), tc.desc[i])
		// }
		require.Equalf(t, tc.valExprs[i], rExpr, tc.desc[i])
	}
	require.Zero(t, m.CurrNB())
}

func TestGetPkExprValue(t *testing.T) {
	m := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcessWithMPool(m)
	type testCase struct {
		desc       []string
		exprs      []*plan.Expr
		expectVals [][]int64
		canEvals   []bool
		hasNull    []bool
	}
	equalToVecFn := func(expect []int64, actual any) bool {
		vec := vector.NewVec(types.T_any.ToType())
		_ = vec.UnmarshalBinary(actual.([]byte))
		actualVals := vector.MustFixedCol[int64](vec)
		if len(expect) != len(actualVals) {
			return false
		}
		for i := range expect {
			if expect[i] != actualVals[i] {
				return false
			}
		}
		return true
	}
	equalToValFn := func(expect []int64, actual any) bool {
		if len(expect) != 1 {
			return false
		}
		actualVal := actual.(int64)
		return expect[0] == actualVal
	}

	nullExpr := plan2.MakePlan2Int64ConstExprWithType(0)
	nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true

	tc := testCase{
		desc: []string{
			"a=2 and a=1",
			"a in vec(1,2)",
			"a=2 or a=1 or a=3",
			"a in vec(1,10) or a=5 or (a=6 and a=7)",
			"a=null",
			"a=1 or a=null or a=2",
		},
		canEvals: []bool{
			true, true, true, true, false, true,
		},
		hasNull: []bool{
			false, false, false, false, true, false,
		},
		exprs: []*plan.Expr{
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(2),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
			}),
			makeFunctionExprForTest("in", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(2)),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(2),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(1),
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(3),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("in", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(5),
					}),
				}),
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(6),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(7),
					}),
				}),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				nullExpr,
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						plan2.MakePlan2Int64ConstExprWithType(1),
					}),
					makeFunctionExprForTest("=", []*plan.Expr{
						makeColExprForTest(0, types.T_int64),
						nullExpr,
					}),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(2),
				}),
			}),
		},
		expectVals: [][]int64{
			{2}, {1, 2}, {1, 2, 3}, {1, 5, 6, 10}, {}, {1, 2},
		},
	}
	for i, expr := range tc.exprs {
		canEval, isNull, isVec, val := getPkValueByExpr(expr, "a", types.T_int64, false, proc)
		require.Equalf(t, tc.hasNull[i], isNull, tc.desc[i])
		require.Equalf(t, tc.canEvals[i], canEval, tc.desc[i])
		if !canEval {
			continue
		}
		if isVec {
			require.Truef(t, equalToVecFn(tc.expectVals[i], val), tc.desc[i])
		} else {
			require.Truef(t, equalToValFn(tc.expectVals[i], val), tc.desc[i])
		}
	}
	expr := makeFunctionExprForTest("in", []*plan.Expr{
		makeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
	})
	canEval, _, _, _ := getPkValueByExpr(expr, "a", types.T_int64, true, proc)
	require.False(t, canEval)
	canEval, _, _, _ = getPkValueByExpr(expr, "a", types.T_int64, false, proc)
	require.True(t, canEval)

	expr = makeFunctionExprForTest("in", []*plan.Expr{
		makeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64VecExprWithType(m, int64(1)),
	})
	canEval, _, _, val := getPkValueByExpr(expr, "a", types.T_int64, true, proc)
	require.True(t, canEval)
	require.True(t, equalToValFn([]int64{1}, val))

	proc.FreeVectors()
	require.Zero(t, m.CurrNB())
}

func TestEvalExprListToVec(t *testing.T) {
	m := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcessWithMPool(m)
	type testCase struct {
		desc     []string
		oids     []types.T
		exprs    []*plan.Expr_List
		canEvals []bool
		expects  []*vector.Vector
	}
	tc := testCase{
		desc: []string{
			"nil",
			"[i64(2), i64(1)]",
			"[i64(1), i64(2)]",
			"[i64(2), i64vec(1,10), [i64vec(4,8), i64(5)]]",
		},
		oids: []types.T{
			types.T_int64, types.T_int64, types.T_int64, types.T_int64,
		},
		exprs: []*plan.Expr_List{
			nil,
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(2),
						plan2.MakePlan2Int64ConstExprWithType(1),
					},
				},
			},
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(1),
						plan2.MakePlan2Int64ConstExprWithType(2),
					},
				},
			},
			{
				List: &plan.ExprList{
					List: []*plan.Expr{
						plan2.MakePlan2Int64ConstExprWithType(2),
						plan2.MakePlan2Int64VecExprWithType(m, int64(1), int64(10)),
						{
							Expr: &plan.Expr_List{
								List: &plan.ExprList{
									List: []*plan.Expr{
										plan2.MakePlan2Int64VecExprWithType(m, int64(4), int64(8)),
										plan2.MakePlan2Int64ConstExprWithType(5),
									},
								},
							},
						},
					},
				},
			},
		},
		canEvals: []bool{
			false, true, true, true,
		},
	}
	tc.expects = append(tc.expects, nil)
	vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	tc.expects = append(tc.expects, vec)
	vec = vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	tc.expects = append(tc.expects, vec)
	vec = vector.NewVec(types.T_int64.ToType())
	vector.AppendAny(vec, int64(1), false, m)
	vector.AppendAny(vec, int64(2), false, m)
	vector.AppendAny(vec, int64(4), false, m)
	vector.AppendAny(vec, int64(5), false, m)
	vector.AppendAny(vec, int64(8), false, m)
	vector.AppendAny(vec, int64(10), false, m)
	tc.expects = append(tc.expects, vec)

	for i, expr := range tc.exprs {
		// for _, e2 := range expr.List.List {
		// 	t.Log(plan2.FormatExpr(e2))
		// }
		canEval, vec, put := evalExprListToVec(tc.oids[i], expr, proc)
		require.Equalf(t, tc.canEvals[i], canEval, tc.desc[i])
		if canEval {
			require.NotNil(t, vec)
			require.Equal(t, tc.expects[i].String(), vec.String())
		} else {
			require.Equal(t, tc.expects[i], vec)
		}
		if put != nil {
			put()
		}
		if tc.expects[i] != nil {
			tc.expects[i].Free(m)
		}
	}
	proc.FreeVectors()
	require.Zero(t, m.CurrNB())
}

func Test_removeIf(t *testing.T) {
	strs := []string{"abc", "bc", "def"}

	del1 := make(map[string]struct{})
	del1["abc"] = struct{}{}
	res1 := removeIf[string](strs, func(t string) bool {
		return find[string](del1, t)
	})
	assert.Equal(t, []string{"bc", "def"}, res1)

	del2 := make(map[string]struct{})
	for _, str := range strs {
		del2[str] = struct{}{}
	}
	res2 := removeIf[string](strs, func(t string) bool {
		return find[string](del2, t)
	})
	assert.Equal(t, []string{}, res2)

	assert.Equal(t, []string(nil), removeIf[string](nil, nil))
}

func Test_ConstructBasePKFilter(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)
	exprStrings := []string{
		"a=10",
		"a=20 and a=10",
		"30=a and 20=a",
		"a in (1,2)",
		"b=40 and a=50",
		"a=60 or b=70",
		"b=80 and c=90",
		"a=60 or a=70",
		"a=60 or (a in (70,80))",
		"(a=10 or b=20) or a=30",
		"(a=10 or b=20) and a=30",
		"(b=10 and a=20) or a=30",

		"a>=1 and a<=3",
		"a>1 and a<=3",
		"a>=1 and a<3",
		"a>1 and a<3",

		"a>=1 or a<=3",
		"a>1 or a<=3",
		"a>=1 or a<3",
		"a>1 or a<3",

		"a>1 and a>3",
		"a>=1 and a>3",
		"a>=1 and a>=3",
		"a>1 and a>=3",

		"a>1 or a>3",
		"a>=1 or a>3",
		"a>=1 or a>=3",
		"a>1 or a>=3",

		"a<1 and a<3",
		"a<=1 and a<3",
		"a<=1 and a<=3",
		"a<1 and a<=3",

		"a<1 or a<3",
		"a<=1 or a<3",
		"a<=1 or a<=3",
		"a<1 or a<=3",

		"a<10 and a=5",
		"a<10 and a=15",
		"a>10 and a=5",
		"a>10 and a=15",
		"a>=10 and a=10",
		"a<=10 and a=10",

		"a<10 or a=5",
		"a<10 or a=15",
		"a>10 or a=5",
		"a>10 or a=15",
		"a>=10 or a=10",
		"a<=10 or a=10",

		"a<99",
		"a<=99",
		"a>99",
		"a>=99",
	}

	encodeVal := func(val int64) []byte {
		return types.EncodeInt64(&val)
	}
	encodeVec := func(vals []int64) []byte {
		vec := vector.NewVec(types.T_int64.ToType())
		for i := range vals {
			vector.AppendFixed(vec, vals[i], false, m)
		}

		bb, err := vec.MarshalBinary()
		require.Nil(t, err)
		vec.Free(m)

		return bb
	}

	filters := []basePKFilter{
		// "a=10",
		{op: function.EQUAL, valid: true, lb: encodeVal(10)},
		{valid: false},
		{valid: false},
		{op: function.IN, valid: true, vec: encodeVec([]int64{1, 2})},
		// "b=40 and a=50",
		{valid: true, op: function.EQUAL, lb: encodeVal(50)},
		{valid: false},
		{valid: false},
		{valid: false},
		{valid: false},
		{valid: false},
		{valid: true, op: function.EQUAL, lb: encodeVal(30)},
		{valid: false},

		// "a>=1 and a<=3",
		{valid: true, op: function.BETWEEN, lb: encodeVal(1), ub: encodeVal(3)},
		{valid: true, op: rangeLeftOpen, lb: encodeVal(1), ub: encodeVal(3)},
		{valid: true, op: rangeRightOpen, lb: encodeVal(1), ub: encodeVal(3)},
		{valid: true, op: rangeBothOpen, lb: encodeVal(1), ub: encodeVal(3)},

		{valid: false},
		{valid: false},
		{valid: false},
		{valid: false},

		{valid: true, op: function.GREAT_THAN, lb: encodeVal(3)},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(3)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(3)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(3)},

		{valid: true, op: function.GREAT_THAN, lb: encodeVal(1)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(1)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(1)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.LESS_THAN, lb: encodeVal(1)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(3)},
		{valid: true, op: function.LESS_THAN, lb: encodeVal(3)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(3)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(3)},

		{valid: true, op: function.EQUAL, lb: encodeVal(5)},
		{valid: false},
		{valid: false},
		{valid: true, op: function.EQUAL, lb: encodeVal(15)},
		{valid: true, op: function.EQUAL, lb: encodeVal(10)},
		{valid: true, op: function.EQUAL, lb: encodeVal(10)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(10)},
		{valid: false},
		{valid: false},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(10)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(10)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(10)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(99)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(99)},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(99)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(99)},
	}

	exprs := []*plan.Expr{
		// a=10
		makeFunctionExprForTest("=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		}),
		// a=20 and a=10
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		// 30=a and 20=a
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(30),
				makeColExprForTest(0, types.T_int64),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(20),
				makeColExprForTest(0, types.T_int64),
			}),
		}),
		// a in (1,2)
		makeInExprForTest[int64](
			makeColExprForTest(0, types.T_int64),
			[]int64{1, 2},
			types.T_int64,
			m,
		),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(40),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(50),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(70),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(80),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(2, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(90),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(70),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			makeInExprForTest[int64](
				makeColExprForTest(0, types.T_int64),
				[]int64{70, 80},
				types.T_int64,
				m,
			),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),

		// a>=1 and a<=3
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>=1 or a<=3
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>1 and a>3
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>1 or a>3
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<1 and a<3
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<1 or a<3
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<10 and a=5
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),

		// a<10 or a=5
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
	}

	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a"},
		},
	}

	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: "a",
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	})

	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: "b",
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	})

	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: "c",
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
	})

	tableDef.Pkey.PkeyColName = "a"
	for i, expr := range exprs {
		if exprStrings[i] == "b=40 and a=50" {
			x := 0
			x++
		}
		basePKFilter := newBasePKFilter(expr, tableDef, proc)
		require.Equal(t, filters[i].valid, basePKFilter.valid, exprStrings[i])
		if filters[i].valid {
			require.Equal(t, filters[i].op, basePKFilter.op, exprStrings[i])
			require.Equal(t, filters[i].lb, basePKFilter.lb, exprStrings[i])
			require.Equal(t, filters[i].ub, basePKFilter.ub, exprStrings[i])
		}
	}

	require.Zero(t, m.CurrNB())
}

func TestConcurrentExecutor_AppendTask(t *testing.T) {
	ce := newConcurrentExecutor(10)
	ta := func() error { return nil }
	for i := 0; i < 10; i++ {
		ce.AppendTask(ta)
	}
	require.Equal(t, 10, len(ce.(*concurrentExecutor).tasks))
}

func TestConcurrentExecutor_Exec(t *testing.T) {
	ce := newConcurrentExecutor(10)
	ta := func() error {
		return nil
	}
	for i := 0; i < 20; i++ {
		ce.AppendTask(ta)
	}
	err := ce.Exec()
	require.NoError(t, err)

	ta1 := func() error {
		return context.DeadlineExceeded
	}
	for i := 0; i < 20; i++ {
		ce.AppendTask(ta1)
	}
	err = ce.Exec()
	require.Error(t, err)
}
