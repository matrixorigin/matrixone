// Copyright 2021-2024 Matrix Origin
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

package engine_util

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_ConstructBasePKFilter(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool("", m)
	exprStrings := []string{
		"a=10",
		"a=20 and a=10",
		"30=a and 20=a", // 3
		"a in (1,2)",
		"b=40 and a=50", // 5
		"a=60 or b=70",
		"b=80 and c=90", // 7
		"a=60 or a=70",
		"a=60 or (a in (70,80))", // 9
		"(a=10 or b=20) or a=30",
		"(a=10 or b=20) and a=30", // 11
		"(b=10 and a=20) or a=30",

		"a>=1 and a<=3", // 13
		"a>1 and a<=3",
		"a>=1 and a<3", // 15
		"a>1 and a<3",

		"a>=1 or a<=3", // 17
		"a>1 or a<=3",
		"a>=1 or a<3", // 19
		"a>1 or a<3",

		"a>1 and a>3", // 21
		"a>=1 and a>3",
		"a>=1 and a>=3", // 23
		"a>1 and a>=3",

		"a>1 or a>3", // 25
		"a>=1 or a>3",
		"a>=1 or a>=3", //27
		"a>1 or a>=3",

		"a<1 and a<3", // 29
		"a<=1 and a<3",
		"a<=1 and a<=3", // 31
		"a<1 and a<=3",

		"a<1 or a<3", // 33
		"a<=1 or a<3",
		"a<=1 or a<=3", // 35
		"a<1 or a<=3",

		"a<10 and a=5", // 37
		"a<10 and a=15",
		"a>10 and a=5", // 39
		"a>10 and a=15",
		"a>=10 and a=10", // 41
		"a<=10 and a=10",

		"a<10 or a=5", // 43
		"a<10 or a=15",
		"a>10 or a=5", // 45
		"a>10 or a=15",
		"a>=10 or a=10", // 47
		"a<=10 or a=10",

		"a<99", // 49
		"a<=99",
		"a>99", // 51
		"a>=99",
	}

	var needFreeVecs []*vector.Vector

	encodeVal := func(val int64) []byte {
		return types.EncodeInt64(&val)
	}
	encodeVec := func(vals []int64) *vector.Vector {
		vec := vector.NewVec(types.T_int64.ToType())
		for i := range vals {
			vector.AppendFixed(vec, vals[i], false, m)
		}

		needFreeVecs = append(needFreeVecs, vec)
		//bb, err := vec.MarshalBinary()
		//require.Nil(t, err)
		//vec.Free(m)

		return vec
	}

	filters := []BasePKFilter{
		// "a=10",
		{op: function.EQUAL, valid: true, lb: encodeVal(10)},
		{valid: true, op: function.EQUAL, lb: encodeVal(20)},
		{valid: true, op: function.EQUAL, lb: encodeVal(30)}, // 3
		{op: function.IN, valid: true, vec: encodeVec([]int64{1, 2})},
		// "b=40 and a=50",
		{valid: true, op: function.EQUAL, lb: encodeVal(50)}, // 5
		{valid: false},
		{valid: false}, // 7
		{valid: false},
		{valid: false}, // 9
		{valid: false},
		{valid: true, op: function.EQUAL, lb: encodeVal(30)}, // 11
		{valid: false},

		// "a>=1 and a<=3",
		{valid: true, op: function.BETWEEN, lb: encodeVal(1), ub: encodeVal(3)}, //13
		{valid: true, op: rangeLeftOpen, lb: encodeVal(1), ub: encodeVal(3)},
		{valid: true, op: rangeRightOpen, lb: encodeVal(1), ub: encodeVal(3)}, //15
		{valid: true, op: rangeBothOpen, lb: encodeVal(1), ub: encodeVal(3)},

		{valid: false}, // 17
		{valid: false},
		{valid: false}, // 19
		{valid: false},

		{valid: true, op: function.GREAT_THAN, lb: encodeVal(3)}, // 21
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(3)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(3)}, // 23
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(3)},

		{valid: true, op: function.GREAT_THAN, lb: encodeVal(1)}, // 25
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(1)}, // 27
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(1)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(1)}, // 29
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(1)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(1)}, // 31
		{valid: true, op: function.LESS_THAN, lb: encodeVal(1)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(3)}, // 33
		{valid: true, op: function.LESS_THAN, lb: encodeVal(3)},
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(3)}, // 35
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(3)},

		{valid: true, op: function.EQUAL, lb: encodeVal(5)}, // 37
		{valid: true, op: function.LESS_THAN, lb: encodeVal(10)},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(10)}, // 39
		{valid: true, op: function.EQUAL, lb: encodeVal(15)},
		{valid: true, op: function.EQUAL, lb: encodeVal(10)}, // 41
		{valid: true, op: function.EQUAL, lb: encodeVal(10)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(10)}, // 43
		{valid: false},
		{valid: false}, // 45
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(10)},
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(10)}, // 47
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(10)},

		{valid: true, op: function.LESS_THAN, lb: encodeVal(99)}, // 49
		{valid: true, op: function.LESS_EQUAL, lb: encodeVal(99)},
		{valid: true, op: function.GREAT_THAN, lb: encodeVal(99)}, // 51
		{valid: true, op: function.GREAT_EQUAL, lb: encodeVal(99)},
	}

	exprs := []*plan.Expr{
		// a=10
		MakeFunctionExprForTest("=", []*plan.Expr{
			MakeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		}),
		// a=20 and a=10
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		// 30=a and 20=a
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(30),
				MakeColExprForTest(0, types.T_int64),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(20),
				MakeColExprForTest(0, types.T_int64),
			}),
		}),
		// a in (1,2)
		MakeInExprForTest[int64](
			MakeColExprForTest(0, types.T_int64),
			[]int64{1, 2},
			types.T_int64,
			m,
		),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(40),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(50),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(70),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(80),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(2, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(90),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(70),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(60),
			}),
			MakeInExprForTest[int64](
				MakeColExprForTest(0, types.T_int64),
				[]int64{70, 80},
				types.T_int64,
				m,
			),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("or", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("and", []*plan.Expr{
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				MakeFunctionExprForTest("=", []*plan.Expr{
					MakeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(20),
				}),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(30),
			}),
		}),

		// a>=1 and a<=3
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>=1 or a<=3
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>1 and a>3
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a>1 or a>3
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<1 and a<3
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<1 or a<3
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(3),
			}),
		}),

		// a<10 and a=5
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		MakeFunctionExprForTest("and", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),

		// a<10 or a=5
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(15),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest(">=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		}),
		MakeFunctionExprForTest("or", []*plan.Expr{
			MakeFunctionExprForTest("<=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
			MakeFunctionExprForTest("=", []*plan.Expr{
				MakeColExprForTest(0, types.T_int64),
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
		BasePKFilter, err := ConstructBasePKFilter(expr, tableDef, proc)
		require.NoError(t, err)
		require.Equal(t, filters[i].valid, BasePKFilter.valid, exprStrings[i])
		if filters[i].valid {
			require.Equal(t, filters[i].op, BasePKFilter.op, exprStrings[i])
			require.Equal(t, filters[i].lb, BasePKFilter.lb, exprStrings[i])
			require.Equal(t, filters[i].ub, BasePKFilter.ub, exprStrings[i])
		}
	}

	for i := range needFreeVecs {
		needFreeVecs[i].Free(m)
	}

	require.Zero(t, m.CurrNB())
}
