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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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
		{Op: function.EQUAL, Valid: true, LB: encodeVal(10)},
		{Valid: true, Op: function.EQUAL, LB: encodeVal(20)},
		{Valid: true, Op: function.EQUAL, LB: encodeVal(30)}, // 3
		{Op: function.IN, Valid: true, Vec: encodeVec([]int64{1, 2})},
		// "b=40 and a=50",
		{Valid: true, Op: function.EQUAL, LB: encodeVal(50)}, // 5
		{Valid: false},
		{Valid: false}, // 7
		{Valid: false},
		{Valid: false}, // 9
		{Valid: false},
		{Valid: true, Op: function.EQUAL, LB: encodeVal(30)}, // 11
		{Valid: false},

		// "a>=1 and a<=3",
		{Valid: true, Op: function.BETWEEN, LB: encodeVal(1), UB: encodeVal(3)}, //13
		{Valid: true, Op: RangeLeftOpen, LB: encodeVal(1), UB: encodeVal(3)},
		{Valid: true, Op: RangeRightOpen, LB: encodeVal(1), UB: encodeVal(3)}, //15
		{Valid: true, Op: RangeBothOpen, LB: encodeVal(1), UB: encodeVal(3)},

		{Valid: false}, // 17
		{Valid: false},
		{Valid: false}, // 19
		{Valid: false},

		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(3)}, // 21
		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(3)},
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(3)}, // 23
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(3)},

		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(1)}, // 25
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(1)},
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(1)}, // 27
		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(1)},

		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(1)}, // 29
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(1)},
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(1)}, // 31
		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(1)},

		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(3)}, // 33
		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(3)},
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(3)}, // 35
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(3)},

		{Valid: true, Op: function.EQUAL, LB: encodeVal(5)}, // 37
		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(10)},
		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(10)}, // 39
		{Valid: true, Op: function.EQUAL, LB: encodeVal(15)},
		{Valid: true, Op: function.EQUAL, LB: encodeVal(10)}, // 41
		{Valid: true, Op: function.EQUAL, LB: encodeVal(10)},

		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(10)}, // 43
		{Valid: false},
		{Valid: false}, // 45
		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(10)},
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(10)}, // 47
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(10)},

		{Valid: true, Op: function.LESS_THAN, LB: encodeVal(99)}, // 49
		{Valid: true, Op: function.LESS_EQUAL, LB: encodeVal(99)},
		{Valid: true, Op: function.GREAT_THAN, LB: encodeVal(99)}, // 51
		{Valid: true, Op: function.GREAT_EQUAL, LB: encodeVal(99)},
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
	var exes []colexec.ExpressionExecutor

	for _, expr := range exprs {
		plan2.ReplaceFoldExpr(proc, expr, &exes)
	}
	for i, expr := range exprs {
		plan2.EvalFoldExpr(proc, expr, &exes)

		BasePKFilter, err := ConstructBasePKFilter(expr, tableDef, proc)
		require.NoError(t, err)
		require.Equal(t, filters[i].Valid, BasePKFilter.Valid, exprStrings[i])
		if filters[i].Valid {
			require.Equal(t, filters[i].Op, BasePKFilter.Op, exprStrings[i])
			require.Equal(t, filters[i].LB, BasePKFilter.LB, exprStrings[i])
			require.Equal(t, filters[i].UB, BasePKFilter.UB, exprStrings[i])
		}
	}

	for _, exe := range exes {
		exe.Free()
	}
	for i := range needFreeVecs {
		needFreeVecs[i].Free(m)
	}

	require.Zero(t, m.CurrNB())
}

func TestConstructBlockPKFilter(t *testing.T) {
	mp, err := mpool.NewMPool("", mpool.GB, 0)
	require.NoError(t, err)

	ops := []int{
		function.LESS_EQUAL, function.LESS_THAN,
		function.GREAT_EQUAL, function.GREAT_THAN,
		function.EQUAL, function.BETWEEN,
	}

	tys := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128, types.T_varchar, types.T_enum,
	}

	lb, ub := 1, 2

	var basePKFilters []BasePKFilter
	for _, op := range ops {
		for _, ty := range tys {
			var llb, uub []byte

			if ty == types.T_decimal128 {
				llb = types.EncodeDecimal128(&types.Decimal128{B0_63: uint64(lb), B64_127: uint64(lb)})
				uub = types.EncodeDecimal128(&types.Decimal128{B0_63: uint64(ub), B64_127: uint64(ub)})

			} else if ty == types.T_varchar {
				llb = []byte(strconv.Itoa(lb))
				uub = []byte(strconv.Itoa(ub))

			} else {
				llb = types.EncodeFixed(lb)
				uub = types.EncodeFixed(ub)
			}

			basePKFilters = append(basePKFilters, BasePKFilter{
				Valid: true,
				Op:    op,
				LB:    llb,
				UB:    uub,
				Vec:   nil,
				Oid:   ty,
			})
		}
	}

	for i := range basePKFilters {
		blkPKFilter, err := ConstructBlockPKFilter(
			false,
			basePKFilters[i],
		)
		require.NoError(t, err)

		require.True(t, blkPKFilter.Valid)
		require.NotNil(t, blkPKFilter.SortedSearchFunc)
		require.NotNil(t, blkPKFilter.UnSortedSearchFunc)

		vec := vector.NewVec(basePKFilters[i].Oid.ToType())
		switch basePKFilters[i].Oid {
		case types.T_int8:
			vector.AppendFixed(vec, int8(lb), false, mp)
			vector.AppendFixed(vec, int8(ub), false, mp)
		case types.T_int16:
			vector.AppendFixed(vec, int16(lb), false, mp)
			vector.AppendFixed(vec, int16(ub), false, mp)
		case types.T_int32:
			vector.AppendFixed(vec, int32(lb), false, mp)
			vector.AppendFixed(vec, int32(ub), false, mp)
		case types.T_int64:
			vector.AppendFixed(vec, int64(lb), false, mp)
			vector.AppendFixed(vec, int64(ub), false, mp)
		case types.T_uint8:
			vector.AppendFixed(vec, uint8(lb), false, mp)
			vector.AppendFixed(vec, uint8(ub), false, mp)
		case types.T_uint16:
			vector.AppendFixed(vec, uint16(lb), false, mp)
			vector.AppendFixed(vec, uint16(ub), false, mp)
		case types.T_uint32:
			vector.AppendFixed(vec, uint32(lb), false, mp)
			vector.AppendFixed(vec, uint32(ub), false, mp)
		case types.T_uint64:
			vector.AppendFixed(vec, uint64(lb), false, mp)
			vector.AppendFixed(vec, uint64(ub), false, mp)
		case types.T_float32:
			vector.AppendFixed(vec, float32(lb), false, mp)
			vector.AppendFixed(vec, float32(ub), false, mp)
		case types.T_float64:
			vector.AppendFixed(vec, float64(lb), false, mp)
			vector.AppendFixed(vec, float64(ub), false, mp)
		}

		sel1 := blkPKFilter.SortedSearchFunc([]*vector.Vector{vec})
		sel2 := blkPKFilter.UnSortedSearchFunc([]*vector.Vector{vec})

		require.Equal(t, sel1, sel2, basePKFilters[i].String())
	}
}

func TestMergeBaseFilterInKind(t *testing.T) {
	mp, err := mpool.NewMPool("", mpool.GB, 0)
	require.NoError(t, err)

	proc := testutil.NewProc()

	tys := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
	}

	var lvec, rvec *vector.Vector
	var lvals, rvals = []float64{1, 1, 2, 2, 3, 3}, []float64{3, 3, 4, 4, 5}
	var left, right = BasePKFilter{}, BasePKFilter{}

	for _, ty := range tys {
		lvec = vector.NewVec(ty.ToType())
		rvec = vector.NewVec(ty.ToType())
		switch ty {
		case types.T_int8:
			for i := range rvals {
				vector.AppendFixed(rvec, int8(rvals[i]), false, mp)
				vector.AppendFixed(lvec, int8(lvals[i]), false, mp)
			}

		case types.T_int16:
			for i := range rvals {
				vector.AppendFixed(rvec, int16(rvals[i]), false, mp)
				vector.AppendFixed(lvec, int16(lvals[i]), false, mp)
			}
		case types.T_int32:
			for i := range rvals {
				vector.AppendFixed(rvec, int32(rvals[i]), false, mp)
				vector.AppendFixed(lvec, int32(lvals[i]), false, mp)
			}
		case types.T_int64:
			for i := range rvals {
				vector.AppendFixed(rvec, int64(rvals[i]), false, mp)
				vector.AppendFixed(lvec, int64(lvals[i]), false, mp)
			}
		case types.T_uint8:
			for i := range rvals {
				vector.AppendFixed(rvec, uint8(rvals[i]), false, mp)
				vector.AppendFixed(lvec, uint8(lvals[i]), false, mp)
			}
		case types.T_uint16:
			for i := range rvals {
				vector.AppendFixed(rvec, uint16(rvals[i]), false, mp)
				vector.AppendFixed(lvec, uint16(lvals[i]), false, mp)
			}
		case types.T_uint32:
			for i := range rvals {
				vector.AppendFixed(rvec, uint32(rvals[i]), false, mp)
				vector.AppendFixed(lvec, uint32(lvals[i]), false, mp)
			}
		case types.T_uint64:
			for i := range rvals {
				vector.AppendFixed(rvec, uint64(rvals[i]), false, mp)
				vector.AppendFixed(lvec, uint64(lvals[i]), false, mp)
			}
		case types.T_float32:
			for i := range rvals {
				vector.AppendFixed(rvec, float32(rvals[i]), false, mp)
				vector.AppendFixed(lvec, float32(lvals[i]), false, mp)
			}
		case types.T_float64:
			for i := range rvals {
				vector.AppendFixed(rvec, float64(rvals[i]), false, mp)
				vector.AppendFixed(lvec, float64(lvals[i]), false, mp)
			}
		}

		left.Vec = lvec
		right.Vec = rvec
		left.Oid = ty
		right.Oid = ty

		ret, err := mergeBaseFilterInKind(left, right, false, proc)
		require.NoError(t, err)
		require.Equal(t, int(1), int(ret.Vec.Length()), ret.Oid.String())

		ret, err = mergeBaseFilterInKind(left, right, true, proc)
		require.NoError(t, err)
		require.Equal(t, int(5), int(ret.Vec.Length()))

	}
}
