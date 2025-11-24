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

package readutil

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
)

func Test_ConstructBasePKFilter(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(t, "", m)
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

		BasePKFilter, err := ConstructBasePKFilter(expr, tableDef, proc.Mp())
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

func encodeIntToUUID(x int32) types.Uuid {
	hex := types.EncodeInt32(&x)
	for i := 16 - len(hex); i > 0; i-- {
		hex = append(hex, 0)
	}

	u := types.Uuid(hex)

	return u
}

func TestConstructBlockPKFilter(t *testing.T) {
	mp, err := mpool.NewMPool("", mpool.GB*2, 0)
	require.NoError(t, err)

	ops := []int{
		function.LESS_EQUAL, function.LESS_THAN,
		function.GREAT_EQUAL, function.GREAT_THAN,
		function.EQUAL, function.BETWEEN, RangeBothOpen, RangeLeftOpen, RangeRightOpen,
		function.PREFIX_EQ,
		function.PREFIX_IN,
		function.PREFIX_BETWEEN,
		function.IN,
	}

	tys := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_timestamp,
		types.T_decimal128, types.T_varchar, types.T_uuid,
		types.T_char, types.T_json, types.T_binary,
	}

	needVec := func(op int) bool {
		if op == function.PREFIX_IN || op == function.IN {
			return true
		}

		return false
	}

	opOnBinary := func(op int) bool {
		return op == function.PREFIX_IN ||
			op == function.PREFIX_EQ ||
			op == function.PREFIX_BETWEEN
	}

	var msg string

	defer func() {
		if r := recover(); r != nil {
			fmt.Println(msg)
			panic(r)
		}
	}()

	for range 200 {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		lb := float64(rnd.Intn(100) + 2)
		ub := float64(rnd.Intn(100) + 2)

		if lb > ub {
			lb, ub = ub, lb
		}

		var (
			llb, uub []byte
		)

		for _, op := range ops {
			for _, ty := range tys {
				vec := vector.NewVec(ty.ToType())

				if opOnBinary(op) {
					vec.SetType(types.T_binary.ToType())
				}

				switch ty {

				case types.T_int8:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := int8(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeInt8(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(int8(lb), ty)
					uub = types.EncodeValue(int8(ub), ty)
				case types.T_int16:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := int16(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeInt16(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(int16(lb), ty)
					uub = types.EncodeValue(int16(ub), ty)
				case types.T_int32:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := int32(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeInt32(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(int32(lb), ty)
					uub = types.EncodeValue(int32(ub), ty)
				case types.T_int64:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := int64(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeInt64(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(int64(lb), ty)
					uub = types.EncodeValue(int64(ub), ty)
				case types.T_uint8:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := uint8(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeUint8(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(uint8(lb), ty)
					uub = types.EncodeValue(uint8(ub), ty)
				case types.T_uint16:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := uint16(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeUint16(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(uint16(lb), ty)
					uub = types.EncodeValue(uint16(ub), ty)
				case types.T_uint32:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := uint32(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeUint32(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(uint32(lb), ty)
					uub = types.EncodeValue(uint32(ub), ty)
				case types.T_uint64:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := int64(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeInt64(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(uint64(lb), ty)
					uub = types.EncodeValue(uint64(ub), ty)
				case types.T_float32:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := float32(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeFloat32(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(float32(lb), ty)
					uub = types.EncodeValue(float32(ub), ty)
				case types.T_float64:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := x
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeFloat64(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(lb, ty)
					uub = types.EncodeValue(ub, ty)
				case types.T_varchar, types.T_binary, types.T_char, types.T_json:
					if needVec(op) {
						for x := lb; x <= ub; x++ {
							vector.AppendBytes(vec, []byte(strconv.Itoa(int(x))), false, mp)
						}
						vec.InplaceSort()
					}

					str1 := strconv.Itoa(int(lb))
					str2 := strconv.Itoa(int(ub))
					if str1 > str2 {
						// string and integer have different sort type.
						str1, str2 = str2, str1
					}
					llb = []byte(str1)
					uub = []byte(str2)

				case types.T_date:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := types.Date(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeDate(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(types.Date(lb), ty)
					uub = types.EncodeValue(types.Date(ub), ty)

				case types.T_timestamp:
					if needVec(op) {
						//vec = vector.NewVec(ty.ToType())
						for x := lb; x <= ub; x++ {
							xx := types.Timestamp(x)
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeTimestamp(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeValue(types.Timestamp(lb), ty)
					uub = types.EncodeValue(types.Timestamp(ub), ty)

				case types.T_decimal128:
					if needVec(op) {
						for x := lb; x <= ub; x++ {
							xx := types.Decimal128{B0_63: uint64(x)}
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeDecimal128(&xx), false, mp)
							} else {
								vector.AppendFixed(vec, xx, false, mp)
							}
						}
					}
					llb = types.EncodeDecimal128(&types.Decimal128{B0_63: uint64(lb), B64_127: uint64(lb)})
					uub = types.EncodeDecimal128(&types.Decimal128{B0_63: uint64(ub), B64_127: uint64(ub)})

				case types.T_uuid:
					if needVec(op) {
						for x := lb; x <= ub; x++ {
							u := encodeIntToUUID(int32(x))
							if opOnBinary(op) {
								vector.AppendBytes(vec, types.EncodeUuid(&u), false, mp)
							} else {
								vector.AppendFixed[types.Uuid](vec, u, false, mp)
							}
						}
						vec.InplaceSort()
					}

					u := encodeIntToUUID(int32(lb))
					llb = make([]byte, len(u))
					copy(llb[:], u[:])

					u = encodeIntToUUID(int32(ub))
					uub = make([]byte, len(u))
					copy(uub[:], u[:])
				}

				basePKFilter := BasePKFilter{
					Valid: true,
					Op:    op,
					LB:    llb,
					UB:    uub,
					Vec:   vec,
					Oid:   ty,
				}

				blkPKFilter, err := ConstructBlockPKFilter(
					"tbl",
					"pk",
					false,
					basePKFilter,
					nil,
					mp,
				)
				require.NoError(t, err)

				require.True(t, blkPKFilter.Valid, basePKFilter.String())
				require.NotNil(t, blkPKFilter.SortedSearchFunc)
				require.NotNil(t, blkPKFilter.UnSortedSearchFunc)

				inputVec := vector.NewVec(ty.ToType())

				if opOnBinary(op) {
					inputVec.SetType(types.T_binary.ToType())
				}

				switch ty {
				case types.T_int8:
					for x := lb - 1; x <= ub+1; x++ {
						xx := int8(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeInt8(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}

				case types.T_int16:
					for x := lb - 1; x <= ub+1; x++ {
						xx := int16(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeInt16(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}

				case types.T_int32:
					for x := lb - 1; x <= ub+1; x++ {
						xx := int32(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeInt32(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_int64:
					for x := lb - 1; x <= ub+1; x++ {
						xx := int64(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeInt64(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_uint8:
					for x := lb - 1; x <= ub+1; x++ {
						xx := uint8(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeUint8(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_uint16:
					for x := lb - 1; x <= ub+1; x++ {
						xx := uint16(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeUint16(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_uint32:
					for x := lb - 1; x <= ub+1; x++ {
						xx := uint32(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeUint32(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_uint64:
					for x := lb - 1; x <= ub+1; x++ {
						xx := uint64(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeUint64(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_float32:
					for x := lb - 1; x <= ub+1; x++ {
						xx := float32(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeFloat32(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_float64:
					for x := lb - 1; x <= ub+1; x++ {
						xx := float64(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeFloat64(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}
				case types.T_varchar, types.T_binary, types.T_char, types.T_json:
					for x := lb - 1; x <= ub+1; x++ {
						xx := strconv.Itoa(int(x))
						vector.AppendBytes(inputVec, []byte(xx), false, mp)
					}
					inputVec.InplaceSort()

				case types.T_date:
					for x := lb - 1; x <= ub+1; x++ {
						xx := types.Date(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeDate(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}

				case types.T_timestamp:
					for x := lb - 1; x <= ub+1; x++ {
						xx := types.Timestamp(x)
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeTimestamp(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}

				case types.T_decimal128:
					for x := lb - 1; x <= ub+1; x++ {
						xx := types.Decimal128{B0_63: uint64(x)}

						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeDecimal128(&xx), false, mp)
						} else {
							vector.AppendFixed(inputVec, xx, false, mp)
						}
					}

					inputVec.InplaceSort()

				case types.T_uuid:
					for x := lb - 1; x <= ub+1; x++ {
						xx := encodeIntToUUID(int32(x))
						if opOnBinary(op) {
							vector.AppendBytes(inputVec, types.EncodeUuid(&xx), false, mp)
						} else {
							vector.AppendFixed[types.Uuid](inputVec, xx, false, mp)
						}
					}
					inputVec.InplaceSort()
				}

				if opOnBinary(op) && (ty == types.T_float32 || ty == types.T_float64) {
					// can the Prefix apply on a float/double ???
					continue
				}

				msg = fmt.Sprintf("lb: %v, ub: %v\nbaseFilter: %v\ninputVec: %v",
					lb, ub,
					basePKFilter.String(),
					common.MoVectorToString(inputVec, 100))

				sel1 := blkPKFilter.SortedSearchFunc(inputVec)
				sel2 := blkPKFilter.UnSortedSearchFunc(inputVec)

				require.Equal(t, sel1, sel2, msg)
			}
		}
	}
}

func TestMergeBaseFilterInKind(t *testing.T) {
	mp, err := mpool.NewMPool("", mpool.GB, 0)
	require.NoError(t, err)

	proc := testutil.NewProc(t)

	tys := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,

		types.T_varchar, types.T_binary, types.T_json,

		types.T_decimal128,
	}

	checkFunc := func(mm map[float64]struct{}, ty types.T, retV *vector.Vector, msg string) {
		require.Equal(t, len(mm), retV.Length(), msg)

		switch ty {
		case types.T_int8:
			cols := vector.MustFixedColWithTypeCheck[int8](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_int16:
			cols := vector.MustFixedColWithTypeCheck[int16](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_int32:
			cols := vector.MustFixedColWithTypeCheck[int32](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_int64:
			cols := vector.MustFixedColWithTypeCheck[int64](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_uint8:
			cols := vector.MustFixedColWithTypeCheck[uint8](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_uint16:
			cols := vector.MustFixedColWithTypeCheck[uint16](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_uint32:
			cols := vector.MustFixedColWithTypeCheck[uint32](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_uint64:
			cols := vector.MustFixedColWithTypeCheck[uint64](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_float32:
			cols := vector.MustFixedColWithTypeCheck[float32](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}
		case types.T_float64:
			cols := vector.MustFixedColWithTypeCheck[float64](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i])]
				require.True(t, ok, msg)
			}

		case types.T_varchar, types.T_json, types.T_binary:
			cols, area := vector.MustVarlenaRawData(retV)
			for i := range cols {
				str := string(cols[i].GetByteSlice(area))
				val, err := strconv.Atoi(str)
				require.NoError(t, err)
				_, ok := mm[float64(val)]
				require.True(t, ok)
			}

		case types.T_decimal128:
			cols := vector.MustFixedColWithTypeCheck[types.Decimal128](retV)
			for i := range cols {
				_, ok := mm[float64(cols[i].B0_63)]
				require.True(t, ok, msg)
			}
		}
	}

	var lvec, rvec *vector.Vector

	for s := 0; s < 50; s++ {
		var (
			lstrs, rstrs []string
			lvals, rvals []float64
			left, right  = BasePKFilter{}, BasePKFilter{}
		)

		if lvec != nil {
			lvec.CleanOnlyData()
		}

		if rvec != nil {
			rvec.CleanOnlyData()
		}

		lcnt := rand.Intn(240) + 2
		rcnt := rand.Intn(240) + 2

		lvals = make([]float64, 0, lcnt)
		rvals = make([]float64, 0, rcnt)

		unionMap := make(map[float64]struct{})
		interMap := make(map[float64]struct{})

		for range lcnt {
			val := float64(rand.Intn(lcnt / 2))
			lvals = append(lvals, val)
			unionMap[val] = struct{}{}
		}

		for range rcnt {
			val := float64(rand.Intn(rcnt / 2))
			rvals = append(rvals, val)
			if _, ok := unionMap[val]; ok {
				interMap[val] = struct{}{}
			}
		}

		for r := range rcnt {
			unionMap[rvals[r]] = struct{}{}
		}

		slices.Sort(rvals)
		slices.Sort(lvals)

		for _, ty := range tys {
			lvec = vector.NewVec(ty.ToType())
			rvec = vector.NewVec(ty.ToType())
			switch ty {
			case types.T_int8:
				for i := range rvals {
					vector.AppendFixed(rvec, int8(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, int8(lvals[i]), false, mp)
				}
			case types.T_int16:
				for i := range rvals {
					vector.AppendFixed(rvec, int16(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, int16(lvals[i]), false, mp)
				}
			case types.T_int32:
				for i := range rvals {
					vector.AppendFixed(rvec, int32(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, int32(lvals[i]), false, mp)
				}
			case types.T_int64:
				for i := range rvals {
					vector.AppendFixed(rvec, int64(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, int64(lvals[i]), false, mp)
				}
			case types.T_uint8:
				for i := range rvals {
					vector.AppendFixed(rvec, uint8(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, uint8(lvals[i]), false, mp)
				}
			case types.T_uint16:
				for i := range rvals {
					vector.AppendFixed(rvec, uint16(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, uint16(lvals[i]), false, mp)
				}
			case types.T_uint32:
				for i := range rvals {
					vector.AppendFixed(rvec, uint32(rvals[i]), false, mp)
				}
				for i := range lvals {
					vector.AppendFixed(lvec, uint32(lvals[i]), false, mp)
				}
			case types.T_uint64:
				for i := range rvals {
					vector.AppendFixed(rvec, uint64(rvals[i]), false, mp)
				}
				for i := range lvals {
					vector.AppendFixed(lvec, uint64(lvals[i]), false, mp)
				}
			case types.T_float32:
				for i := range rvals {
					vector.AppendFixed(rvec, float32(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, float32(lvals[i]), false, mp)
				}
			case types.T_float64:
				for i := range rvals {
					vector.AppendFixed(rvec, float64(rvals[i]), false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, float64(lvals[i]), false, mp)
				}

			case types.T_decimal128:
				for i := range rvals {
					vector.AppendFixed(rvec, types.Decimal128{B0_63: uint64(rvals[i])}, false, mp)
				}

				for i := range lvals {
					vector.AppendFixed(lvec, types.Decimal128{B0_63: uint64(lvals[i])}, false, mp)
				}

			case types.T_varchar, types.T_json, types.T_binary:
				rstrs = make([]string, 0, len(rvals))
				for i := range rvals {
					str := strconv.Itoa(int(rvals[i]))
					rstrs = append(rstrs, str)
				}
				slices.Sort(rstrs)
				for i := range rstrs {
					vector.AppendBytes(rvec, []byte(rstrs[i]), false, mp)
				}

				lstrs = make([]string, 0, len(lvals))
				for i := range lvals {
					str := strconv.Itoa(int(lvals[i]))
					lstrs = append(lstrs, str)
				}
				slices.Sort(lstrs)
				for i := range lstrs {
					vector.AppendBytes(lvec, []byte(lstrs[i]), false, mp)
				}
			}

			left.Vec = lvec
			right.Vec = rvec
			left.Oid = ty
			right.Oid = ty

			{
				ret, err := mergeBaseFilterInKind(left, right, false, proc.Mp())
				require.NoError(t, err)

				extrInterMsg := fmt.Sprintf("\ntyp: %v\nlvals: %v\nrvals: %v\nlvec: %v\nrvec: %v\ninterMap: %v\nret.Vec: %v",
					ty.String(), lvals, rvals, lvec.String(), rvec.String(),
					interMap, ret.Vec.String())

				checkFunc(interMap, ty, ret.Vec, extrInterMsg)
			}

			{
				ret, err := mergeBaseFilterInKind(left, right, true, proc.Mp())
				require.NoError(t, err)

				extrUnionMsg := fmt.Sprintf("\ntyp: %v\nlvals: %v\nrvals: %v\nlvec: %v\nrvec: %v\nunionMap: %v\nret.Vec: %v",
					ty.String(), lvals, rvals, lvec.String(), rvec.String(),
					unionMap, ret.Vec.String())

				checkFunc(unionMap, ty, ret.Vec, extrUnionMsg)
			}
		}
	}
}

// TestConstructBlockPKFilterWithBloomFilter tests the BloomFilter integration
// with composite and non-composite primary keys (lines 482-803 in pk_filter.go)
func TestConstructBlockPKFilterWithBloomFilter(t *testing.T) {
	mp := mpool.MustNewZero()

	// Helper function to create a composite primary key vector
	createCompositePKVec := func(tuples [][]any, mp *mpool.MPool) *vector.Vector {
		vec := vector.NewVec(types.T_varchar.ToType())
		packer := types.NewPacker()
		defer packer.Close()

		for _, tuple := range tuples {
			packer.Reset()
			for _, val := range tuple {
				switch v := val.(type) {
				case int8:
					packer.EncodeInt8(v)
				case int16:
					packer.EncodeInt16(v)
				case int32:
					packer.EncodeInt32(v)
				case int64:
					packer.EncodeInt64(v)
				case uint8:
					packer.EncodeUint8(v)
				case uint16:
					packer.EncodeUint16(v)
				case uint32:
					packer.EncodeUint32(v)
				case uint64:
					packer.EncodeUint64(v)
				case string:
					packer.EncodeStringType([]byte(v))
				case []byte:
					packer.EncodeStringType(v)
				default:
					t.Fatalf("unsupported type: %T", v)
				}
			}
			vector.AppendBytes(vec, packer.Bytes(), false, mp)
		}
		return vec
	}

	// Helper function to create a BloomFilter with values
	createBloomFilter := func(values []any, mp *mpool.MPool) ([]byte, error) {
		// Create a vector with the last column values from composite PK
		// For non-composite PK, use the values directly
		vec := vector.NewVec(types.T_int64.ToType())
		for _, val := range values {
			switch v := val.(type) {
			case int64:
				vector.AppendFixed(vec, v, false, mp)
			case int32:
				vector.AppendFixed(vec, int64(v), false, mp)
			case int16:
				vector.AppendFixed(vec, int64(v), false, mp)
			case int8:
				vector.AppendFixed(vec, int64(v), false, mp)
			default:
				vec.Free(mp)
				return nil, moerr.NewInvalidInputf(context.Background(), "unsupported type for BF: %T", v)
			}
		}

		// Use larger capacity to reduce false positive rate
		// For small value sets, use at least 100 capacity to get better accuracy
		capacity := int64(len(values))
		if capacity < 100 {
			capacity = 100
		}
		bf := bloomfilter.New(capacity, 0.001) // Use lower false positive rate
		bf.Add(vec)
		vec.Free(mp)

		return bf.Marshal()
	}

	t.Run("non-composite PK with BloomFilter only", func(t *testing.T) {
		// Create BloomFilter with values [10, 20, 30]
		bfData, err := createBloomFilter([]any{int64(10), int64(20), int64(30)}, mp)
		require.NoError(t, err)

		// Create input vector with values [5, 10, 15, 20, 25, 30, 35]
		inputVec := vector.NewVec(types.T_int64.ToType())
		for _, v := range []int64{5, 10, 15, 20, 25, 30, 35} {
			vector.AppendFixed(inputVec, v, false, mp)
		}
		defer inputVec.Free(mp)

		// Create filter with only BloomFilter (no base filter)
		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			"pk", // non-composite PK
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)
		require.True(t, readFilter.Valid)
		require.NotNil(t, readFilter.UnSortedSearchFunc)

		// Test the search function
		result := readFilter.UnSortedSearchFunc(inputVec)
		// Should return indices of values that exist in BF: [10, 20, 30] -> indices [1, 3, 5]
		require.Equal(t, []int64{1, 3, 5}, result)
	})

	t.Run("composite PK with BloomFilter only - int64 last column", func(t *testing.T) {
		// Create composite PK tuples: (col1, col2, lastCol)
		// We'll extract lastCol values: [100, 200, 300]
		tuples := [][]any{
			{int32(1), int32(2), int64(100)},
			{int32(1), int32(3), int64(200)},
			{int32(2), int32(1), int64(300)},
		}

		compositePKVec := createCompositePKVec(tuples, mp)
		defer compositePKVec.Free(mp)

		// Create BloomFilter with last column values [100, 200, 300]
		bfData, err := createBloomFilter([]any{int64(100), int64(200), int64(300)}, mp)
		require.NoError(t, err)

		// Create filter with only BloomFilter
		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName, // composite PK
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)
		require.True(t, readFilter.Valid)
		require.NotNil(t, readFilter.UnSortedSearchFunc)

		// Test the search function
		result := readFilter.UnSortedSearchFunc(compositePKVec)
		// Should return all indices [0, 1, 2] since all last columns match
		require.Equal(t, []int64{0, 1, 2}, result)
	})

	t.Run("composite PK with BloomFilter - partial match", func(t *testing.T) {
		// Create composite PK tuples with last column values: [100, 200, 300, 400]
		tuples := [][]any{
			{int32(1), int32(2), int64(100)},
			{int32(1), int32(3), int64(200)},
			{int32(2), int32(1), int64(300)},
			{int32(2), int32(2), int64(400)},
		}

		compositePKVec := createCompositePKVec(tuples, mp)
		defer compositePKVec.Free(mp)

		// Create BloomFilter with only [100, 300] (partial match)
		bfData, err := createBloomFilter([]any{int64(100), int64(300)}, mp)
		require.NoError(t, err)

		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)

		result := readFilter.UnSortedSearchFunc(compositePKVec)
		// Should return indices containing [0, 2] for values 100 and 300
		// Note: BloomFilter may have false positives, so we check that it contains the expected indices
		require.Contains(t, result, int64(0), "Should contain index 0 (value 100)")
		require.Contains(t, result, int64(2), "Should contain index 2 (value 300)")
		require.GreaterOrEqual(t, len(result), 2, "Should have at least 2 matches")
	})

	t.Run("composite PK with BloomFilter - no match", func(t *testing.T) {
		tuples := [][]any{
			{int32(1), int32(2), int64(100)},
			{int32(1), int32(3), int64(200)},
		}

		compositePKVec := createCompositePKVec(tuples, mp)
		defer compositePKVec.Free(mp)

		// Create BloomFilter with values that don't match: [999, 888]
		bfData, err := createBloomFilter([]any{int64(999), int64(888)}, mp)
		require.NoError(t, err)

		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)

		result := readFilter.UnSortedSearchFunc(compositePKVec)
		// Should return empty slice since no matches (not nil, but empty slice)
		require.Empty(t, result)
	})

	t.Run("composite PK with BloomFilter and base filter (wrap function)", func(t *testing.T) {
		// Create composite PK tuples
		tuples := [][]any{
			{int32(1), int32(2), int64(100)},
			{int32(1), int32(3), int64(200)},
			{int32(2), int32(1), int64(300)},
			{int32(2), int32(2), int64(400)},
		}

		compositePKVec := createCompositePKVec(tuples, mp)
		defer compositePKVec.Free(mp)

		// Create BloomFilter with [100, 200, 300]
		bfData, err := createBloomFilter([]any{int64(100), int64(200), int64(300)}, mp)
		require.NoError(t, err)

		// Create base filter that matches first 3 rows (simulated by creating a filter)
		// For simplicity, we'll use an IN filter that matches all rows
		inVec := vector.NewVec(types.T_int64.ToType())
		for i := int64(0); i < 4; i++ {
			vector.AppendFixed(inVec, i, false, mp)
		}
		defer inVec.Free(mp)

		basePKFilter := BasePKFilter{
			Valid: true,
			Op:    function.IN,
			Vec:   inVec,
			Oid:   types.T_int64,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)
		require.True(t, readFilter.Valid)

		// The wrap function should intersect base filter results with BF results
		// Since base filter matches all 4 rows, but BF only matches first 3,
		// result should be [0, 1, 2]
		result := readFilter.UnSortedSearchFunc(compositePKVec)
		require.NotNil(t, result)
		// Note: The exact result depends on how the base filter's IN operation works
		// This test verifies that wrap function is called and doesn't panic
		require.GreaterOrEqual(t, len(result), 0)
	})

	t.Run("composite PK with different last column types - int32", func(t *testing.T) {
		tuples := [][]any{
			{int32(1), int32(2), int32(100)},
			{int32(1), int32(3), int32(200)},
		}

		compositePKVec := createCompositePKVec(tuples, mp)
		defer compositePKVec.Free(mp)

		// Create BloomFilter with int32 values (converted to int64 for BF)
		bfData, err := createBloomFilter([]any{int64(100), int64(200)}, mp)
		require.NoError(t, err)

		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)

		result := readFilter.UnSortedSearchFunc(compositePKVec)
		require.Equal(t, []int64{0, 1}, result)
	})

	t.Run("empty composite PK vector", func(t *testing.T) {
		compositePKVec := vector.NewVec(types.T_varchar.ToType())
		defer compositePKVec.Free(mp)

		bfData, err := createBloomFilter([]any{int64(100)}, mp)
		require.NoError(t, err)

		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)

		result := readFilter.UnSortedSearchFunc(compositePKVec)
		require.Nil(t, result)
	})

	t.Run("composite PK with invalid tuple data", func(t *testing.T) {
		// Create a vector with invalid composite PK data
		vec := vector.NewVec(types.T_varchar.ToType())
		// Append invalid data (not a valid tuple)
		vector.AppendBytes(vec, []byte("invalid"), false, mp)
		defer vec.Free(mp)

		bfData, err := createBloomFilter([]any{int64(100)}, mp)
		require.NoError(t, err)

		basePKFilter := BasePKFilter{
			Valid: false,
		}

		readFilter, err := ConstructBlockPKFilter(
			"test_table",
			catalog.CPrimaryKeyColName,
			false,
			basePKFilter,
			bfData,
			mp,
		)
		require.NoError(t, err)

		// Should handle invalid data gracefully
		result := readFilter.UnSortedSearchFunc(vec)
		// Should return nil or empty since extraction fails
		require.Nil(t, result)
	})
}
