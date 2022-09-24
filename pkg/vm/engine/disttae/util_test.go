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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func makeColExprForTest(idx int32, typ types.T) *plan.Expr {
	containerType := typ.ToType()
	exprType := plan2.MakePlan2Type(&containerType)

	return &plan.Expr{
		Typ: exprType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: idx,
				Name:   "",
			},
		},
	}
}

func makeFunctionExprForTest(name string, args []*plan.Expr) *plan.Expr {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = plan2.MakeTypeByPlan2Expr(arg)
	}

	funId, returnType, _, _ := function.GetFunctionByName(name, argTypes)

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

func makeColumnMetaForTest(typ types.T, min []byte, max []byte) *ColumnMeta {
	zm, _ := NewZoneMap(0, min, max)
	bf := NewBloomFilter(0, 0, []byte{})
	return &ColumnMeta{
		typ: uint8(typ),
		idx: 0,
		alg: 0,
		location: Extent{
			id:         0,
			offset:     0,
			length:     0,
			originSize: 0,
		},
		zoneMap:     zm,
		bloomFilter: bf,
		dummy:       [32]byte{},
		checksum:    0,
	}
}

func makeBlockMetaForTest() BlockMeta {
	column1Min := make([]byte, 8)
	column1Max := make([]byte, 8)
	column2Min := make([]byte, 8)
	column2Max := make([]byte, 8)
	column3Min := make([]byte, 8)
	column3Max := make([]byte, 8)
	column4Min := make([]byte, 8)
	column4Max := make([]byte, 8)

	binary.LittleEndian.PutUint64(column1Min, 10)
	binary.LittleEndian.PutUint64(column1Max, 100)
	binary.LittleEndian.PutUint64(column2Min, 20)
	binary.LittleEndian.PutUint64(column2Max, 200)
	binary.LittleEndian.PutUint64(column3Min, 30)
	binary.LittleEndian.PutUint64(column3Max, 300)
	binary.LittleEndian.PutUint64(column4Min, 1)
	binary.LittleEndian.PutUint64(column4Max, 8)

	return BlockMeta{
		header: BlockHeader{},
		columns: []*ColumnMeta{
			makeColumnMetaForTest(types.T_int64, column1Min, column1Max),
			makeColumnMetaForTest(types.T_int64, column2Min, column2Max),
			makeColumnMetaForTest(types.T_int64, column3Min, column3Max),
			makeColumnMetaForTest(types.T_int64, column4Min, column4Max),
		},
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
	return _getTableDefBySchemaAndType("t1", columns, schema, types)
}

func TestCheckExprIsMonotonical(t *testing.T) {
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

	t.Run("test checkExprIsMonotonical", func(t *testing.T) {
		for i, testCase := range testCases {
			isMonotonical := checkExprIsMonotonical(testCase.expr)
			if isMonotonical != testCase.result {
				t.Fatalf("checkExprIsMonotonical testExprs[%d] is different with expected", i)
			}
		}
	})
}

func TestNeedRead(t *testing.T) {
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
	}

	t.Run("test needRead", func(t *testing.T) {
		for i, testCase := range testCases {
			result := needRead(testCase.expr, blockMeta, makeTableDefForTest(testCase.columns), testutil.NewProc())
			if result != testCase.result {
				t.Fatalf("test needRead at cases[%d], get result is different with expected", i)
			}
		}
	})
}
