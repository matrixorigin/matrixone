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
		zoneMap: ZoneMap{
			idx: 0,
			min: min,
			max: max,
		},
		bloomFilter: Extent{
			id:         0,
			offset:     0,
			length:     0,
			originSize: 0,
		},
		dummy:    [32]byte{},
		checksum: 0,
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
	testExprs := []*plan.Expr{
		// a > 1  -> true
		makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		}),
		// a >= b -> true
		makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		// abs(a) -> false
		makeFunctionExprForTest("abs", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
		}),
	}

	expected := []bool{true, true, false}

	t.Run("test checkExprIsMonotonical", func(t *testing.T) {
		for i, expr := range testExprs {
			isMonotonical := checkExprIsMonotonical(expr)
			if isMonotonical != expected[i] {
				t.Fatalf("checkExprIsMonotonical testExprs[%d] is different with expected", i)
			}
		}
	})
}

func TestNeedRead(t *testing.T) {
	blockMeta := makeBlockMetaForTest()

	testExprs := []*plan.Expr{
		makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		}),
		makeFunctionExprForTest("<", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(-1),
		}),
		makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(3000000),
		}),
		makeFunctionExprForTest("<", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("+", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				makeColExprForTest(2, types.T_int64),
			}),
		}),
	}
	testColumns := [][]string{
		{"a"},
		{"a"},
		{"a"},
		{"a", "d"},
		{"a", "d"},
		{"c", "a", "d"}, // c > (a + d)
	}
	expected := []bool{
		true,
		false,
		false,
		false,
		true,
		true,
	}

	t.Run("test needRead", func(t *testing.T) {
		for i, expr := range testExprs {
			result := needRead(expr, blockMeta, makeTableDefForTest(testColumns[i]), testutil.NewProc())
			if result != expected[i] {
				t.Fatalf("test needRead at cases[%d], get result is different with expected", i)
			}
		}
	})
}
