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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
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

func makeColumnMetaForTest(idx uint16, typ types.T, min any, max any) *ColumnMeta {
	zm := index.NewZoneMap(typ.ToType())
	_ = zm.Update(min)
	_ = zm.Update(max)
	buf, _ := zm.Marshal()
	czm, _ := objectio.NewZoneMap(idx, buf)

	bf := objectio.NewBloomFilter(0, 0, []byte{})
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
		zoneMap:     czm,
		bloomFilter: bf,
		dummy:       [32]byte{},
		checksum:    0,
	}
}

func makeBlockMetaForTest() BlockMeta {
	return BlockMeta{
		header: BlockHeader{},
		columns: []*ColumnMeta{
			makeColumnMetaForTest(0, types.T_int64, int64(10), int64(100)),
			makeColumnMetaForTest(1, types.T_int64, int64(20), int64(200)),
			makeColumnMetaForTest(2, types.T_int64, int64(30), int64(300)),
			makeColumnMetaForTest(3, types.T_int64, int64(1), int64(8)),
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

func TestWriteAndRead(t *testing.T) {
	ctx := context.TODO()
	bm := makeBlockMetaForTest()
	testFs := testutil.NewFS()
	inputBat := testutil.NewBatch([]types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}, true, 20, testutil.NewMheap())

	blks, err := blockWrite(ctx, bm, inputBat, testFs)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// fmt.Printf("%v", blks)

	extent := blks[0].GetExtent()
	bm.localExtent.length = extent.Length()
	bm.localExtent.offset = extent.Offset()
	bm.localExtent.originSize = extent.OriginSize()

	// bm.header.blockId
	columns := []string{"a", "b", "c", "d"}
	outputBat, err := blockRead(ctx, columns, bm, testFs, makeTableDefForTest(columns))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// fmt.Printf("%v", outputBat)
	if len(inputBat.Vecs) != 4 || len(inputBat.Vecs) != len(outputBat.Vecs) {
		t.Error("input not equal output: length not match")
	}
	cols1 := vector.MustTCols[int64](inputBat.Vecs[0])
	cols2 := vector.MustTCols[int64](outputBat.Vecs[0])
	if len(cols1) != 20 || len(cols1) != len(cols2) {
		t.Error("input not equal output: vec[0]'s length not match")
	}
	for i := 0; i < len(cols1); i++ {
		if cols1[i] != cols2[i] {
			t.Errorf("input not equal output: vec[0][%d]'s value not match", i)
		}
	}
}
