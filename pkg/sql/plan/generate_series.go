// Copyright 2022 Matrix Origin
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

package plan

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
	"strings"
)

var (
	int32ColDef = &plan.ColDef{
		Name: "result",
		Typ: &plan.Type{
			Id:       int32(types.T_int32),
			Nullable: false,
		},
	}
	int64ColDef = &plan.ColDef{
		Name: "result",
		Typ: &plan.Type{
			Id:       int32(types.T_int64),
			Nullable: false,
		},
	}
	datetimeColDef = &plan.ColDef{
		Name: "result",
		Typ: &plan.Type{
			Id:       int32(types.T_datetime),
			Nullable: false,
		},
	}
)

func (builder *QueryBuilder) buildGenerateSeries(tbl *tree.TableFunction, ctx *BindContext) (int32, error) {
	var (
		err             error
		externParamData []byte
		scanNode        *plan.Node
		childId         int32 = -1
		precision       int32
		colDefs         []*plan.ColDef
	)
	if externParamData, precision, err = genParam(tbl); err != nil {
		return 0, err
	}
	scanNode = &plan.Node{
		NodeType: plan.Node_VALUE_SCAN,
		TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Param: externParamData}},
		ProjectList: []*plan.Expr{
			{
				Typ:  &plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			},
		},
	}
	childId = builder.appendNode(scanNode, ctx)
	if precision == -1 {
		colDefs = []*plan.ColDef{int32ColDef}
	} else if precision == -2 {
		colDefs = []*plan.ColDef{int64ColDef}
	} else {
		colDefs = []*plan.ColDef{_dupColDef(datetimeColDef)}
		colDefs[0].Typ.Precision = precision
	}
	node := &plan.Node{
		NodeType: plan.Node_TABLE_FUNCTION,
		Cost:     &plan.Cost{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "generate_series",
			},
			Cols: colDefs,
		},
		BindingTags: []int32{builder.genNewTag()},
	}
	node.Children = []int32{childId}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}

func genParam(tbl *tree.TableFunction) ([]byte, int32, error) {
	var (
		err        error
		e1, e2, e3 *tree.NumVal
		ok         bool
		args       []string
		ret        []byte
	)

	exprs := tbl.Func.Exprs

	if e1, ok = exprs[0].(*tree.NumVal); !ok {
		return nil, 0, moerr.NewInvalidInput("generate_series args is invalid")
	}
	if e2, ok = exprs[1].(*tree.NumVal); !ok {
		return nil, 0, moerr.NewInvalidInput("generate_series args is invalid")
	}
	if e3, ok = exprs[2].(*tree.NumVal); !ok {
		return nil, 0, moerr.NewInvalidInput("generate_series args is invalid")
	}
	args = []string{e1.String(), e2.String(), e3.String()}
	ret, err = json.Marshal(args)
	if err != nil {
		return nil, 0, err
	}
	if _, err := strconv.ParseInt(e1.String(), 10, 32); err == nil {
		return ret, -1, nil
	}
	if _, err := strconv.ParseFloat(e1.String(), 64); err == nil {
		return ret, -2, nil
	}
	precision := findMaxPrecision(e1.String(), e2.String())
	return ret, precision, nil
}

func findMaxPrecision(s1, s2 string) int32 {
	var (
		precision = int32(0)
	)
	if strings.Contains(s1, ".") {
		precision = int32(len(s1) - strings.Index(s1, ".") - 1)
	}
	if strings.Contains(s2, ".") {
		precision2 := int32(len(s2) - strings.Index(s2, ".") - 1)
		if precision2 > precision {
			precision = precision2
		}
	}
	if precision > 6 {
		precision = 6
	}
	return precision
}
