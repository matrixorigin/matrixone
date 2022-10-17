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
		childId         int32
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
		err  error
		args []string
		ret  []byte
	)

	exprs := tbl.Func.Exprs
	if len(exprs) < 2 || len(exprs) > 3 {
		return nil, 0, moerr.NewInvalidInput("generate_series: invalid number of arguments, expected 2 or 3, got %d", len(exprs))
	}
	if ret, err = genInt32Args(exprs); err == nil {
		return ret, -1, nil
	}
	if ret, err = genInt64Args(exprs); err == nil {
		return ret, -2, nil
	}
	if len(exprs) == 2 {
		return nil, 0, moerr.NewInvalidInput("generate_series: invalid arguments, must specify step for datetime")
	}

	args = []string{exprs[0].String(), exprs[1].String(), exprs[2].String()}
	ret, err = json.Marshal(args)
	if err != nil {
		return nil, 0, err
	}

	precision := findMaxPrecision(exprs[0].String(), exprs[1].String())
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

func genInt32Args(exprs tree.Exprs) (ret []byte, err error) {
	for _, expr := range exprs {
		if _, err = strconv.ParseInt(expr.String(), 10, 32); err != nil {
			return
		}
	}
	args := make([]string, len(exprs))
	for i, expr := range exprs {
		args[i] = expr.String()
	}
	ret, err = json.Marshal(args)
	return
}

func genInt64Args(exprs tree.Exprs) (ret []byte, err error) {
	for _, expr := range exprs {
		if _, err = strconv.ParseInt(expr.String(), 10, 64); err != nil {
			return
		}
	}
	args := make([]string, len(exprs))
	for i, expr := range exprs {
		args[i] = expr.String()
	}
	ret, err = json.Marshal(args)
	return
}
