// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TokenizeFuncName is the build TVF of the classic fulltext index: CROSS APPLY'd
// over the source rows, it emits one (doc_id, pos, word) row per token.
const TokenizeFuncName = "fulltext_index_tokenize"

// TokenizeColDefs is the output schema of TokenizeFuncName. doc_id takes the
// source primary key type.
var TokenizeColDefs = []*plan.ColDef{
	{
		Name: "doc_id",
		Typ: plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	},
	{
		Name: "pos",
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: false,
			Width:       4,
		},
	},
	{
		Name: "word",
		Typ:  types.New(types.T_varchar, 0, 0).PlanType(),
	},
}

func init() {
	planplugin.RegisterTableFunc(TokenizeFuncName, buildTokenize)
}

// buildTokenize builds the FUNCTION_SCAN of TokenizeFuncName.
//
// for table scan, primary key type is passed from TableDef
// select f.* from index_table CROSS APPLY fulltext_index_tokenize('param', doc_id, body, title) as f
// arg list [params, doc_id, part1, part2,...]
//
// for values scan, primary key type is passed from second argument
// arg list [params, pktype, doc_id, part1, part2,...]
//
// select f.* from (select cast(column_0 as bigint) as id, column_1 as body, column_2 as title from (values row(1, 'body content', 'title content'))) as src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 23, id, body, title) as f;
//
// for composite primary key,
//
// select f.* from (select serial(cast(column_0 as bigint), cast(column_1 as bigint)) as id, column_2 as body, column_3 as title from
// (values row(1, 2, 'body', 'title'), row(2, 3, 'body is heavy', 'I do not know'))) as src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 61, id, body, title) as f;
//
// for composite key, use hex string X'abcd' as input of primary key and skip serial()
// select unhex(hex(serial(cast(0 as smallint), cast(1 as int))));
func buildTokenize(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32, input *plan.Node) (int32, error) {
	if len(exprs) < 3 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS < 3).")
	}

	colDefs := planplugin.DeepCopyColDefList(TokenizeColDefs)
	params, err := getTokenizeParams(pb, tbl.Func)
	if err != nil {
		return 0, err
	}

	if input == nil {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "fulltext_index_tokenize requires a left input relation")
	}
	// input supplies planning metadata such as the primary-key type. It does not
	// by itself make the relation an execution child; buildTableFunction derives
	// that dependency independently from the normalized arguments.
	if input.NodeType == plan.Node_TABLE_SCAN {
		pkPos := input.TableDef.Name2ColIndex[input.TableDef.Pkey.PkeyColName]
		// set type to source table primary key
		colDefs[0].Typ = input.TableDef.Cols[pkPos].Typ
		// remove the first argment and put the first argument to Param
		exprs = exprs[1:]
	} else {
		// VALUES.  First argument is Params and second argument is pkType
		if len(exprs) < 4 {
			return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS < 4).")
		}
		pkType, err := getTokenizePkeyType(pb, tbl.Func)
		if err != nil {
			return 0, err
		}
		colDefs[0].Typ = pkType
		// remove the first two argments and put the first argument to Param
		exprs = exprs[2:]
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  TokenizeFuncName,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{pb.GenNewBindTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return pb.AppendNode(node, ctx), nil
}

func getTokenizeParams(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}

func getTokenizePkeyType(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (plan.Type, error) {
	if v, ok := fn.Exprs[1].(*tree.NumVal); ok {
		if t64, ok2 := v.Int64(); ok2 {
			return plan.Type{
				Id:          int32(t64),
				NotNullable: false,
			}, nil
		}
	}
	return plan.Type{}, moerr.NewNoConfig(pb.GetContext(), "second parameter must be int32")
}
