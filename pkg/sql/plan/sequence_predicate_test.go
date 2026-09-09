// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func sequenceExprForTest(fid int32) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(fid, 0)},
	}}}
}

func TestContainsSequenceFunctionUsesBoundFunctionID(t *testing.T) {
	require.True(t, ContainsSequenceFunction(sequenceExprForTest(function.NEXTVAL)))
	require.True(t, ContainsSequenceFunction(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LASTVAL, 0)},
		Args: []*plan.Expr{{Expr: &plan.Expr_List{List: &plan.ExprList{
			List: []*plan.Expr{sequenceExprForTest(function.CURRVAL)},
		}}}},
	}}}))
	require.False(t, ContainsSequenceFunction(&plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "nextval"},
	}}}))
	require.False(t, ContainsSequenceFunction(sequenceExprForTest(function.ABS)))
}

func TestLastInsertIDPlacementOnlyTracksExpressionOverload(t *testing.T) {
	read := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LAST_INSERT_ID, 0)},
	}}}
	expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LAST_INSERT_ID, function.LastInsertIDExprOverload)},
	}}}
	require.False(t, ContainsSequenceFunction(read))
	require.True(t, ContainsSequenceFunction(expr))
	require.True(t, ContainsLastInsertIDExpr(expr))
	require.False(t, ContainsLastInsertIDExpr(read))
}

func TestQueryContainsLastInsertIDExpr(t *testing.T) {
	expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LAST_INSERT_ID, function.LastInsertIDExprOverload)},
	}}}
	qry := &plan.Query{Steps: []int32{0}, Nodes: []*plan.Node{{ProjectList: []*plan.Expr{expr}}}}
	require.True(t, QueryContainsLastInsertIDExpr(qry))
	qry.Nodes[0].ProjectList = []*plan.Expr{sequenceExprForTest(function.NEXTVAL)}
	require.False(t, QueryContainsLastInsertIDExpr(qry))
}

func TestQueryContainsSequenceFunctionVisitsSupplementalExpressions(t *testing.T) {
	qry := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{
			NodeType: plan.Node_TABLE_SCAN,
			IndexReaderParam: &plan.IndexReaderParam{
				Limit: sequenceExprForTest(function.NEXTVAL),
			},
		}},
	}
	require.True(t, QueryContainsSequenceFunction(qry))

	qry.Nodes[0].IndexReaderParam.Limit = &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ABS, 0)},
	}}}
	require.False(t, QueryContainsSequenceFunction(qry))
}

func TestQueryContainsSequenceFunctionIgnoresUnexecutedTableDefaults(t *testing.T) {
	defaultExpr := sequenceExprForTest(function.NEXTVAL)
	qry := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{
			NodeType: plan.Node_TABLE_SCAN,
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
				Name:    "id",
				Default: &plan.Default{Expr: defaultExpr},
			}}},
		}},
	}
	require.False(t, QueryContainsSequenceFunction(qry))

	// INSERT planning materializes an evaluated default into the projection;
	// that executable expression must still force the sequence placement rule.
	qry.Nodes[0].ProjectList = []*plan.Expr{defaultExpr}
	require.True(t, QueryContainsSequenceFunction(qry))
}
