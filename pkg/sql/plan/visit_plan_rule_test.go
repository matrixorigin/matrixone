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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

var errWindowParameterVisit = errors.New("window parameter visit failed")

type failWindowParameterVisitRule struct {
	failOn int
	calls  int
}

func (*failWindowParameterVisitRule) MatchNode(*planpb.Node) bool  { return false }
func (*failWindowParameterVisitRule) IsApplyExpr() bool            { return true }
func (*failWindowParameterVisitRule) ApplyNode(*planpb.Node) error { return nil }
func (r *failWindowParameterVisitRule) ApplyExpr(expr *planpb.Expr) (*planpb.Expr, error) {
	r.calls++
	if r.calls == r.failOn {
		return nil, errWindowParameterVisit
	}
	return expr, nil
}

func TestPrepareRulesTraverseEveryWindowSpecParameter(t *testing.T) {
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	window := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc:  param(5),
		PartitionBy: []*planpb.Expr{param(3), nil},
		OrderBy:     []*planpb.OrderBySpec{{Expr: param(4)}, nil},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: param(2)},
			End:   &planpb.FrameBound{Val: param(1)},
		},
	}}}

	get := NewGetParamRule()
	require.NoError(t, applyRuleToWindowSpec(get, nil))
	_, err := get.ApplyExpr(window)
	require.NoError(t, err)
	require.Equal(t, map[int]int{1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, get.params)
	get.SetParamOrder()

	_, err = NewResetParamOrderRule(get.params).ApplyExpr(window)
	require.NoError(t, err)
	require.Equal(t, []int32{4, 2, 3, 1, 0}, []int32{
		window.GetW().WindowFunc.GetP().Pos,
		window.GetW().PartitionBy[0].GetP().Pos,
		window.GetW().OrderBy[0].Expr.GetP().Pos,
		window.GetW().Frame.Start.Val.GetP().Pos,
		window.GetW().Frame.End.Val.GetP().Pos,
	})
}

func TestApplyRuleToWindowSpecPropagatesFieldErrors(t *testing.T) {
	newWindow := func() *planpb.WindowSpec {
		param := func() *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{}}}
		}
		return &planpb.WindowSpec{
			WindowFunc:  param(),
			PartitionBy: []*planpb.Expr{param()},
			OrderBy:     []*planpb.OrderBySpec{{Expr: param()}},
			Frame: &planpb.FrameClause{
				Start: &planpb.FrameBound{Val: param()},
				End:   &planpb.FrameBound{Val: param()},
			},
		}
	}

	for failOn := 1; failOn <= 5; failOn++ {
		t.Run("field", func(t *testing.T) {
			rule := &failWindowParameterVisitRule{failOn: failOn}
			require.ErrorIs(t, applyRuleToWindowSpec(rule, newWindow()), errWindowParameterVisit)
		})
	}
}

func TestResetPreparePlanCollectsHiddenIndexSchemas(t *testing.T) {
	const hiddenTable = "__mo_index_hidden"
	mock := NewMockCompilerContext(false)
	mock.objects[hiddenTable] = &planpb.ObjectRef{
		Db:         10,
		Obj:        20,
		SchemaName: "db",
		ObjName:    hiddenTable,
	}
	mock.tables[hiddenTable] = &planpb.TableDef{Name: hiddenTable, DbId: 10, TblId: 20, Version: 30}

	queryPlan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef: &planpb.ObjectRef{
					Db:         1,
					Obj:        2,
					SchemaName: "db",
					ObjName:    "src",
				},
				TableDef: &planpb.TableDef{
					Name:    "src",
					DbId:    1,
					TblId:   2,
					Version: 3,
					Indexes: []*planpb.IndexDef{{
						IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
						IndexTableName: hiddenTable,
					}},
				},
			}},
		}},
	}

	schemas, _, err := ResetPreparePlan(mock, queryPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "src", schemas[0].ObjName)
	require.Equal(t, hiddenTable, schemas[1].ObjName)
	require.Equal(t, int64(30), schemas[1].Server)
	require.Equal(t, int64(10), schemas[1].Db)
	require.Equal(t, int64(20), schemas[1].Obj)
}
