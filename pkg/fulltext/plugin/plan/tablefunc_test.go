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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// pkg/sql/plan wires planplugin.DeepCopyColDefList at init; this package's tests
// do not import it.
func init() {
	if planplugin.DeepCopyColDefList == nil {
		planplugin.DeepCopyColDefList = func(in []*plan.ColDef) []*plan.ColDef {
			out := make([]*plan.ColDef, len(in))
			for i, c := range in {
				cp := *c
				out[i] = &cp
			}
			return out
		}
	}
}

// tokenizeStubBuilder implements the PlanBuilder methods buildTokenize uses.
type tokenizeStubBuilder struct {
	planplugin.PlanBuilder
	nodes []*plan.Node
	tag   int32
}

func (b *tokenizeStubBuilder) GetContext() context.Context { return context.Background() }

func (b *tokenizeStubBuilder) GenNewBindTag() int32 {
	b.tag++
	return b.tag
}

func (b *tokenizeStubBuilder) AppendNode(node *plan.Node, _ planplugin.BindContext) int32 {
	b.nodes = append(b.nodes, node)
	return int32(len(b.nodes) - 1)
}

func tokenizeTblFunc(args ...tree.Expr) *tree.TableFunction {
	return &tree.TableFunction{Func: &tree.FuncExpr{Exprs: args}}
}

func tokenizeArgs(n int) []*plan.Expr {
	out := make([]*plan.Expr, n)
	for i := range out {
		out[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	}
	return out
}

func TestTokenizeRegistered(t *testing.T) {
	_, ok := planplugin.TableFunc(TokenizeFuncName)
	require.True(t, ok)
	require.False(t, planplugin.TableFuncRequiresCoordinator(TokenizeFuncName))
}

func TestBuildTokenizeTableScanInput(t *testing.T) {
	pkType := plan.Type{Id: int32(types.T_varchar), Width: 32}
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{
			Cols:          []*plan.ColDef{{Name: "body"}, {Name: "id", Typ: pkType}},
			Name2ColIndex: map[string]int32{"body": 0, "id": 1},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
	param := tree.NewNumVal[string](`{"parser":"ngram"}`, `{"parser":"ngram"}`, false, tree.P_char)
	b := &tokenizeStubBuilder{}
	id, err := buildTokenize(b, tokenizeTblFunc(param), nil, tokenizeArgs(3), nil, scan)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, TokenizeFuncName, node.TableDef.TblFunc.Name)
	require.Equal(t, `{"parser":"ngram"}`, string(node.TableDef.TblFunc.Param))
	require.Equal(t, pkType, node.TableDef.Cols[0].Typ)
	require.Len(t, node.TblFuncExprList, 2, "the param argument is moved to TblFunc.Param")
	require.Equal(t, int32(types.T_any), TokenizeColDefs[0].Typ.Id, "the shared column defs are copied, not mutated")
}

func TestBuildTokenizeValuesInput(t *testing.T) {
	param := tree.NewNumVal[string](`{}`, `{}`, false, tree.P_char)
	pk := tree.NewNumVal[int64](int64(types.T_int64), "23", false, tree.P_int64)
	values := &plan.Node{NodeType: plan.Node_VALUE_SCAN}
	b := &tokenizeStubBuilder{}
	id, err := buildTokenize(b, tokenizeTblFunc(param, pk), nil, tokenizeArgs(4), nil, values)
	require.NoError(t, err)
	node := b.nodes[id]
	require.Equal(t, int32(types.T_int64), node.TableDef.Cols[0].Typ.Id)
	require.Len(t, node.TblFuncExprList, 2, "the param and pk-type arguments are removed")
}

func TestBuildTokenizeErrors(t *testing.T) {
	param := tree.NewNumVal[string](`{}`, `{}`, false, tree.P_char)
	notParam := tree.NewUnresolvedName(tree.NewCStr("col", 0))
	notType := tree.NewUnresolvedName(tree.NewCStr("pk_type", 0))
	values := &plan.Node{NodeType: plan.Node_VALUE_SCAN}
	b := &tokenizeStubBuilder{}

	_, err := buildTokenize(b, tokenizeTblFunc(param), nil, tokenizeArgs(2), nil, values)
	require.ErrorContains(t, err, "NARGS < 3")
	_, err = buildTokenize(b, tokenizeTblFunc(notParam), nil, tokenizeArgs(3), nil, values)
	require.ErrorContains(t, err, "first parameter must be string")
	_, err = buildTokenize(b, tokenizeTblFunc(param), nil, tokenizeArgs(3), nil, nil)
	require.ErrorContains(t, err, "requires a left input relation")
	_, err = buildTokenize(b, tokenizeTblFunc(param, notType), nil, tokenizeArgs(3), nil, values)
	require.ErrorContains(t, err, "NARGS < 4")
	_, err = buildTokenize(b, tokenizeTblFunc(param, notType), nil, tokenizeArgs(4), nil, values)
	require.ErrorContains(t, err, "second parameter must be int32")
	require.Empty(t, b.nodes)
}
