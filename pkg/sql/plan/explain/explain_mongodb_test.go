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

package explain

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func mongodbExplainTestColumn(position int32, name string, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: position, Name: name}},
	}
}

func mongodbExplainTestString(value string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: value},
		}},
	}
}

func mongodbExplainTestFunction(name string, arguments ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name},
			Args: arguments,
		}},
	}
}

func mongodbExplainTestNode(filters ...*plan.Expr) *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{ColId: 1, Name: "value", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{ColId: catalog.ExternalQueryColId, Name: catalog.ExternalQuery,
				Typ: plan.Type{Id: int32(types.T_varchar)}},
		}},
		FilterList: filters,
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_MONGODB_TB),
			MongodbScan: &plan.MongoScan{
				TableId: 7,
				Columns: []*plan.MongoColumnMapping{{Name: "value", Path: "value"}},
			},
		},
	}
}

func TestMongoDBExtraInfoShowsOperationAndDigestWithoutQueryText(t *testing.T) {
	rawQuery := `{"pipeline":[{"$count":"secret_literal"}]}`
	node := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_MONGODB_TB),
			MongodbScan: &plan.MongoScan{
				TableId:         7,
				Columns:         []*plan.MongoColumnMapping{{Name: "count", Path: "count"}},
				UserQueryKind:   2,
				UserQueryDigest: strings.Repeat("a", 64),
			},
		},
	}
	lines, err := NewNodeDescriptionImpl(node).GetExtraInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "operation=aggregate")
	require.Contains(t, lines[0], "query_digest=aaaaaaaaaaaa")
	require.NotContains(t, lines[0], rawQuery)
	require.NotContains(t, lines[0], "secret_literal")
}

func TestMongoDBExplainDerivesAndRedactsPlannerFormSelector(t *testing.T) {
	queryColumn := mongodbExplainTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	valueColumn := mongodbExplainTestColumn(0, "value", types.T_varchar)
	rawQuery := `{"pipeline":[{"$count":"secret_literal"}]}`
	node := mongodbExplainTestNode(
		mongodbExplainTestFunction("=", queryColumn, mongodbExplainTestString(rawQuery)),
		mongodbExplainTestFunction("=", valueColumn, mongodbExplainTestString("ordinary-marker")),
	)

	lines, err := NewNodeDescriptionImpl(node).GetExtraInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Len(t, lines, 2)
	require.Contains(t, lines[0], "operation=aggregate")
	require.NotContains(t, lines[0], "query_digest=none")
	require.Contains(t, lines[1], "ordinary-marker")
	for _, line := range lines {
		require.NotContains(t, line, rawQuery)
		require.NotContains(t, line, "secret_literal")
		require.NotContains(t, line, catalog.ExternalQuery)
	}
}

func TestMongoDBExplainRedactsUnsupportedSelectorShape(t *testing.T) {
	queryColumn := mongodbExplainTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	node := mongodbExplainTestNode(
		mongodbExplainTestFunction("like", queryColumn, mongodbExplainTestString("%secret-fragment%")),
	)

	lines, err := NewNodeDescriptionImpl(node).GetExtraInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "operation=explicit")
	require.NotContains(t, lines[0], "secret-fragment")
	require.NotContains(t, lines[0], catalog.ExternalQuery)
}

func TestMongoDBStructuredPlanRedactsSelector(t *testing.T) {
	queryColumn := mongodbExplainTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	rawQuery := `{"filter":{"password":"super-secret-value"}}`
	node := mongodbExplainTestNode(
		mongodbExplainTestFunction("=", queryColumn, mongodbExplainTestString(rawQuery)),
	)

	labels, err := NewMarshalNodeImpl(node).GetNodeLabels(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, labels)
	for _, label := range labels {
		rendered := fmt.Sprint(label.Value)
		require.NotContains(t, rendered, "password")
		require.NotContains(t, rendered, "super-secret-value")
		require.NotEqual(t, Label_Filter_Conditions, label.Name)
	}
	serialized := fmt.Sprint(BuildJsonPlan(context.Background(), uuid.New(), &MarshalPlanOptions, &plan.Query{
		Nodes: []*plan.Node{node}, Steps: []int32{0},
	}))
	require.NotContains(t, serialized, "password")
	require.NotContains(t, serialized, "super-secret-value")
}
