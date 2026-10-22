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

	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildViewColumnsRejectsDirectUse(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"select * from nation cross apply mo_view_columns(n_nationkey) mc", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "private to information_schema metadata views")
}

func TestBuildDerivedViewColumnsPreservesNamesAndDisablesCache(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	compiler := &viewMetadataTestCompiler{viewSchemaTestCompiler: f.compiler, ctx: r.Context()}
	for _, name := range []string{ViewColumnsFunctionName, SubscriptionViewColumnsFunctionName} {
		builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
		builder.persistedViewTarget = objectkey.Encode("information_schema", "columns")
		ctx := NewBindContext(builder, nil)
		tf := &tree.TableFunction{Func: &tree.FuncExpr{FuncName: tree.NewCStr(name, 1)}}
		args := []*Expr{makePlan2StringConstExprWithType("database"), makePlan2StringConstExprWithType("view_name"), makePlan2Uint64ConstExprWithType(10)}
		nodeID, err := builder.buildViewColumns(tf, ctx, args, nil)
		require.NoError(t, err)
		node := builder.qry.Nodes[nodeID]
		require.True(t, node.NotCacheable)
		for _, arg := range node.TblFuncExprList[:2] {
			require.Equal(t, int32(types.MaxVarcharLen), arg.Typ.Width)
		}
		_, err = builder.buildViewColumns(tf, ctx, args[:1], nil)
		require.Error(t, err)
		builder.persistedViewTarget = ""
		_, err = builder.buildViewColumns(tf, ctx, args, nil)
		require.ErrorContains(t, err, "private to information_schema")
	}
}
