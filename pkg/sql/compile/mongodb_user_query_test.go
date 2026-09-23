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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func mongoUserQueryNode() *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{ColId: 1, Name: "value", Typ: plan.Type{Id: int32(types.T_int64)}},
			{ColId: catalog.ExternalQueryColId, Name: catalog.ExternalQuery, Hidden: true, Typ: plan.Type{Id: int32(types.T_varchar)}},
		}},
		ExternScan: &plan.ExternScan{
			Type:        int32(plan.ExternType_MONGODB_TB),
			MongodbScan: &plan.MongoScan{TableId: 7, Database: "db", Collection: "events", MaxParallelism: 1},
		},
	}
}

func mongoQueryTestColumn(position int32, name string, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: position, Name: name}},
	}
}

func mongoQueryTestString(value string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: value},
		}},
	}
}

func mongoQueryTestFunction(name string, functionID int64, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name, Obj: functionID << 32},
			Args: args,
		}},
	}
}

func newMongoUserQueryTestCompiler(t *testing.T) *Compile {
	t.Helper()
	proc := testutil.NewProcess(t)
	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	return &Compile{proc: proc}
}

func TestConfigureMongoUserQuerySeparatesSelectorAndResidual(t *testing.T) {
	node := mongoUserQueryNode()
	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	valueColumn := mongoQueryTestColumn(0, "value", types.T_int64)
	source := `{"filter":{"value":{"$gte":10}}}`
	selector := mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(source))
	ordinary := mongoQueryTestFunction("=", function.EQUAL, valueColumn, &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 11},
		}},
	})
	node.FilterList = []*plan.Expr{selector, ordinary}
	node.ProjectList = []*plan.Expr{valueColumn}
	compiler := newMongoUserQueryTestCompiler(t)

	require.NoError(t, compiler.configureMongoUserQuery(node))
	require.Len(t, node.FilterList, 1)
	require.Equal(t, ordinary, node.FilterList[0])
	require.False(t, node.ExternScan.MongodbScan.IncludeQueryColumn)
	require.False(t, node.ExternScan.MongodbScan.EmptyResult)
	require.Equal(t, int32(mongodb.UserQueryFilter), node.ExternScan.MongodbScan.UserQueryKind)
	restored, err := mongodb.UserQueryFromPlan(t.Context(), node.ExternScan.MongodbScan)
	require.NoError(t, err)
	require.Equal(t, mongodb.UserQueryFilter, restored.Kind)
}

func TestConfigureMongoUserQueryReconstructsSourceForExplicitProjection(t *testing.T) {
	node := mongoUserQueryNode()
	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	source := `{"pipeline":[{"$match":{}},{"$count":"value"}]}`
	node.FilterList = []*plan.Expr{
		mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(source)),
	}
	node.ProjectList = []*plan.Expr{queryColumn}
	compiler := newMongoUserQueryTestCompiler(t)

	require.NoError(t, compiler.configureMongoUserQuery(node))
	require.Empty(t, node.FilterList)
	scan := node.ExternScan.MongodbScan
	require.True(t, scan.IncludeQueryColumn)
	require.Equal(t, int32(mongodb.UserQueryPipeline), scan.UserQueryKind)
	restored, err := mongodb.UserQueryFromPlan(t.Context(), scan)
	require.NoError(t, err)
	require.Equal(t, source, restored.Source)
}

func TestConfigureMongoUserQueryPipelineKeepsHiddenCarrierForResidualFilter(t *testing.T) {
	node := mongoUserQueryNode()
	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	node.FilterList = []*plan.Expr{
		mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"pipeline":[{"$count":"value"}]}`)),
	}
	compiler := newMongoUserQueryTestCompiler(t)

	require.NoError(t, compiler.configureMongoUserQuery(node))
	require.True(t, node.ExternScan.MongodbScan.IncludeQueryColumn)
	require.Equal(t, int32(mongodb.UserQueryPipeline), node.ExternScan.MongodbScan.UserQueryKind)
}

func TestConfigureMongoUserQueryRejectsUnsupportedAndUnsafeSelectors(t *testing.T) {
	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	compiler := &Compile{proc: testutil.NewProcess(t)}

	t.Run("query filter without equality generator", func(t *testing.T) {
		node := mongoUserQueryNode()
		node.FilterList = []*plan.Expr{
			mongoQueryTestFunction("like", function.LIKE, queryColumn, mongoQueryTestString("%filter%")),
		}
		require.ErrorContains(t, compiler.configureMongoUserQuery(node), "requires __mo_query = <constant>")
	})

	t.Run("multiple candidates", func(t *testing.T) {
		node := mongoUserQueryNode()
		node.FilterList = []*plan.Expr{
			mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"filter":{"value":1}}`)),
			mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"filter":{"value":2}}`)),
		}
		require.ErrorContains(t, compiler.configureMongoUserQuery(node), "exactly one")
	})

	t.Run("unsafe stage", func(t *testing.T) {
		node := mongoUserQueryNode()
		node.FilterList = []*plan.Expr{
			mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"pipeline":[{"$out":"archive"}]}`)),
		}
		require.ErrorContains(t, compiler.configureMongoUserQuery(node), "is not allowed")
	})
}

func TestConfigureMongoUserQueryMarksPrunedCandidateAsEmpty(t *testing.T) {
	node := mongoUserQueryNode()
	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	source := `{"filter":{"value":1}}`
	node.FilterList = []*plan.Expr{
		mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(source)),
		mongoQueryTestFunction("like", function.LIKE, queryColumn, mongoQueryTestString("does-not-match")),
	}
	compiler := newMongoUserQueryTestCompiler(t)

	require.NoError(t, compiler.configureMongoUserQuery(node))
	require.True(t, node.ExternScan.MongodbScan.EmptyResult)
	require.Zero(t, node.ExternScan.MongodbScan.UserQueryKind)
	require.Empty(t, node.FilterList)
}

func TestConfigureMongoUserQueryLeavesLegacyFindPathUnchanged(t *testing.T) {
	node := mongoUserQueryNode()
	valueColumn := mongoQueryTestColumn(0, "value", types.T_int64)
	node.ProjectList = []*plan.Expr{valueColumn}
	compiler := &Compile{proc: testutil.NewProcess(t)}
	require.NoError(t, compiler.configureMongoUserQuery(node))
	require.Zero(t, node.ExternScan.MongodbScan.UserQueryKind)
	require.False(t, node.ExternScan.MongodbScan.IncludeQueryColumn)
	require.False(t, node.ExternScan.MongodbScan.EmptyResult)
}

func TestConfigureMongoUserQueryRequiresCompatibleProtocol(t *testing.T) {
	compiler := &Compile{proc: testutil.NewProcess(t)}
	rt := runtime.ServiceRuntime(compiler.proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion43)
		}
	})
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion43)

	queryColumn := mongoQueryTestColumn(1, catalog.ExternalQuery, types.T_varchar)
	for name, node := range map[string]*plan.Node{
		"explicit query": func() *plan.Node {
			node := mongoUserQueryNode()
			node.FilterList = []*plan.Expr{
				mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"filter":{"value":1}}`)),
			}
			return node
		}(),
		"query column carrier": func() *plan.Node {
			node := mongoUserQueryNode()
			node.ProjectList = []*plan.Expr{queryColumn}
			return node
		}(),
		"pruned empty result": func() *plan.Node {
			node := mongoUserQueryNode()
			node.FilterList = []*plan.Expr{
				mongoQueryTestFunction("=", function.EQUAL, queryColumn, mongoQueryTestString(`{"filter":{"value":1}}`)),
				mongoQueryTestFunction("like", function.LIKE, queryColumn, mongoQueryTestString("does-not-match")),
			}
			return node
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			err := compiler.configureMongoUserQuery(node)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
		})
	}
}

func TestSupportsRemoteMongoUserQueryRejectsMissingRuntime(t *testing.T) {
	const service = "mongodb-missing-compile-runtime"
	require.Nil(t, runtime.ServiceRuntime(service))
	require.False(t, supportsRemoteMongoUserQuery(service))
}
