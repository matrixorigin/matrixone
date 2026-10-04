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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/stretchr/testify/require"
)

func TestMongoDBInsertSelectPrimaryKeyUsesShuffleDedup(t *testing.T) {
	mock := NewMockOptimizer(true)
	mapping := mongodb.TableMapping{
		Connection: "mongodb_ci", Database: "mongodb_source", Collection: "nation",
		SchemaMode: mongodb.SchemaExplicit, Conversion: mongodb.ConversionStrict, MaxParallelism: 1,
		Columns: []mongodb.ColumnMapping{
			{Name: "n_nationkey", Path: "n_nationkey", TypeID: int32(types.T_int32), Conversion: mongodb.ConversionStrict},
			{Name: "n_name", Path: "n_name", TypeID: int32(types.T_varchar), Width: 25, Conversion: mongodb.ConversionStrict},
			{Name: "n_regionkey", Path: "n_regionkey", TypeID: int32(types.T_int32), Conversion: mongodb.ConversionStrict},
			{Name: "n_comment", Path: "n_comment", TypeID: int32(types.T_varchar), Width: 152, Conversion: mongodb.ConversionStrict},
		},
	}
	mock.ctxt.objects["mongo_nation"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "mongo_nation", Obj: 4242}
	mock.ctxt.tables["mongo_nation"] = &planpb.TableDef{
		Name: "mongo_nation", TableType: catalog.SystemExternalRel,
		FeatureFlag: features.MongoDBExternal,
		Createsql:   mongodb.BuildCreateSQLEnvelope(mapping),
		Cols: []*planpb.ColDef{
			{Name: "n_nationkey", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "n_name", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 25}},
			{Name: "n_regionkey", Typ: planpb.Type{Id: int32(types.T_int32)}},
			{Name: "n_comment", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 152}},
		},
	}

	logicPlan, err := runOneStmt(mock, t,
		"insert into tpch.nation select n_nationkey,n_name,n_regionkey,n_comment from tpch.mongo_nation where n_nationkey=1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Len(t, query.Steps, 1)
	require.Equal(t, planpb.Node_MULTI_UPDATE, query.Nodes[query.Steps[0]].NodeType)

	var foundMongoScan, foundShuffleDedup bool
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_EXTERNAL_SCAN && node.ExternScan != nil &&
			node.ExternScan.Type == int32(planpb.ExternType_MONGODB_TB) {
			foundMongoScan = true
		}
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_DEDUP &&
			node.Stats != nil && node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle {
			foundShuffleDedup = true
		}
	}
	require.True(t, foundMongoScan)
	require.True(t, foundShuffleDedup)
}
