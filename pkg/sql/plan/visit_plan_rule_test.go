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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

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
