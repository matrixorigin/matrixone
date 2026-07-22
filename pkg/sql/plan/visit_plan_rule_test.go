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

type resolveErrorCompilerContext struct {
	*MockCompilerContext
	err error
}

func (c *resolveErrorCompilerContext) Resolve(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	return nil, nil, c.err
}

func TestResetPreparePlanCollectsDdlSchemas(t *testing.T) {
	testCases := []struct {
		name string
		ddl  *planpb.DataDefinition
	}{
		{
			name: "alter table",
			ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_AlterTable{
				AlterTable: &planpb.AlterTable{
					Database: "db", TableDef: &planpb.TableDef{Name: "tbl"},
				},
			}},
		},
		{
			name: "create index",
			ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_CreateIndex{
				CreateIndex: &planpb.CreateIndex{Database: "db", Table: "tbl"},
			}},
		},
		{
			name: "drop index",
			ddl: &planpb.DataDefinition{Definition: &planpb.DataDefinition_DropIndex{
				DropIndex: &planpb.DropIndex{Database: "db", Table: "tbl"},
			}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			mock.objects["tbl"] = &planpb.ObjectRef{ServerName: "server"}
			mock.tables["tbl"] = &planpb.TableDef{Name: "tbl", DbId: 10, TblId: 20, Version: 30}

			schemas, _, err := ResetPreparePlan(mock, &planpb.Plan{
				Plan: &planpb.Plan_Ddl{Ddl: testCase.ddl},
			})
			require.NoError(t, err)
			require.Equal(t, []*planpb.ObjectRef{{
				Server:     30,
				Db:         10,
				Schema:     10,
				Obj:        20,
				ServerName: "server",
				SchemaName: "db",
				ObjName:    "tbl",
			}}, schemas)
		})
	}
}

func TestGetPrepareDdlSchemasRejectsMissingTable(t *testing.T) {
	ddl := &planpb.DataDefinition{Definition: &planpb.DataDefinition_DropIndex{
		DropIndex: &planpb.DropIndex{Database: "db", Table: "tbl"},
	}}
	mock := NewMockCompilerContext(false)
	_, err := getPrepareDdlSchemas(mock, ddl)
	require.Error(t, err)
}

func TestGetPrepareDdlSchemasPropagatesResolveError(t *testing.T) {
	expected := errors.New("resolve failed")
	ctx := &resolveErrorCompilerContext{MockCompilerContext: NewMockCompilerContext(false), err: expected}
	ddl := &planpb.DataDefinition{Definition: &planpb.DataDefinition_DropIndex{
		DropIndex: &planpb.DropIndex{Database: "db", Table: "tbl"},
	}}

	_, err := getPrepareDdlSchemas(ctx, ddl)
	require.ErrorIs(t, err, expected)
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
