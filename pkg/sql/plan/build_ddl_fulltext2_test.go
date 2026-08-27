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
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type fulltext2SessionVarCompilerContext struct {
	*MockCompilerContext
}

func (c *fulltext2SessionVarCompilerContext) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if name == "lower_case_table_names" {
		return int64(1), nil
	}
	return c.MockCompilerContext.ResolveVariable(name, isSystemVar, isGlobalVar)
}

func TestBuildFullTextIndexTableCapturesSessionVars(t *testing.T) {
	ctx := &fulltext2SessionVarCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}
	createTable := &pbplan.CreateTable{TableDef: &pbplan.TableDef{}}
	indexInfo := &tree.FullTextIndex{
		Name: "ft",
		IsV2: true,
		KeyParts: []*tree.KeyPart{{
			ColName: tree.NewUnresolvedColName("body"),
		}},
	}
	colMap := map[string]*ColDef{
		"id":   {Name: "id", Typ: pbplan.Type{Id: int32(types.T_int64)}},
		"body": {Name: "body", Typ: pbplan.Type{Id: int32(types.T_text)}},
	}

	err := buildFullTextIndexTable(createTable, []*tree.FullTextIndex{indexInfo}, colMap, nil, "id", ctx)
	require.NoError(t, err)
	require.Len(t, createTable.TableDef.Indexes, 2)
	for _, indexDef := range createTable.TableDef.Indexes {
		sessionVars, err := catalog.IndexParamsSessionVars(indexDef.IndexAlgoParams)
		require.NoError(t, err)
		require.Contains(t, string(sessionVars), "lower_case_table_names")
	}
}
