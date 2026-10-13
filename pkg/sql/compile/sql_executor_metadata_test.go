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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMarkMoColumnsUpdatePlan(t *testing.T) {
	columns := &planpb.TableDef{TblId: catalog.MO_COLUMNS_ID, Name: catalog.MO_COLUMNS}
	other := &planpb.TableDef{TblId: catalog.MO_TABLES_ID, Name: catalog.MO_TABLES}
	pn := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{{
		UpdateCtxList: []*planpb.UpdateCtx{{TableDef: columns}, {TableDef: other}},
	}}}}}

	markMoColumnsUpdatePlan(pn)
	require.Equal(t, catalog.MO_COLUMNS_UPDATE, columns.Name)
	require.Equal(t, catalog.MO_TABLES, other.Name)
}
