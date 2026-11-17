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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestEnumIndexBackfillPlanRequiresAuthoritativeSource(t *testing.T) {
	spec := &executor.EnumIndexBackfillSpec{SourceID: 10}
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{Obj: 10}, TableDef: &plan.TableDef{TblId: 10}}
	query := &plan.Query{Steps: []int32{1}, Nodes: []*plan.Node{scan, {NodeType: plan.Node_PROJECT, Children: []int32{0}}}}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
	require.NoError(t, validateEnumIndexBackfillPlan(p, spec))
	scan.IndexScanInfo.IsIndexScan = true
	require.Error(t, validateEnumIndexBackfillPlan(p, spec))
	scan.IndexScanInfo.IsIndexScan = false
	scan.ObjRef.Obj = 11
	require.Error(t, validateEnumIndexBackfillPlan(p, spec))
	scan.ObjRef.Obj = 10
	query.Nodes = append(query.Nodes, &plan.Node{NodeType: plan.Node_EXTERNAL_SCAN})
	// A discarded optimizer node is not executable; an additional root is.
	require.NoError(t, validateEnumIndexBackfillPlan(p, spec))
	query.Steps = append(query.Steps, 2)
	require.Error(t, validateEnumIndexBackfillPlan(p, spec))
	query.Steps = query.Steps[:1]
	query.BackgroundQueries = []*plan.Query{{}}
	require.Error(t, validateEnumIndexBackfillPlan(p, spec))
	require.NoError(t, validateEnumIndexBackfillPlan(p, nil))
}
