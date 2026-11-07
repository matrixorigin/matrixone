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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCapturedExecutionRequirementIsIndependentOfHint(t *testing.T) {
	node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, IndexScanInfo: pb.IndexScanInfo{IsIndexScan: true}, BlockFilterList: []*pb.Expr{{CandidateFilter: true}}}
	p := &pb.Plan{Plan: &pb.Plan_Query{Query: &pb.Query{Nodes: []*pb.Node{node}}}}
	require.False(t, HasCapturedExecutionAccess(p))
	node.BlockFilterList = nil
	node.IndexScanInfo.RequiresCapturedExecution = true
	require.True(t, HasCapturedExecutionAccess(p), "removing a hint does not revoke the scan requirement")
	copy := DeepCopyNode(node)
	require.True(t, copy.IndexScanInfo.RequiresCapturedExecution)
	data, err := proto.Marshal(copy)
	require.NoError(t, err)
	var decoded pb.Node
	require.NoError(t, proto.Unmarshal(data, &decoded))
	require.True(t, decoded.IndexScanInfo.RequiresCapturedExecution)
	node.NodeType = pb.Node_PROJECT
	require.True(t, HasCapturedExecutionAccess(p), "preflight must observe malformed required nodes")
	require.False(t, CapturedExecutionIndexScan(node))
}

func TestCandidateProofSurvivesFilterRewrites(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	ctx := NewMockCompilerContext(false, proc)
	column := makeExprOptInt64Col(1, 0, "a")
	plain := makeExprOptBinaryInt64Expr(t, ctx, "=", column, 1)
	marked := DeepCopyExpr(plain)
	marked.CandidateFilter = true
	for _, inputs := range [][]*pb.Expr{{plain, marked}, {marked, plain}} {
		filters := deduplicateBlockFilterList(append([]*pb.Expr(nil), inputs...))
		require.Len(t, filters, 2, "an ordinary duplicate must not erase an independent proof")
		node := &pb.Node{TableDef: &pb.TableDef{}, BindingTags: []int32{1}}
		for _, filter := range inputs {
			appendUniqueBlockFilter(node, filter, true)
		}
		require.Len(t, node.BlockFilterList, 2)
		kept := existingCompositeBlockFilters(node)
		require.Len(t, kept, 1)
		require.True(t, kept[0].CandidateFilter, "a static proved hint survives statistics recomputation")
	}

	constant, err := BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*pb.Expr{MakePlan2Int64ConstExprWithType(1), MakePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	constant.CandidateFilter = true
	node := &pb.Node{BlockFilterList: []*pb.Expr{constant}}
	rule.NewConstantFold(false).Apply(node, nil, proc)
	require.True(t, node.BlockFilterList[0].CandidateFilter)
}

func TestOriginalDependencyFallbackKeepsUnrelatedPlansReusable(t *testing.T) {
	builder, _, index := newIndexCostFixture(t)
	scan := builder.qry.Nodes[0]
	scan.TableDef.Indexes = []*pb.IndexDef{index}
	intColumn := GetColExpr(scan.TableDef.Cols[1].Typ, scan.BindingTags[0], 1)
	parameter := &pb.Expr{Typ: pb.Type{Id: int32(types.T_any)}, Expr: &pb.Expr_P{P: &pb.ParamRef{}}}
	predicate := DeepCopyExpr(scan.FilterList[0])
	predicate.GetF().Args = []*pb.Expr{intColumn, parameter}
	scan.FilterList = []*pb.Expr{predicate}
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.False(t, builder.qry.ExecutionDependentAccess, "an unrelated parameter must not make an integer predicate temporal")

	predicate.GetF().Args[0] = GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)
	builder.collectOriginalIndexOwner(0)
	require.True(t, builder.qry.ExecutionDependentAccess, "an unresolved temporal bound cannot prove reusable access")

	// An opaque subtree is unknown, but an index on only an integer column
	// proves that no temporal-index access can be selected.
	scan.TableDef.Indexes = []*pb.IndexDef{{IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true, Parts: []string{"id"}}}
	scan.FilterList = []*pb.Expr{{Expr: &pb.Expr_W{W: &pb.WindowSpec{}}}}
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.False(t, builder.qry.ExecutionDependentAccess)

	// An oversized or cyclic original expression must terminate and remain
	// conservative when an indexed temporal column may be relevant.
	scan.TableDef.Indexes = []*pb.IndexDef{index}
	for _, cyclic := range []bool{false, true} {
		root := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.OR, 0)}}}}
		if cyclic {
			root.GetF().Args = []*pb.Expr{root}
		} else {
			root.GetF().Args = make([]*pb.Expr, 4097)
		}
		scan.FilterList = []*pb.Expr{root}
		builder.qry.ExecutionDependentAccess = false
		builder.collectOriginalIndexOwner(0)
		require.True(t, builder.qry.ExecutionDependentAccess)
	}
}
