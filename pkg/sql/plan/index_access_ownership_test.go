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
	"context"
	"strings"
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

	// The ordinary predicate remains reusable even when its original tree is
	// deeper than the positive index proof can accept. A relevant predicate at
	// the same depth must still protect the lifetime of a cached BASE plan.
	deep := predicate
	for i := 0; i < 128; i++ {
		deep = &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.AND, 0)},
			Args: []*pb.Expr{deep, MakePlan2BoolConstExprWithType(true)},
		}}}
	}
	scan.FilterList = []*pb.Expr{deep}
	builder.collectOriginalIndexOwner(0)
	require.False(t, builder.qry.ExecutionDependentAccess, "a completely inspected deep integer predicate remains reusable")
	predicate.GetF().Args[0] = GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)
	builder.collectOriginalIndexOwner(0)
	require.True(t, builder.qry.ExecutionDependentAccess, "a deep temporal parameter still requires replanning")

	scan.FilterList = []*pb.Expr{predicate}
	builder.qry.ExecutionDependentAccess = false
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
	// Many individually bounded expressions can still exceed the total work
	// budget through nil children, which the shared visitor skips on delivery.
	wide := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{
		Func: &pb.ObjectRef{Obj: function.EncodeOverloadID(function.OR, 0)},
		Args: make([]*pb.Expr, 128),
	}}}
	scan.FilterList = make([]*pb.Expr, 80)
	for i := range scan.FilterList {
		scan.FilterList[i] = wide
	}
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.True(t, builder.qry.ExecutionDependentAccess, "unvisited child references cannot prove a reusable lifetime")
	// Nil filter roots still consume the shared work budget; they cannot make
	// an arbitrarily long malformed filter list look like a complete proof.
	scan.FilterList = make([]*pb.Expr, 4097)
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.True(t, builder.qry.ExecutionDependentAccess)
}

func TestOriginalDependencyRejectsUnprovedIndexMetadata(t *testing.T) {
	builder, _, temporal := newIndexCostFixture(t)
	scan := builder.qry.Nodes[0]
	regular := catalog.MoIndexDefaultAlgo.ToString()
	empty := &pb.IndexDef{IndexAlgo: regular, TableExist: true}
	scan.TableDef.Indexes = []*pb.IndexDef{empty}
	check := originalDependencyCheck{scan: scan, budget: 4096}
	membership, _ := check.indexedColumn(-1, nil, nil)
	require.Equal(t, dependencyUnknown, membership)
	scan.FilterList = []*pb.Expr{{Expr: &pb.Expr_W{W: &pb.WindowSpec{}}}}
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.True(t, builder.qry.ExecutionDependentAccess, "malformed metadata cannot prove a reusable lifetime")

	scan.TableDef.Indexes = []*pb.IndexDef{empty, temporal}
	check = originalDependencyCheck{scan: scan, budget: 4096}
	membership, _ = check.indexedColumn(0, nil, nil)
	require.Equal(t, dependencyYes, membership, "a malformed earlier index must not hide a later witness")

	scan.TableDef.Indexes = []*pb.IndexDef{{IndexAlgo: strings.Repeat(" ", 257), TableExist: true, Parts: []string{"id"}}}
	check = originalDependencyCheck{scan: scan, budget: 4096}
	exclusionBudget := 4096
	membership, excludes := check.indexedColumn(0, context.Background(), &exclusionBudget)
	require.Equal(t, dependencyUnknown, membership)
	require.False(t, excludes, "an unbounded algorithm name cannot prove exclusion")

	scan.TableDef.Indexes = []*pb.IndexDef{{IndexAlgo: regular, TableExist: true, Parts: []string{"id"}}}
	builder.qry.ExecutionDependentAccess = false
	builder.collectOriginalIndexOwner(0)
	require.False(t, builder.qry.ExecutionDependentAccess, "complete integer-only evidence remains reusable")
}
