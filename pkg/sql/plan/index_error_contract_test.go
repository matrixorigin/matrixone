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
	"errors"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// controlledIndexResolver acts only after the hidden relation has really been
// resolved. It makes cancellation and resolution failures deterministic.
type controlledIndexResolver struct {
	CompilerContext
	target   string
	after    func()
	mutate   func(*TableDef)
	failure  error
	omit     bool
	resolved int
}

func TestRegularIndexCancellationDoesNotPublishChild(t *testing.T) {
	base := newIndexMergePlanContext(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	base.SetContext(ctx)
	stop := errors.New("stop after child index resolution")
	compiler := &controlledIndexResolver{
		CompilerContext: base,
		target:          "idx_hint_b",
		after:           func() { cancel(stop) },
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
	table := *base.tables["index_hint_t"]
	table.Indexes = []*planpb.IndexDef{table.Indexes[1]}
	tag := builder.genNewBindTag()
	typ := table.Cols[2].Typ
	bound := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 10}}}}
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{GetColExpr(typ, tag, 2), bound})
	require.NoError(t, err)
	filter.Selectivity = 0.001
	scan := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN, TableDef: &table, ObjRef: base.objects[table.Name],
		BindingTags: []int32{tag}, FilterList: []*planpb.Expr{filter},
		Stats: &planpb.Stats{TableCnt: 200000, Outcnt: 200, Selectivity: 0.001, Cost: 200000},
	}
	bind := NewBindContext(builder, nil)
	scanID := builder.appendNode(scan, bind)
	parent := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{scanID}}
	parentID := builder.appendNode(parent, bind)
	builder.indexHintsByScan = map[int32]*indexHintSet{scanID: {scan: indexHintScopeSet{
		forceSpecified: true, force: map[string]struct{}{"idx_b": {}},
	}}}
	original := proto.Clone(scan)
	names := len(builder.nameByColRef)
	mapping := map[[2]int32]*planpb.Expr{}
	_, err = builder.applyIndices(parentID, map[[2]int32]int{{tag, 0}: 1}, mapping)
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, compiler.resolved)
	require.Equal(t, []int32{scanID}, parent.Children)
	require.Len(t, builder.qry.Nodes, 2)
	require.Len(t, builder.nameByColRef, names)
	require.Empty(t, mapping)
	require.Empty(t, cmp.Diff(original, scan))
}

func TestRegularIndexMergeDeclineReturnsCancellation(t *testing.T) {
	compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx, cancel := context.WithCancelCause(context.Background())
	stop := errors.New("stop before merge enumeration")
	cancel(stop)
	compiler.SetContext(ctx)
	builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
	node := &planpb.Node{NodeId: 7}
	id, err := builder.applyRegularIndexMerge(node, nil, nil, nil, &encodedRegularIndexCostContext{})
	require.Equal(t, node.NodeId, id)
	require.ErrorIs(t, err, stop)
}

func (c *controlledIndexResolver) ResolveIndexTableByRef(
	ref *ObjectRef, name string, snapshot *Snapshot, output *TableResolution,
) (*ObjectRef, *TableDef, error) {
	output.Reset()
	obj, table, err := c.CompilerContext.ResolveIndexTableByRef(ref, name, snapshot, output)
	if name == c.target && err == nil && obj != nil && table != nil {
		c.resolved++
		if c.mutate != nil {
			c.mutate(table)
			output.Reset()
		}
		if c.after != nil {
			c.after()
		}
		if c.failure != nil {
			output.Reset()
			return nil, nil, c.failure
		}
		if c.omit {
			output.Reset()
			return nil, nil, nil
		}
	}
	return obj, table, err
}

func TestRegularIndexBuildPlanErrorAfterResolution(t *testing.T) {
	for _, scenario := range []string{"canceled", "deadline", "decline only", "decline and cancel", "typed failure", "plain failure", "failure and cancel", "sentinel and cancel", "joined failure and cancel", "missing metadata", "missing key column", "missing primary column"} {
		t.Run(scenario, func(t *testing.T) {
			base := newIndexMergePlanContext(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(nil) })
			base.SetContext(ctx)
			cause := errors.New("request stopped after index resolution")
			typed := moerr.NewNoSuchTable(context.Background(), "test", "idx_hint_b")
			plain := errors.New("index metadata service failed")
			compiler := &controlledIndexResolver{CompilerContext: base, target: "idx_hint_b"}
			switch scenario {
			case "canceled":
				compiler.after = func() { cancel(cause) }
			case "deadline":
				expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				t.Cleanup(stop)
				compiler.after = func() { base.SetContext(expired) }
			case "decline only":
				compiler.mutate = func(table *TableDef) { table.Cols[0].Typ = planpb.Type{Id: int32(types.T_json)} }
			case "decline and cancel":
				compiler.mutate = func(table *TableDef) { table.Cols[0].Typ = planpb.Type{Id: int32(types.T_json)} }
				compiler.after = func() { cancel(cause) }
			case "typed failure":
				compiler.failure = typed
			case "plain failure":
				compiler.failure = plain
			case "failure and cancel":
				compiler.after = func() { cancel(cause) }
				compiler.failure = typed
			case "sentinel and cancel":
				compiler.after = func() { cancel(cause) }
				compiler.failure = context.Canceled
			case "joined failure and cancel":
				compiler.after = func() { cancel(cause) }
				compiler.failure = errors.Join(typed, context.Canceled)
			case "missing metadata":
				compiler.omit = true
			case "missing key column":
				compiler.mutate = func(table *TableDef) { table.Cols[0] = nil }
			case "missing primary column":
				compiler.mutate = func(table *TableDef) { table.Cols[1] = nil }
			}

			stmts, err := mysql.Parse(context.Background(), "select id from index_hint_t force index(idx_b) where b=10", 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			result, err := BuildPlan(compiler, stmts[0], false, nil)
			require.Positive(t, compiler.resolved)
			if scenario == "decline only" {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotContains(t, reachableIndexScanNames(result.GetQuery()), "idx_b")
				return
			}
			require.Nil(t, result)
			switch scenario {
			case "canceled", "decline and cancel", "sentinel and cancel":
				require.ErrorIs(t, err, cause)
			case "deadline":
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case "typed failure", "failure and cancel":
				require.ErrorIs(t, err, typed)
				require.NotErrorIs(t, err, cause)
			case "plain failure":
				require.ErrorIs(t, err, plain)
			case "joined failure and cancel":
				require.ErrorIs(t, err, typed)
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorIs(t, err, cause)
			case "missing metadata", "missing key column", "missing primary column":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
			}
		})
	}
}
