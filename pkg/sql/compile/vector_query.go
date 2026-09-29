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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorquery"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// Negative keys cannot collide with the global materialized CTE step namespace.
func vectorQuerySourceKey(node *plan.Node) int32 { return -node.VectorQuerySourceId - 1 }

func vectorQueryExecType(execType plan2.ExecType, qry *plan.Query) plan2.ExecType {
	if qry != nil {
		for _, node := range qry.Nodes {
			if node != nil && node.NodeType == plan.Node_VECTOR_QUERY_TOP {
				return plan2.ExecTypeAP_ONECN
			}
		}
	}
	return execType
}

func (c *Compile) compileVectorQueryTop(step int32, node *plan.Node, nodes []*plan.Node, curNodeIdx int32) ([]*Scope, error) {
	if len(node.Children) != 3 || node.Limit == nil || node.VectorQuerySourceId < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid vector query plan")
	}
	key := vectorQuerySourceKey(node)
	if c.materializedSources == nil {
		c.materializedSources = make(map[int32]*materialized.Source)
	}
	if c.materializedSinkScanNodes == nil {
		c.materializedSinkScanNodes = make(map[int32][]int32)
	}
	if c.materializedReaderIDs == nil {
		c.materializedReaderIDs = make(map[[2]int32]int)
	}
	if c.materializedSources[key] != nil {
		return nil, moerr.NewInternalErrorNoCtx("duplicate vector query source owner")
	}
	source := materialized.NewSource(2)
	c.materializedSources[key] = source
	branches := make([]*Scope, 0, 3)
	for _, id := range node.Children {
		ss, err := c.compilePlanScope(step, id, nodes)
		if err != nil {
			ReleaseScopes(branches)
			return nil, err
		}
		branches = append(branches, c.newMergeScope(ss))
	}
	if len(c.materializedSinkScanNodes[key]) != 2 {
		ReleaseScopes(branches)
		return nil, moerr.NewInternalErrorNoCtx("vector query requires exactly two readers")
	}
	c.setAnalyzeCurrent(nil, int(curNodeIdx))
	rs := c.newMergeScope(branches)
	rs.LazyPreScopes = true
	rs.RootOp.(*merge.Merge).WithPartial(0, 0)
	op := vectorquery.NewArgument()
	op.Source = source
	op.LimitExpr = plan2.DeepCopyExpr(node.Limit)
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rs.setRootOperator(op)
	c.anal.isFirst = false
	return []*Scope{rs}, nil
}

func (c *Compile) compileVectorQuerySource(node *plan.Node) ([]*Scope, error) {
	key := vectorQuerySourceKey(node)
	source := c.materializedSources[key]
	reader := len(c.materializedSinkScanNodes[key])
	if source == nil || reader >= 2 || len(node.Children) != 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid vector query source consumer")
	}
	c.materializedSinkScanNodes[key] = append(c.materializedSinkScanNodes[key], node.NodeId)
	c.materializedReaderIDs[[2]int32{key, node.NodeId}] = reader
	rs := c.newEmptyMergeScope()
	rs.Proc = c.proc.NewNoContextChildProc(0)
	op := merge.NewArgument().WithSinkScan(true)
	op.MaterializedSource = source
	op.MaterializedReaderID = reader
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rs.setRootOperator(op)
	c.hasMergeOp = true
	c.anal.isFirst = false
	return []*Scope{rs}, nil
}
