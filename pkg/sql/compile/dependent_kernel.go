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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unionall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (c *Compile) compileDependentKernel(node *plan.Node, children []*plan.Node) (dependent.KernelFactory, [][]*plan.Expr, error) {
	n := plan2.DeepCopyNode(node)
	inputs := make([][]*plan.Expr, len(children))
	if n.NodeType == plan.Node_FUZZY_FILTER {
		if len(children) != 2 {
			return nil, nil, moerr.NewInternalError(c.proc.Ctx, "dependent fuzzy filter input count")
		}
		check, err := newFuzzyCheck(n)
		if err != nil {
			return nil, nil, err
		}
		c.fuzzys = append(c.fuzzys, check)
		// All sources share the coordinator lane; no asynchronous runtime-filter
		// handshake is needed to make the exact duplicate check progress.
		n.RuntimeFilterBuildList = nil
		left, right := plan2.DeepCopyNode(children[0]), plan2.DeepCopyNode(children[1])
		if left.Stats == nil {
			left.Stats = &plan.Stats{}
		}
		if right.Stats == nil {
			right.Stats = &plan.Stats{}
		}
		return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := constructFuzzyFilter(n, left, right)
			op.Callback = func(b *batch.Batch) error {
				if b == nil || b.IsEmpty() {
					return nil
				}
				return check.fill(proc.Ctx, b)
			}
			op.SetAnalyzeControl(int(n.NodeId), false)
			for _, port := range ports {
				port.SetIdx(int(n.NodeId))
				op.AppendChild(port)
			}
			return op, nil
		}, inputs, nil
	}
	if n.NodeType == plan.Node_FUNCTION_SCAN {
		return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := constructTableFunction(n, nil)
			op.SetAnalyzeControl(int(n.NodeId), false)
			for _, port := range ports {
				port.SetIdx(int(n.NodeId))
				op.AppendChild(port)
			}
			return op, nil
		}, inputs, nil
	}
	if n.NodeType == plan.Node_APPLY {
		if len(children) != 2 {
			return nil, nil, moerr.NewInternalError(c.proc.Ctx, "dependent APPLY input count")
		}
		return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			kind := apply.CROSS
			if n.ApplyType == plan.Node_OUTERAPPLY {
				kind = apply.OUTER
			}
			op := constructApply(n, children[1], kind, proc)
			op.TxnOffset = c.TxnOffset
			op.SetAnalyzeControl(int(n.NodeId), false)
			op.AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}, inputs, nil
	}
	if n.NodeType == plan.Node_JOIN {
		factory, err := c.compileDependentJoin(n, children)
		return factory, inputs, err
	}
	if n.NodeType == plan.Node_UNION_ALL || n.NodeType == plan.Node_UNION {
		if len(children) != 2 {
			return nil, nil, moerr.NewInternalErrorNoCtx("dependent set input count")
		}
		factory := func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			concat := unionall.NewArgument()
			concat.DirectInputs = true
			concat.SetAnalyzeControl(int(n.NodeId), false)
			for _, port := range ports {
				concat.AppendChild(port)
				port.SetIdx(int(n.NodeId))
			}
			if n.NodeType == plan.Node_UNION_ALL {
				return concat, nil
			}
			distinct := group.NewArgument()
			distinct.NeedEval = true
			distinct.SpillMem = n.SpillMem
			distinct.GroupBy = make([]*plan.Expr, len(n.ProjectList))
			for i, e := range n.ProjectList {
				distinct.GroupBy[i] = plan2.GetColExpr(e.Typ, 0, int32(i))
			}
			if len(n.PhysicalEqualityKeyList) > 0 {
				visibleCount := len(distinct.GroupBy)
				for i, key := range n.PhysicalEqualityKeyList {
					distinct.GroupBy = append(distinct.GroupBy, key)
					distinct.GroupByHashKey = append(distinct.GroupByHashKey, int32(visibleCount+i))
				}
				distinct.ProjectList = make([]*plan.Expr, visibleCount)
				for i, e := range n.ProjectList {
					distinct.ProjectList[i] = plan2.GetColExpr(e.Typ, 0, int32(i))
				}
			}
			distinct.SetAnalyzeControl(int(n.NodeId), false)
			distinct.AppendChild(concat)
			return distinct, nil
		}
		return factory, inputs, nil
	}
	switch n.NodeType {
	case plan.Node_MINUS, plan.Node_MINUS_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		if len(children) != 2 {
			return nil, nil, moerr.NewInternalErrorNoCtx("dependent set input count")
		}
		return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			var op vm.Operator
			switch n.NodeType {
			case plan.Node_MINUS:
				arg := minus.NewArgument()
				arg.KeyExprs = n.PhysicalEqualityKeyList
				op = arg
			case plan.Node_MINUS_ALL:
				arg := minusall.NewArgument()
				arg.KeyExprs = n.PhysicalEqualityKeyList
				op = arg
			case plan.Node_INTERSECT:
				arg := intersect.NewArgument()
				arg.KeyExprs = n.PhysicalEqualityKeyList
				op = arg
			case plan.Node_INTERSECT_ALL:
				arg := intersectall.NewArgument()
				arg.KeyExprs = n.PhysicalEqualityKeyList
				op = arg
			}
			op.GetOperatorBase().SetAnalyzeControl(int(n.NodeId), false)
			for _, port := range ports {
				port.SetIdx(int(n.NodeId))
				op.GetOperatorBase().AppendChild(port)
			}
			return op, nil
		}, inputs, nil
	}
	if len(children) != 1 {
		return nil, nil, moerr.NewNYIf(c.proc.Ctx, "dependent native kernel %s", node.NodeType)
	}
	// Preserve the complete physical input image and append only expressions
	// which can suspend. The existing native kernel owns all other evaluation.
	input := make([]*plan.Expr, len(children[0].ProjectList))
	for i, e := range children[0].ProjectList {
		input[i] = plan2.GetColExpr(e.Typ, 0, int32(i))
	}
	lower := func(e *plan.Expr) *plan.Expr {
		needed := false
		_ = plan.VisitExprTree(e, func(e *plan.Expr) error { needed = needed || e.GetDependent() != nil; return nil })
		if !needed {
			return e
		}
		index := int32(len(input))
		input = append(input, e)
		return plan2.GetColExpr(e.Typ, 0, index)
	}
	var factory dependent.KernelFactory
	switch n.NodeType {
	case plan.Node_TIME_WINDOW:
		for i, e := range n.GroupBy {
			n.GroupBy[i] = lower(e)
		}
		for i, e := range n.TimeWindowPartitionBy {
			n.TimeWindowPartitionBy[i] = lower(e)
		}
		for _, e := range n.AggList {
			if f := e.GetF(); f != nil {
				for i, arg := range f.Args {
					f.Args[i] = lower(arg)
				}
			}
		}
		for _, spec := range n.OrderBy {
			spec.Expr = lower(spec.Expr)
		}
		if err := validateAggregateConfigs(n, c.proc); err != nil {
			return nil, nil, err
		}
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			local := order.NewArgument()
			local.OrderBySpec = n.OrderBy
			merge := mergeorder.NewArgument()
			merge.OrderBySpecs = n.OrderBy
			op := constructTimeWindow(proc.Ctx, n, proc)
			local.SetAnalyzeControl(int(n.NodeId), false)
			merge.SetAnalyzeControl(int(n.NodeId), false)
			op.SetAnalyzeControl(int(n.NodeId), false)
			local.AppendChild(ports[0])
			merge.AppendChild(local)
			op.AppendChild(merge)
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	case plan.Node_FILL:
		var setup *colexec.DependentExpressionProgram
		if n.FillType == plan.Node_VALUE {
			needsSetup := false
			for _, expr := range n.FillVal {
				_ = plan.VisitExprTree(expr, func(e *plan.Expr) error { needsSetup = needsSetup || e.GetDependent() != nil; return nil })
			}
			if needsSetup {
				var err error
				setup, err = colexec.CompileDependentExpressions(c.proc.Ctx, n.FillVal)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := constructFill(n)
			// LINEAR normalizes its helper columns during Prepare.
			op.FillVal = plan2.DeepCopyExprList(n.FillVal)
			op.SetAnalyzeControl(int(n.NodeId), false)
			op.AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			if setup != nil {
				owner := &dependent.SetupKernel{Program: setup, Count: len(n.FillVal), Native: op,
					Install: func(eval func(*process.Process) ([]*vector.Vector, error)) { op.ValueSetup = eval }}
				owner.AppendChild(op)
				owner.SetAnalyzeControl(int(n.NodeId), false)
				return owner, nil
			}
			return op, nil
		}
	case plan.Node_SAMPLE:
		for i, e := range n.GroupBy {
			n.GroupBy[i] = lower(e)
		}
		for i, e := range n.AggList {
			n.AggList[i] = lower(e)
		}
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := constructSample(n, false)
			op.SetAnalyzeControl(int(n.NodeId), false)
			op.AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	case plan.Node_LOCK_OP, plan.Node_PRE_INSERT, plan.Node_PRE_INSERT_UK, plan.Node_PRE_INSERT_SK, plan.Node_INSERT, plan.Node_MULTI_UPDATE, plan.Node_DELETE, plan.Node_POSTDML:
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			var op vm.Operator
			var err error
			switch n.NodeType {
			case plan.Node_LOCK_OP:
				op, err = constructLockOp(n, c.e)
			case plan.Node_PRE_INSERT:
				op, err = constructPreInsert(c.anal.qry.Nodes, n, c.e, proc)
			case plan.Node_PRE_INSERT_UK:
				op = constructPreInsertUk(n)
			case plan.Node_PRE_INSERT_SK:
				op = constructPreInsertSk(n)
			case plan.Node_INSERT:
				if iceberg, checkErr := isIcebergAppendInsert(proc.Ctx, n); checkErr != nil {
					return nil, checkErr
				} else if iceberg {
					// Use the execution process without mutating/copying the shared
					// compiler. Overwrite and row-level DML need the same security,
					// scan metadata and object-IO ownership as ordinary Iceberg DML.
					writerCompile := &Compile{
						proc: proc, db: c.db,
						icebergScanPlanner: c.icebergScanPlanner,
						icebergScanPlans:   c.icebergScanPlans,
					}
					var nodes []*plan.Node
					if c.anal != nil && c.anal.qry != nil {
						nodes = c.anal.qry.Nodes
					}
					op, err = writerCompile.constructIcebergInsert(nodes, n)
				} else if isExternalWriteInsert(n) {
					op, err = constructExternalInsert(proc, n, c.e, externalInsertStmtTime(proc, c.startAt))
				} else {
					op, err = constructInsert(proc, n, c.e, false)
				}
			case plan.Node_MULTI_UPDATE:
				op, err = constructMultiUpdate(n, c.e, proc, multi_update.UpdateWriteTable, false)
			case plan.Node_DELETE:
				op, err = constructDeletion(proc, n, c.e)
			case plan.Node_POSTDML:
				op = constructPostDml(n, c.e)
			}
			if err != nil {
				return op, err
			}
			op.GetOperatorBase().SetAnalyzeControl(int(n.NodeId), false)
			op.GetOperatorBase().AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	case plan.Node_AGG:
		for i, e := range n.GroupBy {
			n.GroupBy[i] = lower(e)
		}
		for _, e := range n.AggList {
			f := e.GetF()
			if f == nil {
				return nil, nil, moerr.NewInternalErrorNoCtx("dependent aggregate binding")
			}
			for i, arg := range f.Args {
				f.Args[i] = lower(arg)
			}
		}
		if err := validateAggregateConfigs(n, c.proc); err != nil {
			return nil, nil, err
		}
		bound := constructGroup(c.proc.Ctx, n, children[0], true, 0, c.proc)
		aggs, keys, hashKeys, flags, dynamic := bound.Aggs, bound.GroupBy, bound.GroupByHashKey, bound.GroupingFlag, bound.DynamicGrouping
		bound.Release()
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := group.NewArgument()
			op.Aggs, op.GroupBy, op.GroupByHashKey, op.GroupingFlag, op.DynamicGrouping = aggs, keys, hashKeys, flags, dynamic
			op.NeedEval, op.SpillMem = true, n.SpillMem
			op.SetAnalyzeControl(int(n.NodeId), false)
			op.AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	case plan.Node_SORT:
		for _, order := range n.OrderBy {
			order.Expr = lower(order.Expr)
		}
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			local := order.NewArgument()
			local.OrderBySpec = n.OrderBy
			merge := mergeorder.NewArgument()
			merge.OrderBySpecs = n.OrderBy
			local.SetAnalyzeControl(int(n.NodeId), false)
			merge.SetAnalyzeControl(int(n.NodeId), false)
			local.AppendChild(ports[0])
			merge.AppendChild(local)
			ports[0].SetIdx(int(n.NodeId))
			return merge, nil
		}
	case plan.Node_PARTITION:
		for _, spec := range n.OrderBy {
			spec.Expr = lower(spec.Expr)
		}
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			local := order.NewArgument()
			local.OrderBySpec = n.OrderBy
			op := partition.NewArgument()
			op.OrderBySpecs, op.Limit, op.PartitionByCount, op.WithTies, op.Algorithm, op.SpillMem = n.OrderBy, n.Limit, n.PartitionByCount, n.PartitionTopNWithTies, n.PartitionAlgorithm, n.SpillMem
			local.SetAnalyzeControl(int(n.NodeId), false)
			op.SetAnalyzeControl(int(n.NodeId), false)
			local.AppendChild(ports[0])
			op.AppendChild(local)
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	case plan.Node_WINDOW:
		for _, e := range n.WinSpecList {
			w := e.GetW()
			f := w.WindowFunc.GetF()
			for i, arg := range f.Args {
				f.Args[i] = lower(arg)
			}
			for _, spec := range w.OrderBy {
				spec.Expr = lower(spec.Expr)
			}
		}
		bound := constructWindow(c.proc.Ctx, n, c.proc)
		aggs, specs := bound.Aggs, bound.WinSpecList
		bound.Release()
		factory = func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
			op := window.NewArgument()
			op.Aggs, op.WinSpecList, op.SpillThreshold = aggs, specs, n.SpillMem
			op.SetAnalyzeControl(int(n.NodeId), false)
			op.AppendChild(ports[0])
			ports[0].SetIdx(int(n.NodeId))
			return op, nil
		}
	default:
		return nil, nil, moerr.NewNYIf(c.proc.Ctx, "dependent native kernel %s", node.NodeType)
	}
	if len(input) != len(children[0].ProjectList) {
		inputs[0] = input
	}
	return factory, inputs, nil
}

func (c *Compile) compileDependentJoin(n *plan.Node, children []*plan.Node) (dependent.KernelFactory, error) {
	if len(children) != 2 {
		return nil, moerr.NewInternalErrorNoCtx("dependent join arity")
	}
	if n.JoinType == plan.Node_DEDUP || n.JoinType == plan.Node_ASOF || n.JoinType == plan.Node_ASOF_LEFT {
		return c.compileDependentHashJoin(n, children)
	}
	switch n.JoinType {
	case plan.Node_INNER, plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE, plan.Node_MARK, plan.Node_OUTER:
	default:
		return nil, moerr.NewNYIf(c.proc.Ctx, "dependent native join %s", n.JoinType)
	}
	needsPairEvaluation := false
	for _, expr := range n.OnList {
		_ = plan.VisitExprTree(expr, func(expr *plan.Expr) error {
			needsPairEvaluation = needsPairEvaluation || expr.GetDependent() != nil
			return nil
		})
		needsPairEvaluation = needsPairEvaluation ||
			plan2.ContainsGuardedJoinDiagnosticWithProof(c.proc, expr, c.preparedJoinDiagnosticFree)
	}
	if !needsPairEvaluation {
		if n.JoinType == plan.Node_MARK {
			if canUseHashMarkJoinWithInputs(n, children[0], children[1]) {
				return c.compileDependentHashJoin(n, children)
			}
		} else if c.broadcastJoinUsesHash(n) {
			return c.compileDependentHashJoin(n, children)
		}
	}
	cond := colexec.RewriteFilterExprList(n.OnList)
	var program *colexec.DependentExpressionProgram
	var err error
	if cond != nil {
		program, err = colexec.CompileDependentExpressions(c.proc.Ctx, []*plan.Expr{cond})
		if err != nil {
			return nil, err
		}
	}
	cols := make([]colexec.ResultPos, len(n.ProjectList))
	for i, e := range n.ProjectList {
		ref := e.GetCol()
		if ref == nil {
			return nil, moerr.NewInternalErrorNoCtx("dependent join projection binding")
		}
		cols[i] = colexec.ResultPos{Rel: ref.RelPos, Pos: ref.ColPos}
	}
	inputTypes := make([][]types.Type, 2)
	for side, child := range children {
		for _, e := range child.ProjectList {
			typ, err := types.TypeFromPlan(e.Typ)
			if err != nil {
				return nil, err
			}
			inputTypes[side] = append(inputTypes[side], typ)
		}
	}
	return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
		join := loopjoin.NewArgument()
		join.JoinType = n.JoinType
		join.LeftTypes, join.RightTypes, join.ResultCols = inputTypes[0], inputTypes[1], cols
		join.JoinMapTag = n.NodeId + 1
		build := hashbuild.NewArgument()
		build.NeedBatches, build.DirectInput = true, true
		build.JoinMapTag, build.JoinMapRefCnt = join.JoinMapTag, 1
		root := &dependent.JoinKernel{Build: build, Join: join, Condition: program}
		root.SetAnalyzeControl(int(n.NodeId), false)
		join.SetAnalyzeControl(int(n.NodeId), false)
		build.SetAnalyzeControl(int(n.NodeId), false)
		for _, port := range ports {
			port.SetIdx(int(n.NodeId))
		}
		join.AppendChild(ports[0])
		build.AppendChild(ports[1])
		root.AppendChild(join)
		root.AppendChild(build)
		return root, nil
	}, nil
}

// Hash and dedup joins retain their existing key, nearest-row and conflict
// owners. Only their build/probe scheduling changes to the continuation lane.
func (c *Compile) compileDependentHashJoin(n *plan.Node, children []*plan.Node) (dependent.KernelFactory, error) {
	var updates []*colexec.DependentExpressionProgram
	var updateCols []int32
	if ctx := n.DedupJoinCtx; ctx != nil && ctx.EmitActionRows {
		for _, expr := range ctx.UpdateColExprList {
			program, err := colexec.CompileDependentExpressions(c.proc.Ctx, []*plan.Expr{expr})
			if err != nil {
				return nil, err
			}
			updates = append(updates, program)
		}
		updateCols = ctx.UpdateColIdxList
	}
	n.SendMsgList = []plan.MsgHeader{{MsgTag: n.NodeId + 1, MsgType: int32(message.MsgJoinMap)}}
	n.RuntimeFilterBuildList = nil
	if n.Stats == nil {
		n.Stats = &plan.Stats{}
	}
	inputTypes := make([][]types.Type, 2)
	for side, child := range children {
		for _, e := range child.ProjectList {
			typ, err := types.TypeFromPlan(e.Typ)
			if err != nil {
				return nil, err
			}
			inputTypes[side] = append(inputTypes[side], typ)
		}
	}
	return func(proc *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
		var join vm.Operator
		if n.JoinType == plan.Node_DEDUP {
			join = constructDedupJoin(n, inputTypes[0], inputTypes[1], proc)
		} else {
			var hashJoin *hashjoin.HashJoin
			if n.JoinType == plan.Node_MARK {
				hashJoin = constructBroadcastHashMarkJoin(n, children[0], inputTypes[0], inputTypes[1], proc)
			} else {
				hashJoin = constructHashJoin(n, children[0], inputTypes[0], inputTypes[1], proc)
			}
			// The continuation lane has one broadcast build, even if the ordinary
			// planner's placement statistics selected distributed shuffle.
			hashJoin.IsShuffle = false
			join = hashJoin
		}
		build := constructBroadcastHashBuild(join, proc, 1)
		// The direct-input lane owns materialized build batches, including when
		// the ordinary pipeline could keep only primary-key membership.
		build.NeedBatches, build.DirectInput = true, true
		root := &dependent.JoinKernel{Build: build, Join: join, Updates: updates, UpdateCols: updateCols}
		root.SetAnalyzeControl(int(n.NodeId), false)
		join.GetOperatorBase().SetAnalyzeControl(int(n.NodeId), false)
		build.SetAnalyzeControl(int(n.NodeId), false)
		for _, port := range ports {
			port.SetIdx(int(n.NodeId))
		}
		join.GetOperatorBase().AppendChild(ports[0])
		build.AppendChild(ports[1])
		root.AppendChild(join)
		root.AppendChild(build)
		return root, nil
	}, nil
}
