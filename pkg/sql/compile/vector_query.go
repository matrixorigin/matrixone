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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorquery"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/panjf2000/ants/v2"
)

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
	rs.VectorQueryScope = true
	rs.RootOp.(*merge.Merge).WithPartial(0, 0)
	op := vectorquery.NewArgument()
	op.Source, op.LimitExpr = source, plan2.DeepCopyExpr(node.Limit)
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rs.setRootOperator(op)
	c.anal.isFirst = false
	// 上层 JOIN 添加的 build 依赖必须留在普通域，不能变成未启动的第四分支。
	return []*Scope{c.newMergeScope([]*Scope{rs})}, nil
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
	op.MaterializedSource, op.MaterializedReaderID = source, reader
	op.MaterializedSingleBatch = true
	op.SetAnalyzeControl(c.anal.curNodeIdx, c.anal.isFirst)
	rs.setRootOperator(op)
	c.hasMergeOp, c.anal.isFirst = true, false
	return []*Scope{rs}, nil
}

// runVectorQueryScope 只服务固定三分支的本地 selector，不改变普通 MergeRun。
// 启动声明只在父 pipeline 的单个调用链发生；每个完成槽通过 close 发布结果，
// WaitGroup 覆盖清理完成，不把 EOF 当成 producer 生命周期结束。
func (s *Scope) runVectorQueryScope(c *Compile) (err error) {
	var selector *vectorquery.VectorQuery
	_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
		if a, ok := op.(*vectorquery.VectorQuery); ok {
			selector = a
		}
		return nil
	})
	if selector == nil || len(s.PreScopes) != 3 || len(s.RemoteReceivRegInfos) != 0 {
		err = moerr.NewInternalErrorNoCtx("invalid vector query scope topology")
		cleanUnstartedVectorScope(s, c, err)
		return err
	}
	var wg sync.WaitGroup
	var claimed [3]bool
	var results [3]scopeRunResult
	done := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	finish := func(i int, branchErr error) {
		s.cancelMergeSiblingsOnError(branchErr)
		results[i] = newScopeRunResult(branchErr, s.PreScopes[i])
		close(done[i])
	}
	start := func(i int) error {
		if i < 0 || i >= 3 || claimed[i] {
			return moerr.NewInternalErrorNoCtx("invalid vector query branch activation")
		}
		branch := s.PreScopes[i]
		startErr := context.Cause(s.Proc.Ctx)
		if startErr == nil {
			startErr = func() (initErr error) {
				defer func() {
					if p := recover(); p != nil {
						initErr = moerr.ConvertPanicError(s.Proc.Ctx, p)
					}
				}()
				return branch.InitAllDataSource(c)
			}()
		}
		// 必须先收敛初始化 panic 并发布终态，不能让父 merge 的 Cleanup
		// 等待尚未启动的 producer，形成清理之间的环形等待。
		claimed[i] = true
		if startErr != nil {
			cleanUnstartedVectorScope(branch, c, startErr)
			finish(i, startErr)
			return startErr
		}
		wg.Add(1)
		startErr = ants.Submit(func() {
			defer wg.Done()
			var runErr error
			switch branch.Magic {
			case Normal:
				runErr = branch.Run(c)
			case Merge, MergeInsert:
				runErr = branch.MergeRun(c)
			case Remote:
				runErr = branch.RemoteRun(c)
			default:
				runErr = moerr.NewInternalErrorNoCtx("invalid vector query branch scope")
				cleanUnstartedVectorScope(branch, c, runErr)
			}
			finish(i, runErr)
		})
		if startErr != nil {
			wg.Done()
			cleanUnstartedVectorScope(branch, c, startErr)
			finish(i, startErr)
		}
		return startErr
	}
	wait := func(i int) error {
		if i < 0 || i >= 3 || !claimed[i] {
			return moerr.NewInternalErrorNoCtx("invalid vector query completion wait")
		}
		select {
		case <-done[i]:
			result, _ := results[i].resolveCancelCause()
			return result.err
		case <-s.Proc.Ctx.Done():
			return context.Cause(s.Proc.Ctx)
		}
	}
	selector.SetBranches(start, wait)
	defer func() {
		selector.SetBranches(nil, nil)
		s.cancelMergeSiblingsOnError(err)
		// ParallelRun 的 pipeline cleanup 已取消子 context；未启动子树需要
		// 自底向上发布终态，使每个 merge 只等待确实存在的清理发送者。
		for i, branch := range s.PreScopes {
			if !claimed[i] {
				cleanUnstartedVectorScope(branch, c, context.Canceled)
			}
		}
		wg.Wait()
		// 未启动 reader 不会 Prepare，不能靠它的上一代 released 标记
		// 释放本代所有权。确认全部分支清理完成后由执行域关闭本代 source。
		selector.Source.Close()
		result := newScopeRunResultForProcess(err, s.Proc)
		for i := range claimed {
			if claimed[i] {
				result = preferPrimaryScopeResult(result, results[i])
			}
		}
		result, _ = result.resolveCancelCause()
		err = result.err
	}()
	return s.ParallelRun(c)
}

func cleanUnstartedVectorScope(s *Scope, c *Compile, err error) {
	for _, child := range s.PreScopes {
		cleanUnstartedVectorScope(child, c, err)
	}
	for _, remote := range s.RemoteReceivRegInfos {
		if remote.Idx >= 0 && remote.Idx < len(s.Proc.Reg.MergeReceivers) {
			sendRemoteNotifyCleanupTerminal(s.Proc, s.Proc.Reg.MergeReceivers[remote.Idx], err)
		}
	}
	cleanPipelineWitchStartFail(s, err, c.isPrepare)
}
