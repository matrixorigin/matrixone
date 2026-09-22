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

package adaptivetop

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = (*AdaptiveTop)(nil)

// AdaptiveTop 顺序收集候选的最终页面，仅发布一个成功候选。
// 候选必须已完成谓词、排序及 OFFSET/LIMIT；最后一个必须覆盖完整搜索域。
// 单个 Merge child 每个候选对应一个 receiver，本算子只允许本地 DOP=1。
type AdaptiveTop struct {
	LimitExpr *plan.Expr
	Branches  int
	// FallbackOnEmpty preserves a non-empty partial POST page and activates
	// the final exact candidate only when POST produced no rows.
	FallbackOnEmpty bool
	// 每一执行代由 compile 配置，不能跨 prepared execution 保留预算。
	SpillConfig materialized.SpillConfig

	startBranch func(int) error
	waitBranch  func(int) error
	account     *mpool.AllocationAccount
	ctr         container
	vm.OperatorBase
}

type container struct {
	limitExecutor colexec.ExpressionExecutor
	limit         uint64
	source        *materialized.Source
	output        *batch.Batch
	position      int
	selected      bool
	done          bool
}

func NewArgument() *AdaptiveTop { return reuse.Alloc[AdaptiveTop](nil) }

func init() {
	reuse.CreatePool[AdaptiveTop](
		func() *AdaptiveTop { return &AdaptiveTop{} },
		func(a *AdaptiveTop) { *a = AdaptiveTop{} },
		reuse.DefaultOptions[AdaptiveTop]().WithEnableChecker(),
	)
}

func (a *AdaptiveTop) SetBranchStarter(start func(int) error) { a.startBranch = start }
func (a *AdaptiveTop) ClearBranchStarter()                    { a.startBranch = nil }
func (a *AdaptiveTop) SetBranchWaiter(wait func(int) error)   { a.waitBranch = wait }
func (a *AdaptiveTop) ClearBranchWaiter()                     { a.waitBranch = nil }

// 第一候选由 Call 启动，使动态 LIMIT 0 也不打开候选的数据源。
func (a *AdaptiveTop) DeferFirstBranch() bool { return true }

func (a *AdaptiveTop) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if a.account != nil && a.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	a.account = account
	return nil
}

func (a *AdaptiveTop) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if a.account == nil {
		return nil
	}
	if a.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if a.ctr.source != nil || a.ctr.output != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	a.account = nil
	return nil
}

func (a *AdaptiveTop) discard(proc *process.Process) {
	if a.ctr.output != nil {
		a.ctr.output.Clean(proc.Mp())
		a.ctr.output = nil
	}
	if a.ctr.source != nil {
		// Source 的 producer/reader 都在本算子线程，无异步访问。
		a.ctr.source.Close()
		a.ctr.source = nil
	}
}

func (a *AdaptiveTop) Reset(proc *process.Process, pipelineFailed bool, err error) {
	a.discard(proc)
	if a.ctr.limitExecutor != nil {
		a.ctr.limitExecutor.ResetForNextQuery()
	}
	a.ctr = container{limitExecutor: a.ctr.limitExecutor}
	a.ClearBranchStarter()
	a.ClearBranchWaiter()
	// SpillConfig 属于编译模板；其中预算闭包按当前 process execution generation
	// 取资源，prepared reuse 不能在首轮 Reset 后丢失它。
	// pipeline 先清理 child，之后重置下一代的初始 receiver 范围。
	if len(a.Children) == 1 {
		if child, ok := a.GetChildren(0).(*merge.Merge); ok {
			child.WithPartial(0, 0)
		}
	}
}

func (a *AdaptiveTop) Free(proc *process.Process, pipelineFailed bool, err error) {
	a.Reset(proc, pipelineFailed, err)
	if a.ctr.limitExecutor != nil {
		a.ctr.limitExecutor.Free()
		a.ctr.limitExecutor = nil
	}
}

func (a *AdaptiveTop) Release() {
	if a != nil {
		reuse.Free[AdaptiveTop](a, nil)
	}
}

func (a *AdaptiveTop) GetOperatorBase() *vm.OperatorBase { return &a.OperatorBase }
func (a AdaptiveTop) TypeName() string                   { return opName }
func (a *AdaptiveTop) ExecProjection(_ *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
