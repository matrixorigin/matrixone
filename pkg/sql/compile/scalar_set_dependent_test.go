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
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Observe the real dynamic construction boundary without adding product hooks.
// The native children still receive their own ordinary account bindings.
type scalarDependentProbe struct {
	vm.Operator
	account     *mpool.AllocationAccount
	check       func(*mpool.AllocationAccount)
	beforeClose func()
}

func (p *scalarDependentProbe) SetAllocationAccount(a *mpool.AllocationAccount) error {
	if owner, ok := p.Operator.(executionAllocationAccountOwner); ok {
		if err := owner.SetAllocationAccount(a); err != nil {
			return err
		}
	}
	p.account = a
	return nil
}
func (p *scalarDependentProbe) ClearAllocationAccount(a *mpool.AllocationAccount) error {
	if owner, ok := p.Operator.(executionAllocationAccountOwner); ok {
		if err := owner.ClearAllocationAccount(a); err != nil {
			return err
		}
	}
	p.account = nil
	return nil
}
func (p *scalarDependentProbe) BindDependent(f colexec.DependentProgramFactory, a *vector.AllocationAccountSelection) {
	if binder, ok := p.Operator.(interface {
		BindDependent(colexec.DependentProgramFactory, *vector.AllocationAccountSelection)
	}); ok {
		binder.BindDependent(f, a)
	}
}
func (p *scalarDependentProbe) Call(proc *process.Process) (vm.CallResult, error) {
	p.check(p.account)
	return p.Operator.Call(proc)
}

func (p *scalarDependentProbe) Reset(proc *process.Process, failed bool, err error) {
	if p.beforeClose != nil {
		p.beforeClose()
	}
	p.Operator.Reset(proc, failed, err)
}

func (p *scalarDependentProbe) TakeContinuation() vm.Continuation {
	if d, ok := p.Operator.(interface{ TakeContinuation() vm.Continuation }); ok {
		return d.TakeContinuation()
	}
	return nil
}

func TestScalarSetDependentLifecycle(t *testing.T) {
	const inner = `(select 1 id,cast(-0e0 as double) v union all select 2,cast(-0e0 as double))`
	const scalar = `(select i.v from ` + inner + ` i where i.id>o.id union select i.v from ` + inner + ` i where i.id>o.id)`
	const nested = `(select cast((select i.v from ` + inner + ` i where i.id>m.id union select i.v from ` + inner + ` i where i.id>m.id) as varchar) from (select o.id id) m)`
	for _, mode := range []string{"normal", "nested", "shared-work", "small-cap", "cancel", "forced-spill"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
			defer budget.Close()
			cap := uint64(64 << 20)
			if mode == "small-cap" {
				cap = 4096
			}
			gen, err := budget.OpenGenerationWithCap(1, cap)
			require.NoError(t, err)
			defer gen.Close()
			registry, err := gen.AllocationAccountRegistry()
			require.NoError(t, err)
			c := newTestAllocationLifecycleCompile(t, registry, func(mpool.AllocationAccountTerminalSnapshot) {})
			defer c.proc.Free()
			var spillFS *scalarFallbackSpillFS
			if mode == "forced-spill" {
				base := c.proc.GetFileService()
				local, err := fileservice.Get[fileservice.MutableFileService](base, defines.LocalFileServiceName)
				require.NoError(t, err)
				spillFS = &scalarFallbackSpillFS{MutableFileService: local, live: make(map[string]*os.File)}
				shared, err := fileservice.Get[fileservice.FileService](base, defines.SharedFileServiceName)
				require.NoError(t, err)
				etl, err := fileservice.Get[fileservice.FileService](base, defines.ETLFileServiceName)
				require.NoError(t, err)
				c.proc.Base.FileService, err = fileservice.NewFileServices("", spillFS, shared, etl)
				require.NoError(t, err)
			}

			c.proc.ReplaceTopCtx(ctx)
			c.proc.Ctx = ctx
			restoreRuntimeVariableForTest(t, c.proc.GetService(), moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			require.NoError(t, c.proc.SetExecutionResourceBudgetForTesting(gen))
			c.allocationAccountLimit = cap
			c.allocationControllerProvider = func() (mpool.AllocationCapacityController, error) { return gen, nil }
			value := "cast(" + scalar + " as varchar)"
			if mode == "nested" {
				value = nested
			}
			sql := `select ` + value + `,(select (select o.id+1)) from (select 0 id union all select 3) o order by o.id`
			stmts, err := mysql.Parse(ctx, sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			compilerCtx := plan2.NewEmptyCompilerContext(c.proc)
			q, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			require.NotNil(t, q.Correlation)
			require.True(t, queryUsesScalarSetReaggregation(q))
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
			spilled := map[int32]bool{}
			if mode == "forced-spill" {
				for _, n := range q.Nodes {
					if n.NodeType == plan.Node_AGG {
						n.SpillMem = 1
						spilled[n.NodeId] = false
					}
				}
				require.Len(t, spilled, 2)
			}
			var attempt *statementAllocationAttempt
			sourceOpens, joins, checks := 0, 0, 0
			sourceCompiler := func(n *plan.Node) (dependent.SourceFactory, error) {
				f, err := c.compileDependentSource(n)
				if err != nil {
					return nil, err
				}
				return func(p *process.Process, s *vector.AllocationAccountSelection, a *mpool.AllocationAccount) (dependent.Source, error) {
					sourceOpens++
					require.NotNil(t, attempt)
					require.Same(t, attempt.account, a)
					require.NotNil(t, attempt.scalarSetLease)
					if mode == "cancel" {
						cancel()
					}
					return f(p, s, a)
				}, nil
			}
			kernelCompiler := func(n *plan.Node, ch []*plan.Node) (dependent.KernelFactory, [][]*plan.Expr, error) {
				f, inputs, err := c.compileDependentKernel(n, ch)
				if err != nil {
					return nil, nil, err
				}
				return func(p *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
					native, err := f(p, ports)
					if err != nil {
						return native, err
					}
					var marked *loopjoin.LoopJoin
					require.NoError(t, vm.HandleAllOp(native, func(_ vm.Operator, op vm.Operator) error {
						if j, ok := op.(*loopjoin.LoopJoin); ok && j.ScalarSetReaggregation {
							marked = j
							joins++
						}
						return nil
					}))
					return &scalarDependentProbe{Operator: native, beforeClose: func() {
						if mode == "forced-spill" && n.NodeType == plan.Node_AGG {
							g, ok := native.(*group.Group)
							require.True(t, ok)
							stats := g.OpAnalyzer.GetOpStats()
							spilled[n.NodeId] = stats.SpillRows > 0 && stats.ExtraStats["GroupSpillReloadRecords"] > 0
						}
					}, check: func(a *mpool.AllocationAccount) {
						checks++
						require.Same(t, attempt.account, a)
						if marked != nil {
							require.Same(t, attempt.scalarSetLease, marked.ScalarSetLease)
						}
					}}, nil
				}, inputs, nil
			}
			template, err := dependent.Compile(ctx, q, sourceCompiler, kernelCompiler)
			require.NoError(t, err)
			op := &dependent.Operator{Template: template, ScalarSetReaggregation: true}
			defer op.Free(c.proc, false, nil)
			var previous *process.ScalarSetExecutionLease
			for round := 0; round < 2; round++ {
				func() {
					c.allocationAccountOwners = []executionAllocationAccountOwner{op}
					attempt, err = c.beginAllocationAccountAttempt()
					require.NoError(t, err)
					defer func() {
						op.Reset(c.proc, false, nil)
						_, finishErr := attempt.finish()
						c.allocationAttempt = nil
						require.NoError(t, finishErr)
					}()
					require.NotNil(t, attempt.scalarSetLease)
					require.NotSame(t, previous, attempt.scalarSetLease)
					require.Zero(t, attempt.scalarSetLease.CandidatePairs())
					previous = attempt.scalarSetLease
					if mode == "shared-work" {
						require.NoError(t, attempt.scalarSetLease.ChargeCandidatePairs(process.ScalarSetCandidatePairLimit-1))
					}
					// Admission happens before either dynamic construction or source demand.
					spare, err := gen.AcquireScalarSetExecution()
					require.NoError(t, err)
					defer spare.Close()
					before := sourceOpens
					_, err = gen.AcquireScalarSetExecution()
					require.Error(t, err)
					require.Equal(t, before, sourceOpens)
					spare.Close()
					err = op.Prepare(c.proc)
					var got []string
					for err == nil {
						var out vm.CallResult
						out, err = op.Call(c.proc)
						if err != nil || out.Batch == nil {
							break
						}
						for row := 0; row < out.Batch.RowCount(); row++ {
							v := out.Batch.Vecs[0]
							s := "NULL"
							if !v.IsNull(uint64(row)) {
								s = string(v.GetBytesAt(row))
							}
							got = append(got, s)
						}
					}
					switch mode {
					case "normal", "nested", "forced-spill":
						require.NoError(t, err)
						require.Equal(t, []string{"-0", "NULL"}, got)
					case "shared-work":
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrScalarSetResourceBudgetExceeded), "%v", err)
					default:
						require.Error(t, err)
					}
					op.Reset(c.proc, err != nil, err)
					snapshot, finishErr := attempt.finish()
					require.NoError(t, finishErr)
					require.True(t, drainedScalarSetAccount(attempt.account, snapshot))
					require.Zero(t, gen.Used())
					if mode == "forced-spill" {
						for id, ok := range spilled {
							require.True(t, ok, "dynamic aggregate %d must spill and reload", id)
						}
						assertScalarFallbackSpillClosed(t, spillFS, "mixed-dependent")
						require.Zero(t, gen.Snapshot().SpillDiskUsed)
						require.Zero(t, gen.Snapshot().SpillFDUsed)
					}
					require.Nil(t, op.ScalarSetLease)
					c.allocationAttempt = nil
					permit, err := gen.AcquireScalarSetExecution()
					require.NoError(t, err)
					defer permit.Close()
					permit.Close()
				}()
				if mode == "cancel" {
					break
				}
			}
			if mode == "normal" || mode == "nested" {
				require.Positive(t, joins)
				require.Positive(t, checks)
				require.Positive(t, sourceOpens)
			}
		})
	}
}
