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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newPipelineOutcomeCompile(t *testing.T) (*Compile, context.Context) {
	proc := NewMockCompile(t).proc
	t.Cleanup(proc.Free)
	txnClient, txnOp := newTestTxnClientAndOp(gomock.NewController(t))
	proc.Base.TxnClient, proc.Base.TxnOperator = txnClient, txnOp
	c := NewCompile("local-cn:6002", "test", "select ...", "", "", newStubEngine(), proc, nil, false, nil, time.Now())
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.hasMergeOp = true
	c.affectRows = new(atomic.Uint64)
	query := proc.Base.GetContextBase().BuildQueryCtx(context.Background())
	proc.BuildPipelineContext(query)
	t.Cleanup(func() { proc.Cancel(nil) })
	return c, query
}

func newPipelineOutcomeChild(t *testing.T, c *Compile, inputs int) *process.Process {
	proc := c.proc.NewNoContextChildProc(inputs)
	t.Cleanup(proc.Free)
	return proc
}

func TestPipelineOutcomeLimitStopsReaderWithoutPoisoningLateralEdge(t *testing.T) {
	for _, placeLocally := range []bool{false, true} {
		name := "normal"
		if placeLocally {
			name = "remote-placed-locally"
		}
		t.Run(name, func(t *testing.T) {
			c, query := newPipelineOutcomeCompile(t)
			parent := newPipelineOutcomeChild(t, c, 2)
			slow := newPipelineOutcomeChild(t, c, 0)
			fast := newPipelineOutcomeChild(t, c, 0)
			lateral := newPipelineOutcomeChild(t, c, 1)
			watchdog, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			rel := &issue27261BlockingRelation{started: make(chan struct{})}
			scan := table_scan.NewArgument()
			defer scan.Release()
			slowDispatch := dispatch.NewArgument()
			defer slowDispatch.Release()
			slowDispatch.FuncId = dispatch.SendToAllLocalFunc
			slowDispatch.LocalRegs = []*process.WaitRegister{parent.Reg.MergeReceivers[0], lateral.Reg.MergeReceivers[0]}
			slowDispatch.AppendChild(scan)
			slowScope := &Scope{
				Magic: Normal, Proc: slow, RootOp: slowDispatch, NodeInfo: engine.Node{Mcpu: 1},
				DataSource: &Source{Rel: rel, FilterList: []*plan.Expr{plan2.MakeFalseExpr()}},
			}
			if placeLocally {
				slowScope.Magic, slowScope.NodeInfo.Addr = Remote, c.addr
			}
			fastConnector := connector.NewArgument().WithReg(parent.Reg.MergeReceivers[1])
			defer fastConnector.Release()
			fastInput := colexec.MakeMockBatchs(fast.Mp())
			fastConnector.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{fastInput}).WithBatchCallback(func(_ int) {
				select {
				case <-rel.started:
				case <-watchdog.Done():
					panic("reader construction did not start")
				}
			}))
			receiver := merge.NewArgument()
			defer receiver.Release()
			takeOne := limit.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1))
			defer takeOne.Release()
			takeOne.AppendChild(receiver)
			root := &Scope{
				Magic: Merge, Proc: parent, RootOp: takeOne,
				PreScopes: []*Scope{slowScope, {Magic: Normal, Proc: fast, RootOp: fastConnector}},
			}
			lateralReceiver := merge.NewArgument()
			defer lateralReceiver.Release()
			c.scopes = []*Scope{root, {Magic: Normal, Proc: lateral, RootOp: lateralReceiver}}
			for _, scope := range c.scopes {
				scope.buildContextFromParentCtx(c.proc.Ctx)
			}

			require.NoError(t, c.runOnce())
			require.NoError(t, query.Err())
			require.NotNil(t, moerr.PipelineStopCause(context.Cause(slow.Ctx)))
			require.NoError(t, lateral.Reg.MergeReceivers[0].Err())
			select {
			case <-lateral.Reg.MergeReceivers[0].Done():
			default:
				t.Fatal("lateral edge did not receive a terminal signal")
			}
		})
	}
}

type pipelineOutcomeReader struct {
	mockReaderForParallelOrderBy
	err        error
	readCtxErr error
	closing    chan struct{}
	release    <-chan struct{}
	watchdog   context.Context
	timedOut   bool
}

func (r *pipelineOutcomeReader) Read(ctx context.Context, _ []string, _ *plan.Expr, _ *mpool.MPool, _ *batch.Batch) (bool, error) {
	r.readCtxErr = ctx.Err()
	return true, r.err
}

func (r *pipelineOutcomeReader) Close() error {
	close(r.closing)
	select {
	case <-r.release:
		return nil
	case <-r.watchdog.Done():
		r.timedOut = true
		return r.watchdog.Err()
	}
}

type pipelineOutcomeRelation struct {
	engine.Relation
	reader engine.Reader
}

func (r *pipelineOutcomeRelation) BuildReaders(context.Context, any, *plan.Expr, engine.RelData, int, int, bool, engine.TombstoneApplyPolicy, engine.FilterHint) ([]engine.Reader, error) {
	return []engine.Reader{r.reader}, nil
}

type pipelineOutcomeWitness struct {
	*colexec.MockOperator
	watchdog context.Context
}

func (w *pipelineOutcomeWitness) Call(proc *process.Process) (vm.CallResult, error) {
	select {
	case <-proc.Ctx.Done():
		return vm.CancelResult, moerr.PipelineContextError(proc.Ctx)
	case <-w.watchdog.Done():
		return vm.CancelResult, moerr.NewInternalErrorNoCtx("sibling failure did not stop the waiting pipeline")
	}
}

func TestPipelineOutcomeFirstFailureStopsWorkButForeignKeyWins(t *testing.T) {
	for _, coordinator := range []string{"statement", "merge"} {
		for _, first := range []string{"foreign-key-first", "interrupt-first"} {
			for _, interrupt := range []string{"bare", "encoded"} {
				for _, placement := range []string{"normal", "remote-placed-locally"} {
					t.Run(coordinator+"/"+first+"/"+interrupt+"/"+placement, func(t *testing.T) {
						testPipelineOutcomeFailureOrdering(t, coordinator, first, interrupt, placement)
					})
				}
			}
		}
	}
}

func testPipelineOutcomeFailureOrdering(t *testing.T, coordinator, first, interrupt, placement string) {
	c, query := newPipelineOutcomeCompile(t)
	watchdog, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	waiter := &Scope{
		Magic: Normal, Proc: newPipelineOutcomeChild(t, c, 0),
		RootOp: &pipelineOutcomeWitness{colexec.NewMockOperator(), watchdog},
	}

	var interrupted error = context.Canceled
	if interrupt == "encoded" {
		interrupted = moerr.NewQueryInterrupted(query)
	}
	started := &pipelineOutcomeReader{err: interrupted, closing: make(chan struct{}), watchdog: watchdog}
	last := &pipelineOutcomeReader{err: moerr.NewErrFKNoReferencedRow2(query), closing: make(chan struct{}), watchdog: watchdog}
	if first == "foreign-key-first" {
		started, last = last, started
	}
	// Both reads have produced their errors before the first Close can return.
	// The remaining Close must be released by the coordinator's cancellation.
	started.release = last.closing
	readers := make([]*Scope, 0, 2)
	for _, reader := range []*pipelineOutcomeReader{started, last} {
		scan := table_scan.NewArgument().WithTypes([]plan.Type{{Id: int32(types.T_int32)}})
		defer scan.Release()
		s := &Scope{
			Magic: Normal, Proc: newPipelineOutcomeChild(t, c, 0), RootOp: scan,
			DataSource: &Source{R: reader, Attributes: []string{"k"}},
		}
		if placement == "remote-placed-locally" {
			s.Magic, s.NodeInfo = Remote, engine.Node{Addr: c.addr, Mcpu: 1}
			s.DataSource.R = nil
			s.DataSource.Rel = &pipelineOutcomeRelation{reader: reader}
			s.DataSource.FilterList = []*plan.Expr{plan2.MakeFalseExpr()}
		}
		readers = append(readers, s)
	}
	if coordinator == "merge" {
		waiter.Magic, waiter.PreScopes = Merge, readers
		c.scopes = []*Scope{waiter}
	} else {
		c.scopes = append(readers, waiter)
	}
	for _, scope := range c.scopes {
		scope.buildContextFromParentCtx(c.proc.Ctx)
	}
	last.release = waiter.Proc.Ctx.Done()
	err := c.runOnce()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFKNoReferencedRow2), "%v", err)
	require.NoError(t, started.readCtxErr)
	require.NoError(t, last.readCtxErr)
	require.False(t, started.timedOut)
	require.False(t, last.timedOut, "the waiter should wake from the first result before the last one can finish")
	require.ErrorIs(t, waiter.Proc.Ctx.Err(), context.Canceled)
	require.NoError(t, query.Err())
}
