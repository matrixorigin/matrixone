// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type domainConnectorReader struct {
	engine.Reader
	reads int
	err   error
}

func (r *domainConnectorReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	r.reads++
	if r.reads > 2 {
		return true, r.err
	}
	if err := vector.AppendFixed(out.Vecs[0], int64(r.reads), false, mp); err != nil {
		return false, err
	}
	out.SetRowCount(1)
	return false, nil
}
func (*domainConnectorReader) Close() error { return nil }

func TestLocalDomainConnectorBuildProgress(t *testing.T) {
	for _, mode := range []string{"complete", "source error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			producer, consumer := testutil.NewProcess(t), testutil.NewProcess(t)
			producer.SetMessageBoard(message.NewMessageBoard())
			consumer.SetMessageBoard(producer.GetMessageBoard())
			t.Cleanup(producer.Free)
			t.Cleanup(consumer.Free)
			deadline, stopDeadline := context.WithTimeoutCause(producer.Ctx, 10*time.Second, moerr.NewInternalErrorNoCtx("domain connector test deadline"))
			t.Cleanup(stopDeadline)
			ctx, cancel := context.WithCancelCause(deadline)
			t.Cleanup(func() { cancel(nil) })
			producer.Ctx, producer.Cancel = ctx, cancel
			consumer.Ctx, consumer.Cancel = ctx, cancel
			budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(generation.Close)
			require.NoError(t, producer.SetExecutionResourceBudgetForTesting(generation))
			domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 4, 1, 1, 1000000)
			require.NotNil(t, domain)
			t.Cleanup(domain.Close)
			reader := &domainConnectorReader{}
			failure := moerr.NewInvalidInputNoCtx("domain connector source failure")
			if mode == "source error" {
				reader.err = failure
			}
			scan := table_scan.NewArgument().WithTypes([]plan.Type{{Id: int32(types.T_int64)}})
			scan.Attrs = []string{"key"}
			scan.Reader = reader
			scan.SetAnalyzeControl(0, true)
			scan.SetMaxParallel(1)
			scan.LocalDomainBuilds = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "key"}}
			edge := process.NewPipelineEdge(1, 1)
			consumer.Reg.MergeReceivers = []*process.WaitRegister{edge}
			sender := connector.NewArgument().WithReg(edge)
			sender.AppendChild(scan)
			receiver := merge.NewArgument()

			firstSent, sourceDone := make(chan struct{}), make(chan error, 1)
			finished := make(chan struct{})
			var receiveErr error
			started, receiverPrepared := false, false
			t.Cleanup(func() {
				cancel(nil)
				if started {
					select {
					case <-finished:
					case <-deadline.Done():
						t.Error("source failed to terminate during cleanup")
						return
					}
				}
				if receiverPrepared {
					vm.Reset(receiver, consumer, mode != "complete", receiveErr)
				}
				sender.CleanupDeferredSpool()
				vm.Free(receiver, consumer, false, nil)
				vm.Free(sender, producer, false, nil)
				vm.Free(scan, producer, false, nil)
				receiver.Release()
				sender.Release()
				scan.Release()
			})
			require.NoError(t, scan.Prepare(producer))
			require.NoError(t, domain.PrepareProbe(producer))
			require.NoError(t, sender.Prepare(producer))
			require.NoError(t, receiver.Prepare(consumer))
			receiverPrepared = true
			started = true
			go func() {
				defer close(finished)
				var cause error
				first := true
				for {
					out, err := vm.Exec(sender, producer)
					if err != nil {
						cause = err
						break
					}
					if out.Batch == nil || out.Status == vm.ExecStop {
						break
					}
					if first {
						close(firstSent)
						first = false
					}
				}
				vm.Reset(sender, producer, cause != nil || mode == "cancel", cause)
				vm.Reset(scan, producer, cause != nil || mode == "cancel", cause)
				sourceDone <- cause
			}()
			select {
			case <-firstSent:
			case <-ctx.Done():
				t.Fatal(context.Cause(ctx))
			}
			// The one-slot edge is full; the source cannot pull its EOF yet.
			require.Zero(t, domain.Snapshot().Finished)
			waited := make(chan error, 1)
			go func() { _, _, err := domain.Wait(ctx); waited <- err }()
			if mode == "cancel" {
				cancel(failure)
			}
			rows := 0
			for {
				out, err := vm.Exec(receiver, consumer)
				if err != nil {
					receiveErr = err
					break
				}
				if out.Batch == nil {
					break
				}
				rows += out.Batch.RowCount()
			}
			var sourceErr error
			select {
			case sourceErr = <-sourceDone:
			case <-deadline.Done():
				t.Fatal(context.Cause(deadline))
			}
			select {
			case err := <-waited:
				if mode == "cancel" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-deadline.Done():
				t.Fatal(context.Cause(deadline))
			}
			if mode == "complete" {
				require.NoError(t, sourceErr)
				require.NoError(t, receiveErr)
				require.Equal(t, 2, rows)
				require.Equal(t, 1, domain.Snapshot().Finished)
			}
			if mode == "source error" {
				require.ErrorIs(t, sourceErr, failure)
				require.ErrorIs(t, receiveErr, failure)
			}
		})
	}
}
