// Copyright 2021 Matrix Origin
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

package merge

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Merge)

type container struct {
	receiver             *process.PipelineSignalReceiver
	materializedPosition int
	materializedReleased bool
	// Materialized readers return independent, caller-owned batches. Most
	// consumers release those batches themselves, but pass-through and join
	// pipelines may only release their derived output. Retain the latest batch
	// so the next Call or teardown can idempotently reclaim any remaining vector
	// storage without accumulating one handle per materialized input batch.
	materializedBatch *batch.Batch
}

type Merge struct {
	ctr      container
	SinkScan bool
	Partial  bool  // false means listening on all merge receivers
	StartIDX int32 // if partial, listening on receivers[start:end]
	EndIDX   int32

	MaterializedSource   *materialized.Source
	MaterializedReaderID int
	vm.OperatorBase
}

func (merge *Merge) GetOperatorBase() *vm.OperatorBase {
	return &merge.OperatorBase
}

func init() {
	reuse.CreatePool[Merge](
		func() *Merge {
			return &Merge{}
		},
		func(a *Merge) {
			*a = Merge{}
		},
		reuse.DefaultOptions[Merge]().
			WithEnableChecker(),
	)
}

func (merge Merge) TypeName() string {
	return opName
}

func NewArgument() *Merge {
	return reuse.Alloc[Merge](nil)
}

func (merge *Merge) WithSinkScan(sinkScan bool) *Merge {
	merge.SinkScan = sinkScan
	return merge
}

func (merge *Merge) WithPartial(start, end int32) *Merge {
	merge.Partial = true
	merge.StartIDX = start
	merge.EndIDX = end
	return merge
}

// ActivateReceiverRange moves a partial merge to a new, not-yet-started input
// range after its current range has been exhausted. UNION ALL uses this to
// avoid listening for terminal signals from branches that have not started.
func (merge *Merge) ActivateReceiverRange(proc *process.Process, start, end int32) error {
	if merge.MaterializedSource != nil || !merge.Partial {
		return moerr.NewInternalErrorNoCtx("cannot activate a receiver range on a non-partial merge")
	}
	if start < 0 || end <= start || int(end) > len(proc.Reg.MergeReceivers) {
		return moerr.NewInternalErrorNoCtx("invalid merge receiver range")
	}
	if merge.ctr.receiver != nil && merge.ctr.receiver.State().Alive != 0 {
		return moerr.NewInternalErrorNoCtx("cannot replace an active merge receiver range")
	}
	merge.ctr.receiver = process.InitPipelineSignalReceiver(
		proc.Ctx, proc.Reg.MergeReceivers[start:end])
	return nil
}

// DisableReceiverWaitForStartFailure prevents cleanup from waiting on input
// scopes when a containing lazy scope fails validation before any producer is
// submitted. The containing pipeline still runs normal cleanup so its own
// downstream connector receives the startup error.
func (merge *Merge) DisableReceiverWaitForStartFailure(proc *process.Process) {
	merge.ctr.receiver = process.InitPipelineSignalReceiver(proc.Ctx, nil)
}

func (merge *Merge) Release() {
	if merge != nil {
		reuse.Free[Merge](merge, nil)
	}
}

func (merge *Merge) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if merge.MaterializedSource != nil {
		merge.cleanMaterializedBatch(proc)
		if !merge.ctr.materializedReleased {
			merge.MaterializedSource.ReleaseReader(merge.MaterializedReaderID)
			merge.ctr.materializedReleased = true
		}
		return
	}
	if merge.ctr.receiver == nil {
		_ = merge.Prepare(proc)
	}
	if !merge.ctr.receiver.WaitingEndWithTimeout(process.PipelineCleanupWaitTimeout) {
		state := merge.ctr.receiver.State()
		process.WarnPipelineCleanupf(
			proc,
			"merge_cleanup_wait_end_timeout",
			"merge cleanup timed out waiting for pipeline end signals: timeout=%s alive=%d nil_batch_count=%v channel_len=%v channel_cap=%v pipeline_failed=%t err=%v",
			process.PipelineCleanupWaitTimeout,
			state.Alive,
			state.NilBatches,
			state.ChannelLen,
			state.ChannelCap,
			pipelineFailed,
			err)
	}
}

func (merge *Merge) Free(proc *process.Process, pipelineFailed bool, err error) {
	if merge.MaterializedSource != nil {
		merge.cleanMaterializedBatch(proc)
		if !merge.ctr.materializedReleased {
			merge.MaterializedSource.ReleaseReader(merge.MaterializedReaderID)
			merge.ctr.materializedReleased = true
		}
	}
}

func (merge *Merge) cleanMaterializedBatch(proc *process.Process) {
	if merge.ctr.materializedBatch != nil {
		merge.ctr.materializedBatch.Clean(proc.Mp())
		merge.ctr.materializedBatch = nil
	}
}

func (merge *Merge) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}
