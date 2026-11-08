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

package insert

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/externalwrite"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Insert)

// const (
// 	Process = iota
// 	End
// )

type container struct {
	state               vm.CtrState
	s3Writer            *colexec.CNS3Writer
	partitionS3Writers  []*colexec.CNS3Writer // The array is aligned with the partition number array
	buf                 *batch.Batch
	affectedRows        uint64
	s3MemGranted        int64
	s3MemThrottler      rscthrottler.RSCThrottler
	s3MemNoThresholdCap bool

	source engine.Relation

	// extWriter is used when ToExternal is set: it encodes batches and appends
	// them to a single file in a stage (writable external table).
	extWriter externalwrite.ExternalWriter
	// extCounter is owned by extWriter's complete asynchronous lifetime. It is
	// harvested only after Close or Abort has joined the writer goroutine.
	extCounter *perfcounter.CounterSet
	// extCols are the ColDefs aligned with InsertCtx.Attrs, for the external
	// path's NOT NULL check.
	extCols []*plan.ColDef
	// extFillOwned holds the operator-owned vectors the external IGNORE fill
	// booked for the child's batch during the current insert_external Call,
	// each paired with the child-owned vector it displaced: the fresh
	// constant a constant NULL broadcast was replaced with, and the detached
	// copy an aliased flat fill destination got before its in-place fill.
	// The external path writes the child's batch in place, but the child - a
	// projection in the minimal external plan - reuses its batch and
	// overwrites every slot on its next Call, freeing only its own vectors,
	// so a replacement nobody owns would stay recorded in the mpool's pointer
	// registry forever. The operator is that cleanup owner: insert_external
	// drains the list on every path out of the Call (fill rejection, write
	// failure, success and a panicking unwind alike), so no entry survives
	// its batch and Reset/Free never carry one.
	extFillOwned []extFillReplacement
}

// extFillReplacement pairs one operator-owned IGNORE-fill vector this
// operator installs into the child's borrowed batch with the child-owned
// vector that occupied the slot, so the slot can be returned and the
// replacement's storage released once the batch was consumed.
type extFillReplacement struct {
	slot int
	orig *vector.Vector
	fill *vector.Vector
}

type Insert struct {
	delegated bool
	input     vm.CallResult
	ctr       container
	ToWriteS3 bool // mark if this insert's target is S3 or not.
	// ToExternal marks that this insert writes into a writable external table's
	// backing files (CSV/JSONLine in a stage) instead of an engine relation.
	ToExternal bool
	InsertCtx  *InsertCtx

	vm.OperatorBase
}

func (insert *Insert) GetOperatorBase() *vm.OperatorBase {
	return &insert.OperatorBase
}

func init() {
	reuse.CreatePool[Insert](
		func() *Insert {
			return &Insert{}
		},
		func(a *Insert) {
			*a = Insert{}
		},
		reuse.DefaultOptions[Insert]().
			WithEnableChecker(),
	)
}

func (insert Insert) TypeName() string {
	return opName
}

func NewArgument() *Insert {
	return reuse.Alloc[Insert](nil)
}

func (insert *Insert) Release() {
	if insert != nil {
		reuse.Free[Insert](insert, nil)
	}
}

type InsertCtx struct {
	// insert data into Rel.
	Engine          engine.Engine
	Ref             *plan.ObjectRef
	AddAffectedRows bool     // for hidden table, should not update affect Rows
	Attrs           []string // letter case: origin
	TableDef        *plan.TableDef

	// ExternalConfig is populated at compile time when the target is a writable
	// external table; consumed by the operator to build an ExternalWriter.
	ExternalConfig externalwrite.WriterConfig
}

// retainUnrecordedSinkObjects transfers the writer's persisted /
// possibly-persisted names to the workspace's asynchronous cleanup
// ownership right before the pipeline teardown closes the writer — the
// unrecorded remainder only: names whose metadata record landed stay
// with their record's lifecycle, and the workspace filters them (issue
// #25590 round-19 review, finding 2). Called on the Mpool-free unwind of
// a writer whose stream never reached its recording consumer: a pipeline
// failure between persistence and mergeBlock's record, or a record
// refusal at a saturated admission bound. Without this transfer the
// writer's Close dropped the only remaining reference to physical
// objects, and repeated refused statements — each persisting one more
// object nothing would ever record — leaked without a cleanup owner
// despite the bounded ledger. Safe to call repeatedly: the retainer
// filters recorded names and the caller nils the writer after closing.
func (insert *Insert) retainUnrecordedSinkObjects(proc *process.Process) {
	// All writers can share one worker pool. Cancel every generation before
	// waiting for any snapshot so one writer cannot block another's cleanup.
	insert.ctr.s3Writer.CancelPending()
	for _, writer := range insert.ctr.partitionS3Writers {
		writer.CancelPending()
	}
	var names []string
	if writer := insert.ctr.s3Writer; writer != nil {
		names = append(names, writer.AbandonAndRetainableNames()...)
	}
	for _, writer := range insert.ctr.partitionS3Writers {
		if writer != nil {
			names = append(names, writer.AbandonAndRetainableNames()...)
		}
	}
	if len(names) == 0 {
		return
	}
	txnOp := proc.GetTxnOperator()
	if txnOp == nil {
		return
	}
	ws := txnOp.GetWorkspace()
	if ws == nil {
		return
	}
	retainer, ok := ws.(ioutil.UnrecordedSinkerObjectsRetainer)
	if !ok {
		return
	}
	retainer.RetainUnrecordedS3Objects(
		names, "s3 insert writer unwound before its metadata record: the pipeline failed or the record was refused between persistence and mergeBlock (issue #25590 round-19 review)")
}

func (insert *Insert) Reset(proc *process.Process, pipelineFailed bool, err error) {
	//@todo need add Reset method for s3Writer
	insert.retainUnrecordedSinkObjects(proc)
	if insert.ctr.s3Writer != nil {
		_ = insert.ctr.s3Writer.CloseWithCleanup(proc.Ctx, err)
		insert.ctr.s3Writer = nil
	}
	insert.releaseS3MemGrant()
	if insert.ctr.partitionS3Writers != nil {
		for _, writer := range insert.ctr.partitionS3Writers {
			if writer != nil {
				_ = writer.CloseWithCleanup(proc.Ctx, err)
			}
		}
		insert.ctr.partitionS3Writers = nil
	}
	// A non-nil extWriter here means the input stream never reached its clean
	// end (insert_external nils it after a successful Close), i.e. the pipeline
	// failed or was cancelled: discard the half-written file rather than
	// finalizing it into the stage where readers would see partial rows.
	if insert.ctr.extWriter != nil {
		insert.abortExternalWriter(proc)
	}
	insert.ctr.state = vm.Build

	if insert.ctr.buf != nil {
		insert.ctr.buf.CleanOnlyData()
	}
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (insert *Insert) Free(proc *process.Process, pipelineFailed bool, err error) {
	insert.retainUnrecordedSinkObjects(proc)
	if insert.ctr.s3Writer != nil {
		_ = insert.ctr.s3Writer.CloseWithCleanup(proc.Ctx, err)
		insert.ctr.s3Writer = nil
	}
	insert.releaseS3MemGrant()

	// Free the partition table S3writer object resources
	if insert.ctr.partitionS3Writers != nil {
		for _, writer := range insert.ctr.partitionS3Writers {
			if writer != nil {
				_ = writer.CloseWithCleanup(proc.Ctx, err)
			}
		}
		insert.ctr.partitionS3Writers = nil
	}

	// See Reset: a writer still alive at Free means the stream did not end
	// cleanly; abort instead of persisting a partial file.
	if insert.ctr.extWriter != nil {
		insert.abortExternalWriter(proc)
	}
	insert.ctr.extCols = nil

	if insert.ctr.buf != nil {
		insert.ctr.buf.Clean(proc.Mp())
		insert.ctr.buf = nil
	}
	insert.ctr.source = nil
}

func (insert *Insert) releaseS3MemGrant() {
	if insert.ctr.s3MemThrottler != nil && insert.ctr.s3MemGranted > 0 {
		insert.ctr.s3MemThrottler.Release(insert.ctr.s3MemGranted)
		insert.ctr.s3MemGranted = 0
	}
}

func (insert *Insert) refreshAndReleaseS3MemGrant() {
	type refreshBeforeReleaseDecider interface {
		ShouldRefreshBeforeRelease() bool
	}

	if insert.ctr.s3MemThrottler != nil && insert.ctr.s3MemGranted > 0 {
		if decider, ok := insert.ctr.s3MemThrottler.(refreshBeforeReleaseDecider); !ok || decider.ShouldRefreshBeforeRelease() {
			forcedRefresh(insert.ctr.s3MemThrottler)
		}
	}
	insert.releaseS3MemGrant()
}

func (insert *Insert) GetAffectedRows() uint64 {
	return insert.ctr.affectedRows
}

func (insert *Insert) initBufForS3() {
	insert.ctr.buf = colexec.AllocCNS3ResultBat(false)
}
