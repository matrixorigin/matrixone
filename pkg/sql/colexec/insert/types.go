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
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/externalwrite"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
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
	if insert == nil {
		return
	}
	if insert.ctr.s3Writer != nil {
		return
	}
	for _, writer := range insert.ctr.partitionS3Writers {
		if writer != nil {
			return
		}
	}
	reuse.Free[Insert](insert, nil)
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

func (insert *Insert) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if closeErr := insert.closeS3Writers(proc, pipelineFailed); closeErr != nil {
		logutil.Warn("failed to clean insert S3 writers", zap.Error(closeErr))
	}
	insert.releaseS3MemGrant()
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

func (insert *Insert) retryPendingS3Writers(proc *process.Process) error {
	return insert.closeS3Writers(proc, false)
}

func (insert *Insert) closeS3Writers(proc *process.Process, pipelineFailed bool) error {
	var cleanupErrs []error
	if insert.ctr.s3Writer != nil {
		if closeErr := insert.ctr.s3Writer.CloseWithCleanup(proc.Ctx, pipelineFailed); closeErr != nil {
			cleanupErrs = append(cleanupErrs, closeErr)
			logutil.Warn("failed to clean insert S3 writer", zap.Error(closeErr))
		} else {
			insert.ctr.s3Writer = nil
		}
	}
	if insert.ctr.partitionS3Writers != nil {
		pendingCleanup := false
		for i, writer := range insert.ctr.partitionS3Writers {
			if writer == nil {
				continue
			}
			if closeErr := writer.CloseWithCleanup(proc.Ctx, pipelineFailed); closeErr != nil {
				cleanupErrs = append(cleanupErrs, closeErr)
				pendingCleanup = true
				logutil.Warn("failed to clean partition insert S3 writer", zap.Error(closeErr))
			} else {
				insert.ctr.partitionS3Writers[i] = nil
			}
		}
		if !pendingCleanup {
			insert.ctr.partitionS3Writers = nil
		}
	}
	return errors.Join(cleanupErrs...)
}

func (insert *Insert) retainPendingS3Writers(proc *process.Process) {
	retain := func(writer *colexec.CNS3Writer) bool {
		return colexec.RetainUnpublishedS3Cleanup(proc, func(ctx context.Context) error {
			return writer.CloseWithCleanup(ctx, true)
		})
	}
	if writer := insert.ctr.s3Writer; writer != nil && retain(writer) {
		insert.ctr.s3Writer = nil
	}
	for i, writer := range insert.ctr.partitionS3Writers {
		if writer != nil && retain(writer) {
			insert.ctr.partitionS3Writers[i] = nil
		}
	}
	if len(insert.ctr.partitionS3Writers) != 0 {
		pending := false
		for _, writer := range insert.ctr.partitionS3Writers {
			pending = pending || writer != nil
		}
		if !pending {
			insert.ctr.partitionS3Writers = nil
		}
	}
}

// The Argument for insert data directly to s3 can not be free when this function called as some datastructure still needed.
// therefore, those argument in remote CN will be free in connector operator, and local argument will be free in mergeBlock operator
func (insert *Insert) Free(proc *process.Process, pipelineFailed bool, err error) {
	if closeErr := insert.closeS3Writers(proc, pipelineFailed); closeErr != nil {
		logutil.Warn("failed to clean insert S3 writers", zap.Error(closeErr))
		insert.retainPendingS3Writers(proc)
	}
	insert.releaseS3MemGrant()

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

func (insert *Insert) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (insert *Insert) GetAffectedRows() uint64 {
	return insert.ctr.affectedRows
}

func (insert *Insert) initBufForS3() {
	insert.ctr.buf = colexec.AllocCNS3ResultBat(false)
}
