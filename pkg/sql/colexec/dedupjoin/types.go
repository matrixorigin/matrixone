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

package dedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(DedupJoin)

const (
	Build = iota
	Probe
	Finalize
	End
)

// WorkerJoinMsg carries per-worker state from non-merger workers to the
// merger worker at finalize time. Regular DEDUP JOIN only populates matched;
// the REPLACE INTO merged main-table scan path (OldColCapture) additionally
// populates captured and capturedVecs.
//
// Ownership: once a non-merger worker sends this message on the channel, it
// must relinquish its references to captured / capturedVecs so that the
// merger is the sole owner and is responsible for Free'ing capturedVecs.
type WorkerJoinMsg struct {
	matched      *bitmap.Bitmap
	captured     *bitmap.Bitmap
	capturedVecs []*vector.Vector
}

// freeCapturedVecs releases vectors owned by a WorkerJoinMsg. Intended to be
// called by the merger after it has finished merging captures out of the
// message (ownership was transferred from the sender).
func freeCapturedVecs(vecs []*vector.Vector, proc *process.Process) {
	for _, v := range vecs {
		if v != nil {
			v.Free(proc.GetMPool())
		}
	}
}

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state   int
	lastPos int

	batches       []*batch.Batch
	batchRowCount int64

	exprExecs []colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	savedVecs []*vector.Vector

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched     *bitmap.Bitmap
	handledLast bool

	// Capture buffers for the REPLACE INTO merged main-table scan. When
	// OldColCapturePlaceholderIdxList is non-empty, each entry i in the list
	// owns capturedVecs[i], a vector of length batchRowCount pre-filled with
	// NULL. When a probe row hits build bucket `sel`, we Copy the probe-side
	// source column into capturedVecs[i] at position `sel`. In finalize() the
	// captured values are emitted into the Result slots that point at the
	// build-side placeholder columns.
	capturedVecs     []*vector.Vector
	captured         *bitmap.Bitmap
	captureResultIdx []int32

	maxAllocSize int64
	rbat         *batch.Batch
	buf          []*batch.Batch
}

type DedupJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	Channel  chan *WorkerJoinMsg
	NumCPU   uint64
	IsMerger bool

	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type
	DelColIdx         int32
	UpdateColIdxList  []int32
	UpdateColExprList []*plan.Expr

	// OldColCapturePlaceholderIdxList / OldColCaptureProbeIdxList are parallel
	// arrays. For each i, when probe hits a build bucket the probe-side column
	// at OldColCaptureProbeIdxList[i] is captured and, in finalize(), emitted
	// into every Result entry whose (Rel=1, Pos) equals
	// OldColCapturePlaceholderIdxList[i]. Used by the REPLACE INTO merged
	// main-table scan path; empty for regular INSERT/UPDATE.
	OldColCapturePlaceholderIdxList []int32
	OldColCaptureProbeIdxList       []int32

	vm.OperatorBase
}

func (dedupJoin *DedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &dedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *DedupJoin {
			return &DedupJoin{}
		},
		func(a *DedupJoin) {
			*a = DedupJoin{}
		},
		reuse.DefaultOptions[DedupJoin]().
			WithEnableChecker(),
	)
}

func (dedupJoin DedupJoin) TypeName() string {
	return opName
}

func NewArgument() *DedupJoin {
	return reuse.Alloc[DedupJoin](nil)
}

func (dedupJoin *DedupJoin) Release() {
	if dedupJoin != nil {
		reuse.Free[DedupJoin](dedupJoin, nil)
	}
}

func (dedupJoin *DedupJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	if !ctr.handledLast && dedupJoin.NumCPU > 1 && !dedupJoin.IsMerger {
		dedupJoin.Channel <- nil
	}
	if dedupJoin.OpAnalyzer != nil {
		dedupJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	ctr.maxAllocSize = 0

	ctr.cleanBuf(proc)
	ctr.cleanCaptured(proc)
	ctr.cleanHashMap()
	ctr.resetExprExecutor()
	ctr.resetEvalVectors()
	ctr.handledLast = false
	ctr.state = Build
	ctr.lastPos = 0
}

func (dedupJoin *DedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	ctr.cleanBuf(proc)
	ctr.cleanCaptured(proc)
	ctr.cleanBatch(proc)
	ctr.cleanHashMap()
	ctr.cleanExprExecutor()
	ctr.cleanEvalVectors()
}

func (dedupJoin *DedupJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetExprExecutor() {
	for i := range ctr.exprExecs {
		ctr.exprExecs[i].ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	for i := range ctr.exprExecs {
		ctr.exprExecs[i].Free()
		ctr.exprExecs[i] = nil
	}
}

func (ctr *container) cleanBuf(proc *process.Process) {
	for _, bat := range ctr.buf {
		if bat != nil && bat != ctr.rbat {
			bat.Clean(proc.GetMPool())
		}
	}
	ctr.buf = nil
}

func (ctr *container) cleanCaptured(proc *process.Process) {
	for _, v := range ctr.capturedVecs {
		if v != nil {
			v.Free(proc.GetMPool())
		}
	}
	ctr.capturedVecs = nil
	ctr.captured = nil
	ctr.captureResultIdx = nil
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.batches = nil

	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.GetMPool())
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.GetMPool())
		ctr.joinBat2 = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
		ctr.evecs[i].vec = nil
	}
	ctr.evecs = nil
}

func (ctr *container) resetEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.ResetForNextQuery()
		}
	}
}
