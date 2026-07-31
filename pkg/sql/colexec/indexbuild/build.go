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

package indexbuild

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "index_build"

func (indexBuild *IndexBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": index build ")
}

func (indexBuild *IndexBuild) OpType() vm.OpType {
	return vm.IndexBuild
}

func (indexBuild *IndexBuild) Prepare(proc *process.Process) (err error) {
	if indexBuild.OpAnalyzer == nil {
		indexBuild.OpAnalyzer = process.NewAnalyzer(indexBuild.GetIdx(), indexBuild.IsFirst, indexBuild.IsLast, "index build")
	} else {
		indexBuild.OpAnalyzer.Reset()
	}

	ctr := &indexBuild.ctr
	ctr.runtimeFilterUsable = false
	if spec := indexBuild.RuntimeFilterSpec; spec != nil {
		buildExpr := runtimefilter.BuildKeyExpr(spec)
		if buildExpr == nil || buildExpr.GetCol() == nil ||
			buildExpr.GetCol().ColPos != 0 {
			return nil
		}
		declaredType := types.New(
			types.T(buildExpr.Typ.Id),
			buildExpr.Typ.Width,
			buildExpr.Typ.Scale,
		)
		ctr.runtimeFilterUsable = runtimefilter.ExactKeyEncoding(
			spec, declaredType) != keycodec.ExactRuntimeFilterUnsupported
	}
	return nil
}

func (indexBuild *IndexBuild) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := indexBuild.OpAnalyzer

	result := vm.NewCallResult()
	ctr := &indexBuild.ctr
	for {
		switch ctr.state {
		case ReceiveBatch:
			if err := ctr.build(indexBuild, proc, analyzer); err != nil {
				indexBuild.finalizeBuildFailure(proc)
				return result, err
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(indexBuild, proc); err != nil {
				indexBuild.finalizeBuildFailure(proc)
				return result, err
			}
			ctr.state = End
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// finalizeBuildFailure publishes PASS before Call returns. Runtime-filter
// consumers may already be blocked on this operator, so waiting for Reset can
// deadlock scheduler cleanup. runtimeFilterDone also prevents Reset from
// publishing a contradictory second terminal message for this generation.
func (indexBuild *IndexBuild) finalizeBuildFailure(proc *process.Process) {
	if indexBuild.RuntimeFilterSpec == nil ||
		indexBuild.ctr.runtimeFilterDone {
		return
	}
	message.FinalizeRuntimeFilterOnBuildError(
		indexBuild.RuntimeFilterSpec, proc.GetMessageBoard())
	indexBuild.ctr.runtimeFilterDone = true
}

func (ctr *container) abandonRuntimeFilter(proc *process.Process) {
	ctr.runtimeFilterUsable = false
	if ctr.buf != nil {
		ctr.buf.Clean(proc.Mp())
		ctr.buf = nil
	}
}

func (ctr *container) sendRuntimeFilterPass(
	spec *plan.RuntimeFilterSpec,
	proc *process.Process,
) {
	if spec == nil || ctr.runtimeFilterDone {
		return
	}
	message.SendRuntimeFilter(
		message.RuntimeFilterMessage{
			Tag: spec.Tag,
			Typ: message.RuntimeFilter_PASS,
		},
		spec,
		proc.GetMessageBoard(),
	)
	ctr.runtimeFilterDone = true
}

func (ctr *container) collectBuildBatches(indexBuild *IndexBuild, proc *process.Process, analyzer process.Analyzer) error {
	// A legacy or contradictory contract can only produce PASS. Avoid scanning
	// and retaining the index solely for an optimization we cannot safely send.
	if !ctr.runtimeFilterUsable {
		return nil
	}
	for {
		result, err := vm.ChildrenCall(indexBuild.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		if result.Batch.IsEmpty() {
			continue
		}
		if len(result.Batch.Vecs) != 1 || result.Batch.Vecs[0] == nil ||
			runtimefilter.ExactKeyEncoding(
				indexBuild.RuntimeFilterSpec,
				*result.Batch.Vecs[0].GetType(),
			) == keycodec.ExactRuntimeFilterUnsupported {
			ctr.abandonRuntimeFilter(proc)
			return nil
		}

		inputVec := result.Batch.Vecs[0]
		if inputVec.IsConst() {
			// A constant batch represents one distinct build key regardless of
			// its logical row count. Compact it before copying so a large
			// constant batch cannot consume the IN-cardinality limit or be
			// expanded into an equally large retained vector.
			if !inputVec.IsConstNull() && inputVec.Length() > 0 {
				if ctr.buf == nil {
					ctr.buf = batch.NewOffHeapWithSize(1)
					ctr.buf.Vecs[0] = vector.NewOffHeapVecWithType(
						*inputVec.GetType())
				}
				if err = ctr.buf.UnionOne(result.Batch, 0, proc.Mp()); err != nil {
					ctr.abandonRuntimeFilter(proc)
					return nil
				}
			}
		} else {
			analyzer.Alloc(int64(result.Batch.Size()))
			if ctr.buf == nil {
				// Do not inherit an on-heap source layout for an optional,
				// cardinality-bounded copy. Off-heap growth is tracked by the
				// process pool and can fail open to PASS instead of ending in
				// an unrecoverable Go-heap OOM.
				ctr.buf = batch.NewOffHeapWithSize(1)
				ctr.buf.Vecs[0] = vector.NewOffHeapVecWithType(
					*inputVec.GetType())
			}
			ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
			if err != nil {
				ctr.abandonRuntimeFilter(proc)
				return nil
			}
		}

		// If read index table data exceeds the UpperLimit, abandon reading data from index table
		if ctr.buf.RowCount() > int(indexBuild.RuntimeFilterSpec.UpperLimit) {
			// for index build, can exit early
			ctr.abandonRuntimeFilter(proc)
			return nil
		}
	}
	return nil
}

func (ctr *container) build(ap *IndexBuild, proc *process.Process, anal process.Analyzer) error {
	err := ctr.collectBuildBatches(ap, proc, anal)
	if err != nil {
		return err
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *IndexBuild, proc *process.Process) error {
	if ap.RuntimeFilterSpec == nil {
		return nil
	}
	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if !ctr.runtimeFilterUsable {
		ctr.sendRuntimeFilterPass(ap.RuntimeFilterSpec, proc)
		return nil
	} else if ctr.buf == nil || ctr.buf.RowCount() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit

	if ctr.buf.RowCount() > int(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}

	// A malformed or stale plan is not evidence that probe rows cannot match.
	// Exact runtime filters are optional, so fail open instead of interpreting
	// bytes under a contract inferred from the index payload alone.
	if len(ctr.buf.Vecs) != 1 || ctr.buf.Vecs[0] == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}
	vec := ctr.buf.Vecs[0]
	if vec.IsConst() {
		// Batch.Dup preserves a first-batch constant vector. Materialize only
		// its one distinct value: expanding every repeated row would waste
		// memory and AppendFixed cannot add signed-zero closure to a const vec.
		flat := vector.NewOffHeapVecWithType(*vec.GetType())
		if !vec.IsConstNull() && vec.Length() > 0 {
			if err := flat.UnionOne(vec, 0, proc.Mp()); err != nil {
				flat.Free(proc.Mp())
				ctr.abandonRuntimeFilter(proc)
				ctr.sendRuntimeFilterPass(ap.RuntimeFilterSpec, proc)
				return nil
			}
		}
		defer flat.Free(proc.Mp())
		vec = flat
	}
	encoding := runtimefilter.ExactKeyEncoding(
		ap.RuntimeFilterSpec, *vec.GetType())
	if encoding == keycodec.ExactRuntimeFilterUnsupported {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}
	if vec.Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}
	if encoding == keycodec.ExactRuntimeFilterFloatZeroClosed {
		if err := runtimefilter.CloseFloatSignedZero(vec, proc.Mp(), nil); err != nil {
			ctr.abandonRuntimeFilter(proc)
			ctr.sendRuntimeFilterPass(ap.RuntimeFilterSpec, proc)
			return nil
		}
	}
	if vec.Length() > int(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterDone = true
		return nil
	}

	// InplaceSort reorders data but NOT the null bitmap.
	// NULLs are irrelevant for IN-filter: clear bitmap before sort.
	vec.GetNulls().Reset()
	vec.InplaceSort()
	budget, err := proc.GetHashBuildBudget()
	if err != nil {
		ctr.abandonRuntimeFilter(proc)
		ctr.sendRuntimeFilterPass(ap.RuntimeFilterSpec, proc)
		return nil
	}
	data, release, err := runtimefilter.MarshalExactFilterVector(vec, budget)
	if err != nil {
		ctr.abandonRuntimeFilter(proc)
		ctr.sendRuntimeFilterPass(ap.RuntimeFilterSpec, proc)
		return nil
	}

	runtimeFilter.Typ = message.RuntimeFilter_IN
	runtimeFilter.Card = int32(vec.Length())
	runtimeFilter.Data = data
	runtimeFilter.SetMemoryRelease(release)
	message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
	ctr.runtimeFilterDone = true
	return nil
}
