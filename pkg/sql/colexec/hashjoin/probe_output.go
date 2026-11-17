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

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// identityProbeOutput borrows unchanged probe headers only until the next Call,
// like forwarding projection. It never releases or mutates those source headers.
// The existing owned result buffers remain available for the first mismatch.
type identityProbeOutput struct {
	output        *batch.Batch
	input         *batch.Batch
	optimistic    bool
	forwardedRows int64
	prefixRows    int64
}

func (hashJoin *HashJoin) canForwardProbe() bool {
	ctr := &hashJoin.ctr
	if !ctr.dictionaryBoundary.AllowsDictionaryOutput() ||
		!hashJoin.dictionaryInputSupported() || ctr.spillEngine != nil || ctr.mp == nil ||
		len(ctr.leftBat.ExtraBuf) != 0 || len(hashJoin.ResultCols) == 0 || ctr.probeTrackBuildMatches ||
		ctr.lastIdx != 0 || ctr.leftBat.RowCount() > colexec.DefaultBatchSize {
		return false
	}
	probeOutput := false
	preferOwned := ctr.dictionaryBoundary.PrefersOwnedOutput()
	for _, column := range hashJoin.ResultCols {
		if column.Rel == 0 {
			probeOutput = true
			if preferOwned {
				source := ctr.leftBat.Vecs[column.Pos]
				// A preference for owned input does not require a second copy
				// of an unchanged owned flat column within its Call lifetime.
				if source.IsConst() || source.IsDictionary() || source.HasBorrowedBacking() ||
					!source.CanDetach(vector.BackingData) || !source.CanDetach(vector.BackingArea) {
					return false
				}
			}
		}
		if !hashJoin.IsInner() && column.Rel != 0 {
			return false
		}
	}
	return probeOutput && ((hashJoin.IsInner() && ctr.probeHashOnPK) || ctr.probeLeftSemi || ctr.probeLeftAnti)
}

func (hashJoin *HashJoin) beginProbeOutput() error {
	ctr := &hashJoin.ctr
	if ctr.probeOutput == nil {
		output := batch.NewOffHeapWithSize(len(hashJoin.ResultCols))
		if err := output.SetAllocationAccount(hashJoin.resultAllocation); err != nil {
			output.Clean(nil)
			return err
		}
		ctr.probeOutput = &identityProbeOutput{output: output}
	}
	ctr.probeOutput.input = ctr.leftBat
	ctr.probeOutput.optimistic = true
	return nil
}

// materializePrefix switches exactly once. Every row preceding lastIdx survived,
// so no row map or re-probe is needed. Subsequent chunks use ordinary gathers.
func (s *identityProbeOutput) materializePrefix(hashJoin *HashJoin, proc *process.Process) error {
	s.optimistic = false
	s.prefixRows += int64(hashJoin.ctr.lastIdx)
	if hashJoin.ctr.lastIdx == 0 {
		return nil
	}
	for i, column := range hashJoin.ResultCols {
		if column.Rel == 0 {
			if err := hashJoin.ctr.resBat.Vecs[i].UnionBatch(s.input.Vecs[column.Pos], 0, hashJoin.ctr.lastIdx, nil, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *identityProbeOutput) publish(hashJoin *HashJoin) *batch.Batch {
	owned := hashJoin.ctr.resBat
	if !s.optimistic {
		return owned
	}
	s.forwardedRows += int64(owned.RowCount())
	for i, column := range hashJoin.ResultCols {
		if column.Rel == 0 {
			s.output.Vecs[i] = s.input.Vecs[column.Pos]
		} else {
			s.output.Vecs[i] = owned.Vecs[i]
		}
	}
	s.output.Attrs = append(s.output.Attrs[:0], owned.Attrs...)
	s.output.Recursive, s.output.ShuffleIDX = owned.Recursive, owned.ShuffleIDX
	s.output.SetRowCount(owned.RowCount())
	return s.output
}

func (s *identityProbeOutput) retire() {
	clear(s.output.Vecs)
	s.output.SetRowCount(0)
	s.input = nil
	s.optimistic = false
}

func (s *identityProbeOutput) free(proc *process.Process) {
	s.retire()
	s.output.Clean(proc.Mp())
}

func (hashJoin *HashJoin) observeProbeOutput() {
	output := hashJoin.ctr.probeOutput
	if output != nil && hashJoin.OpAnalyzer != nil {
		stats := hashJoin.OpAnalyzer.GetOpStats()
		stats.AddExtraStat("JoinProbeForwardedRows", output.forwardedRows)
		stats.AddExtraStat("JoinProbeMaterializedPrefixRows", output.prefixRows)
		output.forwardedRows, output.prefixRows = 0, 0
	}
}
