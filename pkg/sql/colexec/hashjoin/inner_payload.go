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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// probeInnerPayload preserves the ordinary probe continuation, but gathers
// resolved tuples across small duplicate groups instead of projecting each
// group/row separately. No lookup result or selection escapes its JoinMap.
func (ctr *container) probeInnerPayload(join *HashJoin, proc *process.Process, result *vm.CallResult) error {
	rows := ctr.leftBat.RowCount()
	outputRows := 0
	for outputRows < colexec.DefaultBatchSize {
		count := 0
		limit := min(hashmap.UnitLimit, colexec.DefaultBatchSize-outputRows)
		for count < limit {
			if len(ctr.sels) > 0 {
				n := min(len(ctr.sels), limit-count)
				for i, buildRow := range ctr.sels[:n] {
					join.uniqueLeftRows[count+i] = int64(ctr.lastIdx - 1)
					join.uniqueRightRows[count+i] = int64(buildRow)
				}
				ctr.sels = ctr.sels[n:]
				count += n
				if len(ctr.sels) == 0 {
					ctr.probeState = psBatchRow
					if ctr.vsIdx == len(ctr.vs) {
						ctr.probeState = psNextBatch
					}
				}
				continue
			}
			if ctr.probeState == psNextBatch {
				if ctr.lastIdx == rows {
					break
				}
				var err error
				ctr.vs, ctr.zvs, err = ctr.itr.Find(ctr.lastIdx, min(rows-ctr.lastIdx, hashmap.UnitLimit), ctr.eqCondVecs)
				if err != nil {
					return err
				}
				ctr.vsIdx, ctr.probeState = 0, psBatchRow
			}
			position := ctr.vsIdx
			ctr.lastIdx++
			ctr.vsIdx++
			if ctr.zvs[position] != 0 && ctr.vs[position] != 0 {
				ctr.sels = ctr.mp.GetSels(ctr.vs[position] - 1)
			}
			if len(ctr.sels) > 0 {
				ctr.probeState = psSelsForOneRow
			} else if ctr.vsIdx == len(ctr.vs) {
				ctr.probeState = psNextBatch
			}
		}
		if count > 0 {
			left, right := join.uniqueLeftRows[:count], join.uniqueRightRows[:count]
			if count == 1 {
				if err := ctr.appendOneMatch(join, proc, left[0], right[0]/colexec.DefaultBatchSize, right[0]%colexec.DefaultBatchSize); err != nil {
					return err
				}
			} else {
				for column, position := range join.ResultCols {
					if position.Rel == 0 {
						if err := ctr.resBat.Vecs[column].Union(ctr.leftBat.Vecs[position.Pos], left, proc.Mp()); err != nil {
							return err
						}
					}
				}
				// Probe payload is now owned; reuse its row scratch for local
				// build ordinals, exactly as the unique-output gather does.
				if _, err := ctr.appendBuildPayloadRows(join, proc, right, left); err != nil {
					return err
				}
			}
			outputRows += count
		}
		if ctr.lastIdx == rows && len(ctr.sels) == 0 {
			ctr.lastIdx = 0
			ctr.leftBat = nil
			ctr.probeState = psNextBatch
			break
		}
	}
	ctr.resBat.AddRowCount(outputRows)
	result.Batch = ctr.resBat
	return nil
}
