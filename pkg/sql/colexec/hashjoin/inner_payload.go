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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// probeInnerPayload preserves the ordinary probe continuation, but gathers
// resolved tuples across small duplicate groups instead of projecting each
// group/row separately. No lookup result or selection escapes its JoinMap.
func (ctr *container) probeInnerPayload(join *HashJoin, proc *process.Process, result *vm.CallResult) error {
	resMP, err := join.resultMP(proc)
	if err != nil {
		return err
	}
	rows := ctr.leftBat.RowCount()
	outputLimit := ctr.maxOutputRows()
	outputRows := 0
	repeatedProbeRows := 0
	repeatPayload := false
	for _, position := range join.ResultCols {
		if position.Rel == 0 && repeatProbeArea(ctr.leftBat.Vecs[position.Pos]) {
			repeatPayload = true
			break
		}
	}
	target := outputAreaTarget()
	boundBuild := ctr.gatherBuildRowBytes > target/int64(outputLimit)
	for outputRows < outputLimit {
		count := 0
		safeEnd := outputLimit
		if boundBuild {
			safeEnd = max(repeatedProbeRows, ctr.innerGatherEnd(join, target, outputRows, outputLimit))
		}
		limit := min(hashmap.UnitLimit, safeEnd-outputRows)
		if limit == 0 {
			break
		}
		for count < limit {
			if len(ctr.sels) > 0 {
				if repeatPayload && repeatedProbeRows == outputRows+count {
					// Copy each long probe value once for its complete duplicate
					// group in this output, even when build gathering spans chunks.
					repeats := min(len(ctr.sels), outputLimit-outputRows-count)
					if ctr.checkProbeArea && outputRows+count > 0 && !ctr.probePayloadFits(join, target) {
						outputLimit = outputRows + count
						break
					}
					if repeats > safeEnd-outputRows-count {
						// Flush staged rows first. Measure this group against the
						// actual remaining space before copying its probe value.
						if count > 0 {
							break
						}
						fit, full := ctr.fitRemainingInnerGroup(join, repeats, outputRows)
						if full {
							repeats = fit
							outputLimit = outputRows + fit
							limit = min(limit, fit)
							if fit == 0 {
								break
							}
						}
					}
					for column, position := range join.ResultCols {
						if position.Rel == 0 && repeatProbeArea(ctr.leftBat.Vecs[position.Pos]) {
							if err := ctr.resBat.Vecs[column].UnionMulti(ctr.leftBat.Vecs[position.Pos], int64(ctr.lastIdx-1), repeats, resMP); err != nil {
								return err
							}
						}
					}
					repeatedProbeRows += repeats
				}
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
			if count == 1 && !repeatPayload {
				batchIndex, row := ctr.locateBuildRow(right[0])
				match := outputRow{left: ctr.leftBat, leftRow: left[0], right: ctr.retainedOutputBatch(batchIndex), rightRow: row}
				if err := ctr.appendOutputRow(join, proc, match); err != nil {
					return err
				}
			} else {
				for column, position := range join.ResultCols {
					if position.Rel == 0 {
						if repeatPayload && repeatProbeArea(ctr.leftBat.Vecs[position.Pos]) {
							continue
						}
						if err := ctr.resBat.Vecs[column].Union(ctr.leftBat.Vecs[position.Pos], left, resMP); err != nil {
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

func repeatProbeArea(source *vector.Vector) bool {
	return source.GetType().IsVarlen() && len(source.GetArea()) != 0
}

// Non-inline probe payload is copied once when its duplicate group starts.
func (ctr *container) probePayloadFits(join *HashJoin, target int64) bool {
	for i, pos := range join.ResultCols {
		if pos.Rel != 0 || !ctr.resBat.Vecs[i].GetType().IsVarlen() {
			continue
		}
		bytes := vector.VarlenaPayloadSize(ctr.leftBat.Vecs[pos.Pos], ctr.lastIdx-1)
		if bytes > target-int64(len(ctr.resBat.Vecs[i].GetArea())) {
			return false
		}
	}
	return true
}

// Use the bytes already copied so a wide, unselected build row does not
// reduce all subsequent output batches to the worst-case row count.
func (ctr *container) innerGatherEnd(join *HashJoin, target int64, rows, end int) int {
	for i, pos := range join.ResultCols {
		dst := ctr.resBat.Vecs[i]
		if pos.Rel == 1 && dst.GetType().IsVarlen() {
			room := target - int64(len(dst.GetArea()))
			end = min(end, rows+int(room/ctr.gatherBuildRowBytes))
		}
	}
	return end
}

func (ctr *container) fitRemainingInnerGroup(join *HashJoin, count, rows int) (int, bool) {
	var scratch [8]outputArea
	areas := ctr.outputAreas(scratch[:0], true)
	for i := range areas {
		area := &areas[i]
		area.remaining -= int64(len(ctr.resBat.Vecs[area.column].GetArea()))
	}
	return ctr.fitGroupOutput(join, areas, int64(ctr.lastIdx-1), count, rows)
}
