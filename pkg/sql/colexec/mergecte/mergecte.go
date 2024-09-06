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

package mergecte

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_cte"

func (mergeCTE *MergeCTE) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge cte ")
}

func (mergeCTE *MergeCTE) OpType() vm.OpType {
	return vm.MergeCTE
}

func (mergeCTE *MergeCTE) Prepare(proc *process.Process) error {
	if mergeCTE.OpAnalyzer == nil {
		mergeCTE.OpAnalyzer = process.NewAnalyzer(mergeCTE.GetIdx(), mergeCTE.IsFirst, mergeCTE.IsLast, "merge cte")
	} else {
		mergeCTE.OpAnalyzer.Reset()
	}

	mergeCTE.ctr.curNodeCnt = int32(mergeCTE.NodeCnt)
	mergeCTE.ctr.status = sendInitial
	return nil
}

func (mergeCTE *MergeCTE) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := mergeCTE.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	result := vm.NewCallResult()
	var err error
	ctr := &mergeCTE.ctr

	switch ctr.status {
	case sendInitial:
		//result, err = mergeCTE.GetChildren(0).Call(proc)
		result, err = vm.ChildrenCall(mergeCTE.GetChildren(0), proc, analyzer)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}

		if result.Batch == nil {
			ctr.status = sendLastTag
		} else {
			if len(ctr.freeBats) > ctr.i {
				if ctr.freeBats[ctr.i] != nil {
					ctr.freeBats[ctr.i].CleanOnlyData()
				}
				ctr.freeBats[ctr.i], err = ctr.freeBats[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
				if err != nil {
					return result, err
				}
			} else {
				appBat, err := result.Batch.Dup(proc.Mp())
				if err != nil {
					return result, err
				}
				analyzer.Alloc(int64(appBat.Size()))
				ctr.freeBats = append(ctr.freeBats, appBat)
			}
			ctr.bats = append(ctr.bats, ctr.freeBats[ctr.i])
			ctr.i++
		}

		fallthrough
	case sendLastTag:
		if mergeCTE.ctr.status == sendLastTag {
			mergeCTE.ctr.status = sendRecursive
			if len(ctr.freeBats) > ctr.i {
				ctr.freeBats[ctr.i].SetLast()
			} else {
				ctr.freeBats = append(ctr.freeBats, makeRecursiveBatch(proc))
			}
			if len(mergeCTE.ctr.bats) == 0 {
				mergeCTE.ctr.bats = append(mergeCTE.ctr.bats, ctr.freeBats[ctr.i])
			} else {
				mergeCTE.ctr.bats[0] = ctr.freeBats[ctr.i]
			}
			ctr.i++
		}
	case sendRecursive:
		for !mergeCTE.ctr.last {
			result, err = vm.ChildrenCall(mergeCTE.GetChildren(1), proc, analyzer)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if result.Batch == nil {
				result.Status = vm.ExecStop
				return result, nil
			}

			if result.Batch.Last() {
				mergeCTE.ctr.curNodeCnt--
				if mergeCTE.ctr.curNodeCnt == 0 {
					mergeCTE.ctr.last = true
					mergeCTE.ctr.curNodeCnt = int32(mergeCTE.NodeCnt)
					if len(ctr.freeBats) > ctr.i {
						if ctr.freeBats[ctr.i] != nil {
							ctr.freeBats[ctr.i].CleanOnlyData()
						}
						ctr.freeBats[ctr.i], err = ctr.freeBats[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
						if err != nil {
							return result, err
						}
					} else {
						appBat, err := result.Batch.Dup(proc.Mp())
						if err != nil {
							return result, err
						}
						analyzer.Alloc(int64(appBat.Size()))
						ctr.freeBats = append(ctr.freeBats, appBat)
					}
					ctr.bats = append(ctr.bats, ctr.freeBats[ctr.i])
					ctr.i++
					break
				}
			} else {
				if len(ctr.freeBats) > ctr.i {
					if ctr.freeBats[ctr.i] != nil {
						ctr.freeBats[ctr.i].CleanOnlyData()
					}
					ctr.freeBats[ctr.i], err = ctr.freeBats[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
					if err != nil {
						return result, err
					}
				} else {
					appBat, err := result.Batch.Dup(proc.Mp())
					if err != nil {
						return result, err
					}
					analyzer.Alloc(int64(appBat.Size()))
					ctr.freeBats = append(ctr.freeBats, appBat)
				}
				ctr.bats = append(ctr.bats, ctr.freeBats[ctr.i])
				ctr.i++
			}

		}
	}

	mergeCTE.ctr.buf = mergeCTE.ctr.bats[0]
	mergeCTE.ctr.bats = mergeCTE.ctr.bats[1:]
	if mergeCTE.ctr.buf.Last() {
		mergeCTE.ctr.last = false
	}

	result.Batch = mergeCTE.ctr.buf
	result.Status = vm.ExecHasMore
	analyzer.Output(result.Batch)
	return result, nil
}

func makeRecursiveBatch(proc *process.Process) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool())
	batch.SetLength(b, 1)
	b.SetLast()
	return b
}
