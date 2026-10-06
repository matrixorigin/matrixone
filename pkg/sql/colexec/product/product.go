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

package product

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "product"

func (product *Product) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": cross join ")
}

func (product *Product) OpType() vm.OpType {
	return vm.Product
}

func (product *Product) Prepare(proc *process.Process) error {
	if product.allocationAccount == nil || product.resultAllocation == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if product.OpAnalyzer == nil {
		product.OpAnalyzer = process.NewAnalyzer(product.GetIdx(), product.IsFirst, product.IsLast, "cross join")
	} else {
		product.OpAnalyzer.Reset()
	}

	return nil
}

func (product *Product) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := product.OpAnalyzer

	ap := product
	ctr := &ap.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if ctr.inBat == nil { // get one batch from leftchild before receive from build side
				result, err = vm.ChildrenCall(product.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				ctr.inBat = result.Batch
				if ctr.inBat == nil {
					ctr.state = End
					continue
				}
				if ctr.inBat.Last() {
					ctr.inBat = nil
					return result, nil
				}
				if ctr.inBat.IsEmpty() {
					ctr.inBat = nil
					continue
				}
			}

			if err = product.build(proc, analyzer); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat == nil {
				result, err = vm.ChildrenCall(product.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				ctr.inBat = result.Batch
				if ctr.inBat == nil {
					ctr.state = End
					continue
				}
				if ctr.inBat.Last() {
					ctr.inBat = nil
					return result, nil
				}
				if ctr.inBat.IsEmpty() {
					ctr.inBat = nil
					continue
				}
			}
			if ctr.mp == nil {
				ctr.inBat = nil
				continue
			}

			if ctr.rbat == nil {
				buildBat := ctr.firstBuildBatch()
				if buildBat == nil {
					ctr.inBat = nil
					continue
				}
				ctr.rbat = batch.NewOffHeapWithSize(len(product.Result))
				for i, rp := range product.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*ctr.inBat.Vecs[rp.Pos].GetType())
					} else {
						ctr.rbat.Vecs[i] = vector.NewOffHeapVecWithType(*buildBat.Vecs[rp.Pos].GetType())
					}
				}
				if err := ctr.rbat.SetAllocationAccount(product.resultAllocation); err != nil {
					ctr.rbat.Clean(proc.Mp())
					ctr.rbat = nil
					return result, err
				}
			} else {
				ctr.rbat.CleanOnlyData()
			}

			if err := ctr.probe(ap, proc, &result); err != nil {
				return result, err
			}

			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (product *Product) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &product.ctr
	mp, err := process.MeasureWait(analyzer, resource.WaitOther, func() (*message.JoinMap, error) {
		return message.ReceiveJoinMap(product.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	})
	if err != nil {
		return err
	}
	if mp == nil {
		return nil
	}
	ctr.mp = mp
	return nil
}

func (ctr *container) probe(ap *Product, proc *process.Process, result *vm.CallResult) error {
	count := ctr.inBat.RowCount()
	batches := ctr.mp.GetBatches()
	for ctr.buildBatIdx < len(batches) {
		buildBat := batches[ctr.buildBatIdx]
		if buildBat == nil || buildBat.RowCount() == 0 {
			ctr.buildBatIdx++
			ctr.buildRowIdx = 0
			continue
		}
		for row := ctr.buildRowIdx; row < buildBat.RowCount(); row++ {
			for probeRow := 0; probeRow < count; probeRow++ {
				for k, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(probeRow), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[k].UnionOne(buildBat.Vecs[rp.Pos], int64(row), proc.Mp()); err != nil {
							return err
						}
					}
				}
			}
			ctr.rbat.AddRowCount(count)
			ctr.buildRowIdx = row + 1
			if ctr.rbat.RowCount() >= colexec.DefaultBatchSize {
				if ctr.buildRowIdx == buildBat.RowCount() {
					ctr.buildBatIdx++
					ctr.buildRowIdx = 0
				}
				result.Batch = ctr.rbat
				return nil
			}
		}
		ctr.buildBatIdx++
		ctr.buildRowIdx = 0
	}
	ctr.buildBatIdx = 0
	ctr.buildRowIdx = 0
	result.Batch = ctr.rbat
	ctr.inBat = nil
	return nil
}

func (ctr *container) firstBuildBatch() *batch.Batch {
	if ctr.mp == nil {
		return nil
	}
	for _, bat := range ctr.mp.GetBatches() {
		if bat != nil && bat.RowCount() > 0 {
			return bat
		}
	}
	return nil
}
