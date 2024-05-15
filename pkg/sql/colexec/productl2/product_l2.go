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

package productl2

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "product_l2"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": product_l2 join ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ap := arg
	ctr := ap.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat != nil {
				if err := ctr.probe(ap, proc, anal, arg.GetIsLast(), &result); err != nil {
					return result, err
				}
				return result, nil
			}
			ctr.inBat, _, err = ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}

			if ctr.inBat == nil {
				ctr.state = End
				continue
			}
			if ctr.inBat.IsEmpty() {
				proc.PutBatch(ctr.inBat)
				ctr.inBat = nil
				continue
			}
			if ctr.bat == nil {
				proc.PutBatch(ctr.inBat)
				ctr.inBat = nil
				continue
			}
			anal.Input(ctr.inBat, arg.GetIsFirst())
			if err := ctr.probe(ap, proc, anal, arg.GetIsLast(), &result); err != nil {
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

func (ctr *container) build(proc *process.Process, anal process.Analyze) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(1, anal)
		if err != nil {
			return err
		}
		if bat == nil {
			break
		}
		ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), bat)
		if err != nil {
			return err
		}
		proc.PutBatch(bat)
	}
	return nil
}

func (ctr *container) probe(ap *Argument, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.inBat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}

	buildCount := ctr.bat.RowCount()
	probeCount := ctr.inBat.RowCount()
	var i, j int

	leastClusterIndex := 0
	leastDistance := 0.0
	for j = ctr.probeIdx; j < probeCount; j++ {

		for i = 0; i < buildCount; i++ {
			// find the nearest cluster center
			//TODO: generalize it for float64.
			clusterEmbedding := types.BytesToArray[float32](ctr.bat.Vecs[0].GetBytesAt(i))
			tblEmbedding := types.BytesToArray[float32](ctr.inBat.Vecs[0].GetBytesAt(i))

			dist, _ := moarray.L2Distance(clusterEmbedding, tblEmbedding)
			if leastDistance < dist {
				leastDistance = dist
				leastClusterIndex = i
			}
		}
		for k, rp := range ap.Result {
			if rp.Rel == 0 {
				if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
					return err
				}
			} else {
				if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(leastClusterIndex), proc.Mp()); err != nil {
					return err
				}
			}
		}

		if ctr.rbat.Vecs[0].Length() >= colexec.DefaultBatchSize {
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
			ctr.probeIdx = j + 1
			return nil
		}
	}
	// ctr.rbat.AddRowCount(count * count2)
	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat

	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}
