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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
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
			msg := ctr.ReceiveFromSingleReg(0, anal)
			if msg.Err != nil {
				return result, msg.Err
			}

			ctr.inBat = msg.Batch
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
	var err error
	for {
		msg := ctr.ReceiveFromSingleReg(1, anal)
		if msg.Err != nil {
			return msg.Err
		}
		bat := msg.Batch
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

//var (
//	arrayF32Pool = sync.Pool{
//		New: func() interface{} {
//			s := make([]float32, 0)
//			return &s
//		},
//	}
//	arrayF64Pool = sync.Pool{
//		New: func() interface{} {
//			s := make([]float64, 0)
//			return &s
//		},
//	}
//)

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

	var leastClusterIndex int
	var leastDistance float64

	centroidColPos := ap.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()

	var clusterEmbeddingF32 []float32
	var tblEmbeddingF32 []float32
	//var normalizeTblEmbeddingPtrF32 *[]float32
	//var normalizeTblEmbeddingF32 []float32

	var clusterEmbeddingF64 []float64
	var tblEmbeddingF64 []float64
	//var normalizeTblEmbeddingPtrF64 *[]float64
	//var normalizeTblEmbeddingF64 []float64

	for j = ctr.probeIdx; j < probeCount; j++ {
		leastClusterIndex = 0
		leastDistance = math.MaxFloat64

		// for each row in probe table,
		// find the nearest cluster center from the build table.
		switch ctr.bat.Vecs[centroidColPos].GetType().Oid {
		case types.T_array_float32:
			tblEmbeddingF32IsNull := ctr.inBat.Vecs[tblColPos].IsNull(uint64(j))
			tblEmbeddingF32 = types.BytesToArray[float32](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))

			//// NOTE: make sure you normalize_l2 probe vector once.
			//normalizeTblEmbeddingPtrF32 = arrayF32Pool.Get().(*[]float32)
			//normalizeTblEmbeddingF32 = *normalizeTblEmbeddingPtrF32
			//if cap(normalizeTblEmbeddingF32) < len(tblEmbeddingF32) {
			//	normalizeTblEmbeddingF32 = make([]float32, len(tblEmbeddingF32))
			//} else {
			//	normalizeTblEmbeddingF32 = normalizeTblEmbeddingF32[:len(tblEmbeddingF32)]
			//}
			//_ = moarray.NormalizeL2[float32](tblEmbeddingF32, normalizeTblEmbeddingF32)

			for i = 0; i < buildCount; i++ {
				clusterEmbeddingF32IsNull := ctr.bat.Vecs[centroidColPos].IsNull(uint64(i))
				clusterEmbeddingF32 = types.BytesToArray[float32](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
				if tblEmbeddingF32IsNull || clusterEmbeddingF32IsNull {
					leastDistance = 0
					leastClusterIndex = i
				} else {
					dist, err := moarray.L2DistanceSq[float32](clusterEmbeddingF32, tblEmbeddingF32)
					if err != nil {
						return err
					}
					if dist < leastDistance {
						leastDistance = dist
						leastClusterIndex = i
					}
				}
			}
			//// article:https://blog.mike.norgate.xyz/unlocking-go-slice-performance-navigating-sync-pool-for-enhanced-efficiency-7cb63b0b453e
			//*normalizeTblEmbeddingPtrF32 = normalizeTblEmbeddingF32
			//arrayF32Pool.Put(normalizeTblEmbeddingPtrF32)
		case types.T_array_float64:
			tblEmbeddingF64IsNull := ctr.inBat.Vecs[tblColPos].IsNull(uint64(j))
			tblEmbeddingF64 = types.BytesToArray[float64](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))

			//normalizeTblEmbeddingPtrF64 = arrayF64Pool.Get().(*[]float64)
			//normalizeTblEmbeddingF64 = *normalizeTblEmbeddingPtrF64
			//if cap(normalizeTblEmbeddingF64) < len(tblEmbeddingF64) {
			//	normalizeTblEmbeddingF64 = make([]float64, len(tblEmbeddingF64))
			//} else {
			//	normalizeTblEmbeddingF64 = normalizeTblEmbeddingF64[:len(tblEmbeddingF64)]
			//}
			//_ = moarray.NormalizeL2[float64](tblEmbeddingF64, normalizeTblEmbeddingF64)

			for i = 0; i < buildCount; i++ {
				clusterEmbeddingF64IsNull := ctr.bat.Vecs[centroidColPos].IsNull(uint64(i))
				clusterEmbeddingF64 = types.BytesToArray[float64](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
				if tblEmbeddingF64IsNull || clusterEmbeddingF64IsNull {
					leastDistance = 0
					leastClusterIndex = i
				} else {
					dist, err := moarray.L2DistanceSq[float64](clusterEmbeddingF64, tblEmbeddingF64)
					if err != nil {
						return err
					}
					if dist < leastDistance {
						leastDistance = dist
						leastClusterIndex = i
					}
				}
			}
			//*normalizeTblEmbeddingPtrF64 = normalizeTblEmbeddingF64
			//arrayF64Pool.Put(normalizeTblEmbeddingPtrF64)
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
