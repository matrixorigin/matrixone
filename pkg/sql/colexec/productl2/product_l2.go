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
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "product_l2"

func (productl2 *Productl2) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": product_l2 join ")
}

func (productl2 *Productl2) OpType() vm.OpType {
	return vm.ProductL2
}

func (productl2 *Productl2) Prepare(proc *process.Process) error {
	if productl2.OpAnalyzer == nil {
		productl2.OpAnalyzer = process.NewAnalyzer(productl2.GetIdx(), productl2.IsFirst, productl2.IsLast, "product_l2")
	} else {
		productl2.OpAnalyzer.Reset()
	}

	return nil
}

func (productl2 *Productl2) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := productl2.OpAnalyzer

	ap := productl2
	ctr := &ap.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := productl2.build(proc, analyzer); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat == nil {
				result, err = vm.ChildrenCall(productl2.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}

				ctr.inBat = result.Batch
				if ctr.inBat == nil {
					ctr.state = End
					continue
				}
				if ctr.inBat.IsEmpty() {
					ctr.inBat = nil
					continue
				}
				if ctr.bat == nil {
					ctr.inBat = nil
					continue
				}
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(productl2.Result))
				for i, rp := range productl2.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.inBat.Vecs[rp.Pos].GetType())
					} else {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.bat.Vecs[rp.Pos].GetType())
					}
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

func (productl2 *Productl2) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &productl2.ctr
	start := time.Now()
	defer analyzer.WaitStop(start)
	mp, err := message.ReceiveJoinMap(productl2.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	if err != nil {
		return err
	}
	if mp == nil {
		return nil
	}
	batches := mp.GetBatches()
	//maybe optimize this in the future
	for i := range batches {
		ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), batches[i])
		if err != nil {
			return err
		}
	}
	mp.Free()
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

func (ctr *container) probe(ap *Productl2, proc *process.Process, result *vm.CallResult) error {
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
			if !tblEmbeddingF32IsNull {
				tblEmbeddingF32 = types.BytesToArray[float32](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
			}

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
				if tblEmbeddingF32IsNull || ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
					leastDistance = 0
					leastClusterIndex = i
				} else {
					clusterEmbeddingF32 = types.BytesToArray[float32](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))

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
			if !tblEmbeddingF64IsNull {
				tblEmbeddingF64 = types.BytesToArray[float64](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
			}

			//normalizeTblEmbeddingPtrF64 = arrayF64Pool.Get().(*[]float64)
			//normalizeTblEmbeddingF64 = *normalizeTblEmbeddingPtrF64
			//if cap(normalizeTblEmbeddingF64) < len(tblEmbeddingF64) {
			//	normalizeTblEmbeddingF64 = make([]float64, len(tblEmbeddingF64))
			//} else {
			//	normalizeTblEmbeddingF64 = normalizeTblEmbeddingF64[:len(tblEmbeddingF64)]
			//}
			//_ = moarray.NormalizeL2[float64](tblEmbeddingF64, normalizeTblEmbeddingF64)

			for i = 0; i < buildCount; i++ {
				if tblEmbeddingF64IsNull || ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
					leastDistance = 0
					leastClusterIndex = i
				} else {
					clusterEmbeddingF64 = types.BytesToArray[float64](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))

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
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
			ctr.probeIdx = j + 1
			return nil
		}
	}
	// ctr.rbat.AddRowCount(count * count2)
	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	result.Batch = ctr.rbat

	ctr.inBat = nil
	return nil
}
