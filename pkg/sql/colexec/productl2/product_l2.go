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
	"runtime"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
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

	metrictype, ok := metric.OpTypeToIvfMetric[strings.ToLower(productl2.VectorOpType)]
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "ProductL2: vector optype not found")
	}
	productl2.ctr.metrictype = metrictype

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

func getCenters[T types.RealNumbers](ap *Productl2, proc *process.Process, analyzer process.Analyzer) *metric.VectorSet[T] {
	ctr := &ap.ctr
	buildCount := ctr.bat.RowCount()
	centroidColPos := ap.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()

	dim := ctr.bat.Vecs[centroidColPos].GetType().Width
	elemSize := uint(ctr.bat.Vecs[centroidColPos].GetType().GetArrayElementSize())

	centers := metric.NewVectorSet[T](uint(buildCount), uint(dim), elemSize)

	for i := 0; i < buildCount; i++ {
		if ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
			centers.SetNull(int64(i))
			continue
		}

		c := types.BytesToArray[T](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
		centers.AddVector(int64(i), c)
	}
	return centers
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

	centroidColPos := productl2.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()
	switch ctr.bat.Vecs[centroidColPos].GetType().Oid {
	case types.T_array_float32:
		ctr.centers = getCenters[float32](productl2, proc, analyzer)
		index := brute_force.CreateIndex[float32]()
		index.BuildIndex(ctr.centers.(metric.VectorSetIf[float32]), ctr.metrictype)
		ctr.brute_force = index

	case types.T_array_float64:
		ctr.centers = getCenters[float64](productl2, proc, analyzer)
		index := brute_force.CreateIndex[float64]()
		index.BuildIndex(ctr.centers.(metric.VectorSetIf[float64]), ctr.metrictype)
		ctr.brute_force = index
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

func newMat[T types.RealNumbers](ctr *container, ap *Productl2) (*metric.VectorSet[T], error) {
	probeCount := ctr.inBat.RowCount()
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()

	dim := ctr.inBat.Vecs[tblColPos].GetType().Width
	elemSize := uint(ctr.inBat.Vecs[tblColPos].GetType().GetArrayElementSize())

	// embedding mat
	var probeset *metric.VectorSet[T]
	if ctr.queries == nil {
		probeset = metric.NewVectorSet[T](uint(probeCount), uint(dim), elemSize)
		ctr.queries = probeset
	} else {
		probeset = ctr.queries.(*metric.VectorSet[T])
		probeset.Reset(uint(probeCount), uint(dim), elemSize)
	}

	for j := 0; j < probeCount; j++ {

		if ctr.inBat.Vecs[tblColPos].IsNull(uint64(j)) {
			probeset.SetNull(int64(j))
			continue
		}
		v := types.BytesToArray[T](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
		probeset.AddVector(int64(j), v)

	}

	return probeset, nil
}

func (ctr *container) probe(ap *Productl2, proc *process.Process, result *vm.CallResult) error {
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()
	switch ctr.inBat.Vecs[tblColPos].GetType().Oid {
	case types.T_array_float32:
		return probeRun[float32](ctr, ap, proc, result)
	case types.T_array_float64:
		return probeRun[float64](ctr, ap, proc, result)
	}
	return nil
}

func probeRun[T types.RealNumbers](ctr *container, ap *Productl2, proc *process.Process, result *vm.CallResult) error {
	probeCount := ctr.inBat.RowCount()
	centers := ctr.centers.(*metric.VectorSet[T])

	ncpu := runtime.NumCPU()
	if probeCount < ncpu {
		ncpu = probeCount
	}

	if ap.MaxParallel > 1 {
		ncpu /= int(ap.MaxParallel)
		if ncpu < 1 {
			ncpu = 1
		}
	}

	leastClusterIndex := make([]int64, probeCount)

	for i := 0; i < probeCount; i++ {
		leastClusterIndex[i] = 0
	}

	probeset, err := newMat[T](ctr, ap)
	if err != nil {
		return err
	}

	if centers.GetCurr() > 0 && probeset.GetCurr() > 0 {

		keys, distances, err := metric.ExactSearch[T](centers, probeset, ctr.metrictype, 1, uint(ncpu))
		if err != nil {
			return err
		}
		_ = distances

		//os.Stderr.WriteString(fmt.Sprintf("keys %v\n", keys))
		//os.Stderr.WriteString(fmt.Sprintf("distances %v\n", distances))

		idxmap := probeset.GetIndexMap()

		for j := 0; j < int(probeset.Count); j++ {

			idx := idxmap[int64(j)]
			if idx == -1 {
				leastClusterIndex[j] = 0
			} else {
				leastClusterIndex[j] = int64(keys[idx])
			}
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(leastClusterIndex[j]), proc.Mp()); err != nil {
						return err
					}
				}
			}
		}
	} else {
		for j := 0; j < int(probeset.Count); j++ {

			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(0), proc.Mp()); err != nil {
						return err
					}
				}
			}
		}
	}

	// ctr.rbat.AddRowCount(count * count2)
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	result.Batch = ctr.rbat

	ctr.inBat = nil
	return nil
}
