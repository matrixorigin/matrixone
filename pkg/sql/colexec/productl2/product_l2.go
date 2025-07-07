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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func newMat[T types.RealNumbers](ctr *container, ap *Productl2) ([][]T, [][]T) {
	buildCount := ctr.bat.RowCount()
	probeCount := ctr.inBat.RowCount()
	centroidColPos := ap.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()
	centroidmat := make([][]T, buildCount)
	for i := 0; i < buildCount; i++ {
		switch ctr.bat.Vecs[centroidColPos].GetType().Oid {
		case types.T_array_float32:
			if ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
				centroidmat[i] = nil
				continue
			}
			centroidmat[i] = types.BytesToArray[T](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
		case types.T_array_float64:
			if ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
				centroidmat[i] = nil
				continue
			}
			centroidmat[i] = types.BytesToArray[T](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
		}
	}

	// embedding mat
	embedmat := make([][]T, probeCount)
	for j := 0; j < probeCount; j++ {

		switch ctr.bat.Vecs[centroidColPos].GetType().Oid {
		case types.T_array_float32:
			if ctr.inBat.Vecs[tblColPos].IsNull(uint64(j)) {
				embedmat[j] = nil
				continue
			}
			embedmat[j] = types.BytesToArray[T](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
		case types.T_array_float64:
			if ctr.inBat.Vecs[tblColPos].IsNull(uint64(j)) {
				embedmat[j] = nil
				continue
			}
			embedmat[j] = types.BytesToArray[T](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
		}

	}

	return centroidmat, embedmat
}

func (ctr *container) probe(ap *Productl2, proc *process.Process, result *vm.CallResult) error {
	centroidColPos := ap.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()
	switch ctr.bat.Vecs[centroidColPos].GetType().Oid {
	case types.T_array_float32:
		return probeRun[float32](ctr, ap, proc, result)
	case types.T_array_float64:
		return probeRun[float64](ctr, ap, proc, result)
	}
	return nil
}

func probeRun[T types.RealNumbers](ctr *container, ap *Productl2, proc *process.Process, result *vm.CallResult) error {
	buildCount := ctr.bat.RowCount()
	probeCount := ctr.inBat.RowCount()

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

	leastClusterIndex := make([]int, probeCount)
	leastDistance := make([]T, probeCount)

	for i := 0; i < probeCount; i++ {
		leastClusterIndex[i] = 0
		leastDistance[i] = metric.MaxFloat[T]()
	}

	centroidmat, embedmat := newMat[T](ctr, ap)
	distfn, err := metric.ResolveDistanceFn[T](ctr.metrictype)
	if err != nil {
		return moerr.NewInternalError(proc.Ctx, "ProductL2: failed to get distance function")
	}

	errs := make(chan error, ncpu)
	var mutex sync.Mutex
	var wg sync.WaitGroup

	for n := 0; n < ncpu; n++ {

		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for j := 0; j < probeCount; j++ {

				if j%ncpu != tid {
					continue
				}

				// for each row in probe table,
				// find the nearest cluster center from the build table.
				for i := 0; i < buildCount; i++ {

					if embedmat[j] == nil || centroidmat[i] == nil {
						leastDistance[j] = 0
						leastClusterIndex[j] = i
					} else {
						dist, err := distfn(centroidmat[i], embedmat[j])
						if err != nil {
							errs <- err
							return
						}
						if dist < leastDistance[j] {
							leastDistance[j] = dist
							leastClusterIndex[j] = i
						}
					}
				}

				err := func() error {
					mutex.Lock()
					defer mutex.Unlock()
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

					return nil
				}()

				if err != nil {
					errs <- err
					return
				}
			}
		}(n)
	}

	wg.Wait()

	if len(errs) > 0 {
		return <-errs
	}

	// ctr.rbat.AddRowCount(count * count2)
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	result.Batch = ctr.rbat

	ctr.inBat = nil
	return nil
}
