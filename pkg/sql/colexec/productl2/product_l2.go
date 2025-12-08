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
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/brute_force"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
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

	// TODO: create End() for ProductL2
	/*
		defer func() {
			if ctr.brute_force != nil {
				ctr.brute_force.Destroy()
				ctr.brute_force = nil
			}
		}()
	*/

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

func NewNullVector[T types.RealNumbers](dim int32) []T {
	// null vector with magnitude 1
	nullvec := make([]T, dim)
	nullvec[0] = 1
	return nullvec
}

func getIndex[T types.RealNumbers](ap *Productl2, proc *process.Process, analyzer process.Analyzer) (cache.VectorIndexSearchIf, error) {
	ctr := &ap.ctr
	buildCount := ctr.bat.RowCount()
	centroidColPos := ap.OnExpr.GetF().GetArgs()[0].GetCol().GetColPos()

	dim := ctr.bat.Vecs[centroidColPos].GetType().Width
	elemSize := uint(ctr.bat.Vecs[centroidColPos].GetType().GetArrayElementSize())
	centers := make([][]T, buildCount)
	nullvec := NewNullVector[T](dim)

	for i := 0; i < buildCount; i++ {
		if ctr.bat.Vecs[centroidColPos].IsNull(uint64(i)) {
			centers[i] = nullvec
			continue
		}

		c := types.BytesToArray[T](ctr.bat.Vecs[centroidColPos].GetBytesAt(i))
		centers[i] = c
	}

	algo, err := brute_force.NewBruteForceIndex[T](centers, uint(dim), ctr.metrictype, elemSize)
	if err != nil {
		return nil, err
	}

	err = algo.Load(sqlexec.NewSqlProcess(proc))
	if err != nil {
		return nil, err
	}

	return algo, nil
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
		ctr.brute_force, err = getIndex[float32](productl2, proc, analyzer)
		if err != nil {
			return err
		}
	case types.T_array_float64:
		ctr.brute_force, err = getIndex[float64](productl2, proc, analyzer)
		if err != nil {
			return err
		}
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

func newMat[T types.RealNumbers](ctr *container, ap *Productl2) ([][]T, error) {
	probeCount := ctr.inBat.RowCount()
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()

	dim := ctr.inBat.Vecs[tblColPos].GetType().Width
	nullvec := NewNullVector[T](dim)

	// embedding mat
	probes := make([][]T, probeCount)
	for j := 0; j < probeCount; j++ {

		if ctr.inBat.Vecs[tblColPos].IsNull(uint64(j)) {
			probes[j] = nullvec
			continue
		}
		v := types.BytesToArray[T](ctr.inBat.Vecs[tblColPos].GetBytesAt(j))
		probes[j] = v
	}

	return probes, nil
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

func (ctr *container) release() {
	if ctr.brute_force != nil {
		ctr.brute_force.Destroy()
		ctr.brute_force = nil
	}
}

func probeRun[T types.RealNumbers](ctr *container, ap *Productl2, proc *process.Process, result *vm.CallResult) error {
	probeCount := ctr.inBat.RowCount()
	tblColPos := ap.OnExpr.GetF().GetArgs()[1].GetCol().GetColPos()

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

	probes, err := newMat[T](ctr, ap)
	if err != nil {
		return err
	}

	rt := vectorindex.RuntimeConfig{Limit: 1, NThreads: uint(ncpu)}

	anykeys, distances, err := ctr.brute_force.Search(sqlexec.NewSqlProcess(proc), probes, rt)
	if err != nil {
		return err
	}
	_ = distances

	leastClusterIndex := anykeys.([]int64)

	//os.Stderr.WriteString(fmt.Sprintf("keys %v\n", keys))
	//os.Stderr.WriteString(fmt.Sprintf("distances %v\n", distances))

	for j := 0; j < probeCount; j++ {

		if ctr.inBat.Vecs[tblColPos].IsNull(uint64(j)) {
			leastClusterIndex[j] = 0
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

	// ctr.rbat.AddRowCount(count * count2)
	ctr.rbat.SetRowCount(ctr.rbat.Vecs[0].Length())
	result.Batch = ctr.rbat

	ctr.inBat = nil
	return nil
}
