// Copyright 2022 - 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type rMethod int

const (
	rMethodInt64 rMethod = iota
	rMethodInt64N
	rMethodFloat64
	rMethodExpFloat64
	rMethodNormalFloat64
	rMethodMax
)

type genRandomState struct {
	next  int64 // next random number
	total int64 // total number of random numbers

	genInt64 bool       // generate int64 or float64
	r        *rand.Rand // random number generator
	method   rMethod    // method to generate random numbers
	iMax     int64      // max value of int64, used by MethodInt64N

	batch *batch.Batch // output batch, we own it.
}

func generateRandomPrepare(proc *process.Process, tableFunction *TableFunction, genInt64 bool) (tvfState, error) {
	var err error
	st := &genRandomState{}
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	if err != nil {
		return nil, err
	}
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	st.genInt64 = genInt64
	return st, nil
}

func generateRandomInt64Prepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	return generateRandomPrepare(proc, tableFunction, true)
}

func generateRandomFloat64Prepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	return generateRandomPrepare(proc, tableFunction, false)
}

func (st *genRandomState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	var err error
	var totalVec, seedVec, methodVec, iMaxVec *vector.Vector

	if len(tf.ctr.executorsForArgs) == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "random tvf: at least one argument is required")
	}

	totalVec = tf.ctr.argVecs[0]
	if len(tf.ctr.executorsForArgs) > 1 {
		seedVec = tf.ctr.argVecs[1]
	}
	if len(tf.ctr.executorsForArgs) > 2 {
		methodVec = tf.ctr.argVecs[2]
	}
	if len(tf.ctr.executorsForArgs) > 3 {
		iMaxVec = tf.ctr.argVecs[3]
	}

	// total is always not null
	total, _, err := getInt64Value(proc, totalVec, nthRow, false, "gen_random total")
	if err != nil {
		return err
	}
	st.total = total

	// seed can be null
	var seed int64
	var seedNotNull bool
	if seedVec != nil {
		seed, seedNotNull, err = getInt64Value(proc, seedVec, nthRow, true, "gen_random seed")
		if err != nil {
			return err
		}
	}
	if seedNotNull {
		st.r = rand.New(rand.NewSource(seed))
	} else {
		st.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// method can be null
	var method string
	if methodVec != nil {
		if methodVec.GetType().Oid != types.T_varchar {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method must be varchar")
		}
		if !methodVec.GetNulls().Contains(uint64(nthRow)) {
			method = methodVec.GetStringAt(nthRow)
		}
	}
	if method == "" {
		if st.genInt64 {
			method = "int64"
		} else {
			method = "float64"
		}
	}
	switch method {
	case "int64":
		if !st.genInt64 {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method int64 can't be used to generate float64 random numbers")
		}
		st.method = rMethodInt64
	case "int64n":
		if !st.genInt64 {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method int64n can't be used to generate float64 random numbers")
		}
		st.method = rMethodInt64N
	case "float64":
		if st.genInt64 {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method float64 can't be used to generate int64 random numbers")
		}
		st.method = rMethodFloat64
	case "exp":
		if st.genInt64 {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method exp can't be used to generate int64 random numbers")
		}
		st.method = rMethodExpFloat64
	case "normal":
		if st.genInt64 {
			return moerr.NewInvalidInput(proc.Ctx, "gen_random method normal can't be used to generate int64 random numbers")
		}
		st.method = rMethodNormalFloat64
	default:
		return moerr.NewInvalidInput(proc.Ctx, "gen_random method must be one of int64, int64n, float64, exp, normal")
	}

	if iMaxVec != nil {
		if st.method != rMethodInt64N {
			return moerr.NewInvalidInput(proc.Ctx, "only int64n can have a max value")
		}
		st.iMax, _, err = getInt64Value(proc, iMaxVec, nthRow, false, "gen_random iMax")
		if err != nil {
			return err
		}
	} else {
		if st.method == rMethodInt64N {
			return moerr.NewInvalidInput(proc.Ctx, "int64n must have a max value")
		}
	}

	// whew, we're done with parameters.
	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}

	return nil
}

func (st *genRandomState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (st *genRandomState) reset(tf *TableFunction, proc *process.Process) {
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
}

func (st *genRandomState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
	}
}

func (st *genRandomState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	st.batch.CleanOnlyData()
	if st.next >= st.total {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}

	var cnt int64

	// generate at most 8192 random numbers a batch
	if st.next+8192 <= st.total {
		cnt = 8192
	} else {
		cnt = st.total - st.next
	}

	switch st.method {
	case rMethodInt64:
		for i := int64(0); i < cnt; i++ {
			vector.AppendFixed(st.batch.Vecs[0], st.next+i+1, false, proc.Mp())
			vector.AppendFixed(st.batch.Vecs[1], st.r.Int63(), false, proc.Mp())
		}
	case rMethodInt64N:
		for i := int64(0); i < cnt; i++ {
			vector.AppendFixed(st.batch.Vecs[0], st.next+i+1, false, proc.Mp())
			vector.AppendFixed(st.batch.Vecs[1], st.r.Int63n(st.iMax), false, proc.Mp())
		}
	case rMethodFloat64:
		for i := int64(0); i < cnt; i++ {
			vector.AppendFixed(st.batch.Vecs[0], st.next+i+1, false, proc.Mp())
			vector.AppendFixed(st.batch.Vecs[1], st.r.Float64(), false, proc.Mp())
		}
	case rMethodExpFloat64:
		for i := int64(0); i < cnt; i++ {
			vector.AppendFixed(st.batch.Vecs[0], st.next+i+1, false, proc.Mp())
			vector.AppendFixed(st.batch.Vecs[1], st.r.ExpFloat64(), false, proc.Mp())
		}
	case rMethodNormalFloat64:
		for i := int64(0); i < cnt; i++ {
			vector.AppendFixed(st.batch.Vecs[0], st.next+i+1, false, proc.Mp())
			vector.AppendFixed(st.batch.Vecs[1], st.r.NormFloat64(), false, proc.Mp())
		}
	}

	st.batch.SetRowCount(int(cnt))
	st.next += cnt
	return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
}
