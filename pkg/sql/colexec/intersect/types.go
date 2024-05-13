// Copyright 2022 Matrix Origin
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

package intersect

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	build = iota
	probe
	end
)

type Argument struct {
	ctr *container

	// hash table bucket related information.
	IBucket uint64
	NBucket uint64

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

type container struct {
	colexec.ReceiverOperator

	// operator state
	state int

	// cnt record for intersect
	cnts [][]int64

	// Hash table for checking duplicate data
	hashTable *hashmap.StrHashMap

	// Result batch of intersec column execute operator
	btc *batch.Batch

	// process bucket mark
	inBuckets []uint8
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		if ctr.hashTable != nil {
			ctr.hashTable.Free()
			ctr.hashTable = nil
		}
		if ctr.btc != nil {
			ctr.btc.Clean(proc.Mp())
			ctr.btc = nil
		}
		if ctr.cnts != nil {
			for i := range ctr.cnts {
				proc.Mp().PutSels(ctr.cnts[i])
			}
			ctr.cnts = nil
		}
		ctr.FreeAllReg()

		arg.ctr = nil
	}
}
