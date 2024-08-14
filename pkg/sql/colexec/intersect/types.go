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

var _ vm.Operator = new(Intersect)

const (
	build = iota
	probe
	end
)

type Intersect struct {
	ctr *container

	vm.OperatorBase
}

func (intersect *Intersect) GetOperatorBase() *vm.OperatorBase {
	return &intersect.OperatorBase
}

func init() {
	reuse.CreatePool[Intersect](
		func() *Intersect {
			return &Intersect{}
		},
		func(a *Intersect) {
			*a = Intersect{}
		},
		reuse.DefaultOptions[Intersect]().
			WithEnableChecker(),
	)
}

func (intersect Intersect) TypeName() string {
	return opName
}

func NewArgument() *Intersect {
	return reuse.Alloc[Intersect](nil)
}

func (intersect *Intersect) Release() {
	if intersect != nil {
		reuse.Free[Intersect](intersect, nil)
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
}

func (intersect *Intersect) Reset(proc *process.Process, pipelineFailed bool, err error) {
	intersect.Free(proc, pipelineFailed, err)
}

func (intersect *Intersect) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := intersect.ctr
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

		intersect.ctr = nil
	}
}
