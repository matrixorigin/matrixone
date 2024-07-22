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

package minus

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Minus)

const (
	buildingHashMap = iota
	probingHashMap
	operatorEnd
)

type Minus struct {
	ctr *container

	vm.OperatorBase
}

func (minus *Minus) GetOperatorBase() *vm.OperatorBase {
	return &minus.OperatorBase
}

func init() {
	reuse.CreatePool[Minus](
		func() *Minus {
			return &Minus{}
		},
		func(a *Minus) {
			*a = Minus{}
		},
		reuse.DefaultOptions[Minus]().
			WithEnableChecker(),
	)
}

func (minus Minus) TypeName() string {
	return opName
}

func NewArgument() *Minus {
	return reuse.Alloc[Minus](nil)
}

func (minus *Minus) Release() {
	if minus != nil {
		reuse.Free[Minus](minus, nil)
	}
}

type container struct {
	colexec.ReceiverOperator

	// operator execution stage.
	state int

	// hash table related
	hashTable *hashmap.StrHashMap

	// result batch of minus column execute operator
	bat *batch.Batch
}

func (minus *Minus) Reset(proc *process.Process, pipelineFailed bool, err error) {
	minus.Free(proc, pipelineFailed, err)
}

func (minus *Minus) Free(proc *process.Process, pipelineFailed bool, err error) {
	mp := proc.Mp()
	if minus.ctr != nil {
		minus.ctr.cleanBatch(mp)
		minus.ctr.cleanHashMap()
		minus.ctr.FreeAllReg()
		minus.ctr = nil
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.hashTable != nil {
		ctr.hashTable.Free()
		ctr.hashTable = nil
	}
}
