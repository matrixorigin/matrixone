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

package intersectall

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(IntersectAll)

type container struct {
	colexec.ReceiverOperator

	// operator state: Build, Probe or End
	state int

	// helper data structure during probe
	counter []uint64

	// built for the smaller of the two relations
	hashTable *hashmap.StrHashMap

	inserted      []uint8
	resetInserted []uint8

	buf *batch.Batch
}

type IntersectAll struct {
	// execution container
	ctr *container

	vm.OperatorBase
}

func (intersectAll *IntersectAll) GetOperatorBase() *vm.OperatorBase {
	return &intersectAll.OperatorBase
}

func init() {
	reuse.CreatePool[IntersectAll](
		func() *IntersectAll {
			return &IntersectAll{}
		},
		func(a *IntersectAll) {
			*a = IntersectAll{}
		},
		reuse.DefaultOptions[IntersectAll]().
			WithEnableChecker(),
	)
}

func (intersectAll IntersectAll) TypeName() string {
	return opName
}

func NewArgument() *IntersectAll {
	return reuse.Alloc[IntersectAll](nil)
}

func (intersectAll *IntersectAll) Release() {
	if intersectAll != nil {
		reuse.Free[IntersectAll](intersectAll, nil)
	}
}

func (intersectAll *IntersectAll) Reset(proc *process.Process, pipelineFailed bool, err error) {
	intersectAll.Free(proc, pipelineFailed, err)
}

func (intersectAll *IntersectAll) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := intersectAll.ctr
	if ctr != nil {
		ctr.cleanHashMap()
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
		intersectAll.ctr = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.hashTable != nil {
		ctr.hashTable.Free()
		ctr.hashTable = nil
	}
}
