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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

var _ vm.Operator = new(MergeGroup)

const (
	Build = iota
	Eval
	End
)

const (
	H0 = iota
	H8
	HStr
)

type container struct {
	state int

	// mergeGroup operator result.
	res GroupResult

	// should use hash map or not and the hash map type.
	typ int

	// hash map related.
	hashKeyWidth   int
	groupByCol     int
	keyNullability bool
}

type MergeGroup struct {
	ctr container

	PartialResults     []any
	PartialResultTypes []types.T

	vm.OperatorBase
	colexec.Projection
}

func (mergeGroup *MergeGroup) GetOperatorBase() *vm.OperatorBase {
	return &mergeGroup.OperatorBase
}

func init() {
	reuse.CreatePool[MergeGroup](
		func() *MergeGroup {
			return &MergeGroup{}
		},
		func(a *MergeGroup) {
			*a = MergeGroup{}
		},
		reuse.DefaultOptions[MergeGroup]().
			WithEnableChecker(),
	)
}

func (mergeGroup MergeGroup) TypeName() string {
	return opName
}
