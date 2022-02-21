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

package mergeorder

import (
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
)

// state values
const (
	running = iota
	end
)

type container struct {
	// state signs the statement of mergeOrder operator
	//	1. if state is running, operator still range the mergeReceivers to do merge-sort.
	//	2. if state is end, operator has done and should push data to next operator.
	state uint8

	attrs []string // sorted list of attributes
	ds    []bool   // Directions, ds[i] == true: the attrs[i] are in descending order

	cmps []compare.Compare // compare structures used to do sort work for attrs

	// bat store the result of merge-order
	bat *batch.Batch
}

type Argument struct {
	// Fields store the order information
	Fields []order.Field
	// ctr stores the attributes needn't do Serialization work
	ctr container
}
