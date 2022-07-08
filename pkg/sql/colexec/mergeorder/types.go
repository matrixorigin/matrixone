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

const (
	Build = iota
	Eval
	End
)

type Container struct {
	n     int // result vector number
	state int
	poses []int32           // sorted list of attributes
	cmps  []compare.Compare // compare structures used to do sort work for attrs

	bat *batch.Batch // bat store the result of merge-order
}

type Argument struct {
	Fs  []order.Field // Fields store the order information
	ctr *Container    // ctr stores the attributes needn't do Serialization work
}
