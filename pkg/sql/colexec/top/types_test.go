// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package top

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func Test_container_reset(t *testing.T) {
	proc := testutil.NewProcess(t)

	bat := batch.New([]string{"id"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	buildBat := batch.New([]string{"id"})
	buildBat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())

	c := &container{
		n:                       0,
		state:                   0,
		sels:                    nil,
		poses:                   nil,
		cmps:                    nil,
		limit:                   0,
		limitExecutor:           nil,
		executorsForOrderColumn: nil,
		desc:                    false,
		topValueZM:              nil,
		bat:                     bat,
		buildBat:                buildBat,
	}
	c.reset(proc)
}
