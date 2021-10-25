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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/hash"
	"github.com/matrixorigin/matrixone/pkg/intmap/fastmap"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
)

const (
	Build = iota
	Eval
	End
)

const (
	UnitLimit = 1024
)

var (
	ZeroBools  []bool
	OneUint64s []uint64
)

type Container struct {
	n      int
	state  int
	rows   int64
	is     []int // index list
	diffs  []bool
	matchs []int64
	attrs  []string
	rattrs []string
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	vec    *vector.Vector
	refer  map[string]uint64
	groups map[uint64][]*hash.Group // hash code -> group list
}

type Argument struct {
	Flg   bool // is local merge
	Gs    []string
	Ctr   Container
	Refer map[string]uint64
	Es    []aggregation.Extend
}
