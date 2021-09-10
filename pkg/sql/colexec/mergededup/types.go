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

package mergededup

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/hash"
	"matrixone/pkg/intmap/fastmap"
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
	diffs  []bool
	matchs []int64
	hashs  []uint64
	sels   [][]int64    // sels
	slots  *fastmap.Map // hash code -> sels index
	bat    *batch.Batch
	groups map[uint64][]*hash.SetGroup // hash code -> group list
}

type Argument struct {
	Flg   bool // is local merge
	Attrs []string
	Ctr   Container
}
