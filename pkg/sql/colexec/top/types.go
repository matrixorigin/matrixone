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

package top

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Eval
)

type container struct {
	n             int // result vector number
	state         int
	sels          []int64
	poses         []int32 // sorted list of attributes
	cmps          []compare.Compare
	NullIdxs      map[uint32]struct{} // records which attr type is T_any
	finishNullIdx bool                // flag to tell the NullIdxs has been filled
	tempVecs      []*vector.Vector
	bat           *batch.Batch
}

type Argument struct {
	Limit int64
	ctr   *container
	Fs    []*plan.OrderBySpec
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) compare(vi, vj int, i, j int64) int {
	for _, pos := range ctr.poses {
		// for order by cols, if there is a T_any type in cols,
		// we won't init a compare
		if ctr.cmps[pos] == nil {
			continue
		}
		if r := ctr.cmps[pos].Compare(vi, vj, i, j); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) Len() int {
	return len(ctr.sels)
}

func (ctr *container) Less(i, j int) bool {
	return ctr.compare(0, 0, ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}

func (ctr *container) SplitTAny() {
	if ctr.bat == nil {
		return
	}
	for i := range ctr.NullIdxs {
		ctr.tempVecs = append(ctr.tempVecs, ctr.bat.Vecs[i])
	}
	tempVecs := ctr.bat.Vecs
	ctr.bat.Vecs = nil
	for i := 0; i < len(tempVecs); i++ {
		if _, ok := ctr.NullIdxs[uint32(i)]; !ok {
			ctr.bat.Vecs = append(ctr.bat.Vecs, tempVecs[i])
		}
	}
}

func (ctr *container) Reset() {
	defer func() {
		ctr.tempVecs = nil
	}()
	if ctr.bat == nil {
		return
	}
	tempVecs := ctr.bat.Vecs
	ctr.bat.Vecs = nil
	j := 0
	k := 0
	for i := 0; i < len(tempVecs)+len(ctr.tempVecs); i++ {
		if _, ok := ctr.NullIdxs[uint32(i)]; !ok {
			ctr.bat.Vecs = append(ctr.bat.Vecs, tempVecs[j])
			j++
		} else {
			ctr.bat.Vecs = append(ctr.bat.Vecs, ctr.tempVecs[k])
			k++
		}
	}
}

func (ctr *container) ExpandTAny() {
	if ctr.bat != nil {
		for i := range ctr.NullIdxs {
			ctr.bat.Vecs[i] = vector.NewConstNull(types.T_any.ToType(), len(ctr.bat.Zs))
		}
	}
}
