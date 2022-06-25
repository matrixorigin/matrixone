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

package mergesort

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"

func Shuffle(col containers.Vector, idx []uint32) {
	ret := containers.MakeVector(col.GetType(), col.Nullable())
	for _, j := range idx {
		ret.Append(col.Get(int(j)))
	}

	col.ResetWithData(ret.Bytes(), ret.NullMask())
	ret.Close()
}

func Multiplex(col []containers.Vector, src []uint32, fromLayout, toLayout []uint32) (ret []containers.Vector) {
	ret = make([]containers.Vector, len(toLayout))
	to := len(toLayout)
	cursors := make([]int, len(fromLayout))

	for i := 0; i < to; i++ {
		ret[i] = containers.MakeVector(col[0].GetType(), col[0].Nullable())
	}

	k := 0
	for i := 0; i < to; i++ {
		for j := 0; j < int(toLayout[i]); j++ {
			s := src[k]
			ret[i].Append(col[s].Get(cursors[s]))
			cursors[s]++
			k++
		}
	}

	for _, v := range col {
		v.Close()
	}
	return
}
