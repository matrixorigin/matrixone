// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type distinctHash struct {
	mp   *mpool.MPool
	maps []*hashmap.StrHashMap
}

func (d *distinctHash) grows(more int) error {
	oldLen, newLen := len(d.maps), len(d.maps)+more
	d.maps = append(d.maps, make([]*hashmap.StrHashMap, more)...)

	var err error
	for i := oldLen; i < newLen; i++ {
		if d.maps[i], err = hashmap.NewStrMap(
			false, 0, 0, d.mp); err != nil {
			return err
		}
	}
	return nil
}

// fill inserts the row into the hash map.
// return true if this is a new value.
func (d *distinctHash) fill(group int, vs []*vector.Vector, row int) (bool, error) {
	insertOK, err := d.maps[group].Insert(vs, row)
	return !insertOK, err
}

// merge was the method to merge two groups of distinct agg.
// but distinct agg should be run in only one node and without any parallel.
// because the distinct agg should store all the source data to make sure the result is correct if we use parallel.
// e.g.
// select count(distinct a) from t;
// and a is a column with 1, 2, 3, 3, 5
// if we use parallel, and the data is split into two parts: [1, 2, 3] and [3, 5].
// once we do the merge, we will get the result 3 + 2, but the correct result should be 3 + 1.
// we need to loop the [3, 5] to do a new data fill to make sure the result is correct, but not do 3 + 2.
// this action to store all the source data is so expensive.
//
// I add this check to make sure the distinct agg is not used in parallel.
func (d *distinctHash) merge(next *distinctHash) error {
	if len(d.maps) > 0 || len(next.maps) > 0 {
		return moerr.NewInternalErrorNoCtx("distinct agg should be run in only one node and without any parallel")
	}
	return nil
}

func (d *distinctHash) free() {
	for _, m := range d.maps {
		if m != nil {
			m.Free()
		}
	}
}
