// Copyright 2023 Matrix Origin
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

package disttae

import (
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type Version[T any] struct {
	Time  timestamp.Timestamp
	Value *T
}

type Versions[T any] struct {
	l        sync.RWMutex
	versions []Version[T]
}

func (v *Versions[T]) Set(ts timestamp.Timestamp, value *T) {
	v.l.Lock()
	defer v.l.Unlock()
	i := sort.Search(len(v.versions), func(i int) bool {
		return ts.GreaterEq(v.versions[i].Time)
	})
	if i < len(v.versions) {
		if v.versions[i].Time.Equal(ts) {
			// update
			v.versions[i].Value = value
		} else {
			// insert
			newSlice := make([]Version[T], 0, len(v.versions)+1)
			newSlice = append(newSlice, v.versions[:i]...)
			newSlice = append(newSlice, Version[T]{
				Time:  ts,
				Value: value,
			})
			newSlice = append(newSlice, v.versions[i:]...)
			v.versions = newSlice
		}
	} else {
		// append
		v.versions = append(v.versions, Version[T]{
			Time:  ts,
			Value: value,
		})
	}
}

func (v *Versions[T]) Get(ts timestamp.Timestamp) *T {
	v.l.RLock()
	defer v.l.RUnlock()
	i := sort.Search(len(v.versions), func(i int) bool {
		return ts.GreaterEq(v.versions[i].Time)
	})
l:
	if i < len(v.versions) {
		version := v.versions[i]
		if version.Time.Equal(ts) {
			if version.Value == nil {
				// deleted
				return nil
			}
			// get older
			i++
			goto l
		} else {
			return version.Value
		}
	}
	return nil
}
