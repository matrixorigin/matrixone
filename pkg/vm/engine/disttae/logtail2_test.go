// Copyright 2022 Matrix Origin
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

package disttae

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var m sync.Map

func sub1(id uint64) {
	m.Store(id, true)
}

func get1(id uint64) bool {
	_, b := m.Load(id)
	return b
}

func TestTableSubscribeMap1(t *testing.T) {
	initTableSubscribeRecord()
	lists := make([][]uint64, 8)
	k := 1000000
	tnum := 1000000
	for i := range lists {
		lists[i] = make([]uint64, 0)
		for j := 0; j < k; j++ {
			lists[i] = append(lists[i], uint64(rand.Int()%tnum))
		}
	}

	testRWtime := func(get func(uint64) bool, set func(uint64), datas [][]uint64) time.Duration {
		ch := make(chan bool, len(datas))
		tstart := time.Now()
		for i := 1; i < len(datas); i++ {
			k := i
			go func() {
				for _, id := range datas[k] {
					if get(id) {
						continue
					}
					set(id)
				}
				ch <- true
			}()
		}
		for i := 1; i < len(datas); i++ {
			<-ch
		}
		return time.Now().Sub(tstart)
	}

	println(fmt.Sprintf("sync map is %d", testRWtime(get1, sub1, lists)))
	println(fmt.Sprintf("btree is    %d", testRWtime(GetTableSubscribe, SetTableSubscribe, lists)))
	return
}
