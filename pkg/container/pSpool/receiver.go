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

package pSpool

import "sync"

const (
	noneLastPop int8 = -1
)

// receiver will be a unlimited queue for int8.
type receiver struct {
	sync.Mutex

	lastPop int8
	queue   []int8
}

func newReceivers(count int) []receiver {
	rs := make([]receiver, count)
	for i := range rs {
		rs[i].lastPop = noneLastPop
	}
	return rs
}

func (r *receiver) getLastPop() int8 {
	return r.lastPop
}

func (r *receiver) popNextIndex() int8 {
	r.Lock()
	r.lastPop = r.queue[0]
	r.queue = r.queue[1:]
	r.Unlock()

	return r.lastPop
}

func (r *receiver) pushNextIndex(index int8) {
	r.Lock()
	r.queue = append(r.queue, index)
	r.Unlock()
}
