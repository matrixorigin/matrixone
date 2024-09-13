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

const (
	noneLastPop int8 = -1
)

// receiver will be a unlimited queue for int8.
type receiver struct {
	// todo: I ensure that this elements will never be full, so there is no need set a lock here.
	lastPop    int8
	head, tail int
	andBase    int
	elements   []int8
}

func newReceivers(count int, cp int32) []receiver {
	// we should make sure cap will be the power of 2.
	if cp&(cp-1) != 0 {
		cp |= cp >> 1
		cp |= cp >> 2
		cp |= cp >> 4
		cp |= cp >> 8
		cp |= cp >> 16
		cp++
	}

	rs := make([]receiver, count)
	ab := int(cp - 1)
	for i := range rs {
		rs[i].lastPop = noneLastPop
		rs[i].elements = make([]int8, cp)
		rs[i].head, rs[i].tail = 0, 0
		rs[i].andBase = ab
	}
	return rs
}

func (r *receiver) getLastPop() int8 {
	return r.lastPop
}

func (r *receiver) popNextIndex() int8 {
	r.lastPop = r.elements[r.head]
	r.head = (r.head + 1) & r.andBase
	return r.lastPop
}

func (r *receiver) pushNextIndex(index int8) {
	r.elements[r.tail] = index
	r.tail = (r.tail + 1) & r.andBase
}
