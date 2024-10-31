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

// receiver will be a unlimited queue for int8.
// I ensure that the elements will never be full, so it is no need set a lock here.
type receiver struct {
	lastPop    uint32
	hasLastPop bool

	head, tail int
	andBase    int
	elements   []uint32
}

func nextPowerOfTwo(k uint32) uint32 {
	if k&(k-1) == 0 {
		return k
	}
	k--
	k |= k >> 1
	k |= k >> 2
	k |= k >> 4
	k |= k >> 8
	k |= k >> 16
	return k + 1
}

func newReceivers(count uint32, cp uint32) []receiver {
	// we should make sure cap will be the power of 2.
	cp = nextPowerOfTwo(cp)

	rs := make([]receiver, count)
	ab := int(cp - 1)
	for i := range rs {
		rs[i].hasLastPop = false
		rs[i].elements = make([]uint32, cp)
		rs[i].head, rs[i].tail = 0, 0
		rs[i].andBase = ab
	}
	return rs
}

func (r *receiver) getLastPop() (uint32, bool) {
	return r.lastPop, r.hasLastPop
}

func (r *receiver) flagLastPopRelease() {
	r.hasLastPop = false
}

func (r *receiver) popNextIndex() uint32 {
	r.lastPop = r.elements[r.head]
	r.hasLastPop = true
	r.head = (r.head + 1) & r.andBase
	return r.lastPop
}

func (r *receiver) pushNextIndex(index uint32) {
	r.elements[r.tail] = index
	r.tail = (r.tail + 1) & r.andBase
}
