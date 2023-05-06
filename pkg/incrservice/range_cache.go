// Copyright 2023 Matrix Origin
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

package incrservice

type ranges struct {
	step   uint64
	values []uint64
}

func (r *ranges) rangeCount() int {
	return len(r.values) / 2
}

func (r *ranges) empty() bool {
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if from < to {
			return false
		}
	}
	return true
}

func (r *ranges) next() uint64 {
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if from < to {
			r.values[2*i] += r.step
			if r.values[2*i] == to {
				r.values = r.values[2*i+2:]
			}
			return from
		}
	}
	return 0
}

func (r *ranges) left() int {
	v := 0
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		v += int((to - from) / r.step)
	}
	return v
}

func (r *ranges) add(from, to uint64) {
	if len(r.values) > 0 && r.values[len(r.values)-1] > from {
		panic("invalid range")
	}
	r.values = append(r.values, from, to)
}
