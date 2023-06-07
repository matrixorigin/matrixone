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
	step        uint64
	values      []uint64
	minCanAdded uint64
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

func (r *ranges) current() uint64 {
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if from < to {
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

func (r *ranges) setManual(
	value uint64,
	skipped *ranges) {
	newValues := r.values[:0]
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if to <= value {
			skipped.add(from, to)
			continue
		}
		if from > value {
			newValues = append(newValues, from, to)
			continue
		}
		skipped.add(from, value)
		if value+1 < to {
			newValues = append(newValues, value+1, to)
		}
	}
	r.values = newValues
}

func (r *ranges) add(from, to uint64) {
	if r.minCanAdded >= to {
		return
	}
	if r.minCanAdded >= from {
		from = r.minCanAdded
	}
	if from < to {
		r.values = append(r.values, from, to)
	}
	r.minCanAdded = to
}

// updateTo after updateTo returns, make sure that the value
// returned by ranges.next() must be greater than value.
// Return true means the value is include in old ranges, otherwise
// the value of this value must be updated to the store to avoid
// skipping the value when restarting or other cache is allocated
// next time.
func (r *ranges) updateTo(value uint64) bool {
	n := r.rangeCount()
	compactTo := -1
	contains := false
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if from > value {
			contains = true
			break
		}
		if value >= to {
			compactTo = 2*i + 2
			continue
		}
		r.values[2*i] = value
		contains = true
		break
	}
	if compactTo != -1 {
		r.values = r.values[compactTo:]
	}
	if !contains {
		r.minCanAdded = value
	}
	return contains
}
