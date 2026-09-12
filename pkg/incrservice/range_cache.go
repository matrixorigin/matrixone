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

import "github.com/matrixorigin/matrixone/pkg/pb/timestamp"

type ranges struct {
	step        uint64
	values      []uint64
	allocatedAt []timestamp.Timestamp
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
				r.allocatedAt = trimTimestamps(r.allocatedAt, i+1)
			}
			return from
		}
	}
	return 0
}

// nextFor returns the first value in the owned ranges which belongs to the
// statement series options.Offset + N*options.Increment.  The ranges are
// still owned in r.step-sized units; this method only advances the cursor and
// never changes r.step.  In production r.step is one, but solving the
// congruence also keeps old/non-unit metadata from silently producing values
// outside the requested series.
func (r *ranges) nextFor(options AutoIncrementOptions) uint64 {
	options = NormalizeAutoIncrementOptions(options.Increment, options.Offset)
	if options.isDefault() {
		return r.next()
	}
	if r.step == 0 || options.Increment == 0 {
		return 0
	}

	for i := 0; i < r.rangeCount(); {
		from, to := r.values[2*i], r.values[2*i+1]
		if from >= to {
			r.removeAt(i)
			continue
		}

		value, period, ok := nextValueInRange(
			from, to, r.step, options.Increment, options.Offset)
		if !ok {
			r.removeAt(i)
			continue
		}

		// There is no representable successor when the LCM or the addition
		// overflows.  The current value is still valid and can be returned.
		nextFrom, overflow := addUint64(value, period)
		if period == 0 || overflow || nextFrom >= to {
			r.removeAt(i)
		} else {
			r.values[2*i] = nextFrom
		}
		return value
	}
	return 0
}

func nextValueInRange(from, to, step, increment, offset uint64) (uint64, uint64, bool) {
	if step == 1 {
		// Production ranges are unit-step spans. Align directly to the session
		// residue; solving a modular inverse for every row is unnecessary. Keep
		// the congruence solver below for existing non-unit table metadata.
		residue, target := from%increment, offset%increment
		var delta uint64
		if target >= residue {
			delta = target - residue
		} else {
			delta = increment - (residue - target)
		}
		value, overflow := addUint64(from, delta)
		if overflow || value >= to {
			return 0, 0, false
		}
		return value, increment, true
	}
	// Solve step*k = offset-from (mod increment).  A solution exists only
	// when the gcd divides the right-hand side.  The first solution is enough
	// because all later solutions are separated by increment/gcd steps.
	g := gcdUint64(step, increment)
	fromResidue := from % increment
	offsetResidue := offset % increment
	var rhs uint64
	if offsetResidue >= fromResidue {
		rhs = offsetResidue - fromResidue
	} else {
		rhs = increment - (fromResidue - offsetResidue)
	}
	if rhs%g != 0 {
		return 0, 0, false
	}

	modulus := increment / g
	k := uint64(0)
	if modulus > 1 {
		inverse, ok := modularInverse((step/g)%modulus, modulus)
		if !ok {
			return 0, 0, false
		}
		product, overflow := multiplyUint64((rhs/g)%modulus, inverse)
		if overflow {
			// The frontend range is small, but remote process payloads are not
			// trusted to retain that bound.  Use overflow-safe modular
			// multiplication so malformed metadata cannot produce a wrong key.
			product = modularMultiply((rhs/g)%modulus, inverse, modulus)
		}
		k = product % modulus
	}

	delta, overflow := multiplyUint64(k, step)
	if overflow {
		return 0, 0, false
	}
	value, overflow := addUint64(from, delta)
	if overflow || value >= to {
		return 0, 0, false
	}
	period, overflow := multiplyUint64(step, modulus)
	if overflow {
		period = 0
	}
	return value, period, true
}

func gcdUint64(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func modularInverse(a, modulus uint64) (uint64, bool) {
	// The frontend limits increment to 65535.  Keep the arithmetic bounded
	// here as well so malformed remote state cannot overflow signed
	// intermediate values in the extended Euclidean algorithm.
	if modulus == 0 || modulus > uint64(^uint64(0)>>1) {
		return 0, false
	}
	oldR, r := int64(a), int64(modulus)
	oldS, s := int64(1), int64(0)
	for r != 0 {
		quotient := oldR / r
		oldR, r = r, oldR-quotient*r
		oldS, s = s, oldS-quotient*s
	}
	if oldR != 1 {
		return 0, false
	}
	if oldS < 0 {
		oldS += int64(modulus)
	}
	return uint64(oldS), true
}

func addUint64(a, b uint64) (uint64, bool) {
	value := a + b
	return value, value < a
}

func multiplyUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > ^uint64(0)/a {
		return 0, true
	}
	return a * b, false
}

func modularMultiply(a, b, modulus uint64) uint64 {
	if modulus == 1 {
		return 0
	}
	result := uint64(0)
	a %= modulus
	for b > 0 {
		if b&1 != 0 {
			if result >= modulus-a {
				result -= modulus - a
			} else {
				result += a
			}
		}
		b >>= 1
		if b == 0 {
			break
		}
		if a >= modulus-a {
			a -= modulus - a
		} else {
			a += a
		}
	}
	return result
}

func (r *ranges) removeAt(i int) {
	copy(r.values[2*i:], r.values[2*i+2:])
	r.values = r.values[:len(r.values)-2]
	if i < len(r.allocatedAt) {
		copy(r.allocatedAt[i:], r.allocatedAt[i+1:])
		r.allocatedAt = r.allocatedAt[:len(r.allocatedAt)-1]
	}
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
	// Each input range emits at most one remaining range, so compacting in
	// place cannot overwrite a range or timestamp that the loop has not read.
	newValues := r.values[:0]
	newAllocatedAt := r.allocatedAt[:0]
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		allocatedAt := timestampAt(r.allocatedAt, i)
		if to <= value {
			skipped.add(from, to)
			continue
		}
		if from > value {
			newValues = append(newValues, from, to)
			newAllocatedAt = append(newAllocatedAt, allocatedAt)
			continue
		}
		skipped.add(from, value)
		if value+1 < to {
			newValues = append(newValues, value+1, to)
			newAllocatedAt = append(newAllocatedAt, allocatedAt)
		}
	}
	r.values = newValues
	r.allocatedAt = newAllocatedAt
}

func (r *ranges) add(from, to uint64) {
	r.addWithTimestamp(from, to, timestamp.Timestamp{})
}

func (r *ranges) addWithTimestamp(from, to uint64, allocatedAt timestamp.Timestamp) {
	if r.minCanAdded >= to {
		return
	}
	if r.minCanAdded >= from {
		from = r.minCanAdded
	}
	if from < to {
		r.normalizeTimestamps()
		r.values = append(r.values, from, to)
		r.allocatedAt = append(r.allocatedAt, allocatedAt)
	}
	r.minCanAdded = to
}

func (r *ranges) oldestAllocateAt() timestamp.Timestamp {
	n := r.rangeCount()
	for i := 0; i < n; i++ {
		if r.values[2*i] < r.values[2*i+1] {
			return timestampAt(r.allocatedAt, i)
		}
	}
	return timestamp.Timestamp{}
}

// updateTo after updateTo returns, make sure that the value
// returned by ranges.next() must be greater than value.
// Return true means the value is include in old ranges, otherwise
// the value of this value must be updated to the store to avoid
// skipping the value when restarting or other cache is allocated
// next time.
func (r *ranges) updateTo(value uint64) bool {
	r.normalizeTimestamps()
	n := r.rangeCount()
	compactTo := 0
	contains := false
	for i := 0; i < n; i++ {
		from, to := r.values[2*i], r.values[2*i+1]
		if from > value {
			contains = true
			break
		}
		if value >= to {
			compactTo = i + 1
			continue
		}
		r.values[2*i] = value
		contains = true
		break
	}
	if compactTo > 0 {
		r.values = r.values[2*compactTo:]
		r.allocatedAt = r.allocatedAt[compactTo:]
	}
	if !contains {
		r.minCanAdded = value
	}
	return contains
}

func (r *ranges) normalizeTimestamps() {
	for len(r.allocatedAt) < r.rangeCount() {
		r.allocatedAt = append(r.allocatedAt, timestamp.Timestamp{})
	}
}

func timestampAt(values []timestamp.Timestamp, idx int) timestamp.Timestamp {
	if idx >= len(values) {
		return timestamp.Timestamp{}
	}
	return values[idx]
}

func trimTimestamps(values []timestamp.Timestamp, count int) []timestamp.Timestamp {
	if count >= len(values) {
		return nil
	}
	return values[count:]
}
