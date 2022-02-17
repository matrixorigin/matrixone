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

package tuplecodec

import "bytes"

// Less decides the key is less than another key
func (tk TupleKey) Less(another TupleKey) bool {
	return bytes.Compare(tk,another) < 0
}

// Compare compares the key with another key
func (tk TupleKey) Compare(another TupleKey) int {
	return bytes.Compare(tk,another)
}

// Equal decides the key is equal to another key
func (tk TupleKey) Equal(another TupleKey) bool {
	return bytes.Equal(tk,another)
}

// IsPredecessor decides the is the predecessor of the another key
func (tk TupleKey) IsPredecessor(another TupleKey) bool {
	//1. the length of the key + 1 == the length of the another key.
	//2. the last byte of the another key is zero.
	//3. other bytes are equal.
	return len(tk) + 1 == len(another) &&
		another[len(another) - 1] == 0 &&
		tk.Equal(another[:len(another) - 1])
}

// SuccessorOfKey gets the successor of the key.
// Carefully, key can not be changed.
func SuccessorOfKey(key TupleKey) TupleKey {
	l := len(key) + 1
	if cap(key) > len(key) {
		ret := key[:l]
		if ret[l - 1] == 0 {
			return ret
		}
	}

	ret := make([]byte, l)
	copy(ret,key)
	ret[l - 1] = 0
	return ret
}

// SuccessorOfPrefix gets the
func SuccessorOfPrefix(prefix TupleKey) TupleKey {
	if len(prefix) == 0 {
		return TupleKey{0xFF}
	}
	ret := make([]byte,len(prefix))
	copy(ret,prefix)
	for i := len(ret) - 1; i >= 0 ; i-- {
		ret[i] = ret[i] + 1
		if ret[i] != 0 {
			return ret[:i + 1]
		}
	}
	return prefix
}

func (r Range) IsValid() bool {
	if len(r.startKey) == 0 &&
			len(r.endKey) == 0 {
		return false
	}

	//endKey can be empty
	if len(r.endKey) == 0{
		return true
	}

	//startKey < endKey
	if bytes.Compare(r.startKey,r.endKey) >= 0 {
		return false
	}

	return true
}

// Contain checks the key in the range
func (r Range) Contain(key TupleKey) bool  {
	//startKey <= key < endKey
	return bytes.Compare(key,r.startKey) >= 0 &&
		bytes.Compare(key,r.endKey) < 0
}

// Equal checks the range is equal to the another range.
func (r Range) Equal(another Range) bool {
	return bytes.Compare(r.startKey,another.startKey) == 0 &&
		bytes.Compare(r.endKey,another.endKey) == 0
}

// Merge merges two ranges
func (r Range) Merge(another Range) Range  {
	if !(r.IsValid() && another.IsValid()) {
		return Range{}
	}

	start := r.startKey
	end := r.startKey
	//choose the end of the range R as the final endKey.
	if len(r.endKey) > 0 {
		end = r.endKey
	}

	if another.startKey.Less(start) {
		start = another.startKey
	}

	if end.Less(another.startKey) {
		end = another.startKey
	}

	if len(another.endKey) > 0 && end.Less(another.endKey) {
		end = another.endKey
	}

	if start.Equal(end) {
		return Range{startKey: start}
	}else if r.startKey.Equal(end) || another.startKey.Equal(end) {
		return Range{startKey: start, endKey: SuccessorOfKey(end)}
	}
	return Range{startKey: start,endKey: end}
}

// Overlap checks the range overlaps or not.
func (r Range) Overlap(another Range) bool {
	if !(r.IsValid() && another.IsValid()) {
		return false
	}

	if len(r.endKey) == 0 && len(another.endKey) == 0 {
		return bytes.Compare(r.startKey,another.startKey) == 0
	}else if len(r.endKey) == 0 {
		return bytes.Compare(r.startKey,another.startKey) >= 0 &&
			bytes.Compare(r.startKey,another.endKey) < 0
	}else if len(another.endKey) == 0 {
		return bytes.Compare(another.startKey,r.startKey) >= 0 &&
			bytes.Compare(another.startKey,r.endKey) < 0
	}
	return bytes.Compare(r.endKey,another.startKey) > 0 &&
		bytes.Compare(r.startKey,another.endKey) < 0
}

// Intersect gets the intersected range.
func (r Range) Intersect(another Range) Range {
	if !r.Overlap(another) {
		return Range{}
	}

	if len(r.endKey) == 0 {
		return r
	}

	if len(another.endKey) == 0 {
		return another
	}

	start := r.startKey
	if start.Less(another.startKey) {
		start = another.startKey
	}

	end := r.endKey
	if another.endKey.Less(end) {
		end = another.endKey
	}
	return Range{start,end}
}

// Contain check the range contains the another range.
func (r Range) ContainRange(another Range) bool {
	if !r.Overlap(another) {
		return false
	}

	if len(r.endKey) == 0 && len(another.endKey) == 0 {
		return r.startKey.Equal(another.startKey)
	}else if len(r.endKey) == 0 {
		return false
	}else if len(another.endKey) == 0 {
		return bytes.Compare(another.startKey,r.startKey) >= 0 &&
			bytes.Compare(another.startKey,r.endKey) < 0
	}
	return bytes.Compare(r.startKey,another.startKey) <= 0 &&
		bytes.Compare(r.endKey,another.endKey) >= 0
}