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

package nulls

// This file holds the Nulls operations outside the Gobra-verified core in
// nulls.go: raw pointers, serialization, string and array conversions,
// closures and generics. They use only the bitmap's own (trusted or
// verified) operations and must keep its invariant.

import (
	"fmt"
	"io"
	"unsafe"

	"golang.org/x/exp/constraints"
)

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.GetBitmap().Ptr()
}

// RawPtrLen returns the address of the first bitmap word and the logical
// length in bits, for passing the null map to C. It returns (0, 0) when there
// is no bitmap storage.
func (nsp *Nulls) RawPtrLen() (uintptr, uintptr) {
	if nsp == nil {
		return 0, 0
	}
	bm := nsp.GetBitmap()
	ptr := bm.Ptr()
	if ptr == nil {
		return 0, 0
	}
	return uintptr(unsafe.Pointer(ptr)), uintptr(bm.Len())
}

func String(nsp *Nulls) string {
	if nsp.IsEmpty() {
		return "[]"
	}
	return fmt.Sprintf("%v", nsp.ToArray())
}

func (nsp *Nulls) Show() ([]byte, error) {
	if nsp.IsEmpty() {
		return nil, nil
	}
	return nsp.np.Marshal(), nil
}

func (nsp *Nulls) MarshalSize() int {
	if nsp.IsEmpty() {
		return 0
	}
	return nsp.np.MarshalSize()
}

func (nsp *Nulls) MarshalTo(w io.Writer) error {
	if nsp.IsEmpty() {
		return nil
	}
	return nsp.np.MarshalTo(w)
}

// ShowV1 in version 1, bitmap is v1
func (nsp *Nulls) ShowV1() ([]byte, error) {
	if nsp.IsEmpty() {
		return nil, nil
	}
	return nsp.np.MarshalV1(), nil
}

func (nsp *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		// don't we need to reset?   Or we always, Read into a blank Nulls?
		// nsp.np.Reset()
		return nil
	}
	nsp.np.Unmarshal(data)
	return nil
}

func (nsp *Nulls) ReadNoCopy(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopy(data)
	return nil
}

func (nsp *Nulls) ReadNoCopyV1(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopyV1(data)
	return nil
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp.IsEmpty() {
		return []uint64{}
	}
	return nsp.np.ToArray()
}

func (nsp *Nulls) ToI64Array() []int64 {
	if nsp.IsEmpty() {
		return []int64{}
	}
	return nsp.np.ToI64Array(nil)
}

func (nsp *Nulls) Foreach(fn func(uint64) bool) {
	if nsp.IsEmpty() {
		return
	}
	itr := nsp.np.IteratorValue()
	for itr.HasNext() {
		row := itr.Next()
		if !fn(row) {
			break
		}
	}
}

func (nsp *Nulls) String() string {
	if nsp.IsEmpty() {
		return fmt.Sprintf("%v", []uint64{})
	}
	return nsp.np.String()
}

func ToArray[T constraints.Integer](nsp *Nulls) []T {
	if nsp.IsEmpty() {
		return []T{}
	}
	ret := make([]T, 0, nsp.Count())
	nsp.Foreach(func(row uint64) bool {
		ret = append(ret, T(row))
		return true
	})
	return ret
}
