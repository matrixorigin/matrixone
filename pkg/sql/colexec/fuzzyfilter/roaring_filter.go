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
package fuzzyfilter

import (
	"strconv"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type canUseRoaring interface {
	int8 | int16 | int32 | uint8 | uint16 | uint32
}

type roaringFilter struct {
	b *roaring.Bitmap

	addFunc        func(f *roaringFilter, v *vector.Vector)
	testFunc       func(f *roaringFilter, v *vector.Vector) (int, any)
	testAndAddFunc func(f *roaringFilter, v *vector.Vector) (int, any)
}

func newRoaringFilter(t types.T) *roaringFilter {
	f := &roaringFilter{b: roaring.New()}

	switch t {
	case types.T_int8:
		f.addFunc = addFunc[int8]
		f.testFunc = testFunc[int8]
		f.testAndAddFunc = testAndAddFunc[int8]
	case types.T_int16:
		f.addFunc = addFunc[int16]
		f.testFunc = testFunc[int16]
		f.testAndAddFunc = testAndAddFunc[int16]
	case types.T_int32:
		f.addFunc = addFunc[int32]
		f.testFunc = testFunc[int32]
		f.testAndAddFunc = testAndAddFunc[int32]
	case types.T_uint8:
		f.addFunc = addFunc[uint8]
		f.testFunc = testFunc[uint8]
		f.testAndAddFunc = testAndAddFunc[uint8]
	case types.T_uint16:
		f.addFunc = addFunc[uint16]
		f.testFunc = testFunc[uint16]
		f.testAndAddFunc = testAndAddFunc[uint16]
	case types.T_uint32:
		f.addFunc = addFunc[uint32]
		f.testFunc = testFunc[uint32]
		f.testAndAddFunc = testAndAddFunc[uint32]
	default:
		panic("unsupported type for roaring filter: " + t.String())
	}

	return f
}

func addFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) {
	ss := vector.MustFixedColNoTypeCheck[T](v)
	nsp := v.GetNulls()
	for i, s := range ss {
		// SQL standard: NULL != NULL, skip NULLs from duplicate check
		if nsp.Contains(uint64(i)) {
			continue
		}
		f.b.Add(uint32(s))
	}
}

func testFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) (int, any) {
	ss := vector.MustFixedColNoTypeCheck[T](v)
	nsp := v.GetNulls()
	for i, s := range ss {
		if nsp.Contains(uint64(i)) {
			continue
		}
		if f.b.Contains(uint32(s)) {
			return i, s
		}
	}
	return -1, nil
}

func testAndAddFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) (int, any) {
	ss := vector.MustFixedColNoTypeCheck[T](v)
	nsp := v.GetNulls()
	for i, s := range ss {
		if nsp.Contains(uint64(i)) {
			continue
		}
		if !f.b.CheckedAdd(uint32(s)) {
			return i, s
		}
	}
	return -1, nil
}

func valueToString(a any) string {
	switch v := a.(type) {
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	}
	return ""
}
