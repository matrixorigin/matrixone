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

	"github.com/RoaringBitmap/roaring"
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

func newroaringFilter(t types.T) *roaringFilter {
	var add func(f *roaringFilter, v *vector.Vector)
	var test func(f *roaringFilter, v *vector.Vector) (int, any)
	var testAndAdd func(f *roaringFilter, v *vector.Vector) (int, any)

	switch t {
	case types.T_int8:
		add = addFunc[int8]
		test = testFunc[int8]
		testAndAdd = testAndAddFunc[int8]
	case types.T_int16:
		add = addFunc[int16]
		test = testFunc[int16]
		testAndAdd = testAndAddFunc[int16]
	case types.T_int32:
		add = addFunc[int32]
		test = testFunc[int32]
		testAndAdd = testAndAddFunc[int32]
	case types.T_uint8:
		add = addFunc[uint8]
		test = testFunc[uint8]
		testAndAdd = testAndAddFunc[uint8]
	case types.T_uint16:
		add = addFunc[uint16]
		test = testFunc[uint16]
		testAndAdd = testAndAddFunc[uint16]
	case types.T_uint32:
		add = addFunc[uint32]
		test = testFunc[uint32]
		testAndAdd = testAndAddFunc[uint32]
	}

	f := &roaringFilter{
		addFunc:        add,
		testFunc:       test,
		testAndAddFunc: testAndAdd,
		b:              roaring.New(),
	}
	return f
}

func addFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) {
	ss := vector.MustFixedCol[T](v)
	for _, s := range ss {
		f.b.Add(uint32(s))
	}
}

func testFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) (int, any) {
	ss := vector.MustFixedCol[T](v)
	for i, s := range ss {
		if f.b.Contains(uint32(s)) {
			return i, s
		}
	}
	return -1, nil
}

func testAndAddFunc[T canUseRoaring](f *roaringFilter, v *vector.Vector) (int, any) {
	ss := vector.MustFixedCol[T](v)
	for i, s := range ss {
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
