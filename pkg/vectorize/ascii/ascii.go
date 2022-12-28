// Copyright 2022 Matrix Origin
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

package ascii

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	ints  = []int64{1e16, 1e8, 1e4, 1e2, 1e1}
	uints = []uint64{1e16, 1e8, 1e4, 1e2, 1e1}
)

func IntBatch[T types.Ints](xs []T, start int, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = IntSingle(int64(x), start)
	}
}

func IntSingle[T types.Ints](val T, start int) uint8 {
	if val < 0 {
		return '-'
	}
	i64Val := int64(val)
	for _, v := range ints[start:] {
		if i64Val >= v {
			i64Val /= v
		}
	}
	return uint8(i64Val) + '0'
}

func UintBatch[T types.UInts](xs []T, start int, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = UintSingle(x, start)
	}
}

func UintSingle[T types.UInts](val T, start int) uint8 {
	u64Val := uint64(val)
	for _, v := range uints[start:] {
		if u64Val >= v {
			u64Val /= v
		}
	}
	return uint8(u64Val) + '0'
}

func StringBatch(xs [][]byte, rs []uint8, nsp *nulls.Nulls) {
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i] = StringSingle(x)
	}
}

func StringSingle(val []byte) uint8 {
	if len(val) == 0 {
		return 0
	}
	return val[0]
}
