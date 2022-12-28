// Copyright 2022 Matrix Origin
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

package ascii

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIntBatch(t *testing.T) {
	xs := []int64{0, 1, 9, 10, 99, 100, 999, 1000, 9999, 10000, -1, -9, -10, -99}
	want := []uint8{'0', '1', '9', '1', '9', '1', '9', '1', '9', '1', '-', '-', '-', '-'}
	got := make([]uint8, len(xs))
	nsp := nulls.NewWithSize(len(xs))
	nsp.Set(10)
	IntBatch(xs, 0, got, nsp)

	require.Equal(t, want[:10], got[:10])
	require.Equal(t, want[11:], got[11:])
}

func TestUintBatch(t *testing.T) {
	xs := []uint64{0, 1, 9, 10, 99, 100, 999, 1000, 9999, 10000, 1, 23454322345543, 987654323456789}
	want := []uint8{'0', '1', '9', '1', '9', '1', '9', '1', '9', '1', '1', '2', '9'}
	got := make([]uint8, len(xs))
	nsp := nulls.NewWithSize(len(xs))
	nsp.Set(10)
	UintBatch(xs, 0, got, nsp)
	require.Equal(t, want[:10], got[:10])
	require.Equal(t, want[11:], got[11:])
}

func TestStringBatch(t *testing.T) {
	xs := [][]byte{
		[]byte(""),
		[]byte("0"),
		[]byte("1"),
		[]byte("9"),
		[]byte("10"),
		[]byte("99"),
		[]byte("100"),
		[]byte("999"),
		[]byte("1000"),
		[]byte("9999"),
		[]byte("10000"),
		[]byte("-1"),
		[]byte("-9"),
		[]byte("-10"),
		[]byte("-99"),
		[]byte("45654345678908765.123456789"),
	}
	want := []uint8{0, '0', '1', '9', '1', '9', '1', '9', '1', '9', '1', '-', '-', '-', '-', '4'}
	got := make([]uint8, len(xs))
	nsp := nulls.NewWithSize(len(xs))
	nsp.Set(10)
	StringBatch(xs, got, nsp)

	require.Equal(t, want[:10], got[:10])
	require.Equal(t, want[11:], got[11:])
}
