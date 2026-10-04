// Copyright 2026 Matrix Origin
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

//go:build !race

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestObjectListUncommittedSeekAllocations(t *testing.T) {
	list := NewObjectList(false)
	list.Set(makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1))
	ts := types.BuildTS(2, 0)

	waitAllocs := testing.AllocsPerRun(100, func() {
		list.WaitUntilCommitted(ts)
	})
	require.LessOrEqual(t, waitAllocs, float64(1))

	txn := txnbase.MockTxnReaderWithNow()
	var count int
	visibleAllocs := testing.AllocsPerRun(100, func() {
		count = 0
		it := list.MakeVisibleCommittedObjectIt(txn)
		for it.Next() {
			count++
		}
		it.Release()
	})
	require.Equal(t, 1, count)
	require.LessOrEqual(t, visibleAllocs, float64(1))
}

func BenchmarkObjectListWaitUntilCommitted(b *testing.B) {
	list := NewObjectList(false)
	list.Set(makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1))
	ts := types.BuildTS(2, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		list.WaitUntilCommitted(ts)
	}
}

func BenchmarkObjectListVisibleCommitted(b *testing.B) {
	list := NewObjectList(false)
	list.Set(makeObjectListOrderTestEntry(1, ObjectListGroupAppendableCreate, 1))
	txn := txnbase.MockTxnReaderWithNow()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		it := list.MakeVisibleCommittedObjectIt(txn)
		for it.Next() {
		}
		it.Release()
	}
}

func makeObjectListDynamicSeekBenchmark() (*ObjectList, types.TS, types.TS) {
	list := NewObjectList(false)
	marker := byte(1)
	for group := ObjectListGroupAppendableCreate; group <= ObjectListGroupNonAppendableDrop; group++ {
		list.Set(makeObjectListOrderTestEntry(marker, group, 1))
		marker++
	}
	from := types.BuildTS(2, 0)
	to := types.BuildTS(3, 0)
	return list, from, to
}

func runObjectListDynamicGroupSeeks(list *ObjectList, from, to types.TS) {
	it := list.loadTree().Iter()
	SeekObjectListGroupBefore(&it, ObjectListGroupAppendableCreate, from)
	SeekObjectListGroup(&it, ObjectListGroupAppendableCreate, from)
	SeekObjectListGroup(&it, ObjectListGroupAppendableDrop, to)
	SeekObjectListGroup(&it, ObjectListGroupNonAppendableCreate, from)
	SeekObjectListGroup(&it, ObjectListGroupNonAppendableDrop, to)
	it.Release()
}

func TestObjectListDynamicGroupSeekAllocations(t *testing.T) {
	list, from, to := makeObjectListDynamicSeekBenchmark()
	allocs := testing.AllocsPerRun(100, func() {
		runObjectListDynamicGroupSeeks(list, from, to)
	})
	require.LessOrEqual(t, allocs, float64(1))
}

func BenchmarkObjectListDynamicGroupSeeks(b *testing.B) {
	list, from, to := makeObjectListDynamicSeekBenchmark()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		runObjectListDynamicGroupSeeks(list, from, to)
	}
}
