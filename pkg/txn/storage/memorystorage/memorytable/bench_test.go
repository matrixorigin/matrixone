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

package memorytable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkTableInsert(b *testing.B) {
	table := NewTable[Int, int, TestRow]()
	var row TestRow
	tx := NewTransaction(Time{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.key = Int(i)
		row.value = i
		err := table.Insert(tx, row)
		assert.Nil(b, err)
	}
}

func BenchmarkParallelTableUpsert(b *testing.B) {
	table := NewTable[Int, int, TestRow]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var row TestRow
		tx := NewTransaction(Time{})
		i := 0
		for pb.Next() {
			row.key = Int(i)
			row.value = i
			err := table.Upsert(tx, row)
			assert.Nil(b, err)
		}
	})
}

func BenchmarkTableBatchInsert(b *testing.B) {
	table := NewTable[Int, int, TestRow]()
	var row TestRow
	tx := NewTransaction(Time{})
	bat, err := table.NewBatch(tx)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.key = Int(i)
		row.value = i
		err := bat.Insert(row)
		assert.Nil(b, err)
	}
	bat.Commit()
}

func BenchmarkTableGet(b *testing.B) {
	table := NewTable[Int, int, TestRow]()
	var row TestRow
	tx := NewTransaction(Time{})
	row.key = Int(42)
	row.value = 42
	err := table.Insert(tx, row)
	assert.Nil(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := table.Get(tx, Int(42))
		if err != nil {
			b.Fatal(err)
		}
		if value != 42 {
			b.Fatal()
		}
	}
}
