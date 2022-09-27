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

package memtable

import (
	"testing"

	"github.com/google/uuid"
)

func BenchmarkDelete(b *testing.B) {
	tx := NewTransaction(uuid.NewString(), Time{}, SnapshotIsolation)
	table := NewTable[Int, int, TestRow]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Int(i)
		if err := table.Delete(tx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert(b *testing.B) {
	tx := NewTransaction(uuid.NewString(), Time{}, SnapshotIsolation)
	table := NewTable[Int, int, TestRow]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Int(i)
		row := TestRow{
			key:   key,
			value: i,
		}
		if err := table.Insert(tx, row); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertAndGet(b *testing.B) {
	tx := NewTransaction(uuid.NewString(), Time{}, SnapshotIsolation)
	table := NewTable[Int, int, TestRow]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Int(i)
		row := TestRow{
			key:   key,
			value: i,
		}
		if err := table.Insert(tx, row); err != nil {
			b.Fatal(err)
		}
		tx.Time.Timestamp.PhysicalTime++
		p, err := table.Get(tx, key)
		if err != nil {
			b.Fatal(err)
		}
		if p != i {
			b.Fatal()
		}
	}
}

func BenchmarkInsertAndIndex(b *testing.B) {
	tx := NewTransaction(uuid.NewString(), Time{}, SnapshotIsolation)
	table := NewTable[Int, int, TestRow]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Int(i)
		row := TestRow{
			key:   key,
			value: i,
		}
		if err := table.Insert(tx, row); err != nil {
			b.Fatal(err)
		}
		tx.Time.Timestamp.PhysicalTime++
		entries, err := table.Index(tx, Tuple{
			Text("foo"), Int(i),
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1 {
			b.Fatal()
		}
	}
}
