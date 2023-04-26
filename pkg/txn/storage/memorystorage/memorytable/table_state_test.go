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
	"math/rand"
	"testing"
)

func TestTableStateEncoding(t *testing.T) {
	table := NewTable[Int, int, TestRow]()
	nums := rand.Perm(128)
	tx := NewTransaction(Time{})
	for _, i := range nums {
		row := TestRow{
			key:   Int(i),
			value: i,
		}
		err := table.Insert(tx, row)
		if err != nil {
			t.Fatal(err)
		}
	}

	m := make(map[Int]int)
	iter, err := table.NewIter(tx)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		key, value, err := iter.Read()
		if err != nil {
			t.Fatal(err)
		}
		m[key] = value
	}
	if len(m) != 128 {
		t.Fatal()
	}

}
