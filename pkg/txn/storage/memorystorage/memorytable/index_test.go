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

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	table := NewTable[Int, int, TestRow]()
	tx := NewTransaction(Time{})

	num := 1024
	for _, i := range rand.Perm(num) {
		row := TestRow{
			key:   Int(i),
			value: i,
		}
		err := table.Insert(tx, row)
		assert.Nil(t, err)
	}

	entries, err := table.Index(tx, Tuple{
		testindex_Value,
		ToOrdered(42),
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, Int(42), entries[0].Key)

}
