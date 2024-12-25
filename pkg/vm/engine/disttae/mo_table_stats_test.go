// Copyright 2024 Matrix Origin
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

package disttae

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func Test_intsJoin(t *testing.T) {
	wg := sync.WaitGroup{}

	for range 100 {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			item := make([]uint64, max(rand.Int()%5, 1))
			for i := range item {
				item[i] = rand.Uint64() % 10
			}

			ret, release := intsJoin(item, ",")
			defer func() {
				release()
			}()

			split := strings.Split(ret, ",")

			//fmt.Println(item, ret)
			for j, s := range split {
				require.Equal(t, strconv.FormatUint(item[j], 10), s, fmt.Sprintf("%v(%d) %v", item, j, ret))
			}
		}()
	}

	wg.Wait()
}

func Test_joinAccountDatabase(t *testing.T) {
	acc := make([]uint64, 10)
	db := make([]uint64, 10)

	for range 10 {
		for i := range acc {
			acc[i] = rand.Uint64() % 10
			db[i] = rand.Uint64() % 10
		}

		ret, release := joinAccountDatabase(acc, db)
		fmt.Println(ret)
		release()
	}
}

func Test_joinAccountDatabaseTable(t *testing.T) {
	acc := make([]uint64, 10)
	db := make([]uint64, 10)
	tbl := make([]uint64, 10)

	for range 10 {
		for i := range acc {
			acc[i] = rand.Uint64() % 10
			db[i] = rand.Uint64() % 10
			tbl[i] = rand.Uint64() % 10
		}

		ret, release := joinAccountDatabaseTable(acc, db, tbl)
		fmt.Println(ret)
		release()
	}
}

func Benchmark_intsJoin(b *testing.B) {
	item := make([]uint64, 100)
	for i := range item {
		item[i] = rand.Uint64()
	}

	for range b.N {
		intsJoin(item, ",")
	}
}

func Benchmark_joinAccountDatabase(b *testing.B) {
	acc := make([]uint64, 100*100)
	db := make([]uint64, 100*100)

	for i := range acc {
		acc[i] = rand.Uint64()
		db[i] = rand.Uint64()
	}

	for range b.N {
		joinAccountDatabase(acc, db)
	}
}

func Benchmark_joinAccountDatabaseTable(b *testing.B) {
	acc := make([]uint64, 100*100)
	db := make([]uint64, 100*100)
	tbl := make([]uint64, 100*100)

	for i := range acc {
		acc[i] = rand.Uint64()
		db[i] = rand.Uint64()
		tbl[i] = rand.Uint64()
	}

	for range b.N {
		joinAccountDatabaseTable(acc, db, tbl)
	}
}
