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
	"context"
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
	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
			}()

			cnt := max(rand.Uint64()%10, 1)
			acc := make([]uint64, cnt)
			db := make([]uint64, cnt)

			for i := range acc {
				acc[i] = rand.Uint64() % (cnt * 2)
				db[i] = rand.Uint64() % (cnt * 2)
			}

			ret, release := joinAccountDatabase(acc, db)

			andStrs := strings.Split(ret, " OR ")
			require.Equal(t, int(cnt), len(andStrs), fmt.Sprintf("cnt = %d, andStrs=%s", cnt, andStrs))

			for i, str := range andStrs {
				str = str[1:]
				str = str[:len(str)-1]

				equalStr := strings.Split(str, " AND ")
				ll := fmt.Sprintf("account_id = %d", acc[i])
				rr := fmt.Sprintf("database_id = %d", db[i])

				require.Equal(t, ll, equalStr[0], fmt.Sprintf("acc: %v, db: %v, ret: %v", acc, db, ret))
				require.Equal(t, rr, equalStr[1], fmt.Sprintf("acc: %v, db: %v, ret: %v", acc, db, ret))
			}

			release()
		}()
	}

	wg.Wait()
}

func Test_constructInStmtByTableId(t *testing.T) {

	t.Run("A", func(t *testing.T) {
		var tblId []uint64
		tblId = append(tblId, 3)
		tblId = append(tblId, 5)
		tblId = append(tblId, 7)
		tblId = append(tblId, 9)

		str, release := constructInStmt(tblId, "table_id")
		defer release()

		require.Equal(t,
			"table_id in (3,5,7,9)",
			str)
	})

	t.Run("B", func(t *testing.T) {
		var tblId []uint64
		tblId = append(tblId, 3)

		str, release := constructInStmt(tblId, "table_id")
		defer release()

		require.Equal(t,
			"table_id in (3)",
			str)
	})
}

func Test_joinAccountDatabaseTable(t *testing.T) {
	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)
		go func() {

			defer func() {
				wg.Done()
			}()

			cnt := max(rand.Uint64()%10, 1)
			acc := make([]uint64, cnt)
			db := make([]uint64, cnt)
			tbl := make([]uint64, cnt)

			for i := range acc {
				acc[i] = rand.Uint64() % (cnt * 2)
				db[i] = rand.Uint64() % (cnt * 2)
				tbl[i] = rand.Uint64() % (cnt * 2)
			}

			ret, release := joinAccountDatabaseTable(acc, db, tbl)

			andStrs := strings.Split(ret, " OR ")
			require.Equal(t, int(cnt), len(andStrs), fmt.Sprintf("cnt = %d, andStrs=%s", cnt, andStrs))

			for i, str := range andStrs {
				str = str[1:]
				str = str[:len(str)-1]

				equalStr := strings.Split(str, " AND ")
				ll := fmt.Sprintf("account_id = %d", acc[i])
				mm := fmt.Sprintf("database_id = %d", db[i])
				rr := fmt.Sprintf("table_id = %d", tbl[i])

				require.Equal(t, ll, equalStr[0], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
				require.Equal(t, mm, equalStr[1], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
				require.Equal(t, rr, equalStr[2], fmt.Sprintf("acc: %v, db: %v, tbl: %v, ret: %v", acc, db, tbl, ret))
			}

			release()
		}()
	}

	wg.Wait()
}

func Benchmark_intsJoin(b *testing.B) {
	item := make([]uint64, 100)
	for i := range item {
		item[i] = rand.Uint64()
	}

	for range b.N {
		_, f := intsJoin(item, ",")
		f()
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
		_, f := joinAccountDatabase(acc, db)
		f()
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
		_, f := joinAccountDatabaseTable(acc, db, tbl)
		f()
	}
}

func TestAlphaTask(t *testing.T) {
	ctx := context.Background()

	t.Run("forbidden beta", func(t *testing.T) {
		eng := &Engine{}

		initMoTableStatsConfig(ctx, eng)

		tps := []tablePair{
			{
				onlyUpdateTS: true,
				valid:        false,
			},
			{
				onlyUpdateTS: true,
				valid:        true,
			},
		}

		eng.dynamicCtx.beta.forbidden = true

		for _, tp := range tps {
			fmt.Println(tp.String())
		}

		fmt.Println(eng.dynamicCtx.beta.String())

		eng.dynamicCtx.alphaTask(ctx, "", tps, t.Name())
	})

	t.Run("normal", func(t *testing.T) {
		eng := &Engine{}

		initMoTableStatsConfig(ctx, eng)

		tps := []tablePair{
			{
				onlyUpdateTS: true,
				valid:        false,
			},
			{
				onlyUpdateTS: true,
				valid:        true,
			},
		}

		for _, tp := range tps {
			fmt.Println(tp.String())
		}

		fmt.Println(eng.dynamicCtx.beta.String())

		eng.dynamicCtx.alphaTask(ctx, "", tps, t.Name())
	})

}
