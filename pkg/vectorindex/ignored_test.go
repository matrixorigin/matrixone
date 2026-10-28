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

package vectorindex

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIgnoredRowsSql(t *testing.T) {
	require.Empty(t, IgnoredRowsSql("db", "t", nil))

	sqls := IgnoredRowsSql("db", "t", []IgnoredRow{{Pk: "1", Reason: `a'b\c`, Ts: 5}, {Pk: "'x'", Reason: "r", Ts: 6}})
	require.Equal(t, []string{
		"REPLACE INTO `db`.`t` (`__mo_index_pk`, `__mo_index_reason`, `__mo_index_ts`) VALUES (1, 'a''b\\\\c', 5), ('x', 'r', 6)",
	}, sqls)

	rows := make([]IgnoredRow, ignoredRowsPerSql+1)
	for i := range rows {
		rows[i] = IgnoredRow{Pk: strconv.Itoa(i), Reason: "r"}
	}
	sqls = IgnoredRowsSql("db", "t", rows)
	require.Len(t, sqls, 2)
	require.Equal(t, ignoredRowsPerSql, strings.Count(sqls[0], "'r'"))
	require.Equal(t, 1, strings.Count(sqls[1], "'r'"))
}
