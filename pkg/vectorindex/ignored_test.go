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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestIgnoredRowsSql(t *testing.T) {
	cols := IgnoredColumns{Pk: "__mo_index_pk", Code: "__mo_index_code", Ts: "__mo_index_ts"}
	require.Empty(t, IgnoredRowsSql("db", "t", cols, nil))

	sqls := IgnoredRowsSql("db", "t", cols, []IgnoredRow{{Pk: "1", Code: moerr.ErrIndexVectorOutOfRange, Ts: 5}, {Pk: "'x'", Code: moerr.ErrIndexVectorNonFinite, Ts: 6}})
	require.Equal(t, []string{
		"REPLACE INTO `db`.`t` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_ts`) VALUES (1, 20347, 5), ('x', 20346, 6)",
	}, sqls)

	rows := make([]IgnoredRow, ignoredRowsPerSql+1)
	for i := range rows {
		rows[i] = IgnoredRow{Pk: strconv.Itoa(i), Code: moerr.ErrIndexVectorDimension}
	}
	sqls = IgnoredRowsSql("db", "t", cols, rows)
	require.Len(t, sqls, 2)
	require.Equal(t, ignoredRowsPerSql, strings.Count(sqls[0], ", 20345, "))
	require.Equal(t, 1, strings.Count(sqls[1], ", 20345, "))
}

func TestUnstorableCode(t *testing.T) {
	for _, err := range []error{
		moerr.NewIndexVectorDimensionNoCtx("3 != 4"),
		moerr.NewIndexVectorNonFiniteNoCtx(),
		moerr.NewIndexVectorOutOfRangeNoCtx("70000 as F16"),
	} {
		code, ok := UnstorableCode(err)
		require.True(t, ok)
		require.Equal(t, err.(*moerr.Error).ErrorCode(), code)
		require.NotEmpty(t, moerr.MessageOf(code))
		require.NotContains(t, moerr.MessageOf(code), "%")
	}
	_, ok := UnstorableCode(moerr.NewInvalidInputNoCtx("x"))
	require.False(t, ok)
	_, ok = UnstorableCode(errors.New("x"))
	require.False(t, ok)

	err := moerr.NewIndexTooManyBadRecordsNoCtx(0, "7", "element 70000")
	require.Contains(t, err.Error(), "max_bad_records = 0")
	require.Contains(t, err.Error(), "row 7: element 70000")
}

// A spill gives back its rows in order, in statements of at most ignoredRowsPerSql rows, and
// removes its file.
func TestIgnoredSpill(t *testing.T) {
	cols := IgnoredColumns{Pk: "pk", Code: "code", Ts: "ts"}
	dir := t.TempDir()

	empty := NewIgnoredSpill(dir)
	require.NoError(t, empty.Sqls("db", "t", cols, 1, func(string) error { t.Fatal("no statement"); return nil }))
	require.NoError(t, empty.Close())

	s := NewIgnoredSpill(dir)
	n := ignoredRowsPerSql + 3
	for i := 0; i < n; i++ {
		pk := strconv.Itoa(i)
		if i == 1 {
			pk = "'a''b\nc'"
		}
		require.NoError(t, s.Add(pk, moerr.ErrIndexVectorOutOfRange))
	}
	require.Equal(t, int64(n), s.Len())
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)

	var sqls []string
	require.NoError(t, s.Sqls("db", "t", cols, 7, func(sql string) error { sqls = append(sqls, sql); return nil }))
	require.Len(t, sqls, 2)
	require.True(t, strings.HasPrefix(sqls[0], "REPLACE INTO `db`.`t` (`pk`, `code`, `ts`) VALUES (0, 20347, 7), ('a''b\nc', 20347, 7), (2, 20347, 7)"))
	require.Equal(t, ignoredRowsPerSql, strings.Count(sqls[0], ", 20347, 7)"))
	require.Equal(t, 3, strings.Count(sqls[1], ", 20347, 7)"))
	require.True(t, strings.HasSuffix(sqls[1], fmt.Sprintf("(%d, 20347, 7)", n-1)))

	boom := errors.New("boom")
	require.ErrorIs(t, s.Sqls("db", "t", cols, 7, func(string) error { return boom }), boom)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}
