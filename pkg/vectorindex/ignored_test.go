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
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// An ENUM key keeps its stored number, 0 included, which names no member.
func TestEnumPkLiteral(t *testing.T) {
	require.Equal(t, "SERIAL_EXTRACT(X'503d14', 0 AS ENUM('a','b'))", EnumPkLiteral(0, "a,b"))
	require.Equal(t, "SERIAL_EXTRACT(X'503d1502', 0 AS ENUM('a','it''s'))", EnumPkLiteral(2, "a,it's"))
}

func TestIgnoredRowsSql(t *testing.T) {
	cols := IgnoredColumns{Pk: "__mo_index_pk", Code: "__mo_index_code", Reason: "__mo_index_reason", Ts: "__mo_index_ts"}
	require.Empty(t, IgnoredRowsSql("db", "t", cols, nil))

	sqls := IgnoredRowsSql("db", "t", cols, []IgnoredRow{{Pk: "1", Code: moerr.ErrIndexVectorOutOfRange, Ts: 5},
		{Pk: "'x'", Code: moerr.ErrIndexDatalink, Reason: "stage://s/a.pdf: it's gone", Ts: 6}})
	require.Equal(t, []string{
		"REPLACE INTO `db`.`t` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_reason`, `__mo_index_ts`) VALUES (1, 20348, NULL, 5), ('x', 20352, 'stage://s/a.pdf: it''s gone', 6)",
	}, sqls)

	rows := make([]IgnoredRow, ignoredRowsPerSql+1)
	for i := range rows {
		rows[i] = IgnoredRow{Pk: strconv.Itoa(i), Code: moerr.ErrIndexVectorDimension}
	}
	sqls = IgnoredRowsSql("db", "t", cols, rows)
	require.Len(t, sqls, 2)
	require.Equal(t, ignoredRowsPerSql, strings.Count(sqls[0], ", 20346, "))
	require.Equal(t, 1, strings.Count(sqls[1], ", 20346, "))
}

func TestIgnoredReason(t *testing.T) {
	require.Empty(t, IgnoredReason(moerr.NewIndexVectorOutOfRangeNoCtx("70000 as F16")))
	err := moerr.NewIndexDatalinkNoCtx("stage://s/a.pdf: file not found")
	require.Equal(t, err.Error(), IgnoredReason(err))
	err = moerr.NewIndexInvalidJSONNoCtx("json text {")
	require.Equal(t, err.Error(), IgnoredReason(err))

	// a message longer than the bound is cut at a character boundary
	for _, doc := range []string{strings.Repeat("{", 65535), strings.Repeat("é", 40000)} {
		err = moerr.NewIndexInvalidJSONNoCtx(doc)
		reason := IgnoredReason(err)
		require.LessOrEqual(t, len(reason), MaxIgnoredReason)
		require.True(t, utf8.ValidString(reason))
		require.True(t, strings.HasSuffix(reason, "..."))
		require.True(t, strings.HasPrefix(err.Error(), strings.TrimSuffix(reason, "...")))
	}
	// invalid UTF-8 is replaced
	reason := IgnoredReason(moerr.NewIndexDatalinkNoCtx("a\xffb"))
	require.True(t, utf8.ValidString(reason))
	require.Contains(t, reason, "a\uFFFDb")
}

func TestUnstorableCode(t *testing.T) {
	for _, err := range []error{
		moerr.NewIndexVectorDimensionNoCtx("3 != 4"),
		moerr.NewIndexVectorNonFiniteNoCtx(),
		moerr.NewIndexVectorOutOfRangeNoCtx("70000 as F16"),
		moerr.NewIndexInvalidJSONNoCtx("json text {"),
		moerr.NewIndexDatalinkNoCtx("stage://s/a.pdf: file not found"),
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
	cols := IgnoredColumns{Pk: "pk", Code: "code", Reason: "reason", Ts: "ts"}
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
		reason := ""
		if i == 2 {
			reason = "file not found"
		}
		require.NoError(t, s.Add(pk, moerr.ErrIndexVectorOutOfRange, reason))
	}
	require.Equal(t, int64(n), s.Len())
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)

	var sqls []string
	require.NoError(t, s.Sqls("db", "t", cols, 7, func(sql string) error { sqls = append(sqls, sql); return nil }))
	require.Len(t, sqls, 2)
	require.True(t, strings.HasPrefix(sqls[0], "REPLACE INTO `db`.`t` (`pk`, `code`, `reason`, `ts`) VALUES (0, 20348, NULL, 7), ('a''b\nc', 20348, NULL, 7), (2, 20348, 'file not found', 7)"))
	require.Equal(t, ignoredRowsPerSql, strings.Count(sqls[0], ", 20348, "))
	require.Equal(t, 3, strings.Count(sqls[1], ", 20348, NULL, 7)"))
	require.True(t, strings.HasSuffix(sqls[1], fmt.Sprintf("(%d, 20348, NULL, 7)", n-1)))

	boom := errors.New("boom")
	require.ErrorIs(t, s.Sqls("db", "t", cols, 7, func(string) error { return boom }), boom)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestCentroidxArgs(t *testing.T) {
	args, err := CentroidxArgs("vector_l2_ops", `{"lists":"2"}`, IndexTableConfig{DbName: "db"})
	require.NoError(t, err)
	require.Equal(t, "'vector_l2_ops'", args, "an index without an ignored table keeps the metric alone")
	args, err = CentroidxArgs("vector_l2_ops", `{"lists":"2"}`, IndexTableConfig{DbName: "db", IgnoredTable: "ign", MaxBadRecords: 3})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(args, `'vector_l2_ops', '{"lists":"2"}', '{"db":"db",`), args)
	require.Contains(t, args, `"ignored":"ign"`)
	require.Contains(t, args, `"max_bad_records":3`)
}

func TestSerialPkString(t *testing.T) {
	p := types.NewPacker()
	defer p.Close()
	p.EncodeInt64(1)
	p.EncodeStringType([]byte("x"))
	require.Equal(t, "X'3B150146017800'", SerialPkString(p.GetBuf()))
}
