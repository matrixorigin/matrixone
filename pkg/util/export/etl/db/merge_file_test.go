// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db_holder

import (
	"bytes"
	"encoding/csv"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/stretchr/testify/require"
)

func TestMergeRowEncoding(t *testing.T) {
	rows := [][]string{
		{"", "plain", "42"},
		{" x", "\u2003开头", `\.`},
		{"a,b", "x\r\ny", "中文"},
		{"a'b", `a\b\N`, `"quotes"`},
		{"x", strings.Repeat(`a'"\,`, 8192), "end"},
	}
	for _, row := range rows {
		escaped := make([]string, len(row))
		for i, field := range row {
			escaped[i] = strings.ReplaceAll(strings.ReplaceAll(field, "\\", "\\\\"), "'", "''")
		}
		var want bytes.Buffer
		csvWriter := csv.NewWriter(&want)
		require.NoError(t, csvWriter.Write(escaped))
		csvWriter.Flush()
		require.NoError(t, csvWriter.Error())
		quotes := make([]bool, len(row))
		size := mergeRowSize(row, quotes)
		require.Equal(t, want.Len(), size)
		require.Equal(t, want.Bytes(), appendMergeRow(make([]byte, 0, size), row, quotes))
	}
}

func TestMergeFileTransaction(t *testing.T) {
	for _, mode := range []string{"single", "commit", "exec failure", "source failure", "commit reply lost", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(t.Context())
			require.NoError(t, err)
			defer conn.Close()
			tbl := &table.Table{Database: "db", Table: "reports"}
			sqlText := func(data string) string {
				return mergeLoadPrefix + data + "' INTO TABLE db.reports FIELDS TERMINATED BY ','"
			}
			suffixSize := len(sqlText(""))
			first := []string{"small"}
			rows := [][]string{{"a'中"}, {"third"}}
			failure := errors.New("injected failure")
			limit, target := suffixSize+20, suffixSize+10
			switch mode {
			case "single":
				rows = rows[:0]
				mock.ExpectExec(sqlText("small\n")).WillReturnResult(sqlmock.NewResult(0, 1))
			case "oversized":
				first[0] = strings.Repeat("'", 11)
			default:
				mock.ExpectBegin()
				mock.ExpectExec(sqlText("small\n")).WillReturnResult(sqlmock.NewResult(0, 1))
				if mode == "source failure" {
					mock.ExpectRollback()
				} else if mode == "exec failure" {
					mock.ExpectExec(sqlText("a''中\n")).WillReturnError(failure)
					mock.ExpectRollback()
				} else {
					mock.ExpectExec(sqlText("a''中\n")).WillReturnResult(sqlmock.NewResult(0, 1))
					mock.ExpectExec(sqlText("third\n")).WillReturnResult(sqlmock.NewResult(0, 1))
					commit := mock.ExpectCommit()
					if mode == "commit reply lost" {
						commit.WillReturnError(failure)
					}
				}
			}
			cursor := 0
			read := func() ([]string, error) {
				if cursor == 1 && mode == "source failure" {
					return nil, failure
				}
				if cursor == len(rows) {
					return nil, nil
				}
				row := rows[cursor]
				cursor++
				return row, nil
			}
			err = writeMergeFile(t.Context(), conn, tbl, first, read, limit, target, time.Second)
			switch mode {
			case "single", "commit":
				require.NoError(t, err)
			case "oversized":
				require.ErrorContains(t, err, "exceeding")
				require.Zero(t, cursor)
			default:
				require.ErrorIs(t, err, failure)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
