// Copyright 2026 Matrix Origin
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

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func campaignRows() *sqlmock.Rows {
	return sqlmock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("amount").OfType("DECIMAL", "").Nullable(true).WithPrecisionAndScale(65, 8),
		sqlmock.NewColumn("text").OfType("VARCHAR", "").Nullable(true),
	)
}

func TestPublicDrainEmptyAndExactRows(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			data := campaignRows()
			if count > 0 {
				data.AddRow("-340282366920938463463374607431768211457.12345678", "")
			}
			if count > 1 {
				data.AddRow(nil, nil)
			}
			mock.ExpectQuery("stream").WillReturnRows(data).RowsWillBeClosed()
			rows, err := db.Query("stream")
			require.NoError(t, err)
			var spool bytes.Buffer
			result, first, err := drainResult(rows, &spool, 4<<30, time.Now())
			require.NoError(t, err)
			require.Equal(t, uint64(count), result.Rows)
			require.Equal(t, int64(spool.Len()), result.Bytes)
			digest := sha256.Sum256(spool.Bytes())
			require.Equal(t, hex.EncodeToString(digest[:]), result.SHA256)
			require.Len(t, result.Schema, 2, "empty results retain complete metadata")
			require.Equal(t, int64(65), result.Schema[0].Precision)
			require.Equal(t, int64(8), result.Schema[0].Scale)
			if count == 0 {
				require.Nil(t, first, "empty results have no first-row observation")
			} else {
				require.NotNil(t, first)
				require.Contains(t, spool.String(), `"value":"-34028236692093846346337460743176821145712345678"`)
				require.Contains(t, spool.String(), `{"type":"varchar","value":""}`)
			}
			if count == 2 {
				require.Equal(t, 2, strings.Count(spool.String(), `"null":true`))
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

type failedCampaignWriter struct{}

func (failedCampaignWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestPublicDrainRejectsLateErrorsAndClosesEveryFailure(t *testing.T) {
	late := errors.New("late driver failure")
	closed := errors.New("result close failure")
	for _, scenario := range []string{"late-error", "close-error", "writer-error", "quota", "second-result", "encode-and-close-error"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			data := campaignRows().AddRow("1.00000000", "first")
			writer, budget := io.Writer(io.Discard), int64(4<<30)
			sets := []*sqlmock.Rows{data}
			switch scenario {
			case "late-error":
				data.AddRow("2.00000000", "second").RowError(1, late)
			case "close-error":
				data.CloseError(closed)
			case "writer-error":
				writer = failedCampaignWriter{}
			case "quota":
				budget = 1
			case "second-result":
				sets = append(sets, campaignRows())
			case "encode-and-close-error":
				data = campaignRows().AddRow("not-a-decimal", "value").CloseError(closed)
				sets = []*sqlmock.Rows{data}
			}
			mock.ExpectQuery("stream").WillReturnRows(sets...).RowsWillBeClosed()
			rows, err := db.Query("stream")
			require.NoError(t, err)
			result, _, err := drainResult(rows, writer, budget, time.Now())
			require.Error(t, err)
			require.Empty(t, result.SHA256, "partial rows never become a success descriptor")
			switch scenario {
			case "late-error":
				require.ErrorIs(t, err, late)
			case "close-error", "encode-and-close-error":
				require.ErrorIs(t, err, closed)
			case "writer-error":
				require.ErrorIs(t, err, io.ErrClosedPipe)
			}
			if scenario == "encode-and-close-error" {
				require.Contains(t, err.Error(), "decimal requires exact digits", "cleanup preserves the original value error")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
