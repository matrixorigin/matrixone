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

package csvparser

import (
	"errors"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestRecordLimitCoversShortFieldsAndQuotes(t *testing.T) {
	for _, input := range []string{strings.Repeat("x,", 40) + "x\n", "\"" + strings.Repeat("x,", 40) + "x\"\n", strings.Repeat(",", 81) + "\n"} {
		p, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}, strings.NewReader(input), 128, false)
		require.NoError(t, err)
		p.LimitRecordBytes(32)
		p.LimitRecordBytes(64) // A later caller cannot weaken the admitted bound.
		p.SetRecordBudget(16, func(need int64) (int64, error) {
			require.LessOrEqual(t, len(p.recordBuffer), 16, "reserve before extending the record")
			return 32, nil
		})
		_, err = p.Read(nil)
		require.ErrorContains(t, err, "txn-entry-size-limit")
		require.LessOrEqual(t, len(p.recordBuffer), 32, "check before growing the accumulated record")
	}
}

func TestRecordBudgetGrowthAndDenial(t *testing.T) {
	for _, deny := range []bool{false, true} {
		p, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", FieldsEnclosedBy: "\"", LinesTerminatedBy: "\n"}, strings.NewReader("abc\n\"01234567890123456789\"\nlast\n"), 4, false)
		require.NoError(t, err)
		p.LimitRecordBytes(32)
		denied := errors.New("reservation denied")
		calls := 0
		p.SetRecordBudget(8, func(need int64) (int64, error) {
			calls++
			require.LessOrEqual(t, len(p.recordBuffer), 8)
			if deny {
				return 0, denied
			}
			return 32, nil
		})
		row, err := p.Read(nil)
		require.NoError(t, err)
		require.Equal(t, "abc", row[0].Val)
		require.Zero(t, calls)
		row, err = p.Read(row)
		if deny {
			require.ErrorIs(t, err, denied)
			require.Equal(t, int64(8), p.recordBudget)
		} else {
			require.NoError(t, err)
			require.Equal(t, "01234567890123456789", row[0].Val)
			row, err = p.Read(row)
			require.NoError(t, err)
			require.Equal(t, "last", row[0].Val)
		}
		require.Equal(t, 1, calls)
	}
}

func TestRecordLimitResetsForNextLine(t *testing.T) {
	p, err := NewCSVParser(&CSVConfig{FieldsTerminatedBy: ",", LinesTerminatedBy: "\n"}, strings.NewReader(strings.Repeat("abc,def\n", 10)), 4, false)
	require.NoError(t, err)
	p.LimitRecordBytes(8)
	for range 10 {
		row, readErr := p.Read(nil)
		require.NoError(t, readErr)
		require.Equal(t, "abc", row[0].Val)
		require.Equal(t, "def", row[1].Val)
	}
}
