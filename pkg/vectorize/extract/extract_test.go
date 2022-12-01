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

package extract

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestExtractFromDate(t *testing.T) {
	a0, _ := types.ParseDateCast("2020-01-02")
	a1, _ := types.ParseDateCast("2021-03-04")
	inputs := make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results := make([]uint32, 2)
	output, err := ExtractFromDate("year", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{2020, 2021}, output)

	a0, _ = types.ParseDateCast("2020-01-02")
	a1, _ = types.ParseDateCast("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{01, 03}, output)

	a0, _ = types.ParseDateCast("2020-01-02")
	a1, _ = types.ParseDateCast("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("year_month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{202001, 202103}, output)

	a0, _ = types.ParseDateCast("2020-01-02")
	a1, _ = types.ParseDateCast("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("quarter", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 1}, output)
}

func TestExtractFromDateTime(t *testing.T) {
	a0, _ := types.ParseDatetime("2020-01-02 11:12:13.123456", 6)
	a1, _ := types.ParseDatetime("1234-01-02 11:12:13.123456", 6)
	inputs := make([]types.Datetime, 2)
	inputs[0] = a0
	inputs[1] = a1
	resultValues := make([]string, 2)
	output, err := ExtractFromDatetime("microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"123456", "123456"}, output)

	output, err = ExtractFromDatetime("second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"13", "13"}, output)

	output, err = ExtractFromDatetime("minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"12", "12"}, output)

	output, err = ExtractFromDatetime("hour", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"11", "11"}, output)

	output, err = ExtractFromDatetime("day", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"02", "02"}, output)

	output, err = ExtractFromDatetime("week", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"01", "01"}, output)

	output, err = ExtractFromDatetime("month", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"01", "01"}, output)

	output, err = ExtractFromDatetime("quarter", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"1", "1"}, output)

	output, err = ExtractFromDatetime("year", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"2020", "1234"}, output)

	output, err = ExtractFromDatetime("second_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"13.123456", "13.123456"}, output)

	output, err = ExtractFromDatetime("minute_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"12:13.123456", "12:13.123456"}, output)

	output, err = ExtractFromDatetime("minute_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"12:13", "12:13"}, output)

	output, err = ExtractFromDatetime("hour_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"11:12:13.123456", "11:12:13.123456"}, output)

	output, err = ExtractFromDatetime("hour_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"11:12:13", "11:12:13"}, output)

	output, err = ExtractFromDatetime("hour_minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"11:12", "11:12"}, output)

	output, err = ExtractFromDatetime("day_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"02 11:12:13.123456", "02 11:12:13.123456"}, output)

	output, err = ExtractFromDatetime("day_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"02 11:12:13", "02 11:12:13"}, output)

	output, err = ExtractFromDatetime("day_minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"02 11:12", "02 11:12"}, output)

	output, err = ExtractFromDatetime("day_hour", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"02 11", "02 11"}, output)

	output, err = ExtractFromDatetime("year_month", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, []string{"202001", "123401"}, output)
}
