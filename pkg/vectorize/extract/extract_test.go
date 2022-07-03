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
	a0, _ := types.ParseDate("2020-01-02")
	a1, _ := types.ParseDate("2021-03-04")
	inputs := make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results := make([]uint32, 2)
	output, err := ExtractFromDate("year", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{2020, 2021}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{01, 03}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("year_month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{202001, 202103}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
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
	resultValues := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err := ExtractFromDatetime("microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "123456123456", string(output.Data))
	require.Equal(t, []uint32{0, 6}, output.Offsets)
	require.Equal(t, []uint32{6, 6}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "1313", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "1212", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("hour", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "1111", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("day", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "0202", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("week", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "0101", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("month", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "0101", string(output.Data))
	require.Equal(t, []uint32{0, 2}, output.Offsets)
	require.Equal(t, []uint32{2, 2}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("quarter", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "11", string(output.Data))
	require.Equal(t, []uint32{0, 1}, output.Offsets)
	require.Equal(t, []uint32{1, 1}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("year", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "20201234", string(output.Data))

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("second_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "13.12345613.123456", string(output.Data))
	require.Equal(t, []uint32{0, 9}, output.Offsets)
	require.Equal(t, []uint32{9, 9}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("minute_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "12:13.12345612:13.123456", string(output.Data))
	require.Equal(t, []uint32{0, 12}, output.Offsets)
	require.Equal(t, []uint32{12, 12}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("minute_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "12:1312:13", string(output.Data))
	require.Equal(t, []uint32{0, 5}, output.Offsets)
	require.Equal(t, []uint32{5, 5}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("hour_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "11:12:13.12345611:12:13.123456", string(output.Data))
	require.Equal(t, []uint32{0, 15}, output.Offsets)
	require.Equal(t, []uint32{15, 15}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("hour_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "11:12:1311:12:13", string(output.Data))
	require.Equal(t, []uint32{0, 8}, output.Offsets)
	require.Equal(t, []uint32{8, 8}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("hour_minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "11:1211:12", string(output.Data))
	require.Equal(t, []uint32{0, 5}, output.Offsets)
	require.Equal(t, []uint32{5, 5}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("day_microsecond", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "02 11:12:13.12345602 11:12:13.123456", string(output.Data))
	require.Equal(t, []uint32{0, 18}, output.Offsets)
	require.Equal(t, []uint32{18, 18}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("day_second", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "02 11:12:1302 11:12:13", string(output.Data))
	require.Equal(t, []uint32{0, 11}, output.Offsets)
	require.Equal(t, []uint32{11, 11}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("day_minute", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "02 11:1202 11:12", string(output.Data))
	require.Equal(t, []uint32{0, 8}, output.Offsets)
	require.Equal(t, []uint32{8, 8}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("day_hour", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "02 1102 11", string(output.Data))
	require.Equal(t, []uint32{0, 5}, output.Offsets)
	require.Equal(t, []uint32{5, 5}, output.Lengths)

	resultValues = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 2),
		Lengths: make([]uint32, 2),
	}
	output, err = ExtractFromDatetime("year_month", inputs, resultValues)
	require.NoError(t, err)
	require.Equal(t, "202001123401", string(output.Data))
	require.Equal(t, []uint32{0, 7}, output.Offsets)
	require.Equal(t, []uint32{7, 7}, output.Lengths)
}
