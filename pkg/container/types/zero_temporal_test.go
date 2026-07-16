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

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseAndFormatZeroDate(t *testing.T) {
	for _, input := range []string{
		"0000-00-00",
		"00000000",
		"0000-00-00 00:00:00",
		"00000000000000",
		" 0000-00-00 ",
	} {
		date, err := ParseDateCast(input)
		require.NoError(t, err)
		require.Equal(t, ZeroDate, date)
		require.Equal(t, "0000-00-00", date.String())
		require.Equal(t, "0000-00-00", string(date.ToBytes(nil)))
	}

	date, err := ParseDateCast("0001-01-01")
	require.NoError(t, err)
	require.NotEqual(t, ZeroDate, date)
	require.Equal(t, "0001-01-01", date.String())
}

func TestParseAndFormatZeroDatetime(t *testing.T) {
	for _, input := range []string{
		"0000-00-00",
		"0000-00-00 00:00:00",
		"0000-00-00 00:00:00.000000",
		"00000000000000",
	} {
		datetime, err := ParseDatetime(input, 6)
		require.NoError(t, err)
		require.Equal(t, ZeroDatetime, datetime)
		require.Equal(t, "0000-00-00 00:00:00", datetime.String())
		require.Equal(t, "0000-00-00 00:00:00.000000", datetime.String2(6))
	}

	datetime, err := ParseDatetime("0001-01-01 00:00:00", 6)
	require.NoError(t, err)
	require.NotEqual(t, ZeroDatetime, datetime)
	require.Equal(t, "0001-01-01 00:00:00.000000", datetime.String2(6))
}

func TestParseAndFormatZeroTimestamp(t *testing.T) {
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("UTC+8", 8*60*60)} {
		timestamp, err := ParseTimestamp(loc, "0000-00-00 00:00:00", 6)
		require.NoError(t, err)
		require.Equal(t, ZeroTimestamp, timestamp)
		require.Equal(t, ZeroDatetime, timestamp.ToDatetime(loc))
		require.Equal(t, "0000-00-00 00:00:00.000000", timestamp.String2(loc, 6))
		require.Equal(t, int64(-1), timestamp.Unix())
		require.Equal(t, float64(-1), timestamp.UnixToFloat())
		decimal, decimalErr := timestamp.UnixToDecimal128()
		require.NoError(t, decimalErr)
		require.Less(t, decimal.Compare(Decimal128{}), 0)
	}

	converted := make([]Datetime, 2)
	valid, err := ParseTimestamp(time.UTC, "2024-01-02 03:04:05", 6)
	require.NoError(t, err)
	_, err = TimestampToDatetime(
		time.FixedZone("UTC+8", 8*60*60),
		[]Timestamp{ZeroTimestamp, valid},
		converted,
	)
	require.NoError(t, err)
	require.Equal(t, ZeroDatetime, converted[0])
	require.Equal(t, "2024-01-02 11:04:05", converted[1].String())
}
