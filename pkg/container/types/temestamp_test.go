// Copyright 2021 Matrix Origin
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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTimestamp_String2(t *testing.T) {
	a, err := ParseTimestamp("2012-01-01 11:11:11", 6)
	require.NoError(t, err)
	resultStr := a.String2(6)
	require.Equal(t, "2012-01-01 11:11:11.000000", resultStr)
	a, err = ParseTimestamp("20120101111111", 6)
	require.NoError(t, err)
	resultStr = a.String2(6)
	require.Equal(t, "2012-01-01 11:11:11.000000", resultStr)

	resultStr1 := a.String2(3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.000", resultStr1)

	resultStr2 := a.String2(0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr2)

	a, err = ParseTimestamp("2012-01-01 11:11:11.123", 6)
	resultStr3 := a.String2(0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr3)
	a, err = ParseTimestamp("20120101111111.123", 6)
	resultStr3 = a.String2(0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr3)

	resultStr4 := a.String2(3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123", resultStr4)

	resultStr5 := a.String2(6)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000", resultStr5)

	a, err = ParseTimestamp("2012-01-01 11:11:11.123456", 3)
	resultStr6 := a.String2(0)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11", resultStr6)

	resultStr7 := a.String2(3)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123", resultStr7)

	resultStr8 := a.String2(6)
	require.NoError(t, err)
	require.Equal(t, "2012-01-01 11:11:11.123000", resultStr8)
}

func TestParseTimestamp(t *testing.T) {
	a, err := ParseTimestamp("0001-01-01 00:00:00", 6)
	require.NoError(t, err)
	require.Equal(t, int64(a)+(localTZ<<20), int64(0))

	a, err = ParseTimestamp("0001-01-01 00:00:00.123", 6)
	require.NoError(t, err)
	require.Equal(t, int64(a)+(localTZ<<20), int64(123000))

	a, err = ParseTimestamp("0001-01-01 00:00:00.123456", 6)
	require.NoError(t, err)
	require.Equal(t, int64(a)+(localTZ<<20), int64(123456))

	a, err = ParseTimestamp("0001-01-01 00:00:00.123456", 3)
	require.NoError(t, err)
	require.Equal(t, int64(a)+(localTZ<<20), int64(123000))
}
