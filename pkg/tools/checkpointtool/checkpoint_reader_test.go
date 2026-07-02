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

package checkpointtool

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVecValueToString_SQLLikeFormatting(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	timeVec := vector.NewVec(types.New(types.T_time, 0, 6))
	defer timeVec.Free(mp)
	timeValue, err := types.ParseTime("12:34:56.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(timeVec, timeValue, false, mp))
	require.Equal(t, "12:34:56.123456", vecValueToString(timeVec, 0))

	datetimeVec := vector.NewVec(types.New(types.T_datetime, 0, 6))
	defer datetimeVec.Free(mp)
	datetimeValue, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(datetimeVec, datetimeValue, false, mp))
	require.Equal(t, "2024-01-02 03:04:05.123456", vecValueToString(datetimeVec, 0))

	timestampVec := vector.NewVec(types.New(types.T_timestamp, 0, 6))
	defer timestampVec.Free(mp)
	timestampValue, err := types.ParseTimestamp(time.Local, "2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(timestampVec, timestampValue, false, mp))
	require.Equal(t, timestampValue.String2(time.Local, 6), vecValueToString(timestampVec, 0))

	decimalVec := vector.NewVec(types.New(types.T_decimal64, 18, 2))
	defer decimalVec.Free(mp)
	decimalValue := types.Decimal64(12345)
	require.NoError(t, vector.AppendFixed(decimalVec, decimalValue, false, mp))
	require.Equal(t, "123.45", vecValueToString(decimalVec, 0))

	uuidVec := vector.NewVec(types.T_uuid.ToType())
	defer uuidVec.Free(mp)
	uuidValue, err := types.ParseUuid("123e4567-e89b-12d3-a456-426614174000")
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(uuidVec, uuidValue, false, mp))
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", vecValueToString(uuidVec, 0))

	jsonVec := vector.NewVec(types.T_json.ToType())
	defer jsonVec.Free(mp)
	require.NoError(t, vector.AppendBytes(jsonVec, []byte(`{"a":1}`), false, mp))
	require.Equal(t, `{"a":1}`, vecValueToString(jsonVec, 0))

	nullVec := vector.NewVec(types.T_int64.ToType())
	defer nullVec.Free(mp)
	require.NoError(t, vector.AppendFixed(nullVec, int64(0), true, mp))
	require.Equal(t, "NULL", vecValueToString(nullVec, 0))
}

func TestShouldIncludeIncrementalCheckpointWithoutBase(t *testing.T) {
	zero := types.TS{}
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)

	assert.True(t, shouldIncludeIncrementalCheckpoint(zero, ts1, zero, ts1, false))
	assert.True(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts2, false))
	assert.False(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts1, false))

	assert.False(t, shouldIncludeIncrementalCheckpoint(zero, ts1, zero, ts1, true))
	assert.True(t, shouldIncludeIncrementalCheckpoint(ts1, ts2, zero, ts2, true))
}
