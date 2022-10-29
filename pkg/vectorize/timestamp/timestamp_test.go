// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package timestamp

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestDateToTimestamp(t *testing.T) {
	testCases := []struct {
		name    string
		args    []types.Date
		want    []types.Timestamp
		success bool
	}{
		{
			name:    "normal test cases",
			args:    []types.Date{types.FromCalendar(2022, 3, 30)},
			want:    []types.Timestamp{types.FromClockUTC(2022, 3, 30, 0, 0, 0, 0)},
			success: true,
		},
	}

	for _, v := range testCases {
		reply := make([]types.Timestamp, len(v.args))
		ns := &nulls.Nulls{}
		reply = DateToTimestamp(time.UTC, v.args, ns, reply)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}

func TestDatetimeToTimestamp(t *testing.T) {
	testCases := []struct {
		name    string
		args    []types.Datetime
		want    []types.Timestamp
		success bool
	}{
		{
			name:    "normal test cases",
			args:    []types.Datetime{types.FromClock(2022, 3, 30, 0, 0, 0, 0)},
			want:    []types.Timestamp{types.FromClockUTC(2022, 3, 30, 0, 0, 0, 0)},
			success: true,
		},
	}

	for _, v := range testCases {
		reply := make([]types.Timestamp, len(v.args))
		ns := &nulls.Nulls{}
		reply = DatetimeToTimestamp(time.UTC, v.args, ns, reply)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}

func TestDateStringToTimestamp(t *testing.T) {
	testCases := []struct {
		name    string
		args    []string
		want    []types.Timestamp
		success bool
	}{
		{
			name:    "normal test cases",
			args:    []string{"2022-03-30 00:00:00"},
			want:    []types.Timestamp{types.FromClockUTC(2022, 3, 30, 0, 0, 0, 0)},
			success: true,
		},
		{
			name:    "normal test cases",
			args:    []string{"1022-03-30 00:00:00"},
			want:    []types.Timestamp{types.FromClockUTC(1022, 3, 30, 0, 0, 0, 0)},
			success: true,
		},
	}

	for _, v := range testCases {
		reply := make([]types.Timestamp, len(v.args))
		ns := &nulls.Nulls{}
		reply = DateStringToTimestamp(time.UTC, v.args, ns, reply)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}
