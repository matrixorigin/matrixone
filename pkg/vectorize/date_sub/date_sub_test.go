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
package date_sub

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateSub(t *testing.T) {
	testCases := []struct {
		name  string
		args1 []types.Date
		args2 []int64
		args3 []int64
		want  []types.Date
	}{
		{
			args1: []types.Date{types.FromCalendar(2021, 8, 13)},
			args2: []int64{1},
			args3: []int64{int64(types.Day)},
			want:  []types.Date{types.FromCalendar(2021, 8, 12)},
		},
		{
			args1: []types.Date{types.FromCalendar(2021, 1, 1)},
			args2: []int64{1},
			args3: []int64{int64(types.Day)},
			want:  []types.Date{types.FromCalendar(2020, 12, 31)},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Date, len(c.args1))
			require.Equal(t, c.want, dateSub(c.args1, c.args2, c.args3, got))
		})
	}

}

func TestDatetimeSub(t *testing.T) {
	testCases := []struct {
		name  string
		args1 []types.Datetime
		args2 []int64
		args3 []int64
		want  []types.Datetime
	}{
		{
			args1: []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2: []int64{1},
			args3: []int64{int64(types.MicroSecond)},
			want:  []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 0)},
		},
		{
			args1: []types.Datetime{types.FromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2: []int64{2},
			args3: []int64{int64(types.Second)},
			want:  []types.Datetime{types.FromClock(2020, 1, 1, 1, 0, 59, 1)},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Datetime, len(c.args1))
			require.Equal(t, c.want, datetimeSub(c.args1, c.args2, c.args3, got))
		})
	}

}
