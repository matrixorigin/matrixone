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

package date

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestDateTimeToDate(t *testing.T) {
	testCases := []struct {
		name string
		args []types.Datetime
		want []types.Date
	}{
		{
			args: []types.Datetime{types.DatetimeFromClock(2022, 3, 30, 20, 20, 20, 20)},
			want: []types.Date{types.DateFromCalendar(2022, 3, 30)},
		},
	}

	for _, v := range testCases {
		reply := make([]types.Date, len(v.args))
		actual := DatetimeToDate(v.args, reply)

		if !isEqual(actual, v.want) {
			t.Errorf("expect the %v, but got %v", v.want, actual)
		}
	}
}

func isEqual(a, b []types.Date) bool {

	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
