// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dayofyear

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDayOfYear(t *testing.T) {
	testCases := []struct {
		name string
		args []types.Date
		want []uint16
	}{
		{
			args: []types.Date{types.DateFromCalendar(2021, 8, 13)},
			want: []uint16{225},
		},
		{
			args: []types.Date{types.DateFromCalendar(2022, 3, 28)},
			want: []uint16{87},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]uint16, len(c.args))
			require.Equal(t, c.want, GetDayOfYear(c.args, got))
		})
	}

}
