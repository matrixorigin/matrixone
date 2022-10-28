// Copyright 2022 Matrix Origin
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

package timediff

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTimeDiffInTime(t *testing.T) {
	testCases := []struct {
		name        string
		firstValue  string
		secondValue string
		want        string
		success     bool
	}{
		{
			name:        "TestCase01",
			firstValue:  "22:22:22",
			secondValue: "11:11:11",
			want:        "11:11:11",
		},
		{
			name:        "TestCase02",
			firstValue:  "22:22:22",
			secondValue: "-11:11:11",
			want:        "33:33:33",
		},
		{
			name:        "TestCase03",
			firstValue:  "-22:22:22",
			secondValue: "11:11:11",
			want:        "-33:33:33",
		},
		{
			name:        "TestCase04",
			firstValue:  "-22:22:22",
			secondValue: "-11:11:11",
			want:        "-11:11:11",
		},
		{
			name:        "TestCase05",
			firstValue:  "11:11:11",
			secondValue: "22:22:22",
			want:        "-11:11:11",
		},
		{
			name:        "TestCase06",
			firstValue:  "11:11:11",
			secondValue: "-22:22:22",
			want:        "33:33:33",
		},
		{
			name:        "TestCase07",
			firstValue:  "-11:11:11",
			secondValue: "-22:22:22",
			want:        "11:11:11",
		},
		{
			name:        "TestCase08",
			firstValue:  "-11:11:11",
			secondValue: "22:22:22",
			want:        "-33:33:33",
		},
		{
			name:        "TestCase09",
			firstValue:  "-838:59:59",
			secondValue: "838:59:59",
			want:        "-838:59:59",
		},
		{
			name:        "TestCase10",
			firstValue:  "-838:59:59",
			secondValue: "-838:59:59",
			want:        "00:00:00",
		},
		{
			name:        "TestCase11",
			firstValue:  "838:59:59",
			secondValue: "838:59:59",
			want:        "00:00:00",
		},
		{
			name:        "TestCase12",
			firstValue:  "838:59:59",
			secondValue: "-838:59:59",
			want:        "838:59:59",
		},
	}

	for _, v := range testCases {
		//input
		firstV, err := types.ParseTime(v.firstValue, 0)
		require.NoError(t, err)
		secondV, err := types.ParseTime(v.secondValue, 0)
		require.NoError(t, err)

		//result
		res, err := timeDiff(firstV, secondV)

		//test
		require.NoError(t, err)
		require.Equal(t, res, v.want)
	}
}

func TestTimeDiffInDateTime(t *testing.T) {
	testCases := []struct {
		name        string
		firstValue  string
		secondValue string
		want        string
		success     bool
	}{
		{
			name:        "TestCase01",
			firstValue:  "2012-12-12 22:22:22",
			secondValue: "2012-12-12 11:11:11",
			want:        "11:11:11",
		},
		{
			name:        "TestCase02",
			firstValue:  "2012-12-12 11:11:11",
			secondValue: "2012-12-12 22:22:22",
			want:        "-11:11:11",
		},
		{
			name:        "TestCase03",
			firstValue:  "2012-12-12 22:22:22",
			secondValue: "2000-12-12 11:11:11",
			want:        "838:59:59",
		},
		{
			name:        "TestCase04",
			firstValue:  "2000-12-12 11:11:11",
			secondValue: "2012-12-12 22:22:22",
			want:        "-838:59:59",
		},
		{
			name:        "TestCase05",
			firstValue:  "2012-12-12 22:22:22",
			secondValue: "2012-10-10 11:11:11",
			want:        "838:59:59",
		},
		{
			name:        "TestCase06",
			firstValue:  "2012-10-10 11:11:11",
			secondValue: "2012-12-12 22:22:22",
			want:        "-838:59:59",
		},
		{
			name:        "TestCase07",
			firstValue:  "2012-12-12 22:22:22",
			secondValue: "2012-12-10 11:11:11",
			want:        "59:11:11",
		},
		{
			name:        "TestCase08",
			firstValue:  "2012-12-10 11:11:11",
			secondValue: "2012-12-12 22:22:22",
			want:        "-59:11:11",
		},
		{
			name:        "TestCase09",
			firstValue:  "2012-12-10 11:11:11",
			secondValue: "2012-12-10 11:11:11",
			want:        "00:00:00",
		},
	}

	for _, v := range testCases {
		//input
		firstV, err := types.ParseDatetime(v.firstValue, 0)
		require.NoError(t, err)
		secondV, err := types.ParseDatetime(v.secondValue, 0)
		require.NoError(t, err)

		//result
		res, err := timeDiff(firstV, secondV)

		//test
		require.NoError(t, err)
		require.Equal(t, res, v.want)
	}
}
