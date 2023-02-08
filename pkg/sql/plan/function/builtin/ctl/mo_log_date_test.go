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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMOLogDate(t *testing.T) {
	convey.Convey("MO_LOG_DATE() with multi line", t, func() {
		cases := []struct {
			dateStr string
			expect  string
			isNull  bool
		}{
			{
				dateStr: "2004/04/30",
				expect:  "2004-04-30",
			},
			{
				dateStr: "2012/05/31",
				expect:  "2012-05-31",
			},
			{
				dateStr: "2009/04/23",
				expect:  "2009-04-23",
			},
			{
				dateStr: "2004/01/31",
				expect:  "2004-01-31",
			},
			{
				dateStr: "2018/07/03",
				expect:  "2018-07-03",
			},
			{
				dateStr: "2014/08/25",
				expect:  "2014-08-25",
			},
			{
				dateStr: "2022/06/30",
				expect:  "2022-06-30",
			},
			{
				dateStr: "etl:/i/am/not/include/date/string",
				expect:  ZeroDate,
				isNull:  true,
			},
			{
				dateStr: "etl:/sys/logs/2023/02/03/system.metric/filename",
				expect:  "2023-02-03",
			},
		}

		var datestrs []string
		var expects []string
		var resultNil []uint64
		for idx, c := range cases {
			datestrs = append(datestrs, c.dateStr)
			expects = append(expects, c.expect)
			if c.isNull {
				resultNil = append(resultNil, uint64(idx))
			}
		}

		datestrVector := testutil.MakeVarcharVector(datestrs, nil)
		expectVector := testutil.MakeDateVector(expects, resultNil)

		proc := testutil.NewProc()
		result, err := MOLogDate([]*vector.Vector{datestrVector}, proc)
		require.Nil(t, err)

		compare := testutil.CompareVectors(expectVector, result)
		require.True(t, compare)
	})
}
