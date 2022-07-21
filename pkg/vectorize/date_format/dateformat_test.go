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

package date_format

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDateFormat(t *testing.T) {
	cases := []struct {
		datestr string
		format  []string
		Expect  []string
	}{
		{
			datestr: "2010-01-07 23:12:34.12345",
			format:  []string{`%b`, `%M`, `%m`, `%c`, `%D`, `%d`, `%e`, `%j`, `%k`, `%h`, `%i`, `%p`, `%r`, `%T`, `%s`, `%f`, `%U`, `%u`, `%V`, `%v`, `%a`, `%W`, `%w`, `%X`, `%x`, `%Y`, `%y`, `%%`},
			Expect:  []string{`Jan`, `January`, `01`, `1`, `7th`, `07`, `7`, `007`, `23`, `11`, `12`, `PM`, `11:12:34 PM`, `23:12:34`, `34`, `123450`, `01`, `01`, `01`, `01`, `Thu`, `Thursday`, `4`, `2010`, `2010`, `2010`, `10`, `%`},
		},
	}

	for _, c := range cases {
		datetime, err := types.ParseDatetime(c.datestr, 5)
		if err != nil {
			t.Fatalf("parse datetime string err %+v", err)
		}

		for i := 0; i < len(c.format); i++ {
			res, err := datetimeFormat(datetime, c.format[i])
			if err != nil {
				t.Fatalf("dateformat exec error: %+v", err)
			}
			require.Equal(t, c.Expect[i], res)
		}
	}
}

func TestDateFromat2(t *testing.T) {
	cases := []struct {
		name    string
		datestr string
		format  string
		expect  string
	}{
		{
			name:    "Test01",
			datestr: "2010-01-07 23:12:34.12345",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  `Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`,
		},
		{
			name:    "Test02",
			datestr: "2012-12-21 23:12:34.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			expect:  "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
		},
		{
			name:    "Test03",
			datestr: "0001-01-01 00:00:00.123456",
			format:  `%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 01 0001 0001 01 %`,
		},
		{
			name:    "Test04",
			datestr: "2016-09-3 00:59:59.123456",
			format:  `abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`,
			expect:  `abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`,
		},
		{
			datestr: "2012-10-01 00:00:00",
			format:  `%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%`,
			expect:  `Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			datetime, err := types.ParseDatetime(c.datestr, 6)
			if err != nil {
				t.Fatalf("parse datetime string err %+v", err)
			}
			result, err := datetimeFormat(datetime, c.format)
			if err != nil {
				t.Fatalf("dateformat exec error: %+v", err)
			}
			require.Equal(t, c.expect, result)
		})
	}
}
