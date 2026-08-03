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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDateCast(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// 1. yyyy-mm-dd
		{
			name: "yyyy-mm-dd",
			args: args{
				s: "2005-02-23",
			},
			want: "2005-02-23",
		},
		// 2. yyyymmdd
		{
			name: "yyyymmdd",
			args: args{
				s: "20050223",
			},
			want: "2005-02-23",
		},
		// 3. range test
		{
			name: "leap_year",
			args: args{
				s: "19990229",
			},
			wantErr: true,
		},
		{
			name: "month_range1",
			args: args{
				s: "20001329",
			},
			wantErr: true,
		},
		{
			name: "month_range2",
			args: args{
				s: "20000029",
			},
			wantErr: true,
		},
		{
			name: "day_range1",
			args: args{
				s: "20000431",
			},
			wantErr: true,
		},
		{
			name: "day_range2",
			args: args{
				s: "20000400",
			},
			wantErr: true,
		},
		// 4. yyyy-m-dd
		{
			name: "yyyy-m-dd",
			args: args{
				s: "2005-2-23",
			},
			want: "2005-02-23",
		},
		// 5. yyyy-mm-d
		{
			name: "yyyy-mm-d",
			args: args{
				s: "2005-02-2",
			},
			want: "2005-02-02",
		},
		// 6. yyyy-m-d
		{
			name: "yyyy-m-d",
			args: args{
				s: "2005-2-3",
			},
			want: "2005-02-03",
		},
		// 7. valid leap year (year 2000 is a leap year)
		{
			name: "leap_year_valid",
			args: args{s: "2000-02-29"},
			want: "2000-02-29",
		},
		// 8. 7-digit compact (3-digit year)
		{
			name: "7-digit_compact",
			args: args{s: "2220919"},
			want: "0222-09-19",
		},
		// 9. date extraction from datetime string
		{
			name: "datetime_string_returns_date",
			args: args{s: "2005-02-23 10:20:30"},
			want: "2005-02-23",
		},
		{
			name: "punctuation_delimited_date",
			args: args{s: "2005/02/23"},
			want: "2005-02-23",
		},
		{
			name: "punctuation_delimited_datetime",
			args: args{s: "2005:02:23T10:20:30"},
			want: "2005-02-23",
		},
		{
			name: "two_digit_year_colon_date",
			args: args{s: "10:11:12"},
			want: "2010-11-12",
		},
		{
			name: "two_digit_year_dash_date",
			args: args{s: "10-11-12"},
			want: "2010-11-12",
		},
		{
			name: "two_digit_year_upper_window",
			args: args{s: "69:01:01"},
			want: "2069-01-01",
		},
		{
			name: "two_digit_year_lower_window",
			args: args{s: "70:01:01"},
			want: "1970-01-01",
		},
		{
			name: "punctuation_delimited_time",
			args: args{s: "2024/01/15 12*34*56"},
			want: "2024-01-15",
		},
		{
			name:    "non-midnight zero datetime remains invalid",
			args:    args{s: "0000-00-00 12:34:56"},
			wantErr: true,
		},
		// 10. leading/trailing whitespace trimmed
		{
			name: "whitespace_trimmed",
			args: args{s: "  2005-02-23  "},
			want: "2005-02-23",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDateCast(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDateCast() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.wantErr {
				return
			}
			if got.String() != tt.want {
				t.Errorf("ParseDateCast() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseDateCastComponents(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		year    int32
		month   uint8
		day     uint8
		wantErr bool
	}{
		{name: "incomplete dashed", input: "2001-11-00", year: 2001, month: 11, day: 0},
		{name: "incomplete datetime", input: "2001-11-00 12:34:56", year: 2001, month: 11, day: 0},
		{name: "incomplete compact", input: "20011100", year: 2001, month: 11, day: 0},
		{name: "incomplete variable width", input: "2001-11-0", year: 2001, month: 11, day: 0},
		{name: "zero datetime", input: "0000-00-00 12:34:56", year: 0, month: 0, day: 0},
		{name: "complete variable width datetime", input: "2001-1-2 12:34:56", year: 2001, month: 1, day: 2},
		{name: "ISO datetime", input: "2024-01-01T12:34:56", year: 2024, month: 1, day: 1},
		{name: "slash date", input: "2024/01/15", year: 2024, month: 1, day: 15},
		{name: "colon datetime", input: "2024:01:15 12:34:56", year: 2024, month: 1, day: 15},
		{name: "mixed punctuation separators", input: "2024/01-15", year: 2024, month: 1, day: 15},
		{name: "dot date", input: "2024.01.15", year: 2024, month: 1, day: 15},
		{name: "two digit colon date", input: "10:11:12", year: 2010, month: 11, day: 12},
		{name: "two digit dashed date", input: "10-11-12", year: 2010, month: 11, day: 12},
		{name: "two digit year upper window", input: "69:01:01", year: 2069, month: 1, day: 1},
		{name: "two digit year lower window", input: "70:01:01", year: 1970, month: 1, day: 1},
		{name: "punctuation delimited time", input: "2024/01/15 12*34*56", year: 2024, month: 1, day: 15},
		{name: "year zero date", input: "0000-01-01", year: 0, month: 1, day: 1},
		{name: "malformed", input: "2001-11-x", wantErr: true},
		{name: "malformed month separator", input: "2024-0x-01", wantErr: true},
		{name: "invalid hour", input: "2024-01-01 24:00:00", wantErr: true},
		{name: "invalid minute", input: "2024-01-01 23:60:00", wantErr: true},
		{name: "oversized year", input: "4294967297-01-01", wantErr: true},
		{name: "dangling ISO separator", input: "2024-01-01T", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			year, month, day, err := ParseDateCastComponents(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.year, year)
			assert.Equal(t, tt.month, month)
			assert.Equal(t, tt.day, day)
		})
	}
}

func TestValidCalendarDateYearZero(t *testing.T) {
	require.True(t, ValidCalendarDate(0, 1, 1))
	require.False(t, ValidCalendarDate(0, 2, 29))
	require.False(t, ValidDate(0, 1, 1))
	require.Equal(t, Sunday, DayOfWeekFromCalendar(0, 1, 1))
	require.Equal(t, 52, WeekFromCalendar(0, 1, 1, 3))
}

func TestParseDateCastStrictValidation(t *testing.T) {
	for _, input := range []string{
		"2024-01x-02",
		"2024-01-01 12:3x:56",
		"2024-01-01 24:00:00",
		"4294967297-01-01",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := ParseDateCast(input)
			require.Error(t, err)
		})
	}
}

func BenchmarkParseDateCast(b *testing.B) {
	inputs := []struct {
		name  string
		value string
	}{
		{name: "dashed", value: "2024-01-15"},
		{name: "compact", value: "20240115"},
		{name: "datetime with fraction", value: "2024-01-15 12:34:56.123456"},
	}

	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := ParseDateCast(input.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_date_toBytes(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// 1. yyyy-mm-dd
		{
			name: "yyyy-mm-dd",
			args: args{
				s: "2005-02-23",
			},
			want: "2005-02-23",
		},
		// 2. yyyymmdd
		{
			name: "yyyymmdd",
			args: args{
				s: "20050223",
			},
			want: "2005-02-23",
		},
		// 4. yyyy-m-dd
		{
			name: "yyyy-m-dd",
			args: args{
				s: "2005-2-23",
			},
			want: "2005-02-23",
		},
		// 5. yyyy-mm-d
		{
			name: "yyyy-mm-d",
			args: args{
				s: "2005-02-2",
			},
			want: "2005-02-02",
		},
		// 6. yyyy-m-d
		{
			name: "yyyy-m-d",
			args: args{
				s: "2005-2-3",
			},
			want: "2005-02-03",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDateCast(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDateCast() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.wantErr {
				return
			}
			var dBytes [DateToBytesLength]byte
			dSlice := got.ToBytes(dBytes[:0])
			s := string(dSlice)
			if s != tt.want {
				t.Errorf("ParseDateCast() got = %v, want %v", s, tt.want)
			}
		})
	}
}

func BenchmarkParseDate(b *testing.B) {
	s := "2020-12-21"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseDateCast(s)
		require.NoError(b, err)
	}
}

func Test_date_String(t *testing.T) {
	for i := 0; i < 100; i++ {
		x, y := i/10, i%10
		chs := hundredToChars[i]
		assert.Equal(t, uint8(x+'0'), chs[0])
		assert.Equal(t, uint8(y+'0'), chs[1])
	}
}
