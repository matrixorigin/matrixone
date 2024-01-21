// Copyright 2021 - 2024 Matrix Origin
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

package function

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// DATE_FORMAT       datetime
// handle '%d/%m/%Y' ->	 22/04/2021
func date_format_combine_pattern1(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	month := int(t.Month())
	day := int(t.Day())
	year := int(t.Year())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Date and month conversion
	buf.WriteByte(byte('0' + (day / 10 % 10)))
	buf.WriteByte(byte('0' + (day % 10)))
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (month / 10 % 10)))
	buf.WriteByte(byte('0' + (month % 10)))
	buf.WriteByte('/')

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))
}

// handle '%Y%m%d' ->   20210422
func date_format_combine_pattern2(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(8) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
}

// handle '%Y'  ->   2021
func date_format_combine_pattern3(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := t.Year()
	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))
}

// %Y-%m-%d	               2021-04-22
func date_format_combine_pattern4(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte('-')
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte('-')
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
}

// handle '%Y-%m-%d %H:%i:%s'  ->   2004-04-03 13:11:10
// handle ' %Y-%m-%d %T'   ->   2004-04-03 13:11:10
func date_format_combine_pattern5(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := int(t.Year())
	month := int(t.Month())
	day := int(t.Day())
	hour := int(t.Hour())
	minute := int(t.Minute())
	sec := int(t.Sec())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(19) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	buf.WriteByte('-')

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	buf.WriteByte('-')

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))

	buf.WriteByte(' ')

	// Hour conversion
	buf.WriteByte(byte('0' + (hour / 10)))
	buf.WriteByte(byte('0' + (hour % 10)))

	buf.WriteByte(':')

	// Minute conversion
	buf.WriteByte(byte('0' + (minute / 10)))
	buf.WriteByte(byte('0' + (minute % 10)))

	buf.WriteByte(':')

	// Second conversion
	buf.WriteByte(byte('0' + (sec / 10)))
	buf.WriteByte(byte('0' + (sec % 10)))
}

// handle '%Y/%m/%d'  ->   2010/01/07
func date_format_combine_pattern6(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := t.Year()
	month := int(t.Month())
	day := int(t.Day())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(10) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	// Month conversion
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	// date conversion
	buf.WriteByte('/')
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))
}

// handle '%Y/%m/%d %H:%i:%s'   ->    2010/01/07 23:12:34
// handle '%Y/%m/%d %T'   ->    2010/01/07 23:12:34
func date_format_combine_pattern7(ctx context.Context, t types.Datetime, buf *bytes.Buffer) {
	year := int(t.Year())
	month := int(t.Month())
	day := int(t.Day())
	hour := int(t.Hour())
	minute := int(t.Minute())
	sec := int(t.Sec())

	// Convert numbers to strings using bitwise operations and append them to a buffer
	buf.Grow(19) // Pre allocate sufficient buffer size

	// Year conversion
	buf.WriteByte(byte('0' + (year / 1000 % 10)))
	buf.WriteByte(byte('0' + (year / 100 % 10)))
	buf.WriteByte(byte('0' + (year / 10 % 10)))
	buf.WriteByte(byte('0' + (year % 10)))

	buf.WriteByte('/')

	// Month conversion
	buf.WriteByte(byte('0' + (month / 10)))
	buf.WriteByte(byte('0' + (month % 10)))

	buf.WriteByte('/')

	// date conversion
	buf.WriteByte(byte('0' + (day / 10)))
	buf.WriteByte(byte('0' + (day % 10)))

	buf.WriteByte(' ')

	// Hour conversion
	buf.WriteByte(byte('0' + (hour / 10)))
	buf.WriteByte(byte('0' + (hour % 10)))

	buf.WriteByte(':')

	// Minute conversion
	buf.WriteByte(byte('0' + (minute / 10)))
	buf.WriteByte(byte('0' + (minute % 10)))

	buf.WriteByte(':')

	// Second conversion
	buf.WriteByte(byte('0' + (sec / 10)))
	buf.WriteByte(byte('0' + (sec % 10)))
}
