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

package function2

// function overview:
// date_format() function used to format the date value according to the format string. If either argument is NULL, the function returns NULL.
// Input parameter type: date type: datetime, format type: string(constant)
// return type: string
// reference linking:https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format

var (
	// WeekdayNames lists names of weekdays, which are used in builtin function `date_format`.
	WeekdayNames = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// MonthNames lists names of months, which are used in builtin function `date_format`.
	MonthNames = []string{
		"January",
		"February",
		"March",
		"April",
		"May",
		"June",
		"July",
		"August",
		"September",
		"October",
		"November",
		"December",
	}

	// AbbrevWeekdayName lists Abbreviation of week names, which are used int builtin function 'date_format'
	AbbrevWeekdayName = []string{
		"Sun",
		"Mon",
		"Tue",
		"Wed",
		"Thu",
		"Fri",
		"Sat",
	}
)
