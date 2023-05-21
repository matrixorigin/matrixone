// Copyright 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"unicode"
)

// From old function code, and they were only used by plan.

const (
	// MaxFsp is the maximum digit of fractional seconds part.
	MaxFsp = 6
)

func ExtractToDateReturnType(format string) (tp types.T, fsp int) {
	isTime, isDate := getTimeFormatType(format)
	if isTime && !isDate {
		tp = types.T_time
	} else if !isTime && isDate {
		tp = types.T_date
	} else {
		tp = types.T_datetime
	}
	//if strings.Contains(format, "%f") {
	//	fsp = MaxFsp
	//}
	return tp, MaxFsp
}

// getTimeFormatType checks the type(Time, Date or Datetime) of a format string.
func getTimeFormatType(format string) (isTime, isDate bool) {
	format = trimWhiteSpace(format)
	var token string
	var succ bool
	for {
		token, format, succ = nextFormatToken(format)
		if len(token) == 0 {
			break
		}
		if !succ {
			isTime, isDate = false, false
			break
		}
		if len(token) >= 2 && token[0] == '%' {
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'f', 'r', 'T':
				isTime = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				isDate = true
			}
		}
		if isTime && isDate {
			break
		}
	}
	return
}

func trimWhiteSpace(input string) string {
	for i, c := range input {
		if !unicode.IsSpace(c) {
			return input[i:]
		}
	}
	return ""
}

func nextFormatToken(format string) (token string, remain string, success bool) {
	if len(format) == 0 {
		return "", "", true
	}

	// Just one character.
	if len(format) == 1 {
		if format[0] == '%' {
			return "", "", false
		}
		return format, "", true
	}

	// More than one character.
	if format[0] == '%' {
		return format[:2], format[2:], true
	}

	return format[:1], format[1:], true
}
