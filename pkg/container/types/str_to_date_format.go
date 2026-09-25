// Copyright 2021 - 2026 Matrix Origin
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

// ClassifyStrToDateFormat classifies format directives for both bound literals
// and runtime formats. A doubled percent sign is literal text, not a directive.
func ClassifyStrToDateFormat(format string) (isTime, isDate, hasMicroseconds bool) {
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		if i+1 == len(format) {
			return false, false, false
		}
		i++
		switch format[i] {
		case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'r', 'T':
			isTime = true
		case 'f':
			isTime, hasMicroseconds = true, true
		case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e', 'j',
			'a', 'W', 'w', 'U', 'u', 'V', 'v', 'X', 'x':
			isDate = true
		}
	}
	return
}
