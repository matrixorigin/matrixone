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

package util

// Abbreviate truncates a string from the beginning to the specified length.
// Parameters:
//   - str: the input string
//   - length: the maximum length to truncate to
//     -1: return the complete string
//     0: return empty string
//     >0: return the first 'length' characters, appending "..." if truncated
func Abbreviate(str string, length int) string {
	if length == 0 || length < -1 {
		return ""
	}

	if length == -1 {
		return str
	}

	l := min(len(str), length)
	if l != len(str) {
		return str[:l] + "..."
	}
	return str[:l]
}
