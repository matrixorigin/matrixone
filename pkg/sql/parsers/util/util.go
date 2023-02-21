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

import (
	"fmt"
	"strings"
)

//func GetUint64(num interface{}) uint64 {
//	switch v := num.(type) {
//	case int64:
//		return uint64(v)
//	case uint64:
//		return v
//	}
//	return 0
//}

func GetInt64(num interface{}) (int64, string) {
	switch v := num.(type) {
	case int64:
		return v, ""
	}
	return -1, fmt.Sprintf("%d is out of range int64", num)
}

func DealCommentString(str string) string {
	buf := new(strings.Builder)
	for _, ch := range str {
		if ch == '\'' {
			buf.WriteRune('\'')
		}
		buf.WriteRune(ch)
	}
	return buf.String()
}
