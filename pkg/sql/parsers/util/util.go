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
	"errors"
	"fmt"
	"strconv"
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

// ParseDataSize parses a data size string like "10G", "1024M", "100K" etc.
// Returns the size in bytes.
// Supported units: K (kilobytes), M (megabytes), G (gigabytes), T (terabytes)
// If no unit is specified, the value is treated as bytes.
func ParseDataSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, errors.New("empty size string")
	}

	// Check if the last character is a unit
	lastChar := s[len(s)-1]
	var multiplier uint64 = 1
	var numStr string

	switch lastChar {
	case 'K', 'k':
		multiplier = 1024
		numStr = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		numStr = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = s[:len(s)-1]
	default:
		// No unit, treat as bytes
		numStr = s
	}

	numStr = strings.TrimSpace(numStr)
	if len(numStr) == 0 {
		return 0, errors.New("invalid size format: no numeric value")
	}

	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size value: %s", err.Error())
	}

	return num * multiplier, nil
}
