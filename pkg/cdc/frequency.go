// Copyright 2024 Matrix Origin
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

package cdc

import (
	"math"
	"regexp"
	"strconv"
	"strings"
)

// IsValidFrequency reports whether a CDC frequency is a positive integer followed by m or h.
func IsValidFrequency(freq string) bool {
	if !strings.HasSuffix(freq, "m") && !strings.HasSuffix(freq, "h") {
		return false
	}

	numPart := strings.TrimSuffix(strings.TrimSuffix(freq, "m"), "h")

	matched, _ := regexp.MatchString(`^[1-9]\d*$`, numPart)
	if !matched {
		return false
	}

	num, err := strconv.Atoi(numPart)
	if err != nil {
		return false
	}

	if num < 0 {
		return false
	}

	if num > 10000000 {
		return false
	}

	return true
}

// FrequencyHours converts a validated CDC frequency to hours, rounding minutes up.
func FrequencyHours(freq string) int64 {
	if strings.HasSuffix(freq, "h") {
		hoursStr := strings.TrimSuffix(freq, "h")
		hours, _ := strconv.Atoi(hoursStr)
		return int64(hours)
	}

	if strings.HasSuffix(freq, "m") {
		minStr := strings.TrimSuffix(freq, "m")
		minutes, _ := strconv.Atoi(minStr)

		hours := int(math.Ceil(float64(minutes) / 60.0))

		return int64(hours)
	}

	return 0
}
