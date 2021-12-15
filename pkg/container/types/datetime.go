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
	"time"
	"unsafe"
)

const (
	secsPerMinute = 60
	secsPerHour   = 60 * secsPerMinute
	secsPerDay    = 24 * secsPerHour
	secsPerWeek   = 7 * secsPerDay
)

// The higher 44 bits holds number of seconds since January 1, year 1 in Gregorian
// calendar, and lower 20 bits holds number of microseconds

func (a Datetime) String() string {
	return ""
}

const (
	tsMask         = ^uint64(0) >> 1
	hasMonotonic   = 1 << 63
	unixToInternal = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secsPerDay
	wallToInternal = (1884*365 + 1884/4 - 1884/100 + 1884/400) * secsPerDay
)

func Now() Datetime {
	t := time.Now()
	wall := *(*uint64)(unsafe.Pointer(&t))
	ext := *(*int64)(unsafe.Pointer(uintptr(unsafe.Pointer(&t)) + unsafe.Sizeof(wall)))
	var sec, nsec int64
	if wall&hasMonotonic != 0 {
		sec = int64(wall<<1>>31) + wallToInternal
		nsec = int64(wall << 34 >> 34)
	} else {
		sec = ext
		nsec = int64(wall)
	}
	return Datetime((sec << 20) + nsec/1000)
}

func ParseDatetime(s []byte) Datetime {
	return 0
}

func (dt Datetime) ToDate() Date {
	return Date((dt.sec() + localTZ) / secsPerDay)
}

func (dt Datetime) Clock() (hour, min, sec int8) {
	t := (dt.sec() + localTZ) % secsPerDay
	hour = int8(t / secsPerHour)
	min = int8(t % secsPerHour / secsPerMinute)
	sec = int8(t % secsPerMinute)
	return
}

func FromClock(year int32, month, day, hour, min, sec uint8, msec uint32) Datetime {
	days := FromCalendar(year, month, day)
	secs := int64(days)*secsPerDay + int64(hour)*secsPerHour + int64(min)*secsPerMinute + int64(sec) - localTZ
	return Datetime((secs << 20) + int64(msec))
}

func (dt Datetime) sec() int64 {
	return int64(dt) >> 20
}
