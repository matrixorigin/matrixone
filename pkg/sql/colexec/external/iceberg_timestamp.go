// Copyright 2026 Matrix Origin
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

package external

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func icebergTimestampLocation(proc *process.Process) *time.Location {
	if proc != nil && proc.Base != nil && proc.Base.SessionInfo.TimeZone != nil {
		return proc.Base.SessionInfo.TimeZone
	}
	return time.Local
}

func icebergLocalTimestampOffsetMicros(loc *time.Location, localMicros int64) int64 {
	if loc == nil {
		loc = time.Local
	}
	offset := icebergOffsetMicrosAt(loc, localMicros)
	utcMicros := localMicros - offset
	next := icebergOffsetMicrosAt(loc, utcMicros)
	if next != offset {
		offset = next
	}
	return offset
}

func icebergOffsetMicrosAt(loc *time.Location, utcMicros int64) int64 {
	seconds := utcMicros / 1_000_000
	nanos := (utcMicros % 1_000_000) * 1_000
	if nanos < 0 {
		seconds--
		nanos += 1_000_000_000
	}
	_, offset := time.Unix(seconds, nanos).In(loc).Zone()
	return int64(offset) * 1_000_000
}
