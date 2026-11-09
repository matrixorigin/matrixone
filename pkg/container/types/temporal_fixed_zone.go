// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import "time"

// FixedZoneOffset proves that loc has one offset for all time. ZoneBounds uses
// the zero Time for both an unbounded edge and the real year-1 instant. Probes
// on both sides of that instant disambiguate those cases without inspecting
// time.Location's private representation.
func FixedZoneOffset(loc *time.Location) (int, bool) {
	if loc == nil {
		return 0, false
	}
	epoch := time.Unix(0, 0).In(loc)
	start, end := epoch.ZoneBounds()
	if !start.IsZero() || !end.IsZero() {
		return 0, false
	}
	_, offset := epoch.Zone()
	zero := time.Time{}
	before := zero.Add(-time.Microsecond).In(loc)
	after := zero.In(loc)
	beforeStart, beforeEnd := before.ZoneBounds()
	afterStart, afterEnd := after.ZoneBounds()
	if !beforeStart.IsZero() || !afterEnd.IsZero() ||
		(!beforeEnd.IsZero() && !beforeEnd.After(before)) ||
		(!afterStart.IsZero() && afterStart.After(after)) {
		return 0, false
	}
	_, beforeOffset := before.Zone()
	_, afterOffset := after.Zone()
	if beforeOffset != offset || afterOffset != offset {
		return 0, false
	}
	return offset, true
}
