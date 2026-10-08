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

package process

import "time"

// TimeZoneLocationName returns a portable IANA identity. Fixed zones keep the
// legacy offset payload. Local must never mean another machine's local zone.
func TimeZoneLocationName(location *time.Location) string {
	if location == nil || IsFixedTimeZone(location) || location.String() == "Local" || location.String() == "" {
		return ""
	}
	name := location.String()
	if _, err := time.LoadLocation(name); err != nil {
		return ""
	}
	return name
}

// IsFixedTimeZone distinguishes a fixed offset from historical/DST rules using
// the public transition bounds. This also recognizes UTC system configurations
// without assuming that every machine's Local zone has the same rules.
func IsFixedTimeZone(location *time.Location) bool {
	if location == nil {
		return false
	}
	start, end := time.Unix(0, 0).In(location).ZoneBounds()
	return start.IsZero() && end.IsZero()
}
