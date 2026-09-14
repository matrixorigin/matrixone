// Copyright 2021 - 2024 Matrix Origin
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

package geo

import "strings"

// DistanceUnitScale returns the multiplier that converts a supported length
// unit to meters. Unit names are case-insensitive; aliases are accepted for
// the names used by MySQL's geographic distance functions.
func DistanceUnitScale(unit string) (float64, bool) {
	switch strings.ToLower(unit) {
	case "metre", "meter", "m":
		return 1, true
	case "kilometre", "kilometer", "km":
		return 1_000, true
	case "centimetre", "centimeter", "cm":
		return 0.01, true
	case "millimetre", "millimeter", "mm":
		return 0.001, true
	case "micrometre", "micrometer", "um":
		return 0.000001, true
	case "inch", "in":
		return 0.0254, true
	case "foot", "feet", "ft":
		return 0.3048, true
	case "yard", "yd":
		return 0.9144, true
	case "mile", "mi":
		return 1609.344, true
	case "nautical mile", "nauticalmile", "nmi":
		return 1852, true
	default:
		return 0, false
	}
}
