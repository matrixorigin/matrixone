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

import (
	"sync"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

type distanceUnit struct {
	name  string
	scale float64
}

var distanceUnits = []distanceUnit{
	{name: "millimetre", scale: 0.001},
	{name: "centimetre", scale: 0.01},
	{name: "metre", scale: 1},
	{name: "foot", scale: 0.3048},
	{name: "US survey foot", scale: 0.30480060960121924},
	{name: "Clarke's foot", scale: 0.3047972654},
	{name: "fathom", scale: 1.8288},
	{name: "nautical mile", scale: 1852},
	{name: "German legal metre", scale: 1.0000135965},
	{name: "US survey chain", scale: 20.11684023368047},
	{name: "US survey link", scale: 0.2011684023368047},
	{name: "US survey mile", scale: 1609.3472186944375},
	{name: "kilometre", scale: 1000},
	{name: "Clarke's yard", scale: 0.9143917962},
	{name: "Clarke's chain", scale: 20.1166195164},
	{name: "Clarke's link", scale: 0.201166195164},
	{name: "British yard (Sears 1922)", scale: 0.9143984146160287},
	{name: "British foot (Sears 1922)", scale: 0.3047994715386762},
	{name: "British chain (Sears 1922)", scale: 20.116765121552632},
	{name: "British link (Sears 1922)", scale: 0.2011676512155263},
	{name: "British yard (Benoit 1895 A)", scale: 0.9143992},
	{name: "British foot (Benoit 1895 A)", scale: 0.3047997333333333},
	{name: "British chain (Benoit 1895 A)", scale: 20.1167824},
	{name: "British link (Benoit 1895 A)", scale: 0.201167824},
	{name: "British yard (Benoit 1895 B)", scale: 0.9143992042898124},
	{name: "British foot (Benoit 1895 B)", scale: 0.30479973476327077},
	{name: "British chain (Benoit 1895 B)", scale: 20.116782494375872},
	{name: "British link (Benoit 1895 B)", scale: 0.2011678249437587},
	{name: "British foot (1865)", scale: 0.30480083333333335},
	{name: "Indian foot", scale: 0.30479951024814694},
	{name: "Indian foot (1937)", scale: 0.30479841},
	{name: "Indian foot (1962)", scale: 0.3047996},
	{name: "Indian foot (1975)", scale: 0.3047995},
	{name: "Indian yard", scale: 0.9143985307444408},
	{name: "Indian yard (1937)", scale: 0.91439523},
	{name: "Indian yard (1962)", scale: 0.9143988},
	{name: "Indian yard (1975)", scale: 0.9143985},
	{name: "Statute mile", scale: 1609.344},
	{name: "Gold Coast foot", scale: 0.3047997101815088},
	{name: "British foot (1936)", scale: 0.3048007491},
	{name: "yard", scale: 0.9144},
	{name: "chain", scale: 20.1168},
	{name: "link", scale: 0.201168},
	{name: "British yard (Sears 1922 truncated)", scale: 0.914398},
	{name: "British foot (Sears 1922 truncated)", scale: 0.30479933333333337},
	{name: "British chain (Sears 1922 truncated)", scale: 20.116756},
	{name: "British link (Sears 1922 truncated)", scale: 0.20116756},
}

var distanceUnitCollatorPool = sync.Pool{
	New: func() any {
		return collate.New(language.Und, collate.Loose)
	},
}
var distanceUnitScales = buildDistanceUnitScales()

func buildDistanceUnitScales() map[string]float64 {
	scales := make(map[string]float64, len(distanceUnits))
	for _, unit := range distanceUnits {
		scales[distanceUnitKey(unit.name)] = unit.scale
	}
	return scales
}

func distanceUnitKey(unit string) string {
	collator := distanceUnitCollatorPool.Get().(*collate.Collator)
	defer distanceUnitCollatorPool.Put(collator)
	var buffer collate.Buffer
	return string(collator.KeyFromString(&buffer, unit))
}

// DistanceUnitScale returns the multiplier that converts a supported length
// unit to meters. The names and factors match MySQL's
// INFORMATION_SCHEMA.ST_UNITS_OF_MEASURE table; lookup follows its
// case- and accent-insensitive collation while preserving spaces and
// punctuation.
func DistanceUnitScale(unit string) (float64, bool) {
	scale, ok := distanceUnitScales[distanceUnitKey(unit)]
	return scale, ok
}
