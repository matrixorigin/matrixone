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

package function

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// temporalLocale contains the names used by DAYNAME, MONTHNAME and the
// locale-sensitive DATE_FORMAT verbs. The server currently exposes the same
// locale set as the compatibility layer; English remains the fallback.
type temporalLocale struct {
	name          string
	weekdays      [7]string // Sunday .. Saturday
	weekdayAbbrev [7]string
	months        [12]string
}

var temporalLocales = map[string]temporalLocale{
	"en_US": {
		name:          "en_US",
		weekdays:      [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"},
		weekdayAbbrev: [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},
		months:        [12]string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"},
	},
	"fr_FR": {
		name:          "fr_FR",
		weekdays:      [7]string{"dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"},
		weekdayAbbrev: [7]string{"dim", "lun", "mar", "mer", "jeu", "ven", "sam"},
		months:        [12]string{"janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"},
	},
	"de_DE": {
		name:          "de_DE",
		weekdays:      [7]string{"Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"},
		weekdayAbbrev: [7]string{"So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"},
		months:        [12]string{"Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"},
	},
	"ja_JP": {
		name:          "ja_JP",
		weekdays:      [7]string{"日曜日", "月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日"},
		weekdayAbbrev: [7]string{"日", "月", "火", "水", "木", "金", "土"},
		months:        [12]string{"1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"},
	},
}

func temporalLocaleForProcess(proc *process.Process) temporalLocale {
	name := "en_US"
	if proc != nil && proc.GetResolveVariableFunc() != nil {
		if value, err := proc.GetResolveVariableFunc()("lc_time_names", true, false); err == nil {
			if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
				name = s
			}
		}
	}
	if locale, ok := temporalLocales[name]; ok {
		return locale
	}
	return temporalLocales["en_US"]
}

func localizedWeekday(proc *process.Process, weekday int) string {
	locale := temporalLocaleForProcess(proc)
	if weekday < 0 || weekday >= len(locale.weekdays) {
		return ""
	}
	return locale.weekdays[weekday]
}

func localizedWeekdayAbbrev(proc *process.Process, weekday int) string {
	locale := temporalLocaleForProcess(proc)
	if weekday < 0 || weekday >= len(locale.weekdayAbbrev) {
		return ""
	}
	return locale.weekdayAbbrev[weekday]
}

func localizedMonth(proc *process.Process, month int) string {
	locale := temporalLocaleForProcess(proc)
	if month < 1 || month > len(locale.months) {
		return ""
	}
	return locale.months[month-1]
}

func localizedMonthAbbrev(proc *process.Process, month int) string {
	value := localizedMonth(proc, month)
	if value == "" {
		return ""
	}
	// MySQL's Japanese month abbreviation is the full numeric month name;
	// other supported locales use the first three Unicode characters.
	if temporalLocaleForProcess(proc).name == "ja_JP" {
		return value
	}
	runes := []rune(value)
	if len(runes) > 3 {
		runes = runes[:3]
	}
	return string(runes)
}
