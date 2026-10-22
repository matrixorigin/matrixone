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
	"sort"
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
	monthAbbrev   [12]string
}

var temporalLocales = map[string]*temporalLocale{
	"en_US": {
		name:          "en_US",
		weekdays:      [7]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"},
		weekdayAbbrev: [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},
		months:        [12]string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"},
		monthAbbrev:   [12]string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"},
	},
	"fr_FR": {
		name:          "fr_FR",
		weekdays:      [7]string{"dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"},
		weekdayAbbrev: [7]string{"dim", "lun", "mar", "mer", "jeu", "ven", "sam"},
		months:        [12]string{"janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"},
		monthAbbrev:   [12]string{"jan", "fév", "mar", "avr", "mai", "jun", "jui", "aoû", "sep", "oct", "nov", "déc"},
	},
	"de_DE": {
		name:          "de_DE",
		weekdays:      [7]string{"Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag"},
		weekdayAbbrev: [7]string{"So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"},
		months:        [12]string{"Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember"},
		monthAbbrev:   [12]string{"Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"},
	},
	"ja_JP": {
		name:          "ja_JP",
		weekdays:      [7]string{"日曜日", "月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日"},
		weekdayAbbrev: [7]string{"日", "月", "火", "水", "木", "金", "土"},
		months:        [12]string{"1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月"},
		monthAbbrev:   [12]string{" 1月", " 2月", " 3月", " 4月", " 5月", " 6月", " 7月", " 8月", " 9月", "10月", "11月", "12月"},
	},
	"zh_CN": {
		name:          "zh_CN",
		weekdays:      [7]string{"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"},
		weekdayAbbrev: [7]string{"日", "一", "二", "三", "四", "五", "六"},
		months:        [12]string{"一月", "二月", "三月", "四月", "五月", "六月", "七月", "八月", "九月", "十月", "十一月", "十二月"},
		monthAbbrev:   [12]string{" 1月", " 2月", " 3月", " 4月", " 5月", " 6月", " 7月", " 8月", " 9月", "10月", "11月", "12月"},
	},
	"ru_RU": {
		name:          "ru_RU",
		weekdays:      [7]string{"Воскресенье", "Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота"},
		weekdayAbbrev: [7]string{"Вск", "Пнд", "Втр", "Срд", "Чтв", "Птн", "Сбт"},
		months:        [12]string{"Января", "Февраля", "Марта", "Апреля", "Мая", "Июня", "Июля", "Августа", "Сентября", "Октября", "Ноября", "Декабря"},
		monthAbbrev:   [12]string{"Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"},
	},
	"tr_TR": {
		name:          "tr_TR",
		weekdays:      [7]string{"Pazar", "Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi"},
		weekdayAbbrev: [7]string{"Paz", "Pzt", "Sal", "Çrş", "Prş", "Cum", "Cts"},
		months:        [12]string{"Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"},
		monthAbbrev:   [12]string{"Oca", "Şub", "Mar", "Nis", "May", "Haz", "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara"},
	},
	"ar_EG": {
		name:          "ar_EG",
		weekdays:      [7]string{"الأحد", "الاثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت"},
		weekdayAbbrev: [7]string{"ح", "ن", "ث", "ر", "خ", "ج", "س"},
		months:        [12]string{"يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر"},
		monthAbbrev:   [12]string{"ينا", "فبر", "مار", "أبر", "ماي", "يون", "يول", "أغس", "سبت", "أكت", "نوف", "ديس"},
	},
}

// TemporalLocaleNames returns the supported temporal locale inventory in a
// stable order. Callers own the returned slice; the evaluation registry stays private.
func TemporalLocaleNames() []string {
	names := make([]string, 0, len(temporalLocales))
	for name := range temporalLocales {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TemporalLocaleNameForProcess resolves the same effective locale as evaluation,
// including variable-resolver precedence over the process snapshot.
func TemporalLocaleNameForProcess(proc *process.Process) string {
	return temporalLocaleForProcess(proc).name
}

func temporalLocaleForProcess(proc *process.Process) *temporalLocale {
	name := "en_US"
	resolved := false
	if proc != nil && proc.GetResolveVariableFunc() != nil {
		if value, err := proc.GetResolveVariableFunc()("lc_time_names", true, false); err == nil {
			if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
				name = s
				resolved = true
			}
		}
	}
	if !resolved && proc != nil && proc.GetSessionInfo() != nil && proc.GetSessionInfo().LCTimeNames != "" {
		name = proc.GetSessionInfo().LCTimeNames
	}
	if locale, ok := temporalLocales[name]; ok {
		return locale
	}
	return temporalLocales["en_US"]
}

func (locale *temporalLocale) localizedWeekday(weekday int) string {
	if weekday < 0 || weekday >= len(locale.weekdays) {
		return ""
	}
	return locale.weekdays[weekday]
}

func (locale *temporalLocale) localizedWeekdayAbbrev(weekday int) string {
	if weekday < 0 || weekday >= len(locale.weekdayAbbrev) {
		return ""
	}
	return locale.weekdayAbbrev[weekday]
}

func (locale *temporalLocale) localizedMonth(month int) string {
	if month < 1 || month > len(locale.months) {
		return ""
	}
	return locale.months[month-1]
}

func (locale *temporalLocale) localizedMonthAbbrev(month int) string {
	if month < 1 || month > len(locale.monthAbbrev) {
		return ""
	}
	return locale.monthAbbrev[month-1]
}
