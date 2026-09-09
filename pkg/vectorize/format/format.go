// Copyright 2022 Matrix Origin
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

package format

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// FormatFunc is the locale format function signature.
type FormatFunc func(string, string) (string, error)

func GetNumberFormat(number, scale, locale string) (string, error) {
	return getFormatFunctionWithLocale(locale)(number, scale)
}

// GetFormatFunctionWithLocate get the format function for sepcific locale.
func getFormatFunctionWithLocale(locale string) FormatFunc {
	formatFunc, exist := localeToFormatFunction[locale]
	if exist {
		return formatFunc
	}
	if formatFunc, exist = localeToFormatFunctionInsensitive[strings.ToLower(locale)]; exist {
		return formatFunc
	}
	return formatENUS
}

// localeToFormatFunction is the string represent of locale format function.
var localeToFormatFunction = map[string]FormatFunc{
	//formatENUS
	"ar_AE": formatENUS,
	"ar_BH": formatENUS,
	"ar_DZ": formatENUS,
	"ar_EG": formatENUS,
	"ar_IN": formatENUS,
	"ar_IQ": formatENUS,
	"ar_JO": formatENUS,
	"ar_KW": formatENUS,
	"ar_LB": formatENUS,
	"ar_LY": formatENUS,
	"ar_MA": formatENUS,
	"ar_OM": formatENUS,
	"ar_QA": formatENUS,
	"ar_SD": formatENUS,
	"ar_SY": formatENUS,
	"ar_TN": formatENUS,
	"ar_YE": formatENUS,
	"en_AU": formatENUS,
	"en_CA": formatENUS,
	"en_GB": formatENUS,
	"en_IN": formatIndian,
	"en_NZ": formatENUS,
	"en_PH": formatENUS,
	"en_US": formatENUS,
	"en_ZA": formatENUS,
	"en_ZW": formatENUS,
	"es_DO": formatENUS,
	"es_GT": formatENUS,
	"es_HN": formatENUS,
	"en_MX": formatENUS,
	"es_NI": formatENUS,
	"es_PA": formatENUS,
	"es_PR": formatENUS,
	"es_SV": formatENUS,
	"es_US": formatENUS,
	"gu_IN": formatENUS,
	"he_IL": formatENUS,
	"hi_IN": formatENUS,
	"ja_JP": formatENUS,
	"ko_KR": formatENUS,
	"ms_MY": formatENUS,
	"ta_IN": formatIndian,
	"te_IN": formatIndian,
	"th_TH": formatENUS,
	"ur_PK": formatENUS,
	"zh_CN": formatENUS,
	"zh_HK": formatENUS,
	"zh_TW": formatENUS,

	//formatARSA
	"ar_SA": formatARSA,
	"sr_RS": formatARSA,

	//formatBEBY
	"be_BY": formatBEBY,
	"da_DK": formatBEBY,
	"de_BE": formatBEBY,
	"de_DE": formatBEBY,
	"de_LU": formatBEBY,
	"es_AR": formatBEBY,
	"es_BO": formatBEBY,
	"es_CL": formatBEBY,
	"es_CO": formatBEBY,
	"es_EC": formatBEBY,
	"es_ES": formatBEBY,
	"es_PY": formatBEBY,
	"es_UY": formatBEBY,
	"es_VE": formatBEBY,
	"fo_FO": formatBEBY,
	"hu_HU": formatBEBY,
	"id_ID": formatBEBY,
	"is_IS": formatBEBY,
	"lt_LT": formatBEBY,
	"mn_MN": formatBEBY,
	"nb_NO": formatBEBY,
	"no_NO": formatBEBY,
	"ru_UA": formatBEBY,
	"sq_AL": formatBEBY,
	"tr_TR": formatBEBY,
	"uk_UA": formatBEBY,
	"vi_VN": formatBEBY,
	"ro_RO": formatBEBY,

	//formatBGBG
	"bg_BG": formatBGBG,
	"cs_CZ": formatBGBG,
	"es_CR": formatBGBG,
	"et_EE": formatBGBG,
	"fi_FI": formatBGBG,
	"lv_LV": formatBGBG,
	"mk_MK": formatBGBG,
	"ru_RU": formatBGBG,
	"sk_SK": formatBGBG,
	"sv_FI": formatBGBG,
	"sv_SE": formatBGBG,

	//formatDECH
	"de_CH": formatDECH,

	//formatCAES
	"ca_ES": formatCAES,
	"de_AT": formatCAES,
	"el_GR": formatCAES,
	"eu_ES": formatCAES,
	"fr_BE": formatCAES,
	"fr_CA": formatCAES,
	"fr_CH": formatCAES,
	"fr_FR": formatCAES,
	"fr_LU": formatCAES,
	"gl_ES": formatCAES,
	"hr_HR": formatCAES,
	"it_IT": formatCAES,
	"nl_BE": formatCAES,
	"nl_NL": formatCAES,
	"pl_PL": formatCAES,
	"pt_BR": formatCAES,
	"pt_PT": formatCAES,
	"sl_SI": formatCAES,

	//formatITCH
	"it_CH": formatITCH,
	"rm_CH": formatITCH,
}

// Keep the canonical map readable while making locale lookup case-insensitive.
// The second map is built once, so a row-wise FORMAT(..., locale) call does not
// scan all locale names or mutate shared state.
var localeToFormatFunctionInsensitive = func() map[string]FormatFunc {
	result := make(map[string]FormatFunc, len(localeToFormatFunction))
	for locale, formatFunc := range localeToFormatFunction {
		result[strings.ToLower(locale)] = formatFunc
	}
	return result
}()

// format number like 20,000,000.0000
func formatENUS(number string, scale string) (string, error) {
	return format(number, scale, []byte{','}, []byte{'.'})
}

// format number like 20000000.0000
func formatARSA(number string, scale string) (string, error) {
	return format(number, scale, []byte{}, []byte{'.'})
}

// format number like 20.000.000,0000
func formatBEBY(number string, scale string) (string, error) {
	return format(number, scale, []byte{'.'}, []byte{','})
}

// format number like 20 000 000,0000
func formatBGBG(number string, scale string) (string, error) {
	return format(number, scale, []byte{' '}, []byte{','})
}

// format number like 20'000'000.0000
func formatDECH(number string, scale string) (string, error) {
	return format(number, scale, []byte{'\''}, []byte{'.'})
}

// format number like 20000000,0000
func formatCAES(number string, scale string) (string, error) {
	return format(number, scale, []byte{}, []byte{','})
}

// format number like 20'000'000.0000
func formatITCH(number string, scale string) (string, error) {
	return format(number, scale, []byte{'\''}, []byte{','})
}

func formatIndian(number string, scale string) (string, error) {
	return formatWithGrouping(number, scale, []byte{','}, []byte{'.'}, 3, 2)
}

const maxFormatDecimals = 30

func format(number, scale string, comma, decimalPoint []byte) (string, error) {
	return formatWithGrouping(number, scale, comma, decimalPoint, 3, 3)
}

func formatWithGrouping(
	number, scale string,
	comma, decimalPoint []byte,
	primaryGroupSize, secondaryGroupSize int,
) (string, error) {
	if len(number) == 0 {
		return "", nil
	}

	decimals := parseFormatScale(scale)
	integer, fraction, negative, scientific, token, ok := parseFormatNumber(number)
	if !ok {
		integer = "0"
		fraction = ""
		negative = false
	}

	if scientific {
		value, err := strconv.ParseFloat(token, 64)
		if err != nil || math.IsInf(value, 0) {
			switch {
			case math.IsInf(value, 1):
				integer, fraction, negative = maxFloatComponents(false)
			case math.IsInf(value, -1):
				integer, fraction, negative = maxFloatComponents(true)
			default:
				// A range error with a zero value is an underflow. It is safe to
				// format it as zero at FORMAT's maximum 30 decimal places.
				integer, fraction, negative = "0", "", false
			}
		} else {
			expanded := strconv.FormatFloat(value, 'f', -1, 64)
			integer, fraction, negative, _, _, ok = parseFormatNumber(expanded)
			if !ok {
				integer, fraction, negative = "0", "", false
			}
		}
	}

	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}

	roundUp := decimals < len(fraction) && fraction[decimals] >= '5'
	if len(fraction) > decimals {
		fraction = fraction[:decimals]
	}
	if len(fraction) < decimals {
		fraction += strings.Repeat("0", decimals-len(fraction))
	}
	if roundUp {
		combined := incrementDecimalDigits(integer + fraction)
		if decimals == 0 {
			integer = combined
			fraction = ""
		} else {
			split := len(combined) - decimals
			integer = combined[:split]
			fraction = combined[split:]
		}
	}

	if isZeroFormatNumber(integer, fraction) {
		negative = false
	}
	var buffer bytes.Buffer
	if negative {
		buffer.WriteByte('-')
	}
	if len(comma) != 0 {
		addGrouped(comma, &buffer, integer, primaryGroupSize, secondaryGroupSize)
	} else {
		buffer.WriteString(integer)
	}
	if decimals > 0 {
		buffer.Write(decimalPoint)
		buffer.WriteString(fraction)
	}
	return buffer.String(), nil
}

func parseFormatScale(scale string) int {
	scale = strings.TrimLeftFunc(scale, unicode.IsSpace)
	if len(scale) == 0 {
		return 0
	}
	if scale[0] == '-' {
		return 0
	}
	if scale[0] == '+' {
		scale = scale[1:]
	}
	decimals := 0
	for i := 0; i < len(scale); i++ {
		if scale[i] < '0' || scale[i] > '9' {
			break
		}
		if decimals < maxFormatDecimals {
			decimals = decimals*10 + int(scale[i]-'0')
			if decimals > maxFormatDecimals {
				return maxFormatDecimals
			}
		}
	}
	return decimals
}

func parseFormatNumber(number string) (
	integer, fraction string,
	negative, scientific bool,
	token string,
	ok bool,
) {
	number = strings.TrimLeftFunc(number, unicode.IsSpace)
	if len(number) == 0 {
		return "", "", false, false, "", false
	}
	i := 0
	negative = number[i] == '-'
	if negative || number[i] == '+' {
		i++
	}
	integerStart := i
	for i < len(number) && number[i] >= '0' && number[i] <= '9' {
		i++
	}
	integer = number[integerStart:i]
	if i < len(number) && number[i] == '.' {
		i++
		fractionStart := i
		for i < len(number) && number[i] >= '0' && number[i] <= '9' {
			i++
		}
		fraction = number[fractionStart:i]
	}
	if len(integer) == 0 && len(fraction) == 0 {
		return "", "", false, false, "", false
	}
	tokenEnd := i
	if i < len(number) && (number[i] == 'e' || number[i] == 'E') {
		j := i + 1
		if j < len(number) && (number[j] == '+' || number[j] == '-') {
			j++
		}
		exponentStart := j
		for j < len(number) && number[j] >= '0' && number[j] <= '9' {
			j++
		}
		if j > exponentStart {
			scientific = true
			tokenEnd = j
		}
	}
	return integer, fraction, negative, scientific, number[:tokenEnd], true
}

func maxFloatComponents(negative bool) (integer, fraction string, isNegative bool) {
	value := strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64)
	point := strings.IndexByte(value, '.')
	if point < 0 {
		return value, "", negative
	}
	return value[:point], value[point+1:], negative
}

func incrementDecimalDigits(value string) string {
	digits := []byte(value)
	for i := len(digits) - 1; i >= 0; i-- {
		if digits[i] != '9' {
			digits[i]++
			return string(digits)
		}
		digits[i] = '0'
	}
	return "1" + string(digits)
}

func isZeroFormatNumber(integer, fraction string) bool {
	return strings.Trim(integer+fraction, "0") == ""
}

func addGrouped(
	comma []byte,
	buffer *bytes.Buffer,
	formatString string,
	primaryGroupSize, secondaryGroupSize int,
) {
	if len(formatString) <= primaryGroupSize {
		buffer.WriteString(formatString)
		return
	}
	first := len(formatString) - primaryGroupSize
	firstGroupSize := first % secondaryGroupSize
	if firstGroupSize == 0 {
		firstGroupSize = secondaryGroupSize
	}
	buffer.WriteString(formatString[:firstGroupSize])
	for pos := firstGroupSize; pos < first; pos += secondaryGroupSize {
		buffer.Write(comma)
		buffer.WriteString(formatString[pos : pos+secondaryGroupSize])
	}
	buffer.Write(comma)
	buffer.WriteString(formatString[first:])
}
