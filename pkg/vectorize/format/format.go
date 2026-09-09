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

type formatRoundingMode uint8

const (
	// formatRoundHalfEven is the mode used by MySQL's approximate/string
	// FORMAT path. It is ties-to-even, matching the floating-point contract.
	formatRoundHalfEven formatRoundingMode = iota
	// formatRoundHalfUp is the mode used by MySQL's exact DECIMAL/INTEGER path.
	formatRoundHalfUp
)

// GetNumberFormat formats a string-domain value. The caller may pass a numeric
// prefix, but the value has already crossed the SQL string boundary and must
// therefore use the approximate (ties-to-even) rounding contract.
func GetNumberFormat(number, scale, locale string) (string, error) {
	return getFormatFunctionWithLocaleMode(locale)(number, scale, formatRoundHalfEven)
}

// GetNumberFormatExact formats an exact DECIMAL/INTEGER value represented as
// decimal text. It intentionally shares parsing and locale grouping with
// GetNumberFormat while retaining exact half-up rounding.
func GetNumberFormatExact(number, scale, locale string) (string, error) {
	return getFormatFunctionWithLocaleMode(locale)(number, scale, formatRoundHalfUp)
}

type formatModeFunc func(string, string, formatRoundingMode) (string, error)

// GetFormatFunctionWithLocate get the format function for sepcific locale.
func getFormatFunctionWithLocale(locale string) FormatFunc {
	formatFunc := getFormatFunctionWithLocaleMode(locale)
	return func(number, scale string) (string, error) {
		return formatFunc(number, scale, formatRoundHalfEven)
	}
}

func getFormatFunctionWithLocaleMode(locale string) formatModeFunc {
	formatFunc, exist := localeToFormatFunction[locale]
	if exist {
		return formatFunc
	}
	if formatFunc, exist = localeToFormatFunctionInsensitive[strings.ToLower(locale)]; exist {
		return formatFunc
	}
	return formatENUSWithMode
}

// localeToFormatFunction is the string represent of locale format function.
var localeToFormatFunction = map[string]formatModeFunc{
	//formatENUSWithMode
	"ar_AE": formatENUSWithMode,
	"ar_BH": formatENUSWithMode,
	"ar_DZ": formatENUSWithMode,
	"ar_EG": formatENUSWithMode,
	"ar_IN": formatENUSWithMode,
	"ar_IQ": formatENUSWithMode,
	"ar_JO": formatENUSWithMode,
	"ar_KW": formatENUSWithMode,
	"ar_LB": formatENUSWithMode,
	"ar_LY": formatENUSWithMode,
	"ar_MA": formatENUSWithMode,
	"ar_OM": formatENUSWithMode,
	"ar_QA": formatENUSWithMode,
	"ar_SD": formatENUSWithMode,
	"ar_SY": formatENUSWithMode,
	"ar_TN": formatENUSWithMode,
	"ar_YE": formatENUSWithMode,
	"en_AU": formatENUSWithMode,
	"en_CA": formatENUSWithMode,
	"en_GB": formatENUSWithMode,
	"en_IN": formatIndianWithMode,
	"en_NZ": formatENUSWithMode,
	"en_PH": formatENUSWithMode,
	"en_US": formatENUSWithMode,
	"en_ZA": formatENUSWithMode,
	"en_ZW": formatENUSWithMode,
	"es_DO": formatENUSWithMode,
	"es_GT": formatENUSWithMode,
	"es_HN": formatENUSWithMode,
	"en_MX": formatENUSWithMode,
	"es_NI": formatENUSWithMode,
	"es_PA": formatENUSWithMode,
	"es_PR": formatENUSWithMode,
	"es_SV": formatENUSWithMode,
	"es_US": formatENUSWithMode,
	"gu_IN": formatENUSWithMode,
	"he_IL": formatENUSWithMode,
	"hi_IN": formatENUSWithMode,
	"ja_JP": formatENUSWithMode,
	"ko_KR": formatENUSWithMode,
	"ms_MY": formatENUSWithMode,
	"ta_IN": formatIndianWithMode,
	"te_IN": formatIndianWithMode,
	"th_TH": formatENUSWithMode,
	"ur_PK": formatENUSWithMode,
	"zh_CN": formatENUSWithMode,
	"zh_HK": formatENUSWithMode,
	"zh_TW": formatENUSWithMode,

	//formatARSAWithMode
	"ar_SA": formatARSAWithMode,
	"sr_RS": formatARSAWithMode,

	//formatBEBYWithMode
	"be_BY": formatBEBYWithMode,
	"da_DK": formatBEBYWithMode,
	"de_BE": formatBEBYWithMode,
	"de_DE": formatBEBYWithMode,
	"de_LU": formatBEBYWithMode,
	"es_AR": formatBEBYWithMode,
	"es_BO": formatBEBYWithMode,
	"es_CL": formatBEBYWithMode,
	"es_CO": formatBEBYWithMode,
	"es_EC": formatBEBYWithMode,
	"es_ES": formatBEBYWithMode,
	"es_PY": formatBEBYWithMode,
	"es_UY": formatBEBYWithMode,
	"es_VE": formatBEBYWithMode,
	"fo_FO": formatBEBYWithMode,
	"hu_HU": formatBEBYWithMode,
	"id_ID": formatBEBYWithMode,
	"is_IS": formatBEBYWithMode,
	"lt_LT": formatBEBYWithMode,
	"mn_MN": formatBEBYWithMode,
	"nb_NO": formatBEBYWithMode,
	"no_NO": formatBEBYWithMode,
	"ru_UA": formatBEBYWithMode,
	"sq_AL": formatBEBYWithMode,
	"tr_TR": formatBEBYWithMode,
	"uk_UA": formatBEBYWithMode,
	"vi_VN": formatBEBYWithMode,
	"ro_RO": formatBEBYWithMode,

	//formatBGBGWithMode
	"bg_BG": formatBGBGWithMode,
	"cs_CZ": formatBGBGWithMode,
	"es_CR": formatBGBGWithMode,
	"et_EE": formatBGBGWithMode,
	"fi_FI": formatBGBGWithMode,
	"lv_LV": formatBGBGWithMode,
	"mk_MK": formatBGBGWithMode,
	"ru_RU": formatBGBGWithMode,
	"sk_SK": formatBGBGWithMode,
	"sv_FI": formatBGBGWithMode,
	"sv_SE": formatBGBGWithMode,

	//formatDECHWithMode
	"de_CH": formatDECHWithMode,

	//formatCAESWithMode
	"ca_ES": formatCAESWithMode,
	"de_AT": formatCAESWithMode,
	"el_GR": formatCAESWithMode,
	"eu_ES": formatCAESWithMode,
	"fr_BE": formatCAESWithMode,
	"fr_CA": formatCAESWithMode,
	"fr_CH": formatCAESWithMode,
	"fr_FR": formatCAESWithMode,
	"fr_LU": formatCAESWithMode,
	"gl_ES": formatCAESWithMode,
	"hr_HR": formatCAESWithMode,
	"it_IT": formatCAESWithMode,
	"nl_BE": formatCAESWithMode,
	"nl_NL": formatCAESWithMode,
	"pl_PL": formatCAESWithMode,
	"pt_BR": formatCAESWithMode,
	"pt_PT": formatCAESWithMode,
	"sl_SI": formatCAESWithMode,

	//formatITCHWithMode
	"it_CH": formatITCHWithMode,
	"rm_CH": formatITCHWithMode,
}

// Keep the canonical map readable while making locale lookup case-insensitive.
// The second map is built once, so a row-wise FORMAT(..., locale) call does not
// scan all locale names or mutate shared state.
var localeToFormatFunctionInsensitive = func() map[string]formatModeFunc {
	result := make(map[string]formatModeFunc, len(localeToFormatFunction))
	for locale, formatFunc := range localeToFormatFunction {
		result[strings.ToLower(locale)] = formatFunc
	}
	return result
}()

// format number like 20,000,000.0000
func formatENUS(number string, scale string) (string, error) {
	return formatENUSWithMode(number, scale, formatRoundHalfEven)
}

func formatENUSWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{','}, []byte{'.'}, mode)
}

// format number like 20000000.0000
func formatARSA(number string, scale string) (string, error) {
	return formatARSAWithMode(number, scale, formatRoundHalfEven)
}

func formatARSAWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{}, []byte{'.'}, mode)
}

// format number like 20.000.000,0000
func formatBEBY(number string, scale string) (string, error) {
	return formatBEBYWithMode(number, scale, formatRoundHalfEven)
}

func formatBEBYWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{'.'}, []byte{','}, mode)
}

// format number like 20 000 000,0000
func formatBGBG(number string, scale string) (string, error) {
	return formatBGBGWithMode(number, scale, formatRoundHalfEven)
}

func formatBGBGWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{' '}, []byte{','}, mode)
}

// format number like 20'000'000.0000
func formatDECH(number string, scale string) (string, error) {
	return formatDECHWithMode(number, scale, formatRoundHalfEven)
}

func formatDECHWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{'\''}, []byte{'.'}, mode)
}

// format number like 20000000,0000
func formatCAES(number string, scale string) (string, error) {
	return formatCAESWithMode(number, scale, formatRoundHalfEven)
}

func formatCAESWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{}, []byte{','}, mode)
}

// format number like 20'000'000.0000
func formatITCH(number string, scale string) (string, error) {
	return formatITCHWithMode(number, scale, formatRoundHalfEven)
}

func formatITCHWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return format(number, scale, []byte{'\''}, []byte{','}, mode)
}

func formatIndian(number string, scale string) (string, error) {
	return formatIndianWithMode(number, scale, formatRoundHalfEven)
}

func formatIndianWithMode(number, scale string, mode formatRoundingMode) (string, error) {
	return formatWithGrouping(number, scale, []byte{','}, []byte{'.'}, 3, 2, mode)
}

const maxFormatDecimals = 30

func format(number, scale string, comma, decimalPoint []byte, mode formatRoundingMode) (string, error) {
	return formatWithGrouping(number, scale, comma, decimalPoint, 3, 3, mode)
}

func formatWithGrouping(
	number, scale string,
	comma, decimalPoint []byte,
	primaryGroupSize, secondaryGroupSize int,
	mode formatRoundingMode,
) (string, error) {
	if len(number) == 0 {
		return "", nil
	}
	if mode == formatRoundHalfEven {
		return formatApproximateWithGrouping(number, scale, comma, decimalPoint, primaryGroupSize, secondaryGroupSize)
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

	roundUp := shouldRoundUp(integer, fraction, decimals, mode)
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

	return renderFormattedNumber(integer, fraction, negative, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
}

func formatApproximateWithGrouping(
	number, scale string,
	comma, decimalPoint []byte,
	primaryGroupSize, secondaryGroupSize int,
) (string, error) {
	decimals := parseFormatScale(scale)
	_, _, _, _, token, ok := parseFormatNumber(number)
	if !ok {
		return renderFormattedNumber("0", "", false, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
	}

	value, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsInf(value, 0) {
		if math.IsInf(value, 1) {
			integer, fraction, negative := maxFloatComponents(false)
			return renderFormattedNumber(integer, fraction, negative, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
		}
		if math.IsInf(value, -1) {
			integer, fraction, negative := maxFloatComponents(true)
			return renderFormattedNumber(integer, fraction, negative, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
		}
		// ParseFloat may return a finite zero with ErrRange for an underflow.
		value = 0
	}

	rounded, ok := roundApproximateValue(value, decimals)
	if !ok {
		rounded = value
	}
	// MySQL first rounds the scaled double, divides by the same factor, and
	// then formats that rounded double with a fixed number of decimals. Keep
	// the shortest fixed representation here so values such as 1.5 at scale 30
	// retain the post-division double (1.5000000000000002) without exposing all
	// of its binary expansion.
	fixed := strconv.FormatFloat(rounded, 'f', -1, 64)
	integer, fraction, parsedNegative, _, _, ok := parseFormatNumber(fixed)
	if !ok {
		return renderFormattedNumber("0", "", false, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
	}
	return renderFormattedNumber(integer, fraction, parsedNegative, decimals, comma, decimalPoint, primaryGroupSize, secondaryGroupSize), nil
}

func roundApproximateValue(value float64, decimals int) (float64, bool) {
	factor := math.Pow10(decimals)
	if math.IsInf(factor, 0) || math.IsInf(value*factor, 0) {
		return 0, false
	}
	return math.RoundToEven(value*factor) / factor, true
}

func renderFormattedNumber(
	integer, fraction string,
	negative bool,
	decimals int,
	comma, decimalPoint []byte,
	primaryGroupSize, secondaryGroupSize int,
) string {
	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}
	if len(fraction) > decimals {
		fraction = fraction[:decimals]
	}
	if len(fraction) < decimals {
		fraction += strings.Repeat("0", decimals-len(fraction))
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
	return buffer.String()
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

func shouldRoundUp(integer, fraction string, decimals int, mode formatRoundingMode) bool {
	if decimals < 0 || decimals >= len(fraction) {
		return false
	}
	first := fraction[decimals]
	if first > '5' {
		return true
	}
	if first < '5' {
		return false
	}
	if mode == formatRoundHalfUp {
		return true
	}
	for _, digit := range fraction[decimals+1:] {
		if digit != '0' {
			return true
		}
	}
	// A true tie is rounded to the nearest even retained digit. The sign is
	// deliberately ignored: MySQL applies the same magnitude rule to negative
	// approximate values and restores the sign afterwards.
	var retained byte
	if decimals == 0 {
		retained = integer[len(integer)-1]
	} else {
		retained = fraction[decimals-1]
	}
	return (retained-'0')%2 == 1
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
