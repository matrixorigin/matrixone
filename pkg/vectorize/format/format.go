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
	"strconv"
	"strings"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// FormatFunc is the locale format function signature.
type FormatFunc func(string, string) (string, error)

func Format(numbers, precisions, locales []string, rowCount int, constVectors []bool, results []string) error {
	var err error
	if constVectors[0] {
		number := numbers[0]
		if constVectors[1] {
			precision := precisions[0]
			if constVectors[2] {
				//scalar - scalar - scalar
				results[0], err = getNumberFormat(number, precision, locales[0])
				if err != nil {
					return err
				}
			} else {
				//scalar - scalar - vector
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(number, precision, locales[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if constVectors[2] {
				locale := locales[0]
				//scalar - vector - scalar
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(number, precisions[i], locale)
					if err != nil {
						return err
					}
				}
			} else {
				//scalar - vector - vector
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(number, precisions[i], locales[i])
					if err != nil {
						return err
					}
				}
			}
		}
	} else {
		if constVectors[1] {
			precision := precisions[0]
			if constVectors[2] {
				locale := locales[0]
				//vector - scalar - scalar
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(numbers[i], precision, locale)
					if err != nil {
						return err
					}
				}
			} else {
				//vaetor - scalar - vector
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(numbers[i], precision, locales[i])
					if err != nil {
						return err
					}
				}
			}
		} else {
			if constVectors[2] {
				locale := locales[0]
				//vector - vector - scalar
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(numbers[i], precisions[i], locale)
					if err != nil {
						return err
					}
				}
			} else {
				//vector - vector - vector
				for i := 0; i < rowCount; i++ {
					results[i], err = getNumberFormat(numbers[i], precisions[i], locales[i])
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func getNumberFormat(number, precision, locale string) (string, error) {
	return getFormatFunctionWithLocale(locale)(number, precision)
}

// GetFormatFunctionWithLocate get the format function for sepcific locale.
func getFormatFunctionWithLocale(locale string) FormatFunc {
	formatFunc, exist := localeToFormatFunction[locale]
	if !exist {
		return formatENUS
	}
	return formatFunc
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
	"en_IN": formatENUS,
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
	"ta_IN": formatENUS,
	"te_IN": formatENUS,
	"th_TH": formatENUS,
	"ur_PK": formatENUS,
	"zh_CN": formatENUS,
	"zh_HK": formatENUS,
	"zh_TW": formatENUS,

	//formatARSA
	"ar_SA": formatARSA,
	"ca_ES": formatARSA,
	"de_AT": formatARSA,
	"el_GR": formatARSA,
	"eu_ES": formatARSA,
	"fr_FR": formatARSA,
	"fr_LU": formatARSA,
	"gl_ES": formatARSA,
	"hr_HR": formatARSA,
	"it_IT": formatARSA,
	"nl_BE": formatARSA,
	"nl_NL": formatARSA,
	"pl_PL": formatARSA,
	"pt_BR": formatARSA,
	"pt_PT": formatARSA,
	"sl_SI": formatARSA,
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
	"it_CH": formatDECH,
	"rm_CH": formatDECH,
}

// format number like 20,000,000.0000
func formatENUS(number string, precision string) (string, error) {
	return format(number, precision, []byte{','}, []byte{'.'})
}

// format number like 20000000.0000
func formatARSA(number string, precision string) (string, error) {
	return format(number, precision, []byte{}, []byte{'.'})
}

// format number like 20.000.000,0000
func formatBEBY(number string, precision string) (string, error) {
	return format(number, precision, []byte{'.'}, []byte{','})
}

// format number like 20 000 000,0000
func formatBGBG(number string, precision string) (string, error) {
	return format(number, precision, []byte{' '}, []byte{','})
}

// format number like 20'000'000.0000
func formatDECH(number string, precision string) (string, error) {
	return format(number, precision, []byte{'\''}, []byte{'.'})
}

func format(number string, precision string, comma, decimalPoint []byte) (string, error) {
	var buffer bytes.Buffer

	if len(number) == 0 {
		return "", nil
	}
	//handle precision
	if unicode.IsDigit(rune(precision[0])) {
		for i, v := range precision {
			if unicode.IsDigit(v) {
				continue
			}
			precision = precision[:i]
			break
		}
	} else {
		precision = "0"
	}

	//handle number
	if number[0] == '-' && number[1] == '.' {
		number = strings.Replace(number, "-", "-0", 1)
	} else if number[0] == '.' {
		number = strings.Replace(number, ".", "0.", 1)
	}

	if (number[:1] == "-" && !unicode.IsDigit(rune(number[1]))) ||
		(!unicode.IsDigit(rune(number[0])) && number[:1] != "-") {
		buffer.Write([]byte{'0'})
		position, err := strconv.ParseUint(precision, 10, 64)
		if err == nil && position > 0 {
			buffer.Write([]byte{'.'})
			buffer.WriteString(strings.Repeat("0", int(position)))
		}
		return buffer.String(), nil
	} else if number[:1] == "-" {
		buffer.Write([]byte{'-'})
		number = number[1:]
	}

	// Check for scientific notition.
	for _, v := range number {
		if v == 'E' || v == 'e' {
			num, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return "", err
			}
			// Convert to non-scientific notition.
			number = strconv.FormatFloat(num, 'f', -1, 64)
			break
		}
	}

	for i, v := range number {
		if unicode.IsDigit(v) {
			continue
		} else if i == 1 && number[1] == '.' {
			continue
		} else if v == '.' && number[1] != '.' {
			continue
		} else {
			number = number[:i]
			break
		}
	}

	parts := strings.Split(number, ".")

	if len(comma) != 0 {
		addComma(comma, &buffer, parts[0])
	} else {
		buffer.WriteString(parts[0])
	}

	//According to the precision to process the decimal parts
	position, err := strconv.ParseUint(precision, 10, 64)
	if err != nil {
		return "", err
	}

	if position > 0 {
		buffer.Write(decimalPoint)
		if len(parts) == 2 {
			if uint64(len(parts[1])) == position {
				buffer.WriteString(parts[1][:position])
			} else if uint64(len(parts[1])) > position {
				//need to cast decimal's length
				if parts[1][position:position+1] >= "5" {
					buffer.Reset()
					floatVar, err := strconv.ParseFloat(parts[0]+"."+parts[1], 64)
					if err != nil {
						return "", moerr.NewInvalidArgNoCtx("format parser error", parts[0]+"."+parts[1])
					}
					newNumber := strconv.FormatFloat(floatVar, 'f', int(position), 64)
					newParts := strings.Split(newNumber, ".")

					if len(comma) != 0 {
						addComma(comma, &buffer, newParts[0])
					} else {
						buffer.WriteString(newParts[0])
					}

					buffer.Write(decimalPoint)
					buffer.WriteString(newParts[1])
				} else {
					buffer.WriteString(parts[1][:position])
				}
			} else {
				buffer.WriteString(parts[1])
				buffer.WriteString(strings.Repeat("0", int(position)-len(parts[1])))
			}
		} else {
			buffer.WriteString(strings.Repeat("0", int(position)))
		}
	}

	return buffer.String(), nil
}

func addComma(comma []byte, buffer *bytes.Buffer, formatString string) {
	pos := 0

	//Add foramt comma for Integr parts
	//If the integer part's length larger than 3
	if len(formatString)%3 != 0 {
		pos += len(formatString) % 3
		buffer.WriteString(formatString[:pos])
		buffer.Write(comma)
	}
	//Add a format comma every three digits
	for ; pos < len(formatString); pos += 3 {
		buffer.WriteString(formatString[pos : pos+3])
		buffer.Write(comma)
	}

	buffer.Truncate(buffer.Len() - 1)

}
