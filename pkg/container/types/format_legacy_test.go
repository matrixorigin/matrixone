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

package types

// The fmt-based formatters before they wrote digits directly, kept verbatim as
// the oracle for TestFormatMatchesLegacy. Delete this file once the new
// formatters have been the only implementation for a release.

import (
	"fmt"
	"strings"
	"time"
)

func legacyDatetimeString2(dt Datetime, scale int32) string {
	if dt == ZeroDatetime {
		if scale > 0 {
			return "0000-00-00 00:00:00." + strings.Repeat("0", int(scale))
		}
		return "0000-00-00 00:00:00"
	}
	y, m, d, _ := dt.ToDate().Calendar(true)
	hour, minute, sec := dt.Clock()

	if scale > 0 {
		msec := int64(dt) % MicroSecsPerSec
		// Format microseconds as 6 digits (max precision we store)
		msecInstr := fmt.Sprintf("%06d", msec)
		// For scale > 6, pad with zeros to the right (e.g., scale 9: "000001" -> "000001000")
		if scale > 6 {
			// Pad to 9 digits by appending zeros
			for len(msecInstr) < 9 {
				msecInstr = msecInstr + "0"
			}
			// Truncate to requested scale (max 9)
			if scale > 9 {
				scale = 9
			}
			msecInstr = msecInstr[:scale]
		} else {
			// For scale <= 6, truncate from the right
			msecInstr = msecInstr[:scale]
		}

		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

func legacyDateString(d Date) string {
	y, m, day, _ := d.Calendar(true)
	return fmt.Sprintf("%04d-%02d-%02d", y, m, day)
}

func legacyTimeString2(t Time, scale int32) string {
	var symbol string
	h, m, s, ms, isNeg := t.ClockFormat()
	if isNeg {
		symbol = "-"
	}
	if scale > 0 {
		// Format microseconds as 6 digits (max precision we store)
		msecInstr := fmt.Sprintf("%06d", ms)
		// For scale > 6, pad with zeros to the right (e.g., scale 9: "000001" -> "000001000")
		if scale > 6 {
			// Pad to 9 digits by appending zeros
			for len(msecInstr) < 9 {
				msecInstr = msecInstr + "0"
			}
			// Truncate to requested scale (max 9)
			if scale > 9 {
				scale = 9
			}
			msecInstr = msecInstr[:scale]
		} else {
			// For scale <= 6, truncate from the right
			msecInstr = msecInstr[:scale]
		}
		return fmt.Sprintf("%s%02d:%02d:%02d"+"."+msecInstr, symbol, h, m, s)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", symbol, h, m, s)
}

func legacyTimestampString2(ts Timestamp, loc *time.Location, scale int32) string {
	if ts == ZeroTimestamp {
		if scale > 0 {
			return "0000-00-00 00:00:00." + strings.Repeat("0", int(scale))
		}
		return "0000-00-00 00:00:00"
	}
	t := time.UnixMicro(int64(ts) - unixEpochMicroSecs).In(loc)
	y, m, d := t.Date()
	hour, minute, sec := t.Clock()
	if scale > 0 {
		msec := t.Nanosecond() / 1000
		msecInstr := fmt.Sprintf("%06d", msec)
		msecInstr = msecInstr[:scale]

		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d"+"."+msecInstr, y, m, d, hour, minute, sec)
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", y, m, d, hour, minute, sec)
}

func legacyDecimal64Format(x Decimal64, scale int32) string {
	a := ""
	signx := (x >> 63) != 0
	if signx {
		x = x.Minus()
	}
	for x != 0 {
		a = fmt.Sprintf("%c", x%10+'0') + a
		x /= 10
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	for scale > 0 {
		a = "0" + a
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	if scale == 0 {
		a = "0" + a
	}
	if signx {
		a = "-" + a
	}
	return a
}

func legacyDecimal128Format(x Decimal128, scale int32) string {
	a := ""
	signx := x.Sign()
	if signx {
		x = x.Minus()
	}
	for x.B64_127 != 0 || x.B0_63 != 0 {
		y, _ := x.Mod128(Decimal128{Pow10[1], 0})
		a = fmt.Sprintf("%c", y.B0_63+'0') + a
		x, _ = x.Div128(Decimal128{Pow10[1], 0})
		if y.B0_63 >= 5 {
			x, _ = x.Sub128(Decimal128{1, 0})
		}
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	for scale > 0 {
		a = "0" + a
		scale--
		if scale == 0 {
			a = "." + a
		}
	}
	if scale == 0 {
		a = "0" + a
	}
	if signx {
		a = "-" + a
	}
	return a
}
