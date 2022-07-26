package date_format

import "strconv"

// FormatIntByWidth: Formatintwidthn is used to format ints with width parameter n. Insufficient numbers are filled with 0.
func FormatIntByWidth(num, n int) string {
	numStr := strconv.FormatInt(int64(num), 10)
	if len(numStr) >= n {
		return numStr
	}
	padBytes := make([]byte, n-len(numStr))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numStr
}

// AbbrDayOfMonth: Get the abbreviation of month of day
func AbbrDayOfMonth(day int) string {
	var str string
	switch day {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}
	return str
}
