package date_format

import "strconv"

//  FormatIntWidthN
//  @Description: FormatIntWidthN uses to format int with width. Insufficient digits are filled by 0.
func FormatIntWidthN(num, n int) string {
	numString := strconv.FormatInt(int64(num), 10)
	if len(numString) >= n {
		return numString
	}
	padBytes := make([]byte, n-len(numString))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numString
}

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
