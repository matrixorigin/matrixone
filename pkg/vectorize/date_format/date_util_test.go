package date_format

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFormatIntWidthN(t *testing.T) {
	cases := []struct {
		num    int
		width  int
		result string
	}{
		{0, 0, "0"},
		{1, 0, "1"},
		{1, 1, "1"},
		{1, 2, "01"},
		{10, 2, "10"},
		{99, 3, "099"},
		{100, 3, "100"},
		{999, 3, "999"},
		{1000, 3, "1000"},
	}
	for _, ca := range cases {
		re := FormatIntWidthN(ca.num, ca.width)
		require.Equal(t, ca.result, re)
	}
}
