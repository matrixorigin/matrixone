package date_format

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFormatIntByWidth(t *testing.T) {
	cases := []struct {
		name  string
		num   int
		width int
		want  string
	}{
		{"Test01", 0, 0, "0"},
		{"Test02", 1, 0, "1"},
		{"Test03", 1, 1, "1"},
		{"Test04", 1, 2, "01"},
		{"Test05", 1, 3, "001"},
		{"Test06", 10, 2, "10"},
		{"Test07", 99, 4, "0099"},
		{"Test08", 100, 3, "100"},
		{"Test09", 888, 3, "888"},
		{"Test10", 428, 4, "0428"},
		{"Test11", 100000, 5, "100000"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := FormatIntByWidth(c.num, c.width)
			require.Equal(t, c.want, res)
		})
	}

}
