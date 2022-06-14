package extract

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExtractFromDate(t *testing.T) {
	a0, _ := types.ParseDate("2020-01-02")
	a1, _ := types.ParseDate("2021-03-04")
	inputs := make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results := make([]uint32, 2)
	output, err := ExtractFromDate("year", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{2020, 2021}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{01, 03}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("year_month", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{202001, 202103}, output)

	a0, _ = types.ParseDate("2020-01-02")
	a1, _ = types.ParseDate("2021-03-04")
	inputs = make([]types.Date, 2)
	inputs[0] = a0
	inputs[1] = a1
	results = make([]uint32, 2)
	output, err = ExtractFromDate("quarter", inputs, results)
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 1}, output)
}
