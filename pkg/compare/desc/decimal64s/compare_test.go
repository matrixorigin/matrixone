package decimal64s

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompare_Compare(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T_decimal64, Scale: 5})
	c.xs[0] = make([]types.Decimal64, 2)
	decimal64Value0, _ := types.ParseStringToDecimal64("12345.6789", 18, 5)
	c.xs[0][0] = decimal64Value0
	c.vs[1] = vector.New(types.Type{Oid: types.T_decimal64, Scale: 5})
	c.xs[1] = make([]types.Decimal64, 2)
	decimal64Value1, _ := types.ParseStringToDecimal64("54321.54321", 18, 5)
	c.xs[1][0] = decimal64Value1

	result := c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)

	decimal64Value0, _ = types.ParseStringToDecimal64("123.4", 18, 5)
	c.xs[0][0] = decimal64Value0
	decimal64Value1, _ = types.ParseStringToDecimal64("54.21", 18, 5)
	c.xs[1][0] = decimal64Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)

	decimal64Value0, _ = types.ParseStringToDecimal64("123.4", 18, 5)
	c.xs[0][0] = decimal64Value0
	decimal64Value1, _ = types.ParseStringToDecimal64("123.4", 18, 5)
	c.xs[1][0] = decimal64Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)

	c.vs[0] = vector.New(types.Type{Oid: types.T_decimal64, Scale: 8})
	c.xs[0] = make([]types.Decimal64, 2)
	decimal64Value0, _ = types.ParseStringToDecimal64("12345.6789", 18, 8)
	c.xs[0][0] = decimal64Value0
	c.vs[1] = vector.New(types.Type{Oid: types.T_decimal64, Scale: 5})
	c.xs[1] = make([]types.Decimal64, 2)
	decimal64Value1, _ = types.ParseStringToDecimal64("54321.54321", 18, 5)
	c.xs[1][0] = decimal64Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)

	decimal64Value1, _ = types.ParseStringToDecimal64("5432.54", 18, 5)
	c.xs[1][0] = decimal64Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)
	decimal64Value1, _ = types.ParseStringToDecimal64("12345.67890", 18, 5)
	c.xs[1][0] = decimal64Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)
}
