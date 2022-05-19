// Copyright 2021 Matrix Origin
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

package decimal128s

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompare_Compare(t *testing.T) {
	c := New()
	c.vs[0] = vector.New(types.Type{Oid: types.T_decimal128, Scale: 5})
	c.xs[0] = make([]types.Decimal128, 2)
	decimal128Value0, _ := types.ParseStringToDecimal128("12345.6789", 38, 5)
	c.xs[0][0] = decimal128Value0
	c.vs[1] = vector.New(types.Type{Oid: types.T_decimal128, Scale: 5})
	c.xs[1] = make([]types.Decimal128, 2)
	decimal128Value1, _ := types.ParseStringToDecimal128("54321.54321", 38, 5)
	c.xs[1][0] = decimal128Value1

	result := c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)

	decimal128Value0, _ = types.ParseStringToDecimal128("123.4", 38, 5)
	c.xs[0][0] = decimal128Value0
	decimal128Value1, _ = types.ParseStringToDecimal128("54.21", 38, 5)
	c.xs[1][0] = decimal128Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)

	decimal128Value0, _ = types.ParseStringToDecimal128("123.4", 38, 5)
	c.xs[0][0] = decimal128Value0
	decimal128Value1, _ = types.ParseStringToDecimal128("123.4", 38, 5)
	c.xs[1][0] = decimal128Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)

	c.vs[0] = vector.New(types.Type{Oid: types.T_decimal128, Scale: 8})
	c.xs[0] = make([]types.Decimal128, 2)
	decimal128Value0, _ = types.ParseStringToDecimal128("12345.6789", 38, 8)
	c.xs[0][0] = decimal128Value0
	c.vs[1] = vector.New(types.Type{Oid: types.T_decimal128, Scale: 5})
	c.xs[1] = make([]types.Decimal128, 2)
	decimal128Value1, _ = types.ParseStringToDecimal128("54321.54321", 38, 5)
	c.xs[1][0] = decimal128Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 1, result)

	decimal128Value1, _ = types.ParseStringToDecimal128("5432.54", 18, 5)
	c.xs[1][0] = decimal128Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, -1, result)
	decimal128Value1, _ = types.ParseStringToDecimal128("12345.67890", 18, 5)
	c.xs[1][0] = decimal128Value1
	result = c.Compare(0, 1, 0, 0)
	require.Equal(t, 0, result)
}
