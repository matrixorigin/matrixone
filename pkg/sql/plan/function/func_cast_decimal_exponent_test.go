// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// The LOAD attributed-DECIMAL staging renderer emits the exact source value
// coeff x 10^-scale as text, and THIS cast quantizer is the downstream
// consumer. With an int32-negation bug a source scale of math.MinInt32
// rendered coeff=1 as "1e-2147483648" — pinned below as quantizing to an
// ACCEPTED zero, silently corrupting an overflowing positive input into
// stored zero. The widened -int64(scale) rendering produces "1e2147483648",
// and every conversion mode must reject it loudly: the strict cast family
// errors, the explicit-cast and MySQL assignment prefix modes clamp to the
// range maximum, never a silent zero.
func TestCastDecimalExtremeExponentRendering(t *testing.T) {
	overflowing := "1e2147483648" // coeff x 10^-scale for scale == math.MinInt32

	// Strict cast family: out of range is an error at every width.
	_, err := parseDecimal64CastString(overflowing, 18, 10)
	require.Error(t, err, "1e2147483648 must not be stored into DECIMAL(18,10)")
	_, err = parseDecimal128CastString(overflowing, 38, 10)
	require.Error(t, err, "1e2147483648 must not be stored into DECIMAL(38,10)")
	_, err = parseDecimal256CastString(overflowing, 76, 10)
	require.Error(t, err, "1e2147483648 must not be stored into DECIMAL(76,10)")

	// Explicit-cast clamp mode: clamps to the range maximum, never zero.
	d128, err := parseDecimal128CastStringMode(overflowing, 38, 10, true)
	require.NoError(t, err)
	require.NotEqual(t, types.Decimal128{}, d128,
		"explicit-cast overflow clamps to the audible maximum, never silent zero")

	// MySQL assignment prefix mode: same loud clamp, never zero storage.
	m64, err := parseMySQLDecimal64Prefix(overflowing, 18, 10)
	require.NoError(t, err)
	require.NotEqual(t, types.Decimal64(0), m64)
	m128, err := parseMySQLDecimal128Prefix(overflowing, 38, 10)
	require.NoError(t, err)
	require.NotEqual(t, types.Decimal128{}, m128)
	m256, err := parseMySQLDecimal256Prefix(overflowing, 76, 10)
	require.NoError(t, err)
	require.NotEqual(t, types.Decimal256{}, m256)

	// Counterexample pinned: the sign-wrapped text the int32 negation
	// produced was accepted by the quantizer as ZERO at every width — the
	// silent corruption the widened rendering prevents.
	z64, err := parseDecimal64CastString("1e-2147483648", 18, 10)
	require.NoError(t, err)
	require.Equal(t, types.Decimal64(0), z64)
	z128, err := parseDecimal128CastString("1e-2147483648", 38, 10)
	require.NoError(t, err)
	require.Equal(t, types.Decimal128{}, z128)
	z256, err := parseDecimal256CastString("1e-2147483648", 76, 10)
	require.NoError(t, err)
	require.Equal(t, types.Decimal256{}, z256)
}
