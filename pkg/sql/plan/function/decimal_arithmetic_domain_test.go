// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestArithmeticDecimalCastsRetainIntegerDomain(t *testing.T) {
	for _, integer := range []types.Type{types.T_int32.ToType(), types.T_int64.ToType(), types.T_uint64.ToType(), types.New(types.T_bit, 64, 0)} {
		for _, temporal := range []types.T{types.T_time, types.T_datetime, types.T_timestamp} {
			for _, scale := range []int32{0, 6} {
				for _, op := range []string{"+", "-", "*", "%", "/", "div"} {
					for _, reverse := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/%s/%s/scale%d/reverse%v", integer.Oid, temporal, op, scale, reverse), func(t *testing.T) {
							inputs := []types.Type{integer, types.New(temporal, 0, scale)}
							idx := 0
							if reverse {
								inputs[0], inputs[1] = inputs[1], inputs[0]
								idx = 1
							}
							resolved, err := GetFunctionByName(t.Context(), op, inputs)
							if integer.Oid == types.T_bit && (op == "/" || op == "div") {
								// These pairs were not supported before the cast fix.
								// Retain that boundary rather than enabling a feature.
								require.Error(t, err)
								return
							}
							require.NoError(t, err)
							targets, cast := resolved.ShouldDoImplicitTypeCast()
							if cast && targets[idx].Oid.IsDecimal() && targets[idx].Width > 0 {
								need := integerIntegralWidth(decimalComparisonSource(integer).Oid)
								require.GreaterOrEqual(t, targets[idx].Width-targets[idx].Scale, need)
							}
							if op == "+" || op == "-" || op == "*" || op == "%" {
								_, left, right := fixedTypeCastRule1(inputs[0], inputs[1])
								if cast && targets[0].Oid.IsDecimal() && targets[1].Oid.IsDecimal() {
									require.Equal(t, left.Scale, targets[0].Scale, "widening must not change operand scales")
									require.Equal(t, right.Scale, targets[1].Scale)
								}
							}
						})
					}
				}
			}
		}
	}
}

func TestIntegerDivDomainWideningPreservesLegacyOriginSignature(t *testing.T) {
	inputs := []types.Type{types.T_time.ToTypeWithScale(6), types.T_int64.ToType()}
	current, err := GetFunctionByName(t.Context(), "div", inputs)
	require.NoError(t, err)
	currentTargets, cast := current.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, types.New(types.T_decimal128, 19, 0), currentTargets[1])

	legacy, err := GetFunctionByName(WithLegacyDecimalIntegerDiv(t.Context()), "div", inputs)
	require.NoError(t, err)
	legacyTargets, cast := legacy.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, types.T_decimal64.ToType(), legacyTargets[1])
	require.Equal(t, current.GetReturnType(), legacy.GetReturnType())
}
