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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecimalCastParseWrappers(t *testing.T) {
	d64, err := ParseDecimal64CastString("0b10", 10, 2)
	require.NoError(t, err)
	require.Equal(t, "2.00", d64.Format(2))
	d128, err := ParseDecimal128CastString("1.25", 30, 2)
	require.NoError(t, err)
	require.Equal(t, "1.25", d128.Format(2))
	d256, err := ParseDecimal256CastString("1.25", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "1.25", d256.Format(2))

	d64, err = ParseExplicitDecimal64CastString("9999999999999999999", 10, 2)
	require.NoError(t, err)
	require.Equal(t, "99999999.99", d64.Format(2))
	d128, err = ParseExplicitDecimal128CastString("999999999999999999999999999999999999999", 30, 2)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("9", 28)+".99", d128.Format(2))
	d256, err = ParseExplicitDecimal256CastString("999999999999999999999999999999999999999999999", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "99999999999999999999999999999999999999.99", d256.Format(2))

	_, err = ParseExplicitDecimal64CastString("not-a-decimal", 10, 2)
	require.Error(t, err)
	_, err = ParseExplicitDecimal128CastString("not-a-decimal", 30, 2)
	require.Error(t, err)
	_, err = ParseExplicitDecimal256CastString("not-a-decimal", 40, 2)
	require.Error(t, err)
}
