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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	require.True(t, Decimal64_Zero.Lt(Decimal64_One))
	require.True(t, Decimal64_Zero.Lt(Decimal64_Ten))
	require.True(t, Decimal64_Zero.Lt(Decimal64Max))
	require.True(t, Decimal64_Zero.Gt(Decimal64Min))
	require.True(t, Decimal128_Zero.Lt(Decimal128_One))
	require.True(t, Decimal128_Zero.Lt(Decimal128_Ten))
	require.True(t, Decimal128_Zero.Lt(Decimal128Max))
	require.True(t, Decimal128_Zero.Gt(Decimal128Min))

	require.True(t, Decimal64_Ten.Eq(Decimal64FromInt32(10)))
	require.True(t, Decimal64_Ten.Eq(Decimal64FromString("10")))
	require.True(t, Decimal64_Ten.Eq(Decimal64FromString("10.000")))
	require.True(t, Decimal128_Ten.Eq(Decimal128FromInt32(10)))
	require.True(t, Decimal128_Ten.Eq(Decimal128FromString("10")))
	require.True(t, Decimal128_Ten.Eq(Decimal128FromString("10.000")))
}

func TestAdd(t *testing.T) {
	require.True(t, Decimal64_One.Eq(Decimal64_Zero.Add(Decimal64_One)))
	require.True(t, Decimal64_Ten.Eq(Decimal64_Zero.Add(Decimal64_Ten)))
	require.True(t, Decimal64Max.Eq(Decimal64_Zero.Add(Decimal64Max)))
	require.True(t, Decimal128_One.Eq(Decimal128_Zero.Add(Decimal128_One)))
	require.True(t, Decimal128_Ten.Eq(Decimal128_Zero.Add(Decimal128_Ten)))
	require.True(t, Decimal128Max.Eq(Decimal128_Zero.Add(Decimal128Max)))
}
