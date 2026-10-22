// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExactNumberBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text string
		tp   TpCode
	}{
		{"9007199254740993", TpCodeInt64},
		{"-9223372036854775808", TpCodeInt64},
		{"18446744073709551615", TpCodeUint64},
		{"18446744073709551616", TpCodeDecimal},
		{"-9223372036854775809", TpCodeDecimal},
		{"9007199254740993.000000000000000001", TpCodeDecimal},
		{"-0.00", TpCodeDecimal},
		{"1E+999999", TpCodeDecimal},
		{"1e-999999", TpCodeDecimal},
	} {
		t.Run(tc.text, func(t *testing.T) {
			bj, err := ParseFromStringExact(tc.text)
			require.NoError(t, err)
			require.Equal(t, tc.tp, bj.Type)
			require.Equal(t, tc.text, bj.String())
			raw, err := bj.Marshal()
			require.NoError(t, err)
			var stored ByteJson
			require.NoError(t, stored.Unmarshal(raw))
			require.Equal(t, bj, stored)
		})
	}
	ordinary, err := ParseFromString("0.1")
	require.NoError(t, err)
	require.Equal(t, TpCodeFloat64, ordinary.Type)
	for _, s := range []string{"", "01", "1.", "NaN", "[1,]", "1 2"} {
		_, err = ParseFromStringExact(s)
		require.Error(t, err, s)
	}
	_, err = ParseFromStringExact(strings.Repeat("[", 101) + "0" + strings.Repeat("]", 101))
	require.Error(t, err)
	_, err = ParseFromStringExact(strings.Repeat("[", 100) + "0" + strings.Repeat("]", 100))
	require.NoError(t, err)
}

func TestParseExactTenThousandDocuments(t *testing.T) {
	rng := rand.New(rand.NewSource(20261012))
	path, err := ParseJsonPath("$.agent.cost")
	require.NoError(t, err)
	for i := 0; i < 10000; i++ {
		// Integer parts above 2^53, very small fractions, signed and exponent
		// forms, inside the nested agent payload where precision matters.
		number := fmt.Sprintf("%d.%018de%+d", rng.Uint64(), rng.Int63n(1e18), rng.Intn(601)-300)
		if i%2 == 0 {
			number = "-" + number
		}
		text := `{"agent":{"cost":` + number + `,"null":null},"events":[true,"工具",18446744073709551615]}`
		bj, err := ParseFromStringExact(text)
		require.NoError(t, err, i)
		var paths = []*Path{&path}
		cost := bj.Query(paths)
		require.Equal(t, TpCodeDecimal, cost.Type, i)
		require.Equal(t, number, cost.String(), i)
		visible, err := bj.MarshalJSON()
		require.NoError(t, err, i)
		require.True(t, json.Valid(visible), i)
		reparsed, err := ParseFromByteSliceExact(visible)
		require.NoError(t, err, i)
		require.Equal(t, bj, reparsed, i)
	}
}

func TestProgrammaticJSONRejectsNonFiniteNumbers(t *testing.T) {
	for _, text := range []string{"NaN", "Inf", "-Inf", "1e9999"} {
		_, err := CreateByteJSON(json.Number(text))
		require.Error(t, err, text)
	}
}
