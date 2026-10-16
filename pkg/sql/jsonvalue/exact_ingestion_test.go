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

package jsonvalue

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestExactIngestionTypedProjection(t *testing.T) {
	target := types.T_decimal128.ToType()
	target.Width, target.Scale = 38, 3
	path, err := bytejson.ParseJsonPath("$.agent.cost")
	require.NoError(t, err)
	for _, number := range []string{"9007199254740993.001", "18446744073709551615", "1e-3"} {
		doc, err := bytejson.ParseFromStringExact(`{"agent":{"cost":` + number + `}}`)
		require.NoError(t, err)
		cost := doc.Query([]*bytejson.Path{&path})
		converted := ConvertScalar(cost, target)
		require.Equal(t, StatusSuccess, converted.Status)
		require.NoError(t, converted.Err)
		want := map[string]string{
			"9007199254740993.001": "9007199254740993.001",
			"18446744073709551615": "18446744073709551615.000",
			"1e-3":                 "0.001",
		}[number]
		require.Equal(t, want, converted.Value.(types.Decimal128).Format(3))
	}
}
