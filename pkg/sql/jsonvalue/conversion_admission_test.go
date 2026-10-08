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

package jsonvalue

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONTableTimestampRequiresExecutionLocation(t *testing.T) {
	value := parseConversionValue(t, `"2024-02-03 04:05:06"`)
	target := types.T_timestamp.ToType()
	scalar := ConvertScalar(value, target)
	require.Equal(t, StatusStatementError, scalar.Status)
	require.Error(t, scalar.Err)
	path, err := bytejson.ParseJsonPath("$")
	require.NoError(t, err)
	for _, convert := range []func(*bytejson.PathIterator) Result{
		func(it *bytejson.PathIterator) Result { return ConvertPathMatches(it, target) },
		func(it *bytejson.PathIterator) Result {
			return ConvertPathMatchesContext(context.Background(), it, target)
		},
		func(it *bytejson.PathIterator) Result { return ConvertPathMatchesWithLimit(it, target, 1024) },
		func(it *bytejson.PathIterator) Result {
			return ConvertPathMatchesWithLimitContext(context.Background(), it, target, 1024)
		},
	} {
		it := bytejson.NewPathIterator(value, &path)
		result := convert(it)
		it.Close()
		require.Equal(t, StatusStatementError, result.Status)
		require.Error(t, result.Err)
	}
}

func TestJSONTablePathTimestampCarriesExecutionLocation(t *testing.T) {
	target := types.T_timestamp.ToType()
	value := parseConversionValue(t, `"2024-02-03 04:05:06"`)
	path, err := bytejson.ParseJsonPath("$")
	require.NoError(t, err)
	timestamps := make([]types.Timestamp, 0, 2)
	for _, location := range []*time.Location{time.UTC, time.FixedZone("UTC+8", 8*60*60)} {
		it := bytejson.NewPathIterator(value, &path)
		result := ConvertPathMatchesWithOptions(context.Background(), it, target, ConversionOptions{Location: location}, 1024)
		it.Close()
		require.Equal(t, StatusSuccess, result.Status)
		require.NoError(t, result.Err)
		timestamps = append(timestamps, result.Value.(types.Timestamp))
		parsed, err := types.ParseTimestamp(location, "2024-02-03 04:05:06", target.Scale)
		require.NoError(t, err)
		require.Equal(t, parsed, result.Value)
	}
	require.Equal(t, int64(8*60*60*1000000), int64(timestamps[0]-timestamps[1]))
	for _, document := range []string{`null`, `{}`} {
		it := conversionIterator(t, document, "$.missing")
		result := ConvertPathMatchesWithOptions(context.Background(), it, target, ConversionOptions{}, 1024)
		it.Close()
		require.Equal(t, StatusMissing, result.Status)
	}
	it := conversionIterator(t, `null`, "$")
	result := ConvertPathMatchesWithOptions(context.Background(), it, target, ConversionOptions{}, 1024)
	it.Close()
	require.Equal(t, StatusJSONNull, result.Status)
}

func TestJSONTableDecimalAdmissionAcrossRoutes(t *testing.T) {
	for _, text := range []string{"+1", "01", "1e", "1e+", " 1", "1 ", "1e2 3", "1E2", "-0.01", "0", "1e9999999999999999999999999"} {
		t.Run(text, func(t *testing.T) {
			valid := json.Valid([]byte(text)) && text[0] != ' ' && text[len(text)-1] != ' '
			payload := binary.AppendUvarint(nil, uint64(len(text)))
			payload = append(payload, text...)
			value := bytejson.ByteJson{Type: bytejson.TpCodeDecimal, Data: payload}
			arrayData := make([]byte, 13+len(payload))
			binary.LittleEndian.PutUint32(arrayData, 1)
			binary.LittleEndian.PutUint32(arrayData[4:], uint32(len(arrayData)))
			arrayData[8] = byte(bytejson.TpCodeDecimal)
			binary.LittleEndian.PutUint32(arrayData[9:], 13)
			copy(arrayData[13:], payload)
			nested := bytejson.ByteJson{Type: bytejson.TpCodeArray, Data: arrayData}
			for _, input := range []bytejson.ByteJson{value, nested} {
				path, err := bytejson.ParseJsonPath("$")
				require.NoError(t, err)
				it := bytejson.NewPathIterator(input, &path)
				results := []Result{ConvertScalar(input, types.T_json.ToType()), ConvertPathMatches(it, types.T_json.ToType())}
				it.Close()
				for _, result := range results {
					if !valid {
						require.Equal(t, StatusStatementError, result.Status)
						require.Error(t, result.Err)
					} else {
						require.Equal(t, StatusSuccess, result.Status)
						output := types.DecodeJson(result.Value.([]byte))
						rendered, err := output.MarshalJSON()
						require.NoError(t, err)
						require.True(t, json.Valid(rendered), string(rendered))
					}
				}
				builder, err := bytejson.NewJSONTableArrayBuilder(1024)
				require.NoError(t, err)
				appendErr := builder.Append(input)
				if !valid {
					require.Error(t, appendErr)
				} else {
					require.NoError(t, appendErr)
					output, err := builder.Build()
					require.NoError(t, err)
					rendered, err := output.MarshalJSON()
					require.NoError(t, err)
					require.True(t, json.Valid(rendered))
				}
				builder.Close()
			}
		})
	}
}
