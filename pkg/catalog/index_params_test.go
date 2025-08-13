// Copyright 2023 Matrix Origin
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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexParams_FulTextV1(t *testing.T) {
	var params IndexParams
	require.False(t, params.Type().IsValid())
	require.Equal(t, uint16(0), params.Version())
	t.Logf("params: %s", params.String())

	params = BuildIndexParamsFullTextV1(
		IndexFullTextParserType_Ngram,
	)
	require.Equal(t, IndexParamType_FullText, params.Type())
	require.Equal(t, uint16(1), params.Version())
	require.Equal(t, IndexFullTextParserType_Ngram, params.ParserType())
	t.Logf("params: %s", params.String())

	params = BuildIndexParamsFullTextV1(
		IndexFullTextParserType_Default,
	)
	require.Equal(t, IndexParamType_FullText, params.Type())
	require.Equal(t, uint16(1), params.Version())
	require.Equal(t, IndexFullTextParserType_Default, params.ParserType())
	t.Logf("params: %s", params.String())

	params = BuildIndexParamsFullTextV1(
		IndexFullTextParserType_JSONValue,
	)
	require.Equal(t, IndexParamType_FullText, params.Type())
	require.Equal(t, uint16(1), params.Version())
	require.Equal(t, IndexFullTextParserType_JSONValue, params.ParserType())
	t.Logf("params: %s", params.String())
}

func TestIndexParams_IVFFLATV1(t *testing.T) {
	var algos = []IndexParamAlgoType{
		IndexParamAlgoType_InnerProduct,
		IndexParamAlgoType_CosineDistance,
		IndexParamAlgoType_L1Distance,
		IndexParamAlgoType_Invalid,
	}

	var lists = []int64{
		100,
		1000,
		10000,
		100000,
	}

	for i, algo := range algos {
		params := BuildIndexParamsIVFFLATV1(
			lists[i],
			algo,
		)
		require.Equal(t, IndexParamType_IVFFLAT, params.Type())
		require.Equal(t, uint16(1), params.Version())
		require.Equal(t, lists[i], params.IVFFLATList())
		require.Equal(t, algo, params.IVFFLATAlgo())
		t.Logf("params: %s", params.String())
	}
}

func TestIndexParams_HNSWV1(t *testing.T) {
	var ms = []int64{
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
	}

	var qs = []IndexParamQuantizationType{
		IndexParamQuantizationType_F32,
		IndexParamQuantizationType_BF16,
		IndexParamQuantizationType_F16,
		IndexParamQuantizationType_F64,
		IndexParamQuantizationType_I8,
		IndexParamQuantizationType_B1,
		IndexParamQuantizationType_Invalid,
	}

	var efConstructions = []int64{
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
	}

	var efSearches = []int64{
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
	}

	for i, m := range ms {
		params := BuildIndexParamsHNSWV1(
			m,
			efConstructions[i],
			efSearches[i],
			qs[i],
		)
		require.Equal(t, IndexParamType_HNSWV1, params.Type())
		require.Equal(t, m, params.HNSWM())
		require.Equal(t, efConstructions[i], params.HNSWEfConstruction())
		require.Equal(t, efSearches[i], params.HNSWEfSearch())
		require.Equal(t, qs[i], params.HNSWQuantization())
		t.Logf("params: %s", params.String())
	}
}
