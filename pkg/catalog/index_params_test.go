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
		1000000000,
	}

	var algos = []IndexParamAlgoType{
		IndexParamAlgoType_L2Distance,
		IndexParamAlgoType_InnerProduct,
		IndexParamAlgoType_InnerProduct,
		IndexParamAlgoType_CosineDistance,
		IndexParamAlgoType_L1Distance,
		IndexParamAlgoType_L1Distance,
		IndexParamAlgoType_Invalid,
	}

	for i, m := range ms {
		params := BuildIndexParamsHNSWV1(
			algos[i],
			m,
			efConstructions[i],
			efSearches[i],
			qs[i],
		)
		require.Equal(t, IndexParamType_HNSW, params.Type())
		require.Equal(t, m, params.HNSWM())
		require.Equal(t, efConstructions[i], params.HNSWEfConstruction())
		require.Equal(t, efSearches[i], params.HNSWEfSearch())
		require.Equal(t, qs[i], params.HNSWQuantization())
		require.Equal(t, algos[i], params.HNSWAlgo())
		t.Logf("params: %s", params.String())
	}
}

func TestIndexParams_AlgoJsonParamStringToIndexParams_FullText(t *testing.T) {
	// 1. invalid algo, empty json param --> empty params, error
	params, err := IndexAlgoJsonParamStringToIndexParams("invalid", "")
	require.Error(t, err)
	require.Truef(t, params.IsEmpty(), "params: %s", params.String())

	// 2. invalid algo, non-empty json param --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("invalid", `{"m": "100"}`)
	require.Error(t, err)

	// 3. fulltext, empty json param --> empty params, no error
	params, err = IndexAlgoJsonParamStringToIndexParams("fulltext", "")
	require.NoError(t, err)
	require.Truef(t, params.IsEmpty(), "params: %s", params.String())

	// 4. fulltext, non-empty json param, no parser name key --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("fulltext", `{"parser2": "ngram"}`)
	require.Error(t, err)

	// 5. fulltext, non-empty json param, invalid parser name --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("fulltext", `{"parser": "invalid"}`)
	require.Error(t, err)

	// 6. fulltext, non-empty json param, valid parser name --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("fulltext", `{"parser": "ngram"}`)
	require.NoError(t, err)
	require.Falsef(t, params.IsEmpty(), "params: %s", params.String())
	require.Equal(t, IndexFullTextParserType_Ngram, params.ParserType())

	retString := params.ToJsonParamString()
	t.Logf("retString: %s", retString)
	params2, err := IndexAlgoJsonParamStringToIndexParams("fulltext", retString)
	require.NoError(t, err)
	require.Equal(t, params.String(), params2.String())

	// 7. fulltext, non-empty json param, valid parser name --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("fulltext", `{"parser": "default"}`)
	require.NoError(t, err)
	require.Falsef(t, params.IsEmpty(), "params: %s", params.String())
	require.Equal(t, IndexFullTextParserType_Default, params.ParserType())

	// 8. fulltext, non-empty json param, valid parser name --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("fulltext", `{"parser": "json_value"}`)
	require.NoError(t, err)
	require.Falsef(t, params.IsEmpty(), "params: %s", params.String())
	require.Equal(t, IndexFullTextParserType_JSONValue, params.ParserType())
}

func TestIndexParams_AlgoJsonParamStringToIndexParams_IVFFLAT(t *testing.T) {
	// 1. ivfflat, empty json param --> error
	_, err := IndexAlgoJsonParamStringToIndexParams("ivfflat", "")
	require.Error(t, err)

	// 2. ivfflat, non-empty json param, no list key or no algo key --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"op_type": "vector_l2_ops"}`)
	require.Error(t, err)
	_, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"lists": "100"}`)
	require.Error(t, err)

	// 3. ivfflat, non-empty json param, invalid list value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"lists": "invalid", "op_type": "vector_ip_ops"}`)
	require.Error(t, err)

	// 4. ivfflat, non-empty json param, valid list value --> no error
	params, err := IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"lists": "100", "op_type": "vector_cosine_ops"}`)
	require.NoErrorf(t, err, "params: %s", params.String())
	require.Equal(t, IndexParamAlgoType_CosineDistance, params.IVFFLATAlgo())
	require.Equal(t, int64(100), params.IVFFLATList())

	retString := params.ToJsonParamString()
	t.Logf("retString: %s", retString)
	params2, err := IndexAlgoJsonParamStringToIndexParams("ivfflat", retString)
	require.NoError(t, err)
	require.Equal(t, params.String(), params2.String())

	// 5. ivfflat, non-empty json param, valid list value --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"lists": "100", "op_type": "vector_l1_ops"}`)
	require.NoErrorf(t, err, "params: %s", params.String())
	require.Equal(t, IndexParamAlgoType_L1Distance, params.IVFFLATAlgo())
	require.Equal(t, int64(100), params.IVFFLATList())

	retString = params.ToJsonParamString()
	t.Logf("retString: %s", retString)
	params2, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", retString)
	require.NoError(t, err)
	require.Equal(t, params.String(), params2.String())

	// 6. ivfflat, non-empty json param, invalid algo value --> error
	params, err = IndexAlgoJsonParamStringToIndexParams("ivfflat", `{"lists": "100", "op_type": "invalid"}`)
	require.Error(t, err)
	require.Truef(t, params.IsEmpty(), "params: %s", params.String())
}

func TestIndexParams_AlgoJsonParamStringToIndexParams_HNSW(t *testing.T) {
	// 1. hnsw, empty json param --> error
	_, err := IndexAlgoJsonParamStringToIndexParams("hnsw", "")
	require.Error(t, err)

	// 2. hnsw, non-empty json param, only op_type key --> no error
	params, err := IndexAlgoJsonParamStringToIndexParams("hnsw", `{"op_type": "vector_l2_ops"}`)
	require.NoError(t, err)
	require.Equal(t, IndexParamAlgoType_L2Distance, params.HNSWAlgo())
	require.Equal(t, int64(0), params.HNSWM())
	require.Equal(t, int64(0), params.HNSWEfConstruction())
	require.Equal(t, int64(0), params.HNSWEfSearch())
	require.Equal(t, IndexParamQuantizationType_F32, params.HNSWQuantization())

	retString := params.ToJsonParamString()
	t.Logf("retString: %s", retString)
	params2, err := IndexAlgoJsonParamStringToIndexParams("hnsw", retString)
	require.NoError(t, err)
	require.Equal(t, params.String(), params2.String())

	// 3. hnsw, non-empty json param, no op_type key --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100"}`)
	require.Error(t, err)

	// 4. hnsw, non-empty json param, invalid m value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "invalid", "op_type": "vector_l2_ops"}`)
	require.Error(t, err)

	// 5. hnsw, non-empty json param, valid m value --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "op_type": "vector_l2_ops"}`)
	require.NoError(t, err)
	require.Equal(t, IndexParamAlgoType_L2Distance, params.HNSWAlgo())
	require.Equal(t, int64(100), params.HNSWM())
	require.Equal(t, int64(0), params.HNSWEfConstruction())
	require.Equal(t, int64(0), params.HNSWEfSearch())
	require.Equal(t, IndexParamQuantizationType_F32, params.HNSWQuantization())

	// 6. hnsw, non-empty json param, valid ef_construction value --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_construction": "100", "op_type": "vector_l2_ops"}`)
	require.NoError(t, err)
	require.Equal(t, IndexParamAlgoType_L2Distance, params.HNSWAlgo())
	require.Equal(t, int64(100), params.HNSWM())
	require.Equal(t, int64(100), params.HNSWEfConstruction())
	require.Equal(t, int64(0), params.HNSWEfSearch())
	require.Equal(t, IndexParamQuantizationType_F32, params.HNSWQuantization())

	// 7. hnsw, non-empty json param, valid ef_search value --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_search": "100", "op_type": "vector_l2_ops"}`)
	require.NoError(t, err)
	require.Equal(t, IndexParamAlgoType_L2Distance, params.HNSWAlgo())
	require.Equal(t, int64(100), params.HNSWM())
	require.Equal(t, int64(0), params.HNSWEfConstruction())
	require.Equal(t, int64(100), params.HNSWEfSearch())
	require.Equal(t, IndexParamQuantizationType_F32, params.HNSWQuantization())

	// 8. hnsw, non-empty json param, valid quantization value --> no error
	params, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_search": "100", "op_type": "vector_l2_ops", "quantization": "f32"}`)
	require.NoError(t, err)
	require.Equal(t, IndexParamAlgoType_L2Distance, params.HNSWAlgo())
	require.Equal(t, int64(100), params.HNSWM())
	require.Equal(t, int64(0), params.HNSWEfConstruction())
	require.Equal(t, int64(100), params.HNSWEfSearch())
	require.Equalf(t, IndexParamQuantizationType_F32, params.HNSWQuantization(), "params: %s", params.String())

	retString = params.ToJsonParamString()
	t.Logf("retString: %s", retString)
	params2, err = IndexAlgoJsonParamStringToIndexParams("hnsw", retString)
	require.NoError(t, err)
	require.Equal(t, params.String(), params2.String())

	// 9. hnsw, non-empty json param, invalid quantization value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_search": "100", "op_type": "vector_l2_ops", "quantization": "invalid"}`)
	require.Error(t, err)

	// 10. hnsw, non-empty json param, invalid algo value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_search": "100", "op_type": "invalid"}`)
	require.Error(t, err)

	// 11. hnsw, non-empty json param, invalid ef_search value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_search": "invalid", "op_type": "vector_l2_ops"}`)
	require.Error(t, err)

	// 12. hnsw, non-empty json param, invalid ef_construction value --> error
	_, err = IndexAlgoJsonParamStringToIndexParams("hnsw", `{"m": "100", "ef_construction": "invalid", "op_type": "vector_l2_ops"}`)
	require.Error(t, err)
}

func TestIndexParams_ToStringList(t *testing.T) {
	var params IndexParams
	res, err := params.ToStringList()
	require.NoError(t, err)
	require.Equal(t, "", res)

	params = BuildIndexParamsFullTextV1(
		IndexFullTextParserType_Ngram,
	)
	res, err = params.ToStringList()
	require.NoError(t, err)
	require.Equal(t, " PARSER ngram ", res)

	params = BuildIndexParamsIVFFLATV1(
		10,
		IndexParamAlgoType_CosineDistance,
	)
	res, err = params.ToStringList()
	require.NoError(t, err)
	require.Equal(t, " lists = 10  op_type = vector_cosine_ops ", res)

	params = BuildIndexParamsHNSWV1(
		IndexParamAlgoType_L2Distance,
		10,
		0,
		1000,
		IndexParamQuantizationType_F32,
	)
	res, err = params.ToStringList()
	require.NoError(t, err)
	require.Equal(t, " m = 10  ef_search = 1000  quantization = F32  op_type = vector_l2_ops ", res)

	params = BuildIndexParamsHNSWV1(
		IndexParamAlgoType_InnerProduct,
		10,
		100,
		1000,
		IndexParamQuantizationType_I8,
	)
	res, err = params.ToStringList()
	require.NoError(t, err)
	require.Equal(t, " m = 10  ef_construction = 100  ef_search = 1000  quantization = I8  op_type = vector_ip_ops ", res)
}
