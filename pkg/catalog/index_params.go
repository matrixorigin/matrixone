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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	usearch "github.com/unum-cloud/usearch/golang"
)

func GetUsearchMetricFromIndexParamAlgoType(algo IndexParamAlgoType) (usearch.Metric, bool) {
	switch algo {
	case IndexParamAlgoType_L2Distance:
		return usearch.L2sq, true
	case IndexParamAlgoType_InnerProduct:
		return usearch.InnerProduct, true
	case IndexParamAlgoType_CosineDistance:
		return usearch.Cosine, true
	default:
		return usearch.L2sq, false
	}
}

// ------------------------[IndexFullTextParserType] ------------------------
type IndexFullTextParserType uint16

const (
	IndexFullTextParserType_Invalid IndexFullTextParserType = iota
	IndexFullTextParserType_Ngram
	IndexFullTextParserType_JSON
	IndexFullTextParserType_JSONValue
	IndexFullTextParserType_Default
)

func (t IndexFullTextParserType) String() string {
	switch t {
	case IndexFullTextParserType_Default:
		return "default"
	case IndexFullTextParserType_Ngram:
		return "ngram"
	case IndexFullTextParserType_JSON:
		return "json"
	case IndexFullTextParserType_JSONValue:
		return "json_value"
	}
	return "invalid"
}

func (t IndexFullTextParserType) IsValid() bool {
	return t > IndexFullTextParserType_Invalid && t <= IndexFullTextParserType_Default
}

func StringToIndexFullTextParserType(s string) IndexFullTextParserType {
	s = strings.ToLower(s)
	switch s {
	case "default":
		return IndexFullTextParserType_Default
	case "ngram":
		return IndexFullTextParserType_Ngram
	case "json":
		return IndexFullTextParserType_JSON
	case "json_value":
		return IndexFullTextParserType_JSONValue
	default:
		return IndexFullTextParserType_Invalid
	}
}

// ------------------------[IndexParamAlgoType] ------------------------
type IndexParamAlgoType uint16

const (
	IndexParamAlgoType_L2Distance     = IndexParamAlgoType(metric.Metric_L2Distance)
	IndexParamAlgoType_InnerProduct   = IndexParamAlgoType(metric.Metric_InnerProduct)
	IndexParamAlgoType_CosineDistance = IndexParamAlgoType(metric.Metric_CosineDistance)
	IndexParamAlgoType_L1Distance     = IndexParamAlgoType(metric.Metric_L1Distance)
	IndexParamAlgoType_Invalid        = IndexParamAlgoType(metric.Metric_TypeCount)
)

func (t IndexParamAlgoType) String() string {
	switch t {
	case IndexParamAlgoType_L2Distance:
		return "vector_l2_ops"
	case IndexParamAlgoType_InnerProduct:
		return "vector_ip_ops"
	case IndexParamAlgoType_CosineDistance:
		return "vector_cosine_ops"
	case IndexParamAlgoType_L1Distance:
		return "vector_l1_ops"
	}
	return "vector_invalid_ops"
}

func (t IndexParamAlgoType) IsValid() bool {
	return t < IndexParamAlgoType_Invalid
}

func StringToIndexParamAlgoType(s string) IndexParamAlgoType {
	s = strings.ToLower(s)
	switch s {
	case "vector_l2_ops":
		return IndexParamAlgoType_L2Distance
	case "vector_ip_ops":
		return IndexParamAlgoType_InnerProduct
	case "vector_cosine_ops":
		return IndexParamAlgoType_CosineDistance
	case "vector_l1_ops":
		return IndexParamAlgoType_L1Distance
	}
	return IndexParamAlgoType_Invalid
}

// ------------------------[IndexParamQuantizationType] ------------------------
type IndexParamQuantizationType uint16

const (
	IndexParamQuantizationType_F32 IndexParamQuantizationType = iota
	IndexParamQuantizationType_BF16
	IndexParamQuantizationType_F16
	IndexParamQuantizationType_F64
	IndexParamQuantizationType_I8
	IndexParamQuantizationType_B1
	IndexParamQuantizationType_Invalid
)

func (t IndexParamQuantizationType) String() string {
	switch t {
	case IndexParamQuantizationType_F32:
		return "F32"
	case IndexParamQuantizationType_BF16:
		return "BF16"
	case IndexParamQuantizationType_F16:
		return "F16"
	case IndexParamQuantizationType_F64:
		return "F64"
	case IndexParamQuantizationType_I8:
		return "I8"
	case IndexParamQuantizationType_B1:
		return "B1"
	}
	return "invalid"
}

func (t IndexParamQuantizationType) IsValid() bool {
	return t < IndexParamQuantizationType_Invalid
}

func StringToIndexParamQuantizationType(s string) IndexParamQuantizationType {
	s = strings.ToLower(s)
	switch s {
	case "f32":
		return IndexParamQuantizationType_F32
	case "bf16":
		return IndexParamQuantizationType_BF16
	case "f16":
		return IndexParamQuantizationType_F16
	case "f64":
		return IndexParamQuantizationType_F64
	case "i8":
		return IndexParamQuantizationType_I8
	case "b1":
		return IndexParamQuantizationType_B1
	}
	return IndexParamQuantizationType_Invalid
}

// ------------------------[IndexParamType] ------------------------
type IndexParamType uint16

const (
	IndexParamType_Invalid IndexParamType = iota
	IndexParamType_FullText
	IndexParamType_IVFFLAT
	IndexParamType_HNSW
)

func (t IndexParamType) String() string {
	switch t {
	case IndexParamType_FullText:
		return "fulltext"
	case IndexParamType_IVFFLAT:
		return "ivfflat"
	case IndexParamType_HNSW:
		return "hnsw"
	}
	return "invalid"
}

func (t IndexParamType) IsValid() bool {
	return t > IndexParamType_Invalid && t <= IndexParamType_HNSW
}

const (
	IndexParamMagicNumber = 0x1234
)

var IndexParamMagicNumberBuf []byte
var IndexParamTypeFullTextBuf []byte
var IndexParamTypeIVFFLATBuf []byte
var IndexParamTypeHNSWV1Buf []byte

func init() {
	IndexParamMagicNumberBuf = types.EncodeFixed(uint16(IndexParamMagicNumber))
	IndexParamTypeFullTextBuf = types.EncodeFixed(uint16(IndexParamType_FullText))
	IndexParamTypeIVFFLATBuf = types.EncodeFixed(uint16(IndexParamType_IVFFLAT))
	IndexParamTypeHNSWV1Buf = types.EncodeFixed(uint16(IndexParamType_HNSW))
}

type IndexParams []byte

// ------------------------[HEADER] IndexParams------------------------
const (
	IndexParams_MagicOff   = 0
	IndexParams_MagicLen   = 2
	IndexParams_TypeOff    = IndexParams_MagicOff + IndexParams_MagicLen
	IndexParams_TypeLen    = 2
	IndexParams_VersionOff = IndexParams_TypeOff + IndexParams_TypeLen
	IndexParams_VersionLen = 2
	IndexParams_HeaderLen  = IndexParams_MagicLen + IndexParams_TypeLen + IndexParams_VersionLen
)

// ------------------------[FULL TEXT V1 PARAMS] IndexParams------------------------
const (
	IndexParams_FullTextV1_ParserOff = IndexParams_HeaderLen
	IndexParams_FullTextV1_ParserLen = 2
	IndexParams_FullTextV1_Size      = IndexParams_FullTextV1_ParserOff + IndexParams_FullTextV1_ParserLen
)

func BuildIndexParamsFullTextV1(
	parser IndexFullTextParserType,
) IndexParams {
	buf := make([]byte, IndexParams_FullTextV1_Size)
	copy(buf, IndexParamMagicNumberBuf)
	copy(buf[IndexParams_TypeOff:], IndexParamTypeFullTextBuf)
	copy(buf[IndexParams_VersionOff:], types.EncodeFixed[uint16](1))
	copy(buf[IndexParams_FullTextV1_ParserOff:], types.EncodeFixed[uint16](uint16(parser)))
	return buf
}

func (params IndexParams) ParserType() IndexFullTextParserType {
	if len(params) < IndexParams_HeaderLen {
		return IndexFullTextParserType_Invalid
	}
	return IndexFullTextParserType(types.DecodeFixed[uint16](params[IndexParams_FullTextV1_ParserOff:]))
}

// ------------------------[IVFFLAT V1 PARAMS] IndexParams------------------------
const (
	IndexParams_IVFFLATV1_ListOff = IndexParams_HeaderLen
	IndexParams_IVFFLATV1_ListLen = 8 // int64
	IndexParams_IVFFLATV1_AlgoOff = IndexParams_IVFFLATV1_ListOff + IndexParams_IVFFLATV1_ListLen
	IndexParams_IVFFLATV1_AlgoLen = 2 // uint16
	IndexParams_IVFFLATV1_Size    = IndexParams_IVFFLATV1_AlgoOff + IndexParams_IVFFLATV1_AlgoLen
)

func DefaultIVFFLATV1Params() IndexParams {
	return BuildIndexParamsIVFFLATV1(1, IndexParamAlgoType_L2Distance)
}

func BuildIndexParamsIVFFLATV1(
	list int64,
	algo IndexParamAlgoType,
) IndexParams {
	buf := make([]byte, IndexParams_IVFFLATV1_Size)
	copy(buf, IndexParamMagicNumberBuf)
	copy(buf[IndexParams_TypeOff:], IndexParamTypeIVFFLATBuf)
	copy(buf[IndexParams_VersionOff:], types.EncodeFixed[uint16](1))
	copy(buf[IndexParams_IVFFLATV1_ListOff:], types.EncodeFixed(list))
	copy(buf[IndexParams_IVFFLATV1_AlgoOff:], types.EncodeFixed(uint16(algo)))
	return buf
}

func (params IndexParams) IVFFLATList() int64 {
	if len(params) < IndexParams_IVFFLATV1_ListOff {
		return 0
	}
	return types.DecodeFixed[int64](params[IndexParams_IVFFLATV1_ListOff:])
}

func (params IndexParams) IVFFLATAlgo() IndexParamAlgoType {
	if len(params) < IndexParams_IVFFLATV1_AlgoOff {
		return IndexParamAlgoType_Invalid
	}
	return IndexParamAlgoType(types.DecodeFixed[uint16](params[IndexParams_IVFFLATV1_AlgoOff:]))
}

// ------------------------[HNSW V1 PARAMS] IndexParams------------------------
const (
	IndexParams_HNSWV1_MOff              = IndexParams_HeaderLen
	IndexParams_HNSWV1_MLen              = 8 // int64
	IndexParams_HNSWV1_EfConstructionOff = IndexParams_HNSWV1_MOff + IndexParams_HNSWV1_MLen
	IndexParams_HNSWV1_EfConstructionLen = 8 // int64
	IndexParams_HNSWV1_EfSearchOff       = IndexParams_HNSWV1_EfConstructionOff + IndexParams_HNSWV1_EfConstructionLen
	IndexParams_HNSWV1_EfSearchLen       = 8 // int64
	IndexParams_HNSWV1_QuantizationOff   = IndexParams_HNSWV1_EfSearchOff + IndexParams_HNSWV1_EfSearchLen
	IndexParams_HNSWV1_QuantizationLen   = 2 // uint16
	IndexParams_HNSWV1_AlgoOff           = IndexParams_HNSWV1_QuantizationOff + IndexParams_HNSWV1_QuantizationLen
	IndexParams_HNSWV1_AlgoLen           = 2 // uint16
	IndexParams_HNSWV1_Size              = IndexParams_HNSWV1_AlgoOff + IndexParams_HNSWV1_AlgoLen
)

func BuildIndexParamsHNSWV1(
	algo IndexParamAlgoType,
	m int64, // optional
	efConstruction int64, // optional
	efSearch int64, // optional
	quantization IndexParamQuantizationType, // optional
) IndexParams {
	buf := make([]byte, IndexParams_HNSWV1_Size)
	copy(buf, IndexParamMagicNumberBuf)
	copy(buf[IndexParams_TypeOff:], IndexParamTypeHNSWV1Buf)
	copy(buf[IndexParams_VersionOff:], types.EncodeFixed[uint16](1))
	copy(buf[IndexParams_HNSWV1_MOff:], types.EncodeFixed(m))
	copy(buf[IndexParams_HNSWV1_EfConstructionOff:], types.EncodeFixed(efConstruction))
	copy(buf[IndexParams_HNSWV1_EfSearchOff:], types.EncodeFixed(efSearch))
	copy(buf[IndexParams_HNSWV1_QuantizationOff:], types.EncodeFixed(uint16(quantization)))
	copy(buf[IndexParams_HNSWV1_AlgoOff:], types.EncodeFixed(uint16(algo)))
	return buf
}

func (params IndexParams) HNSWM() int64 {
	if len(params) < IndexParams_HNSWV1_MOff {
		return 0
	}
	return types.DecodeFixed[int64](params[IndexParams_HNSWV1_MOff:])
}

func (params IndexParams) HNSWEfConstruction() int64 {
	if len(params) < IndexParams_HNSWV1_EfConstructionOff {
		return 0
	}
	return types.DecodeFixed[int64](params[IndexParams_HNSWV1_EfConstructionOff:])
}

func (params IndexParams) HNSWEfSearch() int64 {
	if len(params) < IndexParams_HNSWV1_EfSearchOff {
		return 0
	}
	return types.DecodeFixed[int64](params[IndexParams_HNSWV1_EfSearchOff:])
}

func (params IndexParams) HNSWQuantization() IndexParamQuantizationType {
	if len(params) < IndexParams_HNSWV1_QuantizationOff {
		return IndexParamQuantizationType_Invalid
	}
	return IndexParamQuantizationType(types.DecodeFixed[uint16](params[IndexParams_HNSWV1_QuantizationOff:]))
}

func (params IndexParams) HNSWAlgo() IndexParamAlgoType {
	if len(params) < IndexParams_HNSWV1_AlgoOff {
		return IndexParamAlgoType_Invalid
	}
	return IndexParamAlgoType(types.DecodeFixed[uint16](params[IndexParams_HNSWV1_AlgoOff:]))
}

// ------------------------[IndexParams] ------------------------

func (params IndexParams) IsHNSW() bool {
	return params.Type() == IndexParamType_HNSW
}

func (params IndexParams) IsIVFFLAT() bool {
	return params.Type() == IndexParamType_IVFFLAT
}

func (params IndexParams) IsFullText() bool {
	return params.Type() == IndexParamType_FullText
}

func (params IndexParams) String() string {
	if len(params) < IndexParams_HeaderLen {
		return "invalid index params"
	}
	magic := types.DecodeFixed[uint16](params[:IndexParams_MagicLen])
	if magic != IndexParamMagicNumber {
		return fmt.Sprintf("invalid index params magic number: %x", magic)
	}
	paramType := IndexParamType(types.DecodeFixed[uint16](params[IndexParams_TypeOff:]))
	version := types.DecodeFixed[uint16](params[IndexParams_VersionOff:])
	switch paramType {
	case IndexParamType_FullText:
		parser := IndexFullTextParserType(types.DecodeFixed[uint16](params[IndexParams_FullTextV1_ParserOff:]))
		return fmt.Sprintf(
			"{param_type: %s, version: %d, parser: %s}",
			paramType.String(),
			version,
			parser.String(),
		)
	case IndexParamType_IVFFLAT:
		list := types.DecodeFixed[int64](params[IndexParams_IVFFLATV1_ListOff:])
		algo := IndexParamAlgoType(types.DecodeFixed[uint16](params[IndexParams_IVFFLATV1_AlgoOff:]))
		return fmt.Sprintf(
			"{param_type: %s, version: %d, list: %d, algo: %s}",
			paramType.String(),
			version,
			list,
			algo.String(),
		)
	case IndexParamType_HNSW:
		m := types.DecodeFixed[int64](params[IndexParams_HNSWV1_MOff:])
		efConstruction := types.DecodeFixed[int64](params[IndexParams_HNSWV1_EfConstructionOff:])
		efSearch := types.DecodeFixed[int64](params[IndexParams_HNSWV1_EfSearchOff:])
		quantization := IndexParamQuantizationType(types.DecodeFixed[uint16](params[IndexParams_HNSWV1_QuantizationOff:]))
		return fmt.Sprintf(
			"{param_type: %s, version: %d, m: %d, ef_construction: %d, ef_search: %d, quantization: %s}",
			paramType.String(),
			version,
			m,
			efConstruction,
			efSearch,
			quantization.String(),
		)
	default:
		return fmt.Sprintf("invalid index params type: %s", paramType.String())
	}
}

func (params IndexParams) ToJsonParamString() string {
	if params.IsEmpty() {
		return ""
	}
	res := make(map[string]string)
	switch params.Type() {
	case IndexParamType_FullText:
		res[IndexAlgoParamParserName] = params.ParserType().String()
	case IndexParamType_IVFFLAT:
		res[IndexAlgoParamLists] = strconv.FormatInt(params.IVFFLATList(), 10)
		res[IndexAlgoParamOpType] = params.IVFFLATAlgo().String()
	case IndexParamType_HNSW:
		if params.HNSWM() > 0 {
			res[HnswM] = strconv.FormatInt(params.HNSWM(), 10)
		}
		if params.HNSWEfConstruction() > 0 {
			res[HnswEfConstruction] = strconv.FormatInt(params.HNSWEfConstruction(), 10)
		}
		if params.HNSWEfSearch() > 0 {
			res[HnswEfSearch] = strconv.FormatInt(params.HNSWEfSearch(), 10)
		}
		if params.HNSWQuantization().IsValid() {
			res[HnswQuantization] = params.HNSWQuantization().String()
		}
		res[IndexAlgoParamOpType] = params.HNSWAlgo().String()
	default:
		return ""
	}
	jsonStr, err := json.Marshal(res)
	if err != nil {
		return ""
	}
	return string(jsonStr)
}

func (params IndexParams) ToStringList() (res string, err error) {
	if params.IsEmpty() {
		return
	}
	switch params.Type() {
	case IndexParamType_FullText:
		res = fmt.Sprintf(
			" %s %s ",
			strings.ToUpper(IndexAlgoParamParserName),
			params.ParserType().String(),
		)
	case IndexParamType_IVFFLAT:
		if !params.IVFFLATAlgo().IsValid() {
			err = moerr.NewInternalErrorNoCtxf("invalid algo: %s", params.IVFFLATAlgo().String())
			return
		}
		if params.IVFFLATList() > 0 {
			res += fmt.Sprintf(" %s = %d ", IndexAlgoParamLists, params.IVFFLATList())
		}
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamOpType, params.IVFFLATAlgo().String())
	case IndexParamType_HNSW:
		if !params.HNSWAlgo().IsValid() {
			err = moerr.NewInternalErrorNoCtxf("invalid algo: %s", params.HNSWAlgo().String())
			return
		}
		if !params.HNSWQuantization().IsValid() {
			err = moerr.NewInternalErrorNoCtxf("invalid quantization: %s", params.HNSWQuantization().String())
			return
		}
		if params.HNSWM() > 0 {
			res += fmt.Sprintf(" %s = %d ", HnswM, params.HNSWM())
		}
		if params.HNSWEfConstruction() > 0 {
			res += fmt.Sprintf(" %s = %d ", HnswEfConstruction, params.HNSWEfConstruction())
		}
		if params.HNSWEfSearch() > 0 {
			res += fmt.Sprintf(" %s = %d ", HnswEfSearch, params.HNSWEfSearch())
		}
		res += fmt.Sprintf(" %s = %s ", HnswQuantization, params.HNSWQuantization().String())
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamOpType, params.HNSWAlgo().String())
	default:
		err = moerr.NewInternalErrorNoCtxf("invalid index params type: %s", params.Type().String())
		return
	}
	return
}

func (params IndexParams) IsEmpty() bool {
	return len(params) == 0
}

func (params IndexParams) Type() IndexParamType {
	if len(params) < IndexParams_HeaderLen {
		return IndexParamType_Invalid
	}
	paramType := IndexParamType(types.DecodeFixed[uint16](params[IndexParams_TypeOff:]))
	return paramType
}

func (params IndexParams) Version() uint16 {
	if len(params) < IndexParams_HeaderLen {
		return 0
	}
	version := types.DecodeFixed[uint16](params[IndexParams_VersionOff:])
	return version
}

// ------------------------[Utils] ------------------------

func AstTreeToIndexParams(
	astTree any,
) (params IndexParams, err error) {

	// fulltext index:
	// TODO: why fulltext index is not a tree.Index?
	if fulltext, ok := astTree.(*tree.FullTextIndex); ok {
		if fulltext.IndexOption == nil {
			return
		}
		parserType := StringToIndexFullTextParserType(fulltext.IndexOption.ParserName)
		if !parserType.IsValid() {
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid parser %s", fulltext.IndexOption.ParserName))
			return
		}
		params = BuildIndexParamsFullTextV1(parserType)
		return
	}
	index, ok := astTree.(*tree.Index)
	if !ok {
		err = moerr.NewInternalErrorNoCtxf(
			"invalid ast tree: %v", astTree,
		)
		return
	}
	switch index.KeyType {
	case tree.INDEX_TYPE_IVFFLAT:
		algoList := index.IndexOption.AlgoParamList
		if algoList == 0 {
			algoList = 1
		}
		algo := StringToIndexParamAlgoType(index.IndexOption.AlgoParamVectorOpType)
		if !algo.IsValid() {
			if len(index.IndexOption.AlgoParamVectorOpType) > 0 {
				err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid algo_param_vector_op_type: %s", index.IndexOption.AlgoParamVectorOpType))
				return
			}
			algo = IndexParamAlgoType_L2Distance // Set default algo
		}
		params = BuildIndexParamsIVFFLATV1(algoList, algo)
	case tree.INDEX_TYPE_HNSW:
		if index.IndexOption.HnswM < 0 {
			err = moerr.NewInternalErrorNoCtx("invalid M. hnsw.M must be > 0")
			return
		}
		if index.IndexOption.HnswEfConstruction < 0 {
			err = moerr.NewInternalErrorNoCtx("invalid ef_construction. hnsw.ef_construction must be > 0")
			return
		}
		if index.IndexOption.HnswEfSearch < 0 {
			err = moerr.NewInternalErrorNoCtx("invalid ef_search. hnsw.ef_search must be > 0")
			return
		}
		quantization := StringToIndexParamQuantizationType(index.IndexOption.HnswQuantization)
		if !quantization.IsValid() {
			if len(index.IndexOption.HnswQuantization) > 0 {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw quantization: %s",
					index.IndexOption.HnswQuantization,
				)
				return
			}
			quantization = IndexParamQuantizationType_F32 // Set default quantization
		}
		algo := StringToIndexParamAlgoType(index.IndexOption.AlgoParamVectorOpType)
		if !algo.IsValid() {
			if len(index.IndexOption.AlgoParamVectorOpType) > 0 {
				err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid algo_param_vector_op_type: %s", index.IndexOption.AlgoParamVectorOpType))
				return
			}
			algo = IndexParamAlgoType_L2Distance // Set default algo
		}
		params = BuildIndexParamsHNSWV1(
			algo,
			index.IndexOption.HnswM,
			index.IndexOption.HnswEfConstruction,
			index.IndexOption.HnswEfSearch,
			quantization,
		)
	case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID, tree.INDEX_TYPE_MASTER, tree.INDEX_TYPE_FULLTEXT:
		// do nothing
	}

	return
}

func IndexAlgoJsonParamStringToIndexParams(
	algo string,
	algoJsonParam string,
) (params IndexParams, err error) {
	if len(algoJsonParam) == 0 {
		if algo == "" || algo == "fulltext" {
			return
		}
		err = moerr.NewInternalErrorNoCtxf(
			"invalid algo: %s, algoJsonParam: %s", algo, algoJsonParam,
		)
		return
	}
	var jsonMap map[string]string
	if err = json.Unmarshal([]byte(algoJsonParam), &jsonMap); err != nil {
		return
	}

	switch algo {
	case "hnsw":
		var (
			ok                                              bool
			m, efConstruction, efSearch, quantization, algo string
			mValue, efConstructionValue, efSearchValue      int64
			quantizationValue                               IndexParamQuantizationType
			algoValue                                       IndexParamAlgoType
		)
		if m, ok = jsonMap[HnswM]; ok {
			if mValue, err = strconv.ParseInt(m, 10, 64); err != nil {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw param: %s",
					algoJsonParam,
				)
				return
			}
		}
		if efConstruction, ok = jsonMap[HnswEfConstruction]; ok {
			if efConstructionValue, err = strconv.ParseInt(efConstruction, 10, 64); err != nil {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw param: %s",
					algoJsonParam,
				)
				return
			}
		}
		if efSearch, ok = jsonMap[HnswEfSearch]; ok {
			if efSearchValue, err = strconv.ParseInt(efSearch, 10, 64); err != nil {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw param: %s",
					algoJsonParam,
				)
				return
			}
		}
		if quantization, ok = jsonMap[HnswQuantization]; ok {
			if quantizationValue = StringToIndexParamQuantizationType(quantization); !quantizationValue.IsValid() {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw param: %s",
					algoJsonParam,
				)
				return
			}
		}
		if algo, ok = jsonMap[IndexAlgoParamOpType]; !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"invalid hnsw param: %s",
				algoJsonParam,
			)
			return
		} else {
			algoValue = StringToIndexParamAlgoType(algo)
			if !algoValue.IsValid() {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid hnsw param: %s",
					algoJsonParam,
				)
				return
			}
		}
		params = BuildIndexParamsHNSWV1(
			algoValue,
			mValue,
			efConstructionValue,
			efSearchValue,
			quantizationValue,
		)
	case "ivfflat":
		var (
			ok         bool
			list, algo string
			listValue  int64
			algoValue  IndexParamAlgoType
		)
		if list, ok = jsonMap[IndexAlgoParamLists]; !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"invalid ivfflat param: %s",
				algoJsonParam,
			)
			return
		} else {
			listValue, err = strconv.ParseInt(list, 10, 64)
			if err != nil {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid ivfflat param: %s",
					algoJsonParam,
				)
				return
			}
		}
		if algo, ok = jsonMap[IndexAlgoParamOpType]; !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"invalid ivfflat param: %s",
				algoJsonParam,
			)
			return
		} else {
			algoValue = StringToIndexParamAlgoType(algo)
			if !algoValue.IsValid() {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid ivfflat param: %s",
					algoJsonParam,
				)
				return
			}
		}
		params = BuildIndexParamsIVFFLATV1(listValue, algoValue)
	case "fulltext":
		var parserValue IndexFullTextParserType
		if parser, ok := jsonMap[IndexAlgoParamParserName]; !ok {
			err = moerr.NewInternalErrorNoCtxf(
				"invalid fulltext param: %s",
				algoJsonParam,
			)
			return
		} else {
			parserValue = StringToIndexFullTextParserType(parser)
			if !parserValue.IsValid() {
				err = moerr.NewInternalErrorNoCtxf(
					"invalid fulltext param: %s",
					algoJsonParam,
				)
				return
			}
		}

		params = BuildIndexParamsFullTextV1(parserValue)
	default:
		err = moerr.NewInternalErrorNoCtxf(
			"invalid algo: %s", algo,
		)
	}
	return
}

// paramStr is string(IndexParams)
func IndexParamsStrToJsonParamString(
	paramStr string,
) string {
	if paramStr == "" {
		return ""
	}
	params := IndexParams(util.UnsafeStringToBytes(paramStr))
	return params.ToJsonParamString()
}

func TryToIndexParams(s string) (IndexParams, error) {
	if len(s) <= IndexParams_HeaderLen {
		return IndexParams{}, moerr.NewInternalErrorNoCtxf(
			"invalid index params: %s", s,
		)
	}
	buf := util.UnsafeStringToBytes(s)
	ret := IndexParams(buf)
	if ret.Type() == IndexParamType_Invalid {
		return IndexParams{}, moerr.NewInternalErrorNoCtxf(
			"invalid index params: %s", s,
		)
	}
	return ret, nil
}

func MustIndexParams(s string) IndexParams {
	ret, err := TryToIndexParams(s)
	if err != nil {
		panic(err)
	}
	return ret
}
