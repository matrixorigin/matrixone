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
	"sort"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Index Algorithm names
const (
	MoIndexDefaultAlgo  = tree.INDEX_TYPE_INVALID  // used by UniqueIndex or default SecondaryIndex
	MoIndexBTreeAlgo    = tree.INDEX_TYPE_BTREE    // used for Mocking MySQL behaviour.
	MoIndexRTreeAlgo    = tree.INDEX_TYPE_RTREE    // used for Spatial Index on GEOMETRY columns
	MoIndexIvfFlatAlgo  = tree.INDEX_TYPE_IVFFLAT  // used for IVF flat index on Vector/Array columns
	MOIndexMasterAlgo   = tree.INDEX_TYPE_MASTER   // used for Master Index on VARCHAR columns
	MOIndexFullTextAlgo = tree.INDEX_TYPE_FULLTEXT // used for Fulltext Index on VARCHAR columns
	MoIndexHnswAlgo     = tree.INDEX_TYPE_HNSW     // used for HNSW Index on Vector/Array columns
	MoIndexCagraAlgo    = tree.INDEX_TYPE_CAGRA    // used for CAGRA Index on Vector/Array columns
	MoIndexIvfpqAlgo    = tree.INDEX_TYPE_IVFPQ    // used for IVFPQ Index on Vector/Array columns
)

// ToLower is used for before comparing AlgoType and IndexAlgoParamOpType. Reason why they are strings
//  1. Changing AlgoType from string to Enum will break the backward compatibility.
//     "panic: Unable to find target column from predefined table columns"
//  2. IndexAlgoParamOpType is serialized and stored in the mo_indexes as JSON string.
func ToLower(str string) string {
	return strings.ToLower(strings.TrimSpace(str))
}

// IsNullIndexAlgo is used to skip printing the default "" index algo in the restoreDDL and buildShowCreateTable
func IsNullIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexDefaultAlgo.ToString()
}

// IsRegularIndexAlgo are indexes which will be handled by regular index flow, ie the one where
// we have one hidden table.
func IsRegularIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexDefaultAlgo.ToString() || _algo == MoIndexBTreeAlgo.ToString() || _algo == MoIndexRTreeAlgo.ToString()
}

func IsRTreeIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexRTreeAlgo.ToString()
}

func IsIvfIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexIvfFlatAlgo.ToString()
}

func IsMasterIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MOIndexMasterAlgo.ToString()
}

func IsFullTextIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MOIndexFullTextAlgo.ToString()
}

func IsHnswIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexHnswAlgo.ToString()
}

func IsCagraIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexCagraAlgo.ToString()
}

func IsIvfpqIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexIvfpqAlgo.ToString()
}

// ------------------------[START] IndexAlgoParams------------------------
const (
	IndexAlgoParamLists     = "lists"
	IndexAlgoParamOpType    = "op_type"
	HnswM                   = "m"
	HnswEfConstruction      = "ef_construction"
	HnswEfSearch            = "ef_search"
	Async                   = "async"
	AutoUpdate              = "auto_update"
	Day                     = "day"
	Hour                    = "hour"
	DistributionMode        = "distribution_mode"
	Quantization            = "quantization"
	BitsPerCode             = "bits_per_code"
	IntermediateGraphDegree = "intermediate_graph_degree"
	GraphDegree             = "graph_degree"
	ITopkSize               = "itopk_size"
	// IncludedColumns is catalog/build metadata. SHOW CREATE renders INCLUDE
	// from plan.IndexDef.IncludedColumns, not from flat algo_params, to avoid
	// duplicate INCLUDE clauses when both locations are populated for
	// compatibility.
	IncludedColumns = "included_columns"

	// Index-defining build params, settable as CREATE INDEX options (parsed by
	// each plugin's ParamsFromTree). Written into flat algo_params only when
	// explicitly specified, read back by the build path (table functions /
	// sync), and rendered by IndexParamsToStringList for SHOW CREATE.
	IndexAlgoParamKmeansTrainPercent = "kmeans_train_percent"
	IndexAlgoParamKmeansMaxIteration = "kmeans_max_iteration"
	IndexAlgoParamMaxIndexCapacity   = "max_index_capacity"

	IndexAlgoParamPrefixLengths = "prefix_lengths"
)

func MarshalIncludeColumnsValue(cols []string) (string, error) {
	if len(cols) == 0 {
		return "", nil
	}
	data, err := json.Marshal(cols)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func ParseIncludeColumnsValue(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	if strings.HasPrefix(raw, "[") {
		var cols []string
		if err := json.Unmarshal([]byte(raw), &cols); err == nil {
			return cols, nil
		}
	}

	parts := strings.Split(raw, ",")
	cols := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		cols = append(cols, part)
	}
	if len(cols) == 0 {
		return nil, nil
	}
	return cols, nil
}

/* 1. ToString Functions */

// IndexParamsToStringList used by buildShowCreateTable and restoreDDL
// Eg:- "LIST = 10 op_type 'vector_l2_ops'"
// NOTE: don't set default values here as it is used by SHOW and RESTORE DDL.
func IndexParamsToStringList(indexParams string) (string, error) {
	result, err := IndexParamsStringToMap(indexParams)
	if err != nil {
		return "", err
	}

	res := ""
	if val, ok := result[IndexAlgoParamLists]; ok {
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamLists, val)
	}

	if val, ok := result[HnswM]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswM, val)
	}

	if val, ok := result[HnswEfConstruction]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswEfConstruction, val)
	}

	if val, ok := result[HnswEfSearch]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswEfSearch, val)
	}

	if opType, ok := result[IndexAlgoParamOpType]; ok {
		opType = ToLower(opType)
		if _, ok := metric.OpTypeToIvfMetric[opType]; !ok {
			return "", moerr.NewInternalErrorNoCtxf("invalid op_type: '%s'", opType)
		}

		res += fmt.Sprintf(" %s '%s' ", IndexAlgoParamOpType, opType)
	}

	if val, ok := result[Async]; ok {
		if val == "true" {
			res += fmt.Sprintf(" %s ", Async)
		}
	}

	if val, ok := result[AutoUpdate]; ok {
		if val == "true" {
			res += fmt.Sprintf(" %s = %s ", AutoUpdate, val)
		}
	}

	if val, ok := result[Day]; ok {
		res += fmt.Sprintf(" %s = %s ", Day, val)
	}

	if val, ok := result[Hour]; ok {
		res += fmt.Sprintf(" %s = %s ", Hour, val)
	}

	if val, ok := result[Quantization]; ok {
		res += fmt.Sprintf(" %s '%s' ", Quantization, val)
	}

	if val, ok := result[DistributionMode]; ok {
		res += fmt.Sprintf(" %s '%s' ", DistributionMode, val)
	}

	if val, ok := result[BitsPerCode]; ok {
		res += fmt.Sprintf(" %s = %s ", BitsPerCode, val)
	}

	if val, ok := result[IntermediateGraphDegree]; ok {
		res += fmt.Sprintf(" %s = %s ", IntermediateGraphDegree, val)
	}

	if val, ok := result[GraphDegree]; ok {
		res += fmt.Sprintf(" %s = %s ", GraphDegree, val)
	}

	if val, ok := result[ITopkSize]; ok {
		res += fmt.Sprintf(" %s = %s ", ITopkSize, val)
	}

	if val, ok := result[IndexAlgoParamKmeansTrainPercent]; ok {
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamKmeansTrainPercent, val)
	}

	if val, ok := result[IndexAlgoParamKmeansMaxIteration]; ok {
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamKmeansMaxIteration, val)
	}

	if val, ok := result[IndexAlgoParamMaxIndexCapacity]; ok {
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamMaxIndexCapacity, val)
	}

	return res, nil
}

// IndexParamsToJsonString used by buildSecondaryIndexDef
// Eg:- {"lists":"10","op_type":"vector_l2_ops"}
func IndexParamsToJsonString(def interface{}) (string, error) {

	res, err := indexParamsToMap(def)
	if err != nil {
		return "", err
	}

	if len(res) == 0 {
		return "", nil // don't return empty json "{}" string
	}

	return IndexParamsMapToJsonString(res)
}

// IndexParamsMapToJsonString used by AlterTableInPlace and CreateIndexDef
func IndexParamsMapToJsonString(res map[string]string) (string, error) {
	str, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func AddIndexPrefixLengthsToParams(indexParams string, keyParts []*tree.KeyPart) (string, error) {
	prefixLengths := IndexPrefixLengthsToString(keyParts)
	if prefixLengths == "" {
		return indexParams, nil
	}

	params := make(map[string]string)
	if indexParams != "" {
		existing, err := IndexParamsStringToMap(indexParams)
		if err != nil {
			return "", err
		}
		params = existing
	}
	params[IndexAlgoParamPrefixLengths] = prefixLengths
	return IndexParamsMapToJsonString(params)
}

func IndexPrefixLengthsToString(keyParts []*tree.KeyPart) string {
	if len(keyParts) == 0 {
		return ""
	}

	prefixLengths := make(map[string]int, len(keyParts))
	for _, keyPart := range keyParts {
		if keyPart == nil || keyPart.ColName == nil || keyPart.Length <= 0 {
			continue
		}
		prefixLengths[keyPart.ColName.ColName()] = keyPart.Length
	}
	if len(prefixLengths) == 0 {
		return ""
	}

	parts := make([]string, 0, len(prefixLengths))
	for part := range prefixLengths {
		parts = append(parts, part)
	}
	sort.Strings(parts)

	encoded := make([]string, 0, len(parts))
	for _, part := range parts {
		encoded = append(encoded, fmt.Sprintf("%s:%d", part, prefixLengths[part]))
	}
	return strings.Join(encoded, ",")
}

func IndexPrefixLengthsFromParams(indexParams string) map[string]int {
	prefixLengths, err := IndexPrefixLengthsFromParamsWithError(indexParams)
	if err != nil {
		return nil
	}
	return prefixLengths
}

func IndexPrefixLengthsFromParamsWithError(indexParams string) (map[string]int, error) {
	if indexParams == "" {
		return nil, nil
	}
	params, err := IndexParamsStringToMap(indexParams)
	if err != nil {
		return nil, err
	}

	encoded := params[IndexAlgoParamPrefixLengths]
	if encoded == "" {
		return nil, nil
	}

	prefixLengths := make(map[string]int)
	for _, item := range strings.Split(encoded, ",") {
		part, lengthText, ok := strings.Cut(item, ":")
		if !ok || part == "" || lengthText == "" {
			return nil, moerr.NewInvalidInputNoCtxf("invalid index prefix length item %q", item)
		}
		length, err := strconv.Atoi(lengthText)
		if err != nil || length <= 0 {
			return nil, moerr.NewInvalidInputNoCtxf("invalid index prefix length %q", item)
		}
		prefixLengths[part] = length
	}
	return prefixLengths, nil
}

/* 2. ToMap Functions */

// IndexParamSessionVars is the reserved algo_params key whose value is a
// nested, typed sqlexec.Metadata object ({"cfg":{...}}) carrying the build-time
// session variables captured at CREATE INDEX (e.g. kmeans_train_percent). It is
// NOT a flat string param: IndexParamsStringToMap skips it (so flat consumers
// are unaffected), and it is read back via IndexParamsSessionVars.
const IndexParamSessionVars = "session_vars"

// IndexParamsStringToMap used by buildShowCreateTable and restoreDDL.
// The reserved IndexParamSessionVars key (a nested typed object) is skipped so
// flat-string consumers stay unchanged; read it via IndexParamsSessionVars.
func IndexParamsStringToMap(indexParams string) (map[string]string, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(indexParams), &raw); err != nil {
		return nil, err
	}
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		if k == IndexParamSessionVars {
			continue // nested typed object — see IndexParamsSessionVars
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, err
		}
		result[k] = s
	}
	return result, nil
}

// IndexParamsSessionVars extracts the nested session_vars object (the
// sqlexec.Metadata JSON, {"cfg":{...}}) from an algo_params string, or nil if
// absent. Pass the result to sqlexec.NewMetadata to resolve typed values.
func IndexParamsSessionVars(indexParams string) (json.RawMessage, error) {
	if len(indexParams) == 0 {
		return nil, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(indexParams), &raw); err != nil {
		return nil, err
	}
	return raw[IndexParamSessionVars], nil
}

// IndexParamsMapToJsonStringWithSessionVars marshals the flat params plus the
// nested session_vars object. A nil/empty sessionVars behaves exactly like
// IndexParamsMapToJsonString (no session_vars key), preserving the old format.
func IndexParamsMapToJsonStringWithSessionVars(res map[string]string, sessionVars json.RawMessage) (string, error) {
	if len(sessionVars) == 0 {
		return IndexParamsMapToJsonString(res)
	}
	obj := make(map[string]json.RawMessage, len(res)+1)
	for k, v := range res {
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		obj[k] = b
	}
	obj[IndexParamSessionVars] = sessionVars
	str, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func indexParamsToMap(def interface{}) (map[string]string, error) {
	res := make(map[string]string)

	if idx, ok := def.(*tree.Index); ok {

		switch idx.KeyType {
		case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID, tree.INDEX_TYPE_RTREE:
			// do nothing
		case tree.INDEX_TYPE_MASTER:
		// do nothing
		default:
			// Vector algorithms (IVFFLAT / HNSW / CAGRA / IVFPQ) build their
			// algo_params via the per-plugin plan hook BuildIndexParams; they
			// are dispatched in pkg/sql/plan and never reach this function.
			return nil, moerr.NewInternalErrorNoCtx("invalid index alogorithm type")
		}

		return res, nil
	}
	return res, moerr.NewInternalErrorNoCtx("indexParamsToMap: invalid index type")
}

func DefaultIvfIndexAlgoOptions() map[string]string {
	res := make(map[string]string)
	res[IndexAlgoParamLists] = "1"                       // set lists = 1 as default
	res[IndexAlgoParamOpType] = metric.OpType_L2Distance // set l2 as default
	return res
}

func IsIndexAsync(indexAlgoParams string) (bool, error) {
	if len(indexAlgoParams) > 0 {
		val, err := sonic.Get([]byte(indexAlgoParams), Async)
		if err != nil {
			// key not exist
			return false, nil
		}

		async, err := val.StrictString()
		if err != nil {
			return false, err
		}

		return async == "true", nil
	}
	return false, nil
}

//------------------------[END] IndexAlgoParams------------------------

// ------------------------[START] Aliaser------------------------

// This code is used by "secondary index" to resolve the "programmatically generated PK" appended to the
// end of the index key "__mo_index_idx_col".

const (
	AliasPrefix = "__mo_alias_"
)

func CreateAlias(column string) string {
	return fmt.Sprintf("%s%s", AliasPrefix, column)
}

func ResolveAlias(alias string) string {
	return strings.TrimPrefix(alias, AliasPrefix)
}

func IsAlias(column string) bool {
	return strings.HasPrefix(column, AliasPrefix)
}

// ------------------------[END] Aliaser------------------------
