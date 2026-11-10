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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// These shapes reach the existing wrapper-based comparison loops. Mixed types,
// prepared JSON routing and native/raw-slice branches retain the flat boundary.
func dictionaryComparisonInputs(parameters []*vector.Vector) bool {
	if len(parameters) != 2 || parameters[0] == nil || parameters[1] == nil {
		return false
	}
	for _, v := range parameters {
		if v.IsPreparedJSONComparisonParam() || v.HasGrouping() {
			return false
		}
	}
	// The CHAR branch reads both operands through string wrappers. A bound
	// CHAR literal can retain VARCHAR physical storage; comparison semantics
	// still belong to the first CHAR operand, as on the ordinary flat path.
	if parameters[0].GetType().Oid.IsMySQLString() && parameters[1].GetType().Oid.IsMySQLString() {
		return true
	}
	if parameters[0].GetType().Oid != parameters[1].GetType().Oid {
		return false
	}
	switch parameters[0].GetType().Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return true
	default:
		return false
	}
}

// String probes use logical parameter reads. Keep list initialization on its
// existing flat representation and preserve its independent tuple row domain.
func dictionaryInInputs(parameters []*vector.Vector) bool {
	if len(parameters) != 2 || parameters[0] == nil || parameters[1] == nil || parameters[1].IsDictionary() {
		return false
	}
	oid := parameters[0].GetType().Oid
	if (oid.IsSignedInt() || oid.IsUnsignedInt()) && oid == parameters[1].GetType().Oid {
		return !parameters[0].HasGrouping() && !parameters[1].HasGrouping() && !parameters[0].IsPreparedJSONComparisonParam() && !parameters[1].IsPreparedJSONComparisonParam()
	}
	for _, v := range parameters {
		if !v.GetType().Oid.IsMySQLString() || v.IsPreparedJSONComparisonParam() || v.HasGrouping() {
			return false
		}
	}
	return true
}

// Same-domain integer arithmetic already uses scalar wrapper reads and keeps
// its existing overflow/diagnostic policy. Other arithmetic retains explicit
// normalization, including the flat operand boundary for fused decimal kernels.
func dictionaryArithmeticInputs(parameters []*vector.Vector) bool {
	if !dictionaryComparisonInputs(parameters) {
		return false
	}
	oid := parameters[0].GetType().Oid
	return oid.IsSignedInt() || oid.IsUnsignedInt() || oid == types.T_float32 || oid == types.T_float64
}

// Pure string kernels use logical wrapper reads; bit/JSON/array overloads retain
// their existing physical normalization contract.
func dictionaryUnaryStringInputs(parameters []*vector.Vector) bool {
	return len(parameters) == 1 && parameters[0] != nil && parameters[0].GetType().Oid.IsMySQLString() && !parameters[0].HasGrouping() && !parameters[0].IsPreparedJSONComparisonParam()
}

func dictionaryBetweenInputs(parameters []*vector.Vector) bool {
	if len(parameters) != 3 || parameters[0] == nil || parameters[1] == nil || parameters[2] == nil || !parameters[1].IsConst() || !parameters[2].IsConst() {
		return false
	}
	if !dictionaryComparisonInputs(parameters[:2]) || !dictionaryComparisonInputs([]*vector.Vector{parameters[0], parameters[2]}) {
		return false
	}
	return parameters[0].GetType().Oid != types.T_bool
}

func dictionaryStringArguments(parameters []*vector.Vector) bool {
	if len(parameters) < 2 || len(parameters) > 3 {
		return false
	}
	for _, v := range parameters {
		if v == nil || !v.GetType().Oid.IsMySQLString() || v.HasGrouping() || v.IsPreparedJSONComparisonParam() {
			return false
		}
	}
	return true
}

func dictionarySubstringInputs(parameters []*vector.Vector) bool {
	if len(parameters) < 2 || len(parameters) > 3 || !dictionaryUnaryStringInputs(parameters[:1]) {
		return false
	}
	for _, v := range parameters[1:] {
		if v == nil || v.GetType().Oid != types.T_int64 || v.HasGrouping() || v.IsPreparedJSONComparisonParam() {
			return false
		}
	}
	return true
}

func dictionaryNullPredicateInputs(parameters []*vector.Vector) bool {
	return len(parameters) == 1 && parameters[0] != nil
}
