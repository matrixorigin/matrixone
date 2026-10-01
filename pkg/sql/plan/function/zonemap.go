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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ZoneMapEvaluation identifies the existing owner of a metadata proof. A family
// flag alone never authorizes executing a row kernel on paired endpoints.
type ZoneMapEvaluation uint8

const (
	ZoneMapUnsupported ZoneMapEvaluation = iota
	ZoneMapConstant
	ZoneMapIndex
	ZoneMapEndpoints
	ZoneMapTemporal
)

// GetZoneMapEvaluation closes admission over resolved identity, physical types
// and fixed controls. Consumers still own argument availability and publication.
func GetZoneMapEvaluation(fn *plan.Function) ZoneMapEvaluation {
	if fn == nil || fn.Func == nil {
		return ZoneMapUnsupported
	}
	id := fn.Func.GetObj()
	f, ok := GetFunctionByIdWithoutError(id)
	if !ok || f.CannotFold() {
		return ZoneMapUnsupported
	}
	args := fn.Args
	constant := true
	for _, arg := range args {
		if arg == nil {
			return ZoneMapUnsupported
		}
		// Parameters are a binding promise during planning. Execution still
		// requires the current binding/materialized payload before publication.
		constant = constant && IsConstant(arg, true)
	}
	if !CanUseZoneMapComparison(id, args) {
		return ZoneMapUnsupported
	}
	if constant {
		fid, _ := DecodeOverloadID(id)
		if fid != CASE {
			return ZoneMapConstant
		}
	}
	fixed := func(i int) bool { return i < len(args) && IsConstant(args[i], true) }
	oid := func(i int) types.T { return types.T(args[i].Typ.Id) }
	calendar := func(i int) bool { return oid(i) == types.T_date || oid(i) == types.T_datetime }
	fid, _ := DecodeOverloadID(id)
	switch fid {
	case DIV, INTEGER_DIV, UNARY_PLUS, UNARY_MINUS, CEIL, FLOOR, ROUND, TRUNCATE,
		TS_TO_TIME, DATE, YEAR, DATE_SUB, FROM_UNIXTIME, DATE_TRUNC, CAST:
		// Dynamic overloads use checkFn rather than a static args signature.
		// Their supported physical domains and arities are closed below.
		if f.args != nil && len(f.args) != len(args) {
			return ZoneMapUnsupported
		}
		for i, expected := range f.args {
			if expected != types.T_any && oid(i) != expected {
				return ZoneMapUnsupported
			}
		}
	}
	switch fid {
	case EQUAL, NULL_SAFE_EQUAL, NOT_EQUAL, GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL:
		if len(args) == 2 {
			return ZoneMapIndex
		}
	case BETWEEN:
		if len(args) == 3 {
			return ZoneMapIndex
		}
	case AND, OR:
		if len(args) >= 2 {
			return ZoneMapIndex
		}
	case IN, NOT_IN, PREFIX_IN:
		if len(args) == 2 {
			return ZoneMapIndex
		}
	case ISNULL, ISNOTNULL:
		if len(args) == 1 && args[0].GetCol() != nil {
			return ZoneMapIndex
		}
	case PLUS, MINUS, MULTI:
		if len(args) == 2 && oid(0) == oid(1) && (!oid(0).IsDecimal() || args[0].Typ.Scale == args[1].Typ.Scale) {
			return ZoneMapIndex
		}
	case PREFIX_EQ:
		if len(args) == 2 && fixed(1) {
			return ZoneMapIndex
		}
	case PREFIX_BETWEEN:
		if len(args) == 3 && fixed(1) && fixed(2) {
			return ZoneMapIndex
		}
	case PREFIX_IN_RANGE, IN_RANGE:
		if len(args) == 4 && fixed(1) && fixed(2) && fixed(3) && oid(3) == types.T_uint8 {
			return ZoneMapIndex
		}
	case DIV, INTEGER_DIV:
		if len(args) == 2 && fixed(1) && oid(0) == oid(1) {
			switch oid(0) {
			case types.T_int64, types.T_uint64, types.T_float64:
				return ZoneMapEndpoints
			}
		}
	case UNARY_PLUS:
		if len(args) == 1 {
			return ZoneMapEndpoints
		}
	case UNARY_MINUS:
		if len(args) == 1 {
			switch oid(0) {
			case types.T_int64, types.T_float32, types.T_float64:
				return ZoneMapEndpoints
			}
		}
	case CEIL, FLOOR, ROUND, TRUNCATE:
		if len(args) == 1 || len(args) == 2 && fixed(1) {
			switch oid(0) {
			case types.T_int64, types.T_uint64, types.T_float64:
				return ZoneMapEndpoints
			case types.T_decimal64, types.T_decimal128:
				return ZoneMapEndpoints
			}
		}
	case TS_TO_TIME:
		if len(args) == 2 && oid(0) == types.T_TS && fixed(1) {
			return ZoneMapEndpoints
		}
	case DATE, YEAR:
		if len(args) == 1 && calendar(0) {
			return ZoneMapEndpoints
		}
	case DATE_SUB:
		if len(args) == 3 && fixed(1) && fixed(2) {
			if calendar(0) {
				return ZoneMapEndpoints
			}
			if oid(0) == types.T_timestamp {
				return ZoneMapTemporal
			}
		}
	case FROM_UNIXTIME:
		if len(args) == 1 {
			switch oid(0) {
			case types.T_int64, types.T_uint64, types.T_float64:
				return ZoneMapTemporal
			}
		}
	case DATE_TRUNC:
		if len(args) == 2 && fixed(0) {
			if calendar(1) {
				return ZoneMapEndpoints
			}
			if oid(1) == types.T_timestamp {
				return ZoneMapTemporal
			}
		}
	case CAST:
		if len(args) == 2 && args[1].GetT() != nil && oid(1) == types.T_timestamp {
			if calendar(0) {
				// Native temporal compilation owns the target type and the TZ
				// interval proof. Generic consumers can still refuse this shape.
				return ZoneMapTemporal
			}
		}
	case PI, CURRENT_TIMESTAMP, LOCALTIME:
		// Only statement-constant shapes above have an evaluation owner.
	}
	return ZoneMapUnsupported
}
