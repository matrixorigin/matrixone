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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Prepared parameter values in the shapes the frontend produces. A numeric or
// Boolean parameter is a native value: SQL EXECUTE ... USING carries the user
// variable's type as SourceType, COM_STMT_EXECUTE carries the protocol type as
// RuntimeType.

func sqlParam(v any, typ types.T) ParamValue {
	return ParamValue{Value: v, SourceType: typ.ToType(), HasSourceType: true}
}

// sqlKindParam is a SQL EXECUTE parameter whose user variable has the type of
// the given conversion kind; a String kind is an untyped text value. A DECIMAL
// text value gets the type of its spelling.
func sqlKindParam(v any, kind types.StringConversionKind) ParamValue {
	switch kind {
	case types.StringConversionInteger:
		return sqlParam(v, types.T_int64)
	case types.StringConversionFloat:
		return sqlParam(v, types.T_float64)
	case types.StringConversionBoolean:
		return sqlParam(v, types.T_bool)
	case types.StringConversionDecimal:
		text, _ := v.(string)
		return ParamValue{Value: v, SourceType: decimalParamType(text), HasSourceType: true}
	}
	return ParamValue{Value: v}
}

// decimalParamType is the DECIMAL type of a value's spelling, as a DECIMAL
// parameter carries it.
func decimalParamType(text string) types.Type {
	if _, visible, _, ok := PreparedDecimalRuntimeDomains(text); ok {
		return visible
	}
	return types.New(types.T_decimal128, 38, 0)
}

func sqlIntParam(v int64) ParamValue {
	return sqlParam(v, types.T_int64)
}

func sqlUintParam(v uint64) ParamValue {
	return sqlParam(v, types.T_uint64)
}

func sqlFloatParam(v float64) ParamValue {
	return sqlParam(v, types.T_float64)
}

func sqlBoolParam(v bool) ParamValue {
	return sqlParam(v, types.T_bool)
}

func comStmtIntParam(v int64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true,
		RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}
}

func comStmtUintParam(v uint64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true,
		RuntimeType: types.T_uint64.ToType(), HasRuntimeType: true}
}

func comStmtFloatParam(v float64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true,
		RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}
}

func TestIsNonNegativePreparedIntegerRejectsProtocolText(t *testing.T) {
	require.True(t, isNonNegativePreparedInteger(comStmtIntParam(2)))
	require.True(t, isNonNegativePreparedInteger(sqlIntParam(0)))
	require.False(t, isNonNegativePreparedInteger(comStmtIntParam(-1)))
	// The integer-text form ODBC sends for LIMIT is not a window offset.
	require.False(t, isNonNegativePreparedInteger(ParamValue{Value: "2", IsBinaryProtocol: true}))
	require.False(t, isNonNegativePreparedInteger(ParamValue{IsBinaryProtocol: true}))
}
