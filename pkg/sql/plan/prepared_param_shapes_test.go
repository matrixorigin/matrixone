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
	"strings"
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

// A COM_STMT Boolean or float is validated as the text the wire carried; a
// SQL EXECUTE value of the same Go type keeps its own validation.
func TestPreparedProtocolTextValue(t *testing.T) {
	boolParam := ParamValue{Value: true, RuntimeType: types.T_bool.ToType(), HasRuntimeType: true}
	floatParam := comStmtFloatParam(1)
	for _, tc := range []struct {
		name  string
		param ParamValue
		want  any
	}{
		{"COM_STMT TINY 1", func() ParamValue { p := boolParam; p.IsBinaryProtocol = true; return p }(), "1"},
		{"COM_STMT DOUBLE 1.0", floatParam, "1"},
		{"COM_STMT DOUBLE 1.5", comStmtFloatParam(1.5), "1.5"},
		{"COM_STMT integer", comStmtIntParam(7), int64(7)},
		{"SQL Boolean", boolParam, true},
		{"SQL float", sqlFloatParam(1), float64(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, preparedProtocolTextValue(tc.param))
			require.Equal(t, tc.want, preparedProtocolTextValue(&tc.param))
		})
	}
	require.Equal(t, "raw", preparedProtocolTextValue("raw"))

	srid, _, err := geometrySRIDRuntimeValue(preparedProtocolTextValue(floatParam))
	require.NoError(t, err)
	require.Equal(t, uint32(1), srid)
	_, _, err = geometrySRIDRuntimeValue(preparedProtocolTextValue(comStmtFloatParam(1.5)))
	require.Error(t, err, "a fractional SRID is still rejected")
	_, _, err = geometrySRIDRuntimeValue(preparedProtocolTextValue(sqlFloatParam(1)))
	require.Error(t, err, "SQL EXECUTE keeps rejecting a float SRID")

	// A COM_STMT DECIMAL carries its canonical lexeme; without a visible
	// fraction it is read as the integer the wire spelled.
	for _, tc := range []struct {
		wire string
		want any
	}{
		{"10", "10"},
		{"11", "11"},
		{"0", "0"},
		{"-20", "-20"},
		{"4294967295", "4294967295"},
		{"10.0", "1e1"},
		{"1.5", "15e-1"},
		{"1" + strings.Repeat("0", 30), "1e30"},
	} {
		normalized, visible, canonical, ok := PreparedDecimalRuntimeDomains(tc.wire)
		require.True(t, ok, tc.wire)
		param := ParamValue{
			Value: canonical, IsBinaryProtocol: true,
			RuntimeType: normalized, HasRuntimeType: true,
			DirectResultType: visible, HasDirectResultType: true,
		}
		require.Equal(t, tc.want, preparedProtocolTextValue(param), tc.wire)
	}
}

func TestPreparedDecimalIntegerSpelling(t *testing.T) {
	for in, want := range map[string]string{
		"1e1":   "10",
		"-25e2": "-2500",
		"7":     "7",
		"1e19":  "1" + strings.Repeat("0", 19),
		"1e20":  "1e20",
		"15e-1": "15e-1",
		"1ex":   "1ex",
	} {
		require.Equal(t, want, preparedDecimalIntegerSpelling(in), in)
	}
}
