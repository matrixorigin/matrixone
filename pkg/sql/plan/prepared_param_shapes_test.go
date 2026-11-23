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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// Prepared parameter values in the shapes the frontend produces. A numeric or
// Boolean parameter is a native value: SQL EXECUTE ... USING carries the user
// variable's type as SourceType, COM_STMT_EXECUTE carries the protocol type as
// RuntimeType.

func sqlParam(v any, kind types.StringConversionKind, typ types.T) ParamValue {
	return ParamValue{Value: v, PrepareParamKind: kind, SourceType: typ.ToType(), HasSourceType: true}
}

func sqlIntParam(v int64) ParamValue {
	return sqlParam(v, types.StringConversionInteger, types.T_int64)
}

func sqlUintParam(v uint64) ParamValue {
	return sqlParam(v, types.StringConversionInteger, types.T_uint64)
}

func sqlFloatParam(v float64) ParamValue {
	return sqlParam(v, types.StringConversionFloat, types.T_float64)
}

func sqlBoolParam(v bool) ParamValue {
	return sqlParam(v, types.StringConversionBoolean, types.T_bool)
}

func comStmtIntParam(v int64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true, PrepareParamKind: types.StringConversionInteger,
		RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}
}

func comStmtUintParam(v uint64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true, PrepareParamKind: types.StringConversionInteger,
		RuntimeType: types.T_uint64.ToType(), HasRuntimeType: true}
}

func comStmtFloatParam(v float64) ParamValue {
	return ParamValue{Value: v, IsBinaryProtocol: true, PrepareParamKind: types.StringConversionFloat,
		RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}
}
