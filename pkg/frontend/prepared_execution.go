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

package frontend

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Source domains and consumer hints are separate: BIT_COUNT's sticky numeric
// category must never change another occurrence's SQL source type.
func preparedExecutionBindings(ctx context.Context, values []any, protocolTypes []byte,
	bitCountTypes []types.Type) ([]plan2.PreparedSourceBinding, error) {
	bindings := make([]plan2.PreparedSourceBinding, len(values))
	for i, value := range values {
		binding := &bindings[i]
		binding.Position = int32(i)
		param, ok := value.(plan2.ParamValue)
		if !ok {
			binding.Type, _ = preparedConversionRuntimeType(value)
			continue
		}
		if param.IsBinaryProtocol {
			if i*2+1 >= len(protocolTypes) {
				return nil, moerr.NewInvalidInput(ctx, "prepared parameter has no protocol source type")
			}
			mysqlType := defines.MysqlType(protocolTypes[i*2])
			if param.Value == nil {
				if binaryProtocolPrepareParamIsBinaryString(mysqlType) {
					binding.Type = types.T_blob.ToType()
				}
			} else if param.PrepareParamKind == vector.PrepareParamBoolean {
				binding.Type = types.T_bool.ToType()
			} else if mysqlType == defines.MYSQL_TYPE_DECIMAL || mysqlType == defines.MYSQL_TYPE_NEWDECIMAL {
				binding.Type, binding.NumericType = param.DirectResultType, param.RuntimeType
				if !param.HasDirectResultType || !param.HasRuntimeType {
					return nil, invalidBinaryDecimalParameter(ctx, param.Value)
				}
			} else {
				oid, supported := binaryProtocolPrepareParamConcreteType(mysqlType, protocolTypes[i*2+1]&0x80 != 0)
				if !supported {
					return nil, moerr.NewInvalidInput(ctx, "unsupported prepared parameter source type")
				}
				binding.Type = oid.ToType()
				if binaryProtocolTemporalMysqlType(mysqlType) {
					binding.Type = binaryProtocolTemporalParamType(oid, fmt.Sprint(param.Value))
				}
			}
		} else if param.HasSourceType {
			binding.Type = param.SourceType
		} else if param.Value != nil {
			binding.Type, _ = preparedConversionRuntimeType(param.Value)
		}
		if i < len(bitCountTypes) {
			binding.BitCountType = bitCountTypes[i]
		}
	}
	return bindings, nil
}

// Only metadata observed by binding belongs to cache identity. Consumers that
// inspect spelling mark their plan value-dependent instead of extending this
// key with value hashes or parser-specific categories.
func preparedExecutionBindingKey(bindings []plan2.PreparedSourceBinding, values []any) string {
	var key strings.Builder
	writeType := func(typ types.Type) {
		fmt.Fprintf(&key, "%d:%d:%d:%d;", typ.Oid, typ.Charset, typ.Width, typ.Scale)
	}
	for i, binding := range bindings {
		fmt.Fprintf(&key, "%d;", binding.Position)
		writeType(binding.Type)
		writeType(binding.NumericType)
		writeType(binding.BitCountType)
		if param, ok := values[i].(plan2.ParamValue); ok {
			fmt.Fprintf(&key, "%d:%t:%t:%t:%d:%t:%t;", param.PrepareParamKind,
				param.IsBinaryProtocol, param.IsBin, param.IsBinaryString,
				param.RuntimeStringDomain, param.EnableNumericPrefix, param.Value == nil)
		}
	}
	return key.String()
}

func (prepareStmt *PrepareStmt) rememberBitCountSourceTypes(values []any) {
	if len(prepareStmt.bitCountOverloadParamPositions) == 0 {
		prepareStmt.bitCountNumericParamTypes = nil
		return
	}
	if len(prepareStmt.bitCountNumericParamTypes) != len(values) {
		prepareStmt.bitCountNumericParamTypes = make([]types.Type, len(values))
	}
	for _, pos := range prepareStmt.bitCountOverloadParamPositions {
		if typ, ok := plan2.PreparedParamValueNumericReprepareType(values[pos]); ok {
			prepareStmt.bitCountNumericParamTypes[pos] = typ
		}
	}
}

func buildPreparedBoundQuery(ctx context.Context, ses FeSession, compiler plan2.CompilerContext,
	stmt tree.Statement, bindings []plan2.PreparedSourceBinding, values []any) (*plan2.PreparedExecutionPlan, error) {
	previous := compiler.GetContext()
	planning := ctx
	if planning == nil {
		planning = previous
	}
	if planning == nil {
		planning = context.Background()
	}
	if ses != nil {
		planning = function.WithNoUnsignedSubtraction(planning, mysql.HasSQLMode(sessionSQLMode(ses), "NO_UNSIGNED_SUBTRACTION"))
		planning = function.WithDivPrecisionIncrement(planning, sessionDivPrecisionIncrement(ses))
	}
	sink, _ := ses.(plan2.JSONMergeWarningSink)
	planning = plan2.AttachJSONMergeWarningContext(planning, sink, plan2.JSONMergeWarningInternalReprepare)
	compiler.SetContext(planning)
	defer compiler.SetContext(previous)
	var bound *plan2.PreparedExecutionPlan
	_, err := buildPlanWithStats(planning, ses, compiler, func() (*plan2.Plan, error) {
		var buildErr error
		bound, buildErr = plan2.BuildPreparedExecutionPlan(compiler, stmt, bindings, values)
		if buildErr != nil {
			return nil, buildErr
		}
		return bound.Plan, nil
	})
	return bound, err
}

func (cwft *TxnComputationWrapper) preparedExecutionRetry() *preparedExecutionRetry {
	if len(cwft.paramVals) == 0 && !cwft.preparedJoinDiagnosticFree {
		return nil
	}
	return &preparedExecutionRetry{
		paramVals:          append([]any(nil), cwft.paramVals...),
		bindings:           append([]plan2.PreparedSourceBinding(nil), cwft.paramBindings...),
		diagnosticFreeJoin: cwft.preparedJoinDiagnosticFree,
	}
}
