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
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
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
			if param.Value == nil {
				// NULL bitmap entries need no source type. Connector/J can omit
				// the entire type vector on an initial all-NULL execution.
				if i*2+1 < len(protocolTypes) &&
					binaryProtocolPrepareParamIsBinaryString(defines.MysqlType(protocolTypes[i*2])) {
					binding.Type = types.T_blob.ToType()
				}
			} else {
				if i*2+1 >= len(protocolTypes) {
					return nil, moerr.NewInvalidInput(ctx, "prepared parameter has no protocol source type")
				}
				mysqlType := defines.MysqlType(protocolTypes[i*2])
				if param.PrepareParamKind == vector.PrepareParamBoolean {
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
	// Keep the existing framed identity without allocating formatting arguments.
	var key strings.Builder
	var number [20]byte
	writeInt := func(value int64, separator byte) {
		key.Write(strconv.AppendInt(number[:0], value, 10))
		key.WriteByte(separator)
	}
	writeBool := func(value bool, separator byte) {
		key.WriteString(strconv.FormatBool(value))
		key.WriteByte(separator)
	}
	writeType := func(typ types.Type) {
		writeInt(int64(typ.Oid), ':')
		writeInt(int64(typ.Charset), ':')
		writeInt(int64(typ.Width), ':')
		writeInt(int64(typ.Scale), ';')
	}
	for i, binding := range bindings {
		writeInt(int64(binding.Position), ';')
		writeType(binding.Type)
		writeType(binding.NumericType)
		writeType(binding.BitCountType)
		if param, ok := values[i].(plan2.ParamValue); ok {
			writeInt(int64(param.PrepareParamKind), ':')
			writeBool(param.IsBinaryProtocol, ':')
			writeBool(param.IsBin, ':')
			writeBool(param.IsBinaryString, ':')
			writeInt(int64(param.RuntimeStringDomain), ':')
			writeBool(param.EnableNumericPrefix, ':')
			writeBool(param.Value == nil, ';')
		}
	}
	return key.String()
}

func preparedFieldBindingKey(key string, state *plan2.PreparedFieldTypes) string {
	if state == nil {
		return key
	}
	return key + "field:" + strconv.FormatUint(state.Revision(), 10)
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
	return buildPreparedBoundQueryWithTemporalIntent(ctx, ses, compiler, stmt, bindings, values, nil)
}

func buildPreparedBoundQueryWithTemporalIntent(ctx context.Context, ses FeSession, compiler plan2.CompilerContext,
	stmt tree.Statement, bindings []plan2.PreparedSourceBinding, values []any, intent *plan2.TemporalPlanningIntent) (*plan2.PreparedExecutionPlan, error) {
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
		var selected bool
		bound, selected, buildErr = buildViewSchemaValidationPreparedPlan(planning, ses, compiler, stmt, bindings, values)
		if !selected {
			bound, buildErr = plan2.BuildPreparedExecutionPlanWithTemporalIntent(compiler, stmt, bindings, values, intent)
		}
		if buildErr != nil {
			return nil, buildErr
		}
		return bound.Plan, nil
	})
	if err != nil && bound != nil {
		bound.TemporalScope.Close()
		bound.TemporalScope = nil
	}
	return bound, err
}

func (cwft *TxnComputationWrapper) preparedExecutionRetry() *preparedExecutionRetry {
	if len(cwft.paramVals) == 0 && !cwft.preparedJoinDiagnosticFree {
		return nil
	}
	var preparedPlan *plan2.Plan
	var fieldTypes *plan2.PreparedFieldTypes
	var fieldSchemas []*plan2.ObjectRef
	if cwft.preparedStmt != nil {
		fieldTypes = cwft.preparedStmt.fieldTypes
		if fieldTypes != nil {
			// Keep immutable declaration facts, not another retained plan graph
			// or a later EXECUTE's mutable handle, as the retry generation proof.
			schemas := cwft.preparedStmt.PreparePlan.GetDcl().GetPrepare().GetSchemas()
			fieldSchemas = make([]*plan2.ObjectRef, len(schemas))
			for i, schema := range schemas {
				if schema != nil {
					fieldSchemas[i] = proto.Clone(schema).(*plan2.ObjectRef)
				}
			}
		}
	}
	if cwft.preparedStmt != nil && strings.IndexByte(cwft.preparedStmt.Sql, '@') >= 0 {
		preparedPlan = cwft.preparedStmt.PreparePlan.GetDcl().GetPrepare().Plan
	}
	var intent *plan2.TemporalPlanningIntent
	if cwft.temporalIntentValidated && cwft.temporalPlanningIntent != nil {
		copy := *cwft.temporalPlanningIntent
		intent = &copy
	}
	return &preparedExecutionRetry{
		fieldTypes:         fieldTypes,
		fieldSchemas:       fieldSchemas,
		paramVals:          append([]any(nil), cwft.paramVals...),
		bindings:           append([]plan2.PreparedSourceBinding(nil), cwft.paramBindings...),
		preparedPlan:       preparedPlan,
		diagnosticFreeJoin: cwft.preparedJoinDiagnosticFree,
		temporalIntent:     intent,
	}
}

// Keep inferred domains only for markers owned by integer +/* metadata. NULL
// retains the previous effective domain; a valid non-NULL source replaces it,
// including decimal/string transitions. This state belongs to the statement,
// and is committed before SQL evaluation (an arithmetic error does not undo it).
func (prepareStmt *PrepareStmt) rememberIntegerArithmeticBindings(p *plan2.Plan, bindings []plan2.PreparedSourceBinding, values []any) {
	defaults := plan2.IntegerPrecisionParameterBindings(p)
	if len(defaults) == 0 {
		return
	}
	if len(prepareStmt.integerArithmeticBindings) != len(bindings) {
		prepareStmt.integerArithmeticBindings = make([]plan2.PreparedSourceBinding, len(bindings))
	}
	for _, initial := range defaults {
		pos := int(initial.Position)
		if pos < 0 || pos >= len(bindings) || pos >= len(values) {
			continue
		}
		value := values[pos]
		if param, ok := value.(plan2.ParamValue); ok {
			value = param.Value
		}
		if value == nil {
			previous := prepareStmt.integerArithmeticBindings[pos]
			if previous.Type.Oid == types.T_any {
				previous = initial
			}
			bindings[pos] = previous
		} else {
			prepareStmt.integerArithmeticBindings[pos] = bindings[pos]
		}
	}
}
