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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// A reprepare resolves the original SQL again. COM_STMT_RESET only discards
// execution values and must not call this generation transition.
func (prepareStmt *PrepareStmt) refreshExportSetParamPositions(p *plan2.Plan, paramCount int) {
	positions, domains, bare := plan2.PreparedPlanExportSetParameters(p)
	prepareStmt.exportSetParamPositions = positions
	prepareStmt.exportSetBareParams = bare
	prepareStmt.exportSetParamDefaults = nil
	prepareStmt.exportSetParamTypes = nil
	if len(positions) == 0 {
		return
	}
	prepareStmt.exportSetParamDefaults = plan2.PreparedPlanNumericParameterDefaults(p)
	prepareStmt.exportSetParamTypes = make([]types.Type, paramCount)
	for pos, typ := range domains {
		if pos >= 0 && int(pos) < paramCount && prepareStmt.exportSetParamDefaults[pos].Oid == types.T_any {
			prepareStmt.exportSetParamTypes[pos] = typ
		}
	}
}

func (prepareStmt *PrepareStmt) hasExportSetNumericHistory() bool {
	p := prepareStmt.PreparePlan
	if dcl := p.GetDcl(); dcl != nil && dcl.GetPrepare() != nil {
		p = dcl.GetPrepare().Plan
	}
	_, initial, _ := plan2.PreparedPlanExportSetParameters(p)
	for pos, typ := range prepareStmt.exportSetParamTypes {
		if typ.Oid == types.T_any {
			continue
		}
		original := initial[int32(pos)]
		if typ == original || typ.IsDecimal() && original.IsDecimal() {
			continue
		}
		return true
	}
	return false
}

func exportSetNumericDomainRank(typ types.Type) int {
	if typ.Oid == types.T_float32 || typ.Oid == types.T_float64 {
		return 3
	}
	if typ.IsDecimal() {
		return 2
	}
	if typ.IsNumeric() || typ.Oid == types.T_bool || typ.Oid == types.T_bit || typ.Oid == types.T_year {
		return 1
	}
	return 0
}

// Binding types and resolved domains are distinct. Numeric bindings may widen
// an unresolved/integer/exact domain, but cannot narrow a resolved DOUBLE.
// Strings and NULL use the current domain rather than establish a new one.
func (prepareStmt *PrepareStmt) applyExportSetNullRuntimeTypes(values []any) {
	if len(prepareStmt.exportSetParamPositions) == 0 {
		return
	}
	if len(prepareStmt.exportSetParamTypes) != len(values) {
		prepareStmt.exportSetParamTypes = make([]types.Type, len(values))
	}
	for _, pos := range prepareStmt.exportSetParamPositions {
		if pos < 0 || int(pos) >= len(values) {
			continue
		}
		param, wrapped := values[pos].(plan2.ParamValue)
		if !wrapped {
			param.Value = values[pos]
		}
		domain := prepareStmt.exportSetParamTypes[pos]
		numericString := false
		actualNumeric := false
		if param.Value != nil {
			actual, numeric := plan2.PreparedParamValueNumericReprepareType(values[pos])
			actualNumeric = numeric
			_, textual := param.Value.(string)
			numericString = !numeric && textual && (exportSetNumericDomainRank(domain) > 0 ||
				exportSetNumericDomainRank(prepareStmt.exportSetParamDefaults[pos]) > 0)
			if numeric {
				if param.HasRuntimeType && exportSetNumericDomainRank(param.RuntimeType) > 0 {
					actual = param.RuntimeType
				} else if param.HasSourceType && exportSetNumericDomainRank(param.SourceType) > 0 {
					actual = param.SourceType
				}
				if exportSetNumericDomainRank(actual) >= exportSetNumericDomainRank(domain) {
					domain = actual
				} else if domain.IsDecimal() && exportSetNumericDomainRank(actual) == 1 {
					// Reusing DECIMAL is a category contract, not a demand to
					// squeeze later integers into the first value's precision.
					domain = types.New(types.T_decimal256, 65, 30)
				}
			}
		}
		prepareStmt.exportSetParamTypes[pos] = domain
		if domain.Oid == types.T_any {
			domain = prepareStmt.exportSetParamDefaults[pos]
		}
		if prepareStmt.exportSetBareParams[pos] {
			// A bare EXPORT_SET marker uses val_int on the actual value.
			// Composite/producer consumers instead evaluate in their resolved
			// domain. Only fully numeric strings convert to that domain here.
			if param.Value == nil || actualNumeric {
				continue
			}
			if text, ok := param.Value.(string); ok {
				if _, numeric := plan2.PreparedRuntimeTypeFromString(text); !numeric {
					domain = types.T_text.ToType()
					numericString = false
				}
			}
		}
		if domain.Oid == types.T_any {
			domain = types.T_text.ToType()
		}
		if numericString && domain.IsDecimal() {
			domain = plan2.PreparedNumericPrefixTypeFromString(param.Value.(string))
			domain.Charset = 255
		}
		param.ExportSetNumericString = numericString
		param.RuntimeType = domain
		param.HasRuntimeType = true
		param.ExportSetResolvedDomain = true
		values[pos] = param
	}
}
