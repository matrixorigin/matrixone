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

// refreshExportSetParamPositions retains source types across automatic plan
// rebuilds, but drops positions that no longer belong to EXPORT_SET. An explicit
// PREPARE creates a different statement owner with no inherited type history.
func (stmt *PrepareStmt) refreshExportSetParamPositions(preparePlan *plan2.Plan, paramCount int) {
	stmt.exportSetParamPositions = plan2.PreparedPlanExportSetParamPositions(preparePlan)
	previous := stmt.exportSetParamTypes
	stmt.exportSetParamTypes = nil
	if len(previous) != paramCount || len(previous) == 0 || len(stmt.exportSetParamPositions) == 0 {
		return
	}
	stmt.exportSetParamTypes = make([]types.Type, paramCount)
	for _, pos := range stmt.exportSetParamPositions {
		if pos >= 0 && int(pos) < paramCount {
			stmt.exportSetParamTypes[pos] = previous[pos]
		}
	}
}

func (stmt *PrepareStmt) hasExportSetNumericHistory() bool {
	for _, typ := range stmt.exportSetParamTypes {
		if typ.Oid != types.T_any {
			return true
		}
	}
	return false
}

// applyExportSetNullRuntimeTypes runs after parameter decoding on the owning
// session's serial execution path. A NULL never establishes a numeric domain,
// but a later NULL must not undo a domain established by a concrete value.
// The original NULL and protocol provenance are retained; only its type changes.
func (stmt *PrepareStmt) applyExportSetNullRuntimeTypes(values []any) {
	if len(stmt.exportSetParamPositions) == 0 {
		return
	}
	if len(stmt.exportSetParamTypes) != len(values) {
		stmt.exportSetParamTypes = make([]types.Type, len(values))
	}
	for _, pos := range stmt.exportSetParamPositions {
		if pos < 0 || int(pos) >= len(values) {
			continue
		}
		value, wrapped := values[pos].(plan2.ParamValue)
		isNull := values[pos] == nil || wrapped && value.Value == nil
		if isNull {
			previous := stmt.exportSetParamTypes[pos]
			if previous.Oid == types.T_any || value.HasRuntimeType {
				continue
			}
			value.RuntimeType = previous
			value.HasRuntimeType = true
			values[pos] = value
			continue
		}
		typ, numeric := plan2.PreparedParamValueNumericReprepareType(values[pos])
		if !numeric {
			// Keep existing nonnumeric execution semantics. A concrete text/binary
			// binding ends this numeric history; NULL itself never clears it.
			stmt.exportSetParamTypes[pos] = types.Type{}
			continue
		}
		if wrapped {
			// Unlike BIT_COUNT's fixed reprepare envelope, downstream COALESCE and
			// aggregate consumers need the actual precision and scale of this source.
			if value.HasRuntimeType && (value.RuntimeType.IsNumeric() || value.RuntimeType.Oid == types.T_bool || value.RuntimeType.Oid == types.T_bit) {
				typ = value.RuntimeType
			} else if value.HasSourceType && (value.SourceType.IsNumeric() || value.SourceType.Oid == types.T_bool || value.SourceType.Oid == types.T_bit) {
				typ = value.SourceType
			}
		}
		stmt.exportSetParamTypes[pos] = typ
	}
}
