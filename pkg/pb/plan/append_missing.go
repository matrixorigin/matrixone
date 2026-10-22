// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"math"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const AppendMissingFormatV1 = 1

// The stable Type.id wire value for BOOLEAN. container/types imports plan;
// keep this boundary independent of that package and check the value externally.
const AppendMissingBoolOID int32 = 10

func CloneAppendMissingState(state *AppendMissingState) *AppendMissingState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*AppendMissingState)
}

// ValidateAppendMissingState never derives a birth value from the mutable SQL
// Default. A present but unknown state cannot be treated as legacy NULL fill.
func ValidateAppendMissingState(state *AppendMissingState) error {
	if state == nil {
		return nil
	}
	invalid := func(reason string) error {
		return moerr.NewInternalErrorNoCtxf("append missing state: %s", reason)
	}
	if state.FormatVersion != AppendMissingFormatV1 || len(state.XXX_unrecognized) != 0 {
		return invalid("unsupported format version")
	}
	if len(state.LayoutId) != 16 || bytes.Equal(state.LayoutId, make([]byte, 16)) {
		return invalid("invalid layout identity")
	}
	// The final five uint16 values are reserved for physical system columns.
	const sequenceUpper = math.MaxUint16 - 5
	if state.BaseWidth == 0 || state.BaseWidth >= sequenceUpper || state.PkSeqnum != state.BaseWidth-1 ||
		len(state.Columns) == 0 || uint64(state.BaseWidth)+uint64(len(state.Columns)) > sequenceUpper {
		return invalid("invalid append boundary")
	}
	for i, col := range state.Columns {
		if col == nil || col.Seqnum != state.BaseWidth+uint32(i) || col.BirthAppendOrdinal != uint32(i)+1 || len(col.XXX_unrecognized) != 0 {
			return invalid("incomplete or reordered column identities")
		}
		if col.Type.Id <= 0 || col.Type.Id > math.MaxUint8 || col.Type.NotNullable || col.Type.AutoIncr || len(col.Type.XXX_unrecognized) != 0 {
			return invalid("unsupported nullable column type")
		}
		switch col.Kind {
		case MissingFillKind_MISSING_FILL_NULL_V1:
		case MissingFillKind_MISSING_FILL_BOOL_FALSE_V1:
			if col.Type.Id != AppendMissingBoolOID || col.Type.Width != 0 || col.Type.Scale != 0 {
				return invalid("false fill requires BOOLEAN")
			}
		default:
			return invalid("unknown fill kind")
		}
	}
	return nil
}

// BirthFillTypeEqual compares physical type identity, not expression provenance
// such as the source table name or a query's collation coercibility.
func BirthFillTypeEqual(a, b Type) bool {
	return a.Id == b.Id && a.Width == b.Width && a.Scale == b.Scale &&
		a.Enumvalues == b.Enumvalues && a.Charset == b.Charset && a.CollationVersion == b.CollationVersion &&
		a.NotNullable == b.NotNullable && a.AutoIncr == b.AutoIncr
}

func ValidateAppendMissingTable(table *TableDef) error {
	if table == nil || table.AppendMissingState == nil {
		return nil
	}
	state := table.AppendMissingState
	if err := ValidateAppendMissingState(state); err != nil {
		return err
	}
	next := state.BaseWidth + uint32(len(state.Columns))
	if table.Version != uint32(len(state.Columns)) || uint32(len(table.Cols)) != next+1 || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		table.Pkey.CompPkeyCol.Seqnum != state.PkSeqnum {
		return moerr.NewInternalErrorNoCtx("append missing state: table shape differs")
	}
	rowid := table.Cols[next]
	if rowid == nil || rowid.Name != "__mo_rowid" || !rowid.Hidden || rowid.Seqnum != next || rowid.ColId != uint64(next)+1 {
		return moerr.NewInternalErrorNoCtx("append missing state: trailing rowid identity differs")
	}
	for i, col := range table.Cols[:next] {
		if col == nil || col.Seqnum != uint32(i) || col.ColId != uint64(i)+1 {
			return moerr.NewInternalErrorNoCtx("append missing state: table identity differs")
		}
	}
	for _, fill := range state.Columns {
		col := table.Cols[fill.Seqnum]
		if !BirthFillTypeEqual(fill.Type, col.Typ) || col.Hidden || col.Primary || col.Default == nil || !col.Default.NullAbility {
			return moerr.NewInternalErrorNoCtx("append missing state: column type differs")
		}
	}
	return nil
}

// IsLiteralFalseDefault recognizes only the restricted bound BOOLEAN literal.
// A folded expression, integer zero or current mutable default alone cannot
// create a birth descriptor; caller admission still validates the source AST.
func IsLiteralFalseDefault(col *ColDef) bool {
	if col == nil || col.Typ.Id != AppendMissingBoolOID || col.Typ.NotNullable || col.Default == nil ||
		!col.Default.NullAbility || col.Default.Expr == nil || col.Default.Expr.Typ.Id != AppendMissingBoolOID ||
		!strings.EqualFold(strings.TrimSpace(col.Default.OriginString), "false") {
		return false
	}
	lit := col.Default.Expr.GetLit()
	if lit == nil || lit.Isnull {
		return false
	}
	value, ok := lit.Value.(*Literal_Bval)
	return ok && !value.Bval
}
