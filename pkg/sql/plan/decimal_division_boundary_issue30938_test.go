// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
)

// This file pins the divide arm's original-operand-domain recovery at the
// storage boundary (issue #30938, design section 4.1's divide bullet,
// revision 5 F-CAPPED-CONSUMERS; decision log 74 (iii), invariant I2).
// decimalDivisionReturnType (pkg/sql/plan/function/decimal_division.go:109)
// derives result metadata from the ORIGINAL operand types through
// exactDivisionOperandType - precision leftPrecision + rightScale +
// div_precision_increment, scale leftScale + div_precision_increment - but
// the binder's mixed decimal/integer division overloads insert <=65
// alignment casts (cast(p -> decimal(38,2)), cast(i -> decimal(38,0))), and
// an integer literal's alignment cast folds away entirely before the
// storage-envelope mirror runs. An envelope read over the post-cast
// arguments therefore derives decimal(42,6)/decimal(44,4) where the parent
// publishes decimal(12,6)/decimal(16,4)/decimal(25,4): the cast's declared
// target describes storage, never the operand's SQL precision. The mirror
// recovers each operand's original domain through the generation's decision
// ledger (integer sources contribute integerDecimalDigits verbatim, decimal
// sources their reconstructed envelope) and declines a bare folded literal,
// whose original domain the fold made unrecoverable, so every CTAS and view
// record persists the parent's own publication. All expected tags below are
// derived from decimalDivisionReturnType over the original operand types
// with the default div_precision_increment 4, never from observed candidate
// behavior.

// TestDecimalDivisionStorageBoundaryOperandDomainIssue30938 covers the
// divide arm across the operand families that reach it: mixed
// decimal/integer column pairs whose alignment casts the ledger verifies
// (p/i, i/p), a folded integer literal (2/p, c/2), a no-cast decimal pair
// (p/q), a direct decimal literal and an authored cast (p/2.00, p/cast(2 as
// decimal(12,2))), and the decimal256 route (e/2, e/p).
func TestDecimalDivisionStorageBoundaryOperandDomainIssue30938(t *testing.T) {
	tables := []string{
		"create table m30938dd(p decimal(8,2), q decimal(10,3))",
		"create table m30938i(c decimal(8,2), mi mediumint, i int, bt bit)",
		"create table m30938w50(e decimal(50,3))",
	}
	for _, tc := range []struct {
		from  string
		expr  string
		id    types.T
		width int32
		scale int32
	}{
		// No alignment casts on a decimal pair: 8 + 3 + 4 = 15, 2 + 4 = 6.
		{"m30938dd", "p / q", types.T_decimal128, 15, 6},
		// The ledger-verified look-through recovers the column's (8,2) and
		// int32's 10 digits: 8 + 0 + 4 = 12, 2 + 4 = 6 - never the cast
		// targets' (42,6).
		{"m30938dd, m30938i", "p / i", types.T_decimal128, 12, 6},
		// 10 + 2 + 4 = 16, 0 + 4 = 4 - never (44,4).
		{"m30938dd, m30938i", "i / p", types.T_decimal128, 16, 4},
		// The literal's alignment cast folds away; the parent refines the
		// int64 literal to 19 digits: 19 + 2 + 4 = 25, 0 + 4 = 4.
		{"m30938dd", "2 / p", types.T_decimal128, 25, 4},
		// 8 + 0 + 4 = 12, 2 + 4 = 6 - never (42,6).
		{"m30938i", "c / 2", types.T_decimal128, 12, 6},
		// A direct decimal literal: 8 + 2 + 4 = 14, 2 + 4 = 6.
		{"m30938dd", "p / 2.00", types.T_decimal128, 14, 6},
		// An authored cast keeps its declared domain: 8 + 2 + 4 = 14.
		{"m30938dd", "p / cast(2 as decimal(12,2))", types.T_decimal128, 14, 6},
		// The decimal256 route: 50 + 0 + 4 = 54, 3 + 4 = 7.
		{"m30938w50", "e / 2", types.T_decimal256, 54, 7},
		// 50 + 2 + 4 = 56, 3 + 4 = 7.
		{"m30938w50, m30938dd", "e / p", types.T_decimal256, 56, 7},
	} {
		ctas := buildModBoundarySelectCTAS(t, tables,
			"create table m30938dv_out as select "+tc.expr+" as v from "+tc.from)
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			tc.id, tc.width, tc.scale,
			"CTAS persists the parent's tag for "+tc.expr)
		view := buildModBoundarySelectCTAS(t, tables,
			"create view m30938dv_out as select "+tc.expr+" as v from "+tc.from)
		requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
			tc.id, tc.width, tc.scale,
			"view persists the parent's tag for "+tc.expr)
	}
}
