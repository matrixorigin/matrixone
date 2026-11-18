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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// binaryIntervalFiveDays is the hand-derived microsecond magnitude of five
// days: 5 * 86400s * 10^6. It is the independent oracle for the interval
// binding route; nothing in this file derives it from the decoder.
const binaryIntervalFiveDays int64 = 5 * 86_400_000_000

// binaryLiteralCastExpr pins the bind shape normalizeDecimalIntervalValue
// walks for INTERVAL <IsBin literal>: a constant implicit cast whose source
// literal keeps the raw payload, the IsBin flag, and its literal form.
func binaryLiteralCastExpr(oid types.T, width, scale int32, payload string, form planpb.StringLiteralForm) *Expr {
	return &Expr{
		Typ: planpb.Type{Id: int32(oid), Width: width, Scale: scale},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "cast"},
			Args: []*planpb.Expr{{
				Typ: planpb.Type{Id: int32(types.T_varchar), Width: int32(len(payload)), Scale: 0},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value:       &planpb.Literal_Sval{Sval: payload},
					IsBin:       true,
					LiteralForm: form,
				}},
			}},
		}},
	}
}

// TestNormalizeDecimalIntervalValueBinaryLiteral walks the real interval
// binding route with IsBin hex/bit payloads: the decider is
// ParseDecimalXFromByte, so the interval magnitude is the integer the payload
// spells, at any declared destination scale (#31816). The equal non-IsBin
// text spelling keeps the identical magnitude, and a value whose scaled
// microsecond product cannot stay in int64 surfaces normalize's own
// out-of-range error.
func TestNormalizeDecimalIntervalValueBinaryLiteral(t *testing.T) {
	for _, tc := range []struct {
		name  string
		oid   types.T
		width int32
		scale int32
	}{
		{"decimal64 scale2", types.T_decimal64, 10, 2},
		{"decimal64 scale0", types.T_decimal64, 10, 0},
		{"decimal128 scale2", types.T_decimal128, 20, 2},
		{"decimal256 scale2", types.T_decimal256, 38, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := binaryLiteralCastExpr(tc.oid, tc.width, tc.scale, "\x05",
				planpb.StringLiteralForm_STRING_LITERAL_HEX)
			value, negative, handled, err := normalizeDecimalIntervalValue(expr, types.Day)
			require.NoError(t, err)
			require.True(t, handled)
			require.False(t, negative)
			require.Equal(t, binaryIntervalFiveDays, value,
				"x'05' spells the integer 5: exactly five days, whatever the destination scale")
		})
	}

	t.Run("bit spelling", func(t *testing.T) {
		expr := binaryLiteralCastExpr(types.T_decimal64, 10, 2, "\x05",
			planpb.StringLiteralForm_STRING_LITERAL_BIT)
		value, negative, handled, err := normalizeDecimalIntervalValue(expr, types.Day)
		require.NoError(t, err)
		require.True(t, handled)
		require.False(t, negative)
		require.Equal(t, binaryIntervalFiveDays, value, "b'101' spells the integer 5")
	})

	t.Run("non-IsBin text control", func(t *testing.T) {
		lit := &Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar), Width: 1, Scale: 0},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:       &planpb.Literal_Sval{Sval: "5"},
				LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_TEXT,
			}},
		}
		expr := &Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "cast"},
				Args: []*planpb.Expr{lit},
			}},
		}
		value, negative, handled, err := normalizeDecimalIntervalValue(expr, types.Day)
		require.NoError(t, err)
		require.True(t, handled)
		require.False(t, negative)
		require.Equal(t, binaryIntervalFiveDays, value,
			"the plain text spelling keeps the same five days")
	})

	t.Run("second magnitude out of range", func(t *testing.T) {
		// 2^53+1 = 9007199254740993: decodes cleanly at DECIMAL(18,0) but its
		// microsecond product cannot stay in int64.
		expr := binaryLiteralCastExpr(types.T_decimal64, 18, 0, "\x00\x20\x00\x00\x00\x00\x00\x01",
			planpb.StringLiteralForm_STRING_LITERAL_HEX)
		_, _, handled, err := normalizeDecimalIntervalValue(expr, types.Second)
		require.Error(t, err, "normalize's own int64 bound rejects the magnitude")
		require.False(t, handled)
	})
}

// TestIssue31816DecimalIntervalActualBinding walks today's public DATE_ADD
// binding route end to end for the #31816 consumer: SQL text -> parser ->
// binder -> constant fold, mirroring what the fixture asks over a real
// connection. Each HEX/BIT literal is routed through the explicit public
// CAST(.. AS DECIMAL(10,2)) before the DAY unit binds, so the real IsBin
// decoder and the calendar adjustment both participate in the plan; the
// ordinary integer and the spelled text decimal keep their own controls.
// The hand-built normalizeDecimalIntervalValue cases above remain the
// independent internal contract. The raw (uncast) "interval x'..' day"
// spellings are a separate, pre-existing frontend text-interval route
// (NULL in the round-18 raw evidence) and are NOT claimed fixed here.
func TestIssue31816DecimalIntervalActualBinding(t *testing.T) {
	expectedDays := "the interval literal must spell the integer 5 days so " +
		"2020-01-01 still lands on 2020-01-06 (a raw-payload mis-decode such " +
		"as 0.05 would not move the date)"
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"hex literal via explicit decimal cast", "select date_add('2020-01-01', interval cast(x'05' as decimal(10,2)) day)"},
		{"bit literal via explicit decimal cast", "select date_add('2020-01-01', interval cast(b'101' as decimal(10,2)) day)"},
		{"ordinary integer control", "select date_add('2020-01-01', interval 5 day)"},
		{"text decimal control", "select date_add('2020-01-01', interval 5.00 day)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q, p.String())
			root := q.Nodes[q.Steps[len(q.Steps)-1]]
			require.Len(t, root.ProjectList, 1, p.String())
			proj := root.ProjectList[0]
			if normalizer := findPlanFunctionExpr(p, "to_calendar_interval"); normalizer != nil {
				require.Equal(t, int64(5), foldedIntervalInt64(t, normalizer), expectedDays)
				return
			}
			lit := proj.GetLit()
			if lit == nil {
				// The plain-integer control keeps an ordinary date_add call in
				// the bound plan (varchar-result typing, no calendar
				// normalizer on this route): fold its interval argument
				// through the same ConstantFold machinery instead of
				// requiring an already-folded literal projection.
				fn := proj.GetF()
				require.NotNil(t, fn, proj.String())
				require.Equal(t, "date_add", fn.GetFunc().GetObjName())
				fargs := fn.GetArgs()
				require.NotEmpty(t, fargs)
				require.Equal(t, int64(5), foldedIntervalInt64(t, fargs[1]),
					"the ordinary integer interval keeps its spelled 5 days")
				return
			}
			switch types.T(proj.Typ.Id) {
			case types.T_date:
				want, err := types.ParseDateCast("2020-01-06")
				require.NoError(t, err)
				require.Equal(t, int32(want), lit.GetDateval(), expectedDays)
			case types.T_datetime:
				want, err := types.ParseDatetime("2020-01-06 00:00:00", 6)
				require.NoError(t, err)
				require.Equal(t, int64(want), lit.GetDatetimeval(), expectedDays)
			default:
				t.Fatalf("unexpected folded interval result type %d: %v", proj.Typ.Id, proj.String())
			}
		})
	}

	// Negative control: a NULL interval never reaches the decoder, and a real
	// expression evaluation must observe that NULL — function-shaped or
	// folded projection alike. The readonly result is freed as soon as it is
	// obtained (the executor helper cleans up on its own error paths).
	t.Run("null_interval_result", func(t *testing.T) {
		pn, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
			"select date_add('2020-01-01', interval cast(null as decimal(10,2)) day)")
		require.NoError(t, err)
		qn := pn.GetQuery()
		require.NotNil(t, qn, pn.String())
		rootn := qn.Nodes[qn.Steps[len(qn.Steps)-1]]
		require.Len(t, rootn.ProjectList, 1, pn.String())
		proc := newPlanTestProcess(t)
		result, free, err := colexec.GetReadonlyResultFromExpression(proc, rootn.ProjectList[0],
			[]*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err)
		require.NotNil(t, result)
		defer free()
		require.True(t, result.Length() > 0, "the readonly result carries a real row")
		require.True(t, result.IsConstNull() || result.IsNull(0),
			"a NULL interval evaluates to NULL; the control must not silently skip")
	})
}
