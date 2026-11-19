// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Issue #31175 review: the pairwise tie replay admits exactly the mixes in
// which MySQL's left-to-right aggregation lets a binary operand absorb every
// equal-rank operand it meets. Text/text conflicts, carried merge conflicts,
// explicit collations and binary-free mixes keep the generic verdict.
func TestPairwiseBinaryTieWinner(t *testing.T) {
	unicodeCI := expressionCollation{charset: uint32(types.CharsetUTF8MB4UnicodeCI), version: 1, rank: 2}
	generalCI := expressionCollation{charset: uint32(types.CharsetUTF8), rank: 2}
	textBin := expressionCollation{charset: uint32(types.CharsetUTF8MB4Bin), rank: 2}
	binary := expressionCollation{charset: uint32(types.CharsetBinary), rank: 2}
	for _, tc := range []struct {
		name       string
		candidates []expressionCollation
		want       bool
	}{
		{"binary absorbs a ci column", []expressionCollation{unicodeCI, binary}, true},
		{"binary absorbs a ci column reversed", []expressionCollation{binary, unicodeCI}, true},
		{"binary absorbs a version-zero ci column", []expressionCollation{generalCI, binary}, true},
		{"binary absorbs before a text conflict", []expressionCollation{unicodeCI, binary, generalCI}, true},
		{"binary absorbs a text _bin too", []expressionCollation{unicodeCI, binary, textBin}, true},
		{"identical ci columns still absorb", []expressionCollation{unicodeCI, unicodeCI, binary}, true},
		{"text conflict reached first stays illegal", []expressionCollation{unicodeCI, generalCI, binary}, false},
		{"explicit collation wins before any tie", []expressionCollation{
			{charset: uint32(types.CharsetUTF8MB4UnicodeCI), version: 1, rank: 0}, binary}, false},
		{"merge conflict keeps the generic verdict", []expressionCollation{
			unicodeCI, {charset: uint32(types.CharsetBinary), rank: 2, conflict: true}}, false},
		{"no binary operand no admission", []expressionCollation{unicodeCI, generalCI}, false},
		{"a dominated coercible literal cannot tie", []expressionCollation{
			unicodeCI, {charset: uint32(types.CharsetBinary), rank: 4}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restored, ok := pairwiseBinaryTieWinner(tc.candidates)
			require.Equal(t, tc.want, ok)
			if tc.want {
				require.Equal(t, uint32(types.CharsetBinary), restored.charset)
				require.Zero(t, restored.version)
				require.Equal(t, uint32(2), restored.rank)
				require.False(t, restored.conflict)
			}
		})
	}
}

// Issue #31175 CI: a BINARY cast keeps MySQL's IMPLICIT coercibility (2) even
// after constant folding replaces it with a literal that carries only its
// plan type. Without the rank stamp the folded literal drops to coercible
// (4), a case-insensitive column wins strictly, and the pattern converts to
// the column's collation — folding where MySQL compares byte-wise. The
// BINARY operator binds as the binary() function and carries the same stamp.
func TestPatternBinaryCastCoercibilitySurvivesFolding(t *testing.T) {
	proc := newPlanTestProcess(t)
	bindConstant := func(expr string) *Expr {
		t.Helper()
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+expr, 1)
		require.NoError(t, err)
		defer stmt.Free()
		ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
		bound, err := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil).BindExpr(ast, 0, false)
		require.NoError(t, err)
		return bound
	}
	// Both the explicit CAST and the BINARY operator bind with the implicit
	// rank stamped on the result type, so a later fold cannot lose it. A
	// _binary introducer literal stays coercible (4) and keeps losing to a
	// column's collation exactly like before.
	for _, expr := range []string{"cast('a%' as binary)", "binary 'a%'"} {
		bound := bindConstant(expr)
		require.True(t, bound.Typ.CollationCoercibilitySet, expr)
		require.Equal(t, uint32(2), bound.Typ.CollationCoercibility, expr)
		require.Equal(t, uint32(types.CharsetBinary), bound.Typ.Charset, expr)
	}
	require.False(t, bindConstant("_binary 'a%'").Typ.CollationCoercibilitySet)

	generalCI := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8)
	newCol := func() *Expr {
		return &Expr{Typ: generalCI.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
	}
	newBatch := func(value string) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(generalCI)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, proc.Mp()))
		bat.SetRowCount(1)
		return bat
	}
	evalLike := func(args []*Expr, value string) bool {
		bound, err := BindFuncExprImplByPlanExpr(t.Context(), "like", args)
		require.NoError(t, err)
		executor, err := colexec.NewExpressionExecutor(proc, bound)
		require.NoError(t, err)
		defer executor.Free()
		bat := newBatch(value)
		defer bat.Clean(proc.Mp())
		result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		return vector.MustFixedColNoTypeCheck[bool](result)[0]
	}
	// A version-zero general_ci column ties with the binary operand at rank 2
	// and the byte-wise routing compares without folding; the introducer
	// literal and the plain literal keep folding under the column collation.
	require.False(t, evalLike([]*Expr{newCol(), bindConstant("binary 'a%'")}, "A"))
	require.False(t, evalLike([]*Expr{newCol(), bindConstant("cast('a%' as binary)")}, "A"))
	require.True(t, evalLike([]*Expr{newCol(), bindConstant("_binary 'a%'")}, "A"))
	require.True(t, evalLike([]*Expr{newCol(), bindConstant("'a%'")}, "A"))
	// The binary() operand must reach the executor unconverted: wrapping it in
	// a VARCHAR cast would erase the byte-wise routing.
	bound, err := BindFuncExprImplByPlanExpr(t.Context(), "like",
		[]*Expr{newCol(), bindConstant("binary 'a%'")})
	require.NoError(t, err)
	pattern := bound.GetF().Args[1]
	require.Equal(t, "binary", pattern.GetF().Func.GetObjName())
	require.Equal(t, uint32(types.CharsetBinary), pattern.Typ.Charset)
	// FIND_IN_SET honors the same tie: the binary list stays byte-wise.
	findInSet, err := BindFuncExprImplByPlanExpr(t.Context(), "find_in_set",
		[]*Expr{newCol(), bindConstant("binary 'A,b'")})
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, findInSet)
	require.NoError(t, err)
	defer executor.Free()
	bat := newBatch("a")
	defer bat.Clean(proc.Mp())
	result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, []int32{0}, vector.MustFixedColNoTypeCheck[int32](result))
}
