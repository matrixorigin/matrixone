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
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestRegexpWitnessDeclarationConservation(t *testing.T) {
	for _, tc := range []struct {
		source   string
		mismatch bool
	}{
		{"repeat(cast('abc' as char),3)", true},
		{"repeat(cast('abc' as char),ceil(2.1))", true},
		{"(select repeat(cast('abc' as char),ceil(2.1)))", true},
		{"(select v from (select repeat(cast('abc' as char),ceil(2.1)) v) s)", true},
		{"max(repeat(cast('abc' as char),ceil(2.1)))", true},
		{"max(repeat(cast('abc' as char),ceil(2.1))) over()", true},
		{"repeat('',-1)", true},
		{"repeat(cast('abc' as char(0)),-1)", true},
		{"repeat(left(@str_var,0),-1)", true},
		{"left(left(@str_var,3),ceil(65536.1))", true},
		{"substring(left(@str_var,3),1,ceil(65536.1))", true},
		{"repeat('',0)", true},
		{"repeat('',1)", true},
		{"repeat('',null)", false},
		{"repeat('',cast(null as signed))", false},
		{"repeat('',@n)", false},
		{"repeat(cast('abc' as char(0)),null)", false},
		{"repeat(cast('' as binary(0)),null)", false},
		{"repeat('abc',-1)", false},
		{"repeat(@str_var,-1)", false},
		{"repeat(cast(null as char(0)),-1)", true},
		// MySQL 8.4.8 Item_func_substr::resolve_type applies start before length.
		{"substring(@str_var,0)", true},
		{"substring(cast('abc' as char(3)),-20000)", true},
		{"substring(@str_var,0,null)", true},
		{"substring(repeat(cast('abc' as char(3)),7000),20000)", true},
		{"substring(repeat(cast('abc' as char(3)),7000),20000,20000)", true},
		{"substring(@str_var,1)", false},
		{"substring(@str_var,1,null)", false},
		{"substring(repeat(cast('abc' as char(3)),7000),1,null)", false},
		{"substring(@str_var,null,3)", false},
		{"substring(@str_var,cast(null as signed),3)", false},
		{"substring(@str_var,@n,3)", true},
		{"substring(@str_var,0,@n)", true},
		{"substring(@str_var,-2147483648)", false},
		{"substring(repeat(cast('abc' as char(3)),7000),2147483648)", false},
		{"substring(@str_var,1,2147483648)", false},
		{"substring(@str_var,0,2147483648)", true},
		{"mid(@str_var,0,null)", true},
		{"substr(cast('abc' as char(3)),-20000,3)", true},
		{"substring(@str_var,1,cast(18446744073709551615 as unsigned))", false},
		{"substring(@str_var,cast(18446744073709551615 as unsigned),3)", true},
		{"(select substring(@str_var,0,null))", true},
		{"(select v from (select substring(@str_var,0,null) v) s)", true},
		{"max(substring(@str_var,0,null))", true},
		{"max(substring(@str_var,0,null)) over()", true},
	} {
		for _, consumer := range []string{"regexp_like(cast(%s as binary),'a')", "regexp_instr(cast(%s as binary),'a')", "regexp_substr(cast(%s as binary),'a')", "regexp_replace(cast(%s as binary),'a','x')"} {
			sql := "select " + fmt.Sprintf(consumer, tc.source)
			for _, prepare := range []bool{false, true} {
				query := sql
				if prepare {
					query = "prepare witness_contract from '" + strings.ReplaceAll(sql, "'", "''") + "'"
				}
				t.Run(query, func(t *testing.T) {
					_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
					if tc.mismatch {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch), err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestRegexpSubstringUnknownPrepare(t *testing.T) {
	for _, tc := range []struct {
		source   string
		mismatch bool
	}{
		{"substring(@str_var,?,?)", false},
		{"substring(@str_var,?,3)", true},
		{"substring(@str_var,0,?)", true},
		{"substring(@str_var,null,?)", false},
		{"(select substring(?,0,null))", true},
		{"(select v from (select substring(?,0,null) v) s)", true},
	} {
		query := "prepare substring_unknown from 'select regexp_like(cast(" + tc.source + " as binary),''a'')'"
		t.Run(tc.source, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
			if tc.mismatch {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegexpRepeatUnknownCountPrepare(t *testing.T) {
	for _, consumer := range []string{
		"regexp_like(cast(repeat('',?) as binary),'a')",
		"regexp_instr(cast(repeat('',?) as binary),'a')",
		"regexp_substr(cast(repeat('',?) as binary),'a')",
		"regexp_replace(cast(repeat('',?) as binary),'a','x')",
	} {
		query := "prepare repeat_unknown from 'select " + strings.ReplaceAll(consumer, "'", "''") + "'"
		t.Run(query, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, query)
			require.NoError(t, err, "unknown marker count must retain an unbounded declaration")
		})
	}
}

func TestRegexpRepeatUnknownCountDeclaredType(t *testing.T) {
	for _, source := range []string{"repeat('',null)", "repeat('',cast(null as signed))", "repeat('',@n)"} {
		t.Run(source, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+source)
			require.NoError(t, err)
			q := p.GetQuery()
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			declared := regexpDeclaredStringType(expr)
			require.Equal(t, types.T_text, declared.Oid, "unknown count cannot declare zero VARCHAR")
			_, bounded := function.StringResultByteBound(declared)
			require.False(t, bounded)
			require.Equal(t, declared, regexpDeclaredStringType(DeepCopyExpr(expr)))
		})
	}
}

func TestStringDeclarationWitnessBounds(t *testing.T) {
	require.Nil(t, stringDeclarationWitnessArg(nil))
	for _, tc := range []struct {
		source string
		width  int32
	}{
		{"repeat(cast('abc' as char),ceil(2.1))", 9},
		{"repeat('123',ceil(2.1))", 9},
		{"left(left(@str_var,3),ceil(3.1))", 3},
		{"right(left(@str_var,3),ceil(3.1))", 3},
		{"substring(left(@str_var,3),1,ceil(3.1))", 3},
		{"repeat('',-1)", 0},
		{"repeat(left(@str_var,0),-1)", 0},
		{"substring(@str_var,0)", 0},
		{"substring(cast('abc' as char(3)),-20000)", 0},
		{"substring(@str_var,0,null)", 0},
		{"substring(repeat(cast('abc' as char(3)),7000),20000)", 1001},
		{"substring(repeat(cast('abc' as char(3)),7000),20000,20000)", 1001},
		{"substring(repeat(cast('abc' as char(3)),7000),20000,null)", 1001},
		{"substring(cast('abc' as char(3)),-2,3)", 2},
		{"substring(cast('abc' as char(3)),2,3)", 2},
		{"substring(cast('abc' as char(3)),2147483647)", 0},
		{"substring(@str_var,2147483647)", 0},
		{"substring(@str_var,-2147483647)", 0},
		{"substring(cast('abc' as char(3)),null,0)", 3},
		{"substring(cast('abc' as char(3)),cast(null as signed),-1)", 3},
		{"substring(cast('abc' as char(3)),2147483648)", 3},
	} {
		t.Run(tc.source, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+tc.source)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q)
			expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			declared := regexpDeclaredStringType(expr)
			require.Equal(t, types.T_varchar, declared.Oid)
			require.Equal(t, tc.width, declared.Width)
			require.Equal(t, declared, regexpDeclaredStringType(DeepCopyExpr(expr)))
			encoded, err := proto.Marshal(expr)
			require.NoError(t, err)
			decoded := new(Expr)
			require.NoError(t, proto.Unmarshal(encoded, decoded))
			require.Equal(t, declared, regexpDeclaredStringType(decoded))
			copy := stringDeclarationWitnessArg(expr)
			require.Equal(t, declared, regexpDeclaredStringType(copy))
			require.Nil(t, copy.GetPreparedNumeric())
			require.Nil(t, copy.GetLit().Value, "type-only NULL must not carry a mismatched numeric/string payload")
			require.Nil(t, copy.GetLit().GetSrc(), "a declaration fact must not retain an executable tree")
		})
	}
}

func witnessFamilySQL(kind string, depth int, prepare bool) string {
	source := "@str_var"
	if prepare {
		source = "?"
	}
	for level := range depth {
		current := kind
		if kind == "mixed" {
			current = []string{"lpad", "substring", "repeat"}[level%3]
		}
		switch current {
		case "lpad":
			source = "lpad(" + source + ",ceil(2.1),'x')"
		case "substring":
			source = "substring(" + source + ",1,ceil(2.1))"
		case "substring-unknown":
			scalar := "@n"
			if prepare {
				scalar = "?"
			}
			source = "substring(" + source + "," + scalar + "," + scalar + ")"
		case "repeat":
			source = "repeat(" + source + ",ceil(0.1))"
		}
	}
	sql := "select " + source
	if prepare {
		sql = "prepare witness_space from '" + strings.ReplaceAll(sql, "'", "''") + "'"
	}
	return sql
}

func TestStringWitnessFamiliesLinearSpace(t *testing.T) {
	for _, kind := range []string{"lpad", "substring", "substring-unknown", "repeat", "mixed"} {
		for _, prepare := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prepare=%t", kind, prepare), func(t *testing.T) {
				previous := 0
				for _, depth := range []int{16, 32, 64, 128, 256} {
					sql := witnessFamilySQL(kind, depth, prepare)
					p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
					require.NoError(t, err)
					size := proto.Size(p)
					require.LessOrEqual(t, size, 1024*depth+4096, "per-node metadata must be bounded, not an entire source chain")
					if previous != 0 {
						require.LessOrEqual(t, size, 2*previous+4096, "doubling a chain must not quadruple its persisted size")
					}
					require.True(t, proto.Equal(p, DeepCopyPlan(p)), "copy must preserve declarations and marker identity")
					t.Logf("depth=%d SQL=%d plan=%d", depth, len(sql), size)
					previous = size
				}
			})
		}
	}
}

func BenchmarkStringWitnessFamiliesDeepCopy(b *testing.B) {
	for _, kind := range []string{"lpad", "substring", "repeat", "mixed"} {
		for _, depth := range []int{64, 128, 256} {
			b.Run(fmt.Sprintf("%s/%d", kind, depth), func(b *testing.B) {
				p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(b)), b, witnessFamilySQL(kind, depth, false))
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_ = DeepCopyPlan(p)
				}
			})
		}
	}
}
