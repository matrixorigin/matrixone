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

package colexec_test

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type orLikeStringBindingContext struct {
	*plan2.MockCompilerContext
	columnType types.T
}

func (ctx *orLikeStringBindingContext) Resolve(db, table string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	object, definition, err := ctx.MockCompilerContext.Resolve(db, table, snapshot)
	if err != nil || definition == nil {
		return object, definition, err
	}
	definition = proto.Clone(definition).(*plan.TableDef)
	for _, column := range definition.Cols {
		if column.Name == "l_comment" {
			column.Typ = ctx.columnType.ToType().PlanType()
		}
	}
	return object, definition, nil
}

func TestORLikeSubjectReusePublicBinding(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	for _, test := range []struct {
		name     string
		subject  string
		oid      types.T
		coalesce bool
		shared   bool
		folded   bool
		nullLast bool
	}{
		{"ifnull_concat", "lower(concat(ifnull(l_comment, ''), ' ', ifnull(l_comment, '')))", types.T_text, false, true, false, false},
		{"text_lower_control", "lower(l_comment)", types.T_text, false, true, false, true},
		{"text_coalesce", "lower(coalesce(l_comment, l_comment))", types.T_text, true, true, false, true},
		{"constant_prefix", "lower(coalesce('ERROR', l_comment))", types.T_varchar, true, false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := &orLikeStringBindingContext{plan2.NewMockCompilerContext(false, proc), test.oid}
			query := fmt.Sprintf(`select l_comment from tpch.lineitem where
%s like '%%error%%' or %s like '%%timeout%%' or %s like '%%retry%%'`, test.subject, test.subject, test.subject)
			statement, err := mysql.ParseOne(proc.Ctx, query, 1)
			require.NoError(t, err)
			defer statement.Free()
			built, err := plan2.BuildPlan(ctx, statement, false)
			require.NoError(t, err)
			var candidate *plan.Expr
			var column *plan.Expr
			var coalesce *plan.Expr
			var visit func(*plan.Expr)
			visit = func(expression *plan.Expr) {
				if expression == nil {
					return
				}
				if expression.GetCol() != nil {
					column = expression
				}
				if expression.GetF() == nil {
					return
				}
				fid, _ := function.DecodeOverloadID(expression.GetF().Func.Obj)
				if fid == function.OR && candidate == nil {
					candidate = expression
				}
				if fid == function.COALESCE {
					coalesce = expression
				}
				for _, child := range expression.GetF().Args {
					visit(child)
				}
			}
			for _, node := range built.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					visit(filter)
				}
			}
			require.NotNil(t, candidate, "real parser/binder/optimizer must retain the three-LIKE OR")
			require.NotNil(t, column)
			require.Equal(t, int32(test.oid), column.Typ.Id)
			if test.coalesce {
				require.NotNil(t, coalesce, "the public binder must retain COALESCE")
				require.Equal(t, int32(test.oid), coalesce.Typ.Id)
				fid, ordinal := function.DecodeOverloadID(coalesce.GetF().Func.Obj)
				require.Equal(t, int32(function.COALESCE), fid)
				t.Logf("public %s COALESCE registered ordinal: %d", test.oid, ordinal)
			}
			executor, err := colexec.NewExpressionExecutor(proc, candidate)
			require.NoError(t, err)
			defer func() { executor.Free() }()
			raw := executor
			executor = colexec.ReuseFilterExpressionExecutor(candidate, executor, 1024)
			require.Equal(t, test.shared, fmt.Sprintf("%T", executor) == "*colexec.memoRootExpressionExecutor", "bound SQL admission must preserve metadata and lazy folding")
			if !test.shared {
				require.Same(t, raw, executor)
			}
			require.Equal(t, int32(0), column.GetCol().RelPos)
			input := batch.NewWithSize(int(column.GetCol().ColPos) + 1)
			defer input.Clean(proc.Mp())
			values := []string{"ERROR", "ordinary", "TİMEOUT", "RETRY", ""}
			input.Vecs[column.GetCol().ColPos] = vector.NewVec(test.oid.ToType())
			for row, value := range values {
				require.NoError(t, vector.AppendBytes(input.Vecs[column.GetCol().ColPos], []byte(value), row == 4, proc.Mp()))
			}
			input.SetRowCount(len(values))
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, test.folded, result.IsConst(), "constant-prefix SQL must retain the existing const-bool result")
			for row, want := range []bool{true, false, true, true} {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, test.folded || want, vector.GetFixedAtNoTypeCheck[bool](result, row))
			}
			require.Equal(t, test.nullLast, result.IsNull(4))
			if !test.nullLast {
				require.Equal(t, test.folded, vector.GetFixedAtNoTypeCheck[bool](result, 4))
			}
		})
	}
}
