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
	"errors"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type cleanupBorrowCompiler struct {
	CompilerContext
	borrow *BorrowedTable
}

func (c *cleanupBorrowCompiler) Resolve(database, name string, snapshot *Snapshot, output *TableResolution) (*ObjectRef, *TableDef, error) {
	output.Reset()
	ref, def, err := c.CompilerContext.Resolve(database, name, snapshot, nil)
	if err == nil && output != nil {
		output.Borrow = c.borrow
	}
	return ref, def, err
}
func (c *cleanupBorrowCompiler) BorrowedTableStillCurrent(proc *process.Process, borrow *BorrowedTable) bool {
	return proc != nil && borrow != nil && proc.GetTxnOperator() == borrow.Read.Op
}

func TestTemporalPlanningBorrowReleasedOnBuilderExit(t *testing.T) {
	for _, outcome := range []string{"success", "error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			fixture, _, _ := newIndexCostFixture(t)
			scan := fixture.qry.Nodes[0]
			underlying := fixture.compCtx.(*indexCostCompilerFixture).encodedIndexCostTestContext
			underlying.tables["base"] = scan.TableDef
			underlying.objects["base"] = scan.ObjRef
			compiler := &cleanupBorrowCompiler{CompilerContext: underlying, borrow: fixture.scanBorrows[0].borrow}
			stmt, err := mysql.ParseOne(compiler.GetContext(), "select d from base", 1)
			require.NoError(t, err)
			defer stmt.Free()
			root := stmt.(*tree.Select)
			input := NewPlanningInput(root, fixture.planningInput.intent, compiler)
			var builder *QueryBuilder
			var backing []scanBorrow
			marker := errors.New("stop after binding")
			validate := func(*Query) error {
				require.NotNil(t, builder)
				require.Same(t, input, builder.planningInput)
				require.Same(t, compiler, builder.borrowingCompiler())
				require.Len(t, builder.scanBorrows, 1, "the real bind must have retained the borrow")
				backing = builder.scanBorrows
				switch outcome {
				case "error":
					return marker
				case "panic":
					panic(marker)
				}
				return nil
			}
			run := func() error {
				_, err := bindAndOptimizeSelectQueryWithValidatorAndCapture(planpb.Query_SELECT, compiler, root, false, false,
					validate, func(ctx *BindContext) { builder = ctx.builder }, false, "", input)
				return err
			}
			if outcome == "panic" {
				require.PanicsWithValue(t, marker, func() { _ = run() })
			} else if outcome == "error" {
				require.ErrorIs(t, run(), marker)
			} else {
				require.NoError(t, run())
			}
			require.NotNil(t, builder)
			require.Nil(t, builder.planningInput, "the AST and resolver must not survive the attempt")
			require.Nil(t, builder.borrowingCompiler())
			require.Equal(t, fixture.planningInput.intent, input.intent)
			require.Same(t, compiler, input.owner)
			require.False(t, input.claim(root), "cleanup must not reset the one-shot claim")
			require.Nil(t, builder.scanBorrows, "no transaction reference may survive the attempt")
			require.Len(t, backing, 1)
			require.Equal(t, scanBorrow{}, backing[0], "the old backing array must not retain the transaction")
		})
	}
}
