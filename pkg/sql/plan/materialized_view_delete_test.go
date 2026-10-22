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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/mv"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type materializedViewDeleteContext struct {
	*MockCompilerContext
	sql          executor.SQLExecutor
	catalogError error
	missing      bool
}

func (c *materializedViewDeleteContext) getInternalSQLExecutor(*process.Process) (executor.SQLExecutor, bool) {
	return c.sql, true
}
func (c *materializedViewDeleteContext) Resolve(db, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if db == catalog.MO_CATALOG && name == catalog.MO_MV_DEPENDENCIES {
		if c.catalogError != nil {
			return nil, nil, c.catalogError
		}
		if c.missing {
			return nil, nil, nil
		}
		return &ObjectRef{SchemaName: db, ObjName: name}, &TableDef{Name: name}, nil
	}
	return c.MockCompilerContext.Resolve(db, name, snapshot)
}
func TestMaterializedViewSourceDeleteRetainsRelation(t *testing.T) {
	for _, name := range []string{"dependent", "independent", "pre-migration", "missing catalog error", "catalog failure", "probe failure"} {
		t.Run(name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			proc.ReplaceTopCtx(defines.AttachAccountId(context.Background(), 0))
			base := NewMockCompilerContext(true, proc)
			c := &materializedViewDeleteContext{MockCompilerContext: base, missing: name == "pre-migration"}
			injected := errors.New("metadata unavailable")
			if name == "catalog failure" {
				c.catalogError = injected
			}
			if name == "missing catalog error" {
				c.catalogError = moerr.NewNoSuchTable(proc.Ctx, catalog.MO_CATALOG, catalog.MO_MV_DEPENDENCIES)
			}
			setPlanTestGlobalVariable(t, proc.GetService(), mv.RuntimeKey, true)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			calls := 0
			table := base.tables["nation"]
			c.sql = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				calls++
				require.Equal(t, fmt.Sprintf("select mv_id from mo_catalog.mo_mv_dependencies where account_id=0 and source_table_id=%d limit 1", table.TblId), sql)
				if name == "probe failure" {
					return executor.Result{}, injected
				}
				r := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
				if name == "dependent" {
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint64{99}))
				}
				return r.GetResult(), nil
			})
			dml := &DMLContext{tableDefs: []*TableDef{table}, objRefs: []*ObjectRef{{SchemaName: "tpch", ObjName: "nation"}}, isClusterTable: []bool{false}}
			// This modern-planner hint only requests legacy planning; it does
			// not execute a truncate. Probe once at physical eligibility below.
			allowed, err := canDeleteRewriteToTruncate(c, dml)
			require.NoError(t, err)
			require.True(t, allowed)
			require.Zero(t, calls)
			stmt, err := mysql.ParseOne(c.GetContext(), "delete from tpch.nation", 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := buildDelete(stmt.(*tree.Delete), c, false)
			if name == "catalog failure" || name == "probe failure" {
				require.ErrorIs(t, err, injected)
				return
			}
			require.NoError(t, err)
			found := false
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == pb.Node_DELETE && n.DeleteCtx.TableDef.TblId == table.TblId {
					found = true
					require.Equal(t, name != "dependent", n.DeleteCtx.CanTruncate)
				}
			}
			require.True(t, found)
			if name == "dependent" || name == "independent" {
				require.Equal(t, 1, calls, "index eligibility must reuse the base-table decision")
			} else {
				require.Zero(t, calls)
			}
		})
	}
}
