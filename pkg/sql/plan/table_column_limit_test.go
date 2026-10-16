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
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/stretchr/testify/require"
)

func columnLimitCreateSQL(n int, suffix string) string {
	columns := make([]string, n)
	for i := range columns {
		columns[i] = fmt.Sprintf("c%d int", i)
	}
	return "CREATE TABLE wide (" + strings.Join(columns, ",") + suffix + ")"
}

func requireColumnLimitError(t *testing.T, err error) {
	t.Helper()
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooManyFields), "%v", err)
	e := err.(*moerr.Error)
	require.Equal(t, uint16(1117), e.MySQLCode())
	require.Equal(t, "HY000", e.SqlState())
	require.Equal(t, "Too many columns", e.Error())
}

func TestTableColumnLimitCreate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		n      int
		suffix string
		hidden int
	}{
		{"below", 4095, "", 1}, {"fake_pk", 4096, "", 1},
		{"explicit_pk", 4096, ",PRIMARY KEY(c0)", 0},
		{"composite_pk", 4096, ",PRIMARY KEY(c0,c1)", 1},
		{"overflow", 4097, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := buildSingleStmt(mock, t, columnLimitCreateSQL(tc.n, tc.suffix))
			if tc.n > 4096 {
				requireColumnLimitError(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, p.GetDdl().GetCreateTable().TableDef.Cols, tc.n+tc.hidden)
		})
	}
}

func TestTableColumnLimitCTAS(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n        int
		explicit string
		reject   bool
	}{
		{"select_only", 4096, "", false},
		{"overlapping_override", 4096, "(c0 bigint)", false},
		{"distinct_target", 4096, "(extra int)", true},
		{"select_overflow", 4097, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			columns := make([]string, tc.n)
			for i := range columns {
				columns[i] = fmt.Sprintf("1 AS c%d", i)
			}
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := buildSingleStmt(mock, t, "CREATE TABLE wide "+tc.explicit+" AS SELECT "+strings.Join(columns, ","))
			if tc.reject {
				requireColumnLimitError(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, p.GetDdl().GetCreateTable().TableDef.Cols, 4097)
			if tc.explicit != "" {
				require.Equal(t, int32(types.T_int64), p.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.Id)
			}
		})
	}
}

func columnLimitFixture(t *testing.T, n int) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := buildSingleStmt(mock, t, columnLimitCreateSQL(min(n, 4096), ",PRIMARY KEY(c0)"))
	require.NoError(t, err)
	def := p.GetDdl().GetCreateTable().TableDef
	for i := len(def.Cols); i < n; i++ {
		def.Cols = append(def.Cols, &ColDef{Name: fmt.Sprintf("c%d", i), OriginName: fmt.Sprintf("c%d", i), Typ: pb.Type{Id: int32(types.T_int32)}, Default: &pb.Default{NullAbility: true}})
	}
	def.TableType = catalog.SystemOrdinaryRel
	def.TblId = 80001
	def.Version = 1
	for i, col := range def.Cols {
		col.ColId = uint64(i + 1)
		col.Seqnum = uint32(i)
	}
	def.Cols = append(def.Cols, &ColDef{Name: catalog.Row_ID, Hidden: true, Typ: pb.Type{Id: int32(types.T_Rowid)}})
	mock.ctxt.tables["wide"] = def
	mock.ctxt.objects["wide"] = &ObjectRef{SchemaName: "tpch", ObjName: "wide", Obj: 80001}
	return mock
}

func TestTableColumnLimitAlter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		n      int
		action string
		want   int
		reject bool
	}{
		{"last_slot", 4095, "ADD extra int", 4096, false},
		{"over_limit", 4096, "ADD extra int", 0, true},
		{"multiple_adds", 4095, "ADD x int, ADD y int", 0, true},
		{"add_then_drop", 4096, "ADD extra int, DROP c1", 4096, false},
		{"drop_then_add", 4096, "DROP c1, ADD extra int", 4096, false},
		{"legacy_reduce", 4098, "DROP c1", 4097, false},
		{"legacy_replace", 4098, "ADD extra int, DROP c1", 4098, false},
		{"legacy_growth", 4098, "ADD extra int", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := columnLimitFixture(t, tc.n)
			original := proto.Clone(mock.ctxt.tables["wide"]).(*TableDef)
			p, err := buildSingleStmt(mock, t, "ALTER TABLE wide "+tc.action)
			require.True(t, proto.Equal(original, mock.ctxt.tables["wide"]), "planner must not mutate catalog source")
			if tc.reject {
				requireColumnLimitError(t, err)
				return
			}
			require.NoError(t, err)
			copy := p.GetDdl().GetAlterTable().CopyTableDef
			require.Len(t, copy.Cols, tc.want+1)
		})
	}
}

func TestTableColumnLimitLikeAndReplay(t *testing.T) {
	for _, n := range []int{4096, 4098} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			mock := columnLimitFixture(t, n)
			_, err := buildSingleStmt(mock, t, "CREATE TABLE target LIKE wide")
			if n == 4096 {
				require.NoError(t, err)
			} else {
				requireColumnLimitError(t, err)
			}
		})
	}
	mock := columnLimitFixture(t, 4098)
	original := mock.ctxt.tables["wide"]
	target := DeepCopyTableDef(original, true)
	target.Name = "copy"
	target.Cols = append(target.Cols[:4097:4097], target.Cols[len(target.Cols)-1])
	ctx, err := WithAlterTableDDLReplay(t.Context(), mock.ctxt.DefaultDatabase(), original, target)
	require.NoError(t, err)
	mock.ctxt.SetContext(ctx)
	sql := strings.Replace(columnLimitCreateSQL(4097, ",PRIMARY KEY(c0)"), "wide", "copy", 1)
	_, err = buildSingleStmt(mock, t, sql)
	require.NoError(t, err)
	// A different CREATE cannot consume the COPY allowance.
	_, err = buildSingleStmt(mock, t, columnLimitCreateSQL(4097, ""))
	requireColumnLimitError(t, err)
	_, err = buildSingleStmt(mock, t, strings.Replace(columnLimitCreateSQL(4098, ""), "wide", "copy", 1))
	requireColumnLimitError(t, err)
	// After shrinking, the current source width is the bound; no high-water mark.
	_, err = WithAlterTableDDLReplay(t.Context(), mock.ctxt.DefaultDatabase(), target, original)
	requireColumnLimitError(t, err)
	mock.ctxt.SetContext(WithPersistedDDLReplay(t.Context(), original, target))
	_, err = buildSingleStmt(mock, t, sql)
	requireColumnLimitError(t, err)
}

func TestTableColumnLimitUserAccounting(t *testing.T) {
	columns := []*ColDef{{Name: "account_id"}, {Name: "g", GeneratedCol: &pb.GeneratedCol{}}, {Name: "backing", Hidden: true}}
	require.Equal(t, 2, userColumnCount(columns, false))
	require.Equal(t, 1, userColumnCount(columns, true))
	require.Equal(t, "account_id", util.GetClusterTableAttributeName())
}
