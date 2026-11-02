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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// The FK parent DDL error contract below is pinned without referencing the
// moerr constructor symbols introduced by the fix: the MySQL wire code is
// pinned via the pre-existing upstream constants (ER_TRUNCATE_ILLEGAL_FK /
// ER_FK_CANNOT_DROP_PARENT), the SQLSTATE and the message text literally, and
// the new internal codes numerically. On unfixed main these tests therefore
// fail behaviorally at the typed-wire assertions instead of failing to build,
// and they keep asserting on the real planner emission sites (BuildPlan ->
// buildTruncateTable / buildDropTableSingle) rather than a copied stub.

const (
	// fkTruncateInternalCode / fkDropInternalCode are the internal codes of
	// moerr.ErrTruncateIllegalFk / moerr.ErrFkCannotDropParent on the fixed
	// tree, kept numeric here so this file also compiles on unfixed main.
	fkTruncateInternalCode uint16 = 20343
	fkDropInternalCode     uint16 = 20344
)

func requireTruncateIllegalFk(t *testing.T, err error, wantMsg string) {
	t.Helper()
	require.Error(t, err)
	var me *moerr.Error
	require.ErrorAs(t, err, &me)
	require.Equal(t, moerr.ER_TRUNCATE_ILLEGAL_FK, me.MySQLCode())
	require.Equal(t, "42000", me.SqlState())
	require.Equal(t, fkTruncateInternalCode, me.ErrorCode())
	require.Equal(t, wantMsg, err.Error())
}

func requireFkCannotDropParent(t *testing.T, err error, wantMsg string) {
	t.Helper()
	require.Error(t, err)
	var me *moerr.Error
	require.ErrorAs(t, err, &me)
	require.Equal(t, moerr.ER_FK_CANNOT_DROP_PARENT, me.MySQLCode())
	require.Equal(t, "HY000", me.SqlState())
	require.Equal(t, fkDropInternalCode, me.ErrorCode())
	require.Equal(t, wantMsg, err.Error())
}

// fkChecks resolves foreign_key_checks the way the frontend session would:
// unset means enabled; zero explicitly disables it.
func fkChecks(enabled bool) func(string, bool, bool) (interface{}, error) {
	value := interface{}(int64(0))
	if enabled {
		value = nil
	}
	return func(string, bool, bool) (interface{}, error) {
		return value, nil
	}
}

func parseDDLForTest(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	return stmt
}

// TestBuildTruncateRejectsReferencedParentWithTypedError pins the real
// emission site end to end against the mock catalog fixture
// constraint_test.replace_fk_p (77001) referenced by replace_fk_c (77002)
// through fk_replace_c: TRUNCATE must fail with MySQL 1701/42000 naming the
// constraint, never a generic internal error.
func TestBuildTruncateRejectsReferencedParentWithTypedError(t *testing.T) {
	stmt := parseDDLForTest(t, "truncate table constraint_test.replace_fk_p")
	defer stmt.Free()

	_, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
	requireTruncateIllegalFk(t, err,
		"Cannot truncate a table referenced in a foreign key constraint (fk_replace_c)")
}

// TestBuildDropRejectsReferencedParentWithTypedError pins the DROP emission
// site: MySQL 3730/HY000 with the referenced table, the constraint and the
// child table taken from live FK metadata.
func TestBuildDropRejectsReferencedParentWithTypedError(t *testing.T) {
	stmt := parseDDLForTest(t, "drop table constraint_test.replace_fk_p")
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.ResolveVariableFunc = fkChecks(true)
	_, err := BuildPlan(ctx, stmt, false)
	requireFkCannotDropParent(t, err,
		"Cannot drop table 'replace_fk_p' referenced by a foreign key constraint 'fk_replace_c' on table 'replace_fk_c'.")
}

// TestBuildDropReferencedParentAllowedWhenForeignKeyChecksDisabled preserves
// the documented gate asymmetry: with foreign_key_checks=0 DROP passes the FK
// gate and only TRUNCATE keeps rejecting.
func TestBuildDropReferencedParentAllowedWhenForeignKeyChecksDisabled(t *testing.T) {
	stmt := parseDDLForTest(t, "drop table constraint_test.replace_fk_p")
	defer stmt.Free()

	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx.ResolveVariableFunc = fkChecks(false)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.NotNil(t, p.GetDdl().GetDropTable())
}

// TestBuildTruncateNameSelectionBoundedToFirstNonSelfChild pins the bounded,
// deterministic name lookup: the first non-self entry of RefChildTbls decides,
// a first child whose metadata does not reference this parent is not
// rescanned past, and the error still fails closed with empty names.
func TestBuildTruncateNameSelectionBoundedToFirstNonSelfChild(t *testing.T) {
	t.Run("first-non-self-child-wins", func(t *testing.T) {
		stmt := parseDDLForTest(t, "truncate table constraint_test.replace_fk_p")
		defer stmt.Free()

		ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
		// replace_fk_c (77002) is the first non-self child; the later child
		// belonging to another parent must never be consulted.
		ctx.tables["replace_fk_p"].RefChildTbls = []uint64{77002, 77006}
		_, err := BuildPlan(ctx, stmt, false)
		requireTruncateIllegalFk(t, err,
			"Cannot truncate a table referenced in a foreign key constraint (fk_replace_c)")
	})

	t.Run("first-child-without-matching-fk-is-not-rescanned", func(t *testing.T) {
		stmt := parseDDLForTest(t, "truncate table constraint_test.replace_fk_p")
		defer stmt.Free()

		ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
		// First non-self child replace_fk_sc (77006) references another
		// parent (77005): the lookup stops there and fails closed, unnamed.
		ctx.tables["replace_fk_p"].RefChildTbls = []uint64{0, 77006, 77002}
		_, err := BuildPlan(ctx, stmt, false)
		requireTruncateIllegalFk(t, err,
			"Cannot truncate a table referenced in a foreign key constraint ()")
	})
}

// TestBuildTruncatePropagatesChildResolveFailure keeps the resolve error
// propagating: a failing child metadata lookup is reported, never swallowed
// and never converted into a fake success or a different error.
func TestBuildTruncatePropagatesChildResolveFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := NewMockCompilerContext2(ctrl)

	parent := &TableDef{Name: "p9", TblId: 77019, RefChildTbls: []uint64{77029}}
	ctx.EXPECT().Resolve("constraint_test", "p9", gomock.Any()).
		Return(&ObjectRef{}, parent, nil).AnyTimes()
	probe := moerr.NewInternalErrorf(context.Background(), "probe fk child resolve failure")
	ctx.EXPECT().ResolveById(uint64(77029), gomock.Any()).
		Return(nil, nil, probe).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()

	stmt := parseDDLForTest(t, "truncate table constraint_test.p9")
	defer stmt.Free()

	_, err := buildTruncateTable(stmt.(*tree.TruncateTable), ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "probe fk child resolve failure")
}

// TestBuildTruncateUnresolvedChildStillFailsClosed pins the missing-metadata
// boundary: a child id that resolves to nothing still yields the dedicated
// user error, with empty names instead of success or a silent fallback.
func TestBuildTruncateUnresolvedChildStillFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := NewMockCompilerContext2(ctrl)

	parent := &TableDef{Name: "p9", TblId: 77019, RefChildTbls: []uint64{77029}}
	ctx.EXPECT().Resolve("constraint_test", "p9", gomock.Any()).
		Return(&ObjectRef{}, parent, nil).AnyTimes()
	ctx.EXPECT().ResolveById(uint64(77029), gomock.Any()).
		Return(nil, nil, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()

	stmt := parseDDLForTest(t, "truncate table constraint_test.p9")
	defer stmt.Free()

	_, err := buildTruncateTable(stmt.(*tree.TruncateTable), ctx)
	requireTruncateIllegalFk(t, err,
		"Cannot truncate a table referenced in a foreign key constraint ()")
}

// TestBuildDropPropagatesChildResolveFailure is the DROP-side control for the
// same propagation contract, reached through the real buildDropTableSingle.
func TestBuildDropPropagatesChildResolveFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := NewMockCompilerContext2(ctrl)

	parent := &TableDef{Name: "p9", TblId: 77019, RefChildTbls: []uint64{77029}}
	ctx.EXPECT().Resolve("constraint_test", "p9", gomock.Any()).
		Return(&ObjectRef{}, parent, nil).AnyTimes()
	ctx.EXPECT().ResolveVariable("foreign_key_checks", gomock.Any(), gomock.Any()).
		Return(nil, nil).AnyTimes()
	probe := moerr.NewInternalErrorf(context.Background(), "probe fk child resolve failure")
	ctx.EXPECT().ResolveById(uint64(77029), gomock.Any()).
		Return(nil, nil, probe).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()

	stmt := parseDDLForTest(t, "drop table constraint_test.p9")
	defer stmt.Free()

	drop := stmt.(*tree.DropTable)
	_, err := buildDropTableSingle(drop.IfExists, drop.Temporary, drop.Names[0], ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "probe fk child resolve failure")
}
