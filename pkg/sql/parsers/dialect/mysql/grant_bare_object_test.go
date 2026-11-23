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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// parseGrantBare runs the #31804 equality oracle: the bare `ON priv_level`
// form must produce a tree deeply equal to the explicit `ON TABLE priv_level`
// form, must default ObjType to OBJECT_TYPE_TABLE, and its formatted text must
// reparse.
func parseGrantBare(t *testing.T, bareSQL, explicitSQL string) *tree.Grant {
	t.Helper()
	bare, err := ParseOne(context.Background(), bareSQL, 1)
	require.NoError(t, err, bareSQL)
	explicit, err := ParseOne(context.Background(), explicitSQL, 1)
	require.NoError(t, err, explicitSQL)

	bareGrant, ok := bare.(*tree.Grant)
	require.True(t, ok, bareSQL)
	require.Equal(t, tree.GrantTypePrivilege, bareGrant.Typ, bareSQL)
	require.Equal(t, tree.OBJECT_TYPE_TABLE, bareGrant.GrantPrivilege.ObjType, bareSQL)

	require.Equal(t, explicit, bare, bareSQL)
	bareFormatted := tree.String(bare, dialect.MYSQL)
	require.Equal(t, tree.String(explicit, dialect.MYSQL), bareFormatted, bareSQL)
	reparsed, err := ParseOne(context.Background(), bareFormatted, 1)
	require.NoError(t, err, bareFormatted)
	require.Equal(t, bare, reparsed, bareFormatted)
	return bareGrant
}

func TestGrantBareObjectLevelsMatchExplicit(t *testing.T) {
	pairs := []struct {
		bareSQL     string
		explicitSQL string
	}{
		{"grant select on *.* to r1", "grant select on table *.* to r1"},
		{"grant select on db.* to r1", "grant select on table db.* to r1"},
		{"grant select on db.tbl to r1", "grant select on table db.tbl to r1"},
		{"grant select on tbl to r1", "grant select on table tbl to r1"},
		{"grant select, insert, update on db.tbl to r1", "grant select, insert, update on table db.tbl to r1"},
		{"grant all on db.* to r1 with grant option", "grant all on table db.* to r1 with grant option"},
		{"grant all privileges on *.* to r1 with grant option", "grant all privileges on table *.* to r1 with grant option"},
	}
	for _, pair := range pairs {
		t.Run(pair.bareSQL, func(t *testing.T) {
			parseGrantBare(t, pair.bareSQL, pair.explicitSQL)
		})
	}

	starStar := parseGrantBare(t, "grant select on *.* to r1", "grant select on table *.* to r1")
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR, starStar.GrantPrivilege.Level.Level)

	dbStar := parseGrantBare(t, "grant select on db.* to r1", "grant select on table db.* to r1")
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR, dbStar.GrantPrivilege.Level.Level)
	require.Equal(t, "db", dbStar.GrantPrivilege.Level.DbName)

	dbTbl := parseGrantBare(t, "grant select on db.tbl to r1", "grant select on table db.tbl to r1")
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE, dbTbl.GrantPrivilege.Level.Level)
	require.Equal(t, "db", dbTbl.GrantPrivilege.Level.DbName)
	require.Equal(t, "tbl", dbTbl.GrantPrivilege.Level.TabName)

	tbl := parseGrantBare(t, "grant select on tbl to r1", "grant select on table tbl to r1")
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_TABLE, tbl.GrantPrivilege.Level.Level)
	require.Equal(t, "tbl", tbl.GrantPrivilege.Level.TabName)
}

func TestGrantBareMultiPrivilegeAndGrantOption(t *testing.T) {
	grant := parseGrantBare(t,
		"grant select, insert, update on db.tbl to r1 with grant option",
		"grant select, insert, update on table db.tbl to r1 with grant option")
	require.Len(t, grant.GrantPrivilege.Privileges, 3)
	require.True(t, grant.GrantPrivilege.GrantOption)
}

func TestRevokeBareObjectLevelsMatchExplicit(t *testing.T) {
	pairs := []struct {
		bareSQL     string
		explicitSQL string
	}{
		{"revoke select on *.* from r1", "revoke select on table *.* from r1"},
		{"revoke select on db.* from r1", "revoke select on table db.* from r1"},
		{"revoke select on db.tbl from r1", "revoke select on table db.tbl from r1"},
		{"revoke select on tbl from r1", "revoke select on table tbl from r1"},
		{"revoke select, update on db.tbl from r1", "revoke select, update on table db.tbl from r1"},
		{"revoke if exists select on db.* from r1", "revoke if exists select on table db.* from r1"},
	}
	for _, pair := range pairs {
		t.Run(pair.bareSQL, func(t *testing.T) {
			bare, err := ParseOne(context.Background(), pair.bareSQL, 1)
			require.NoError(t, err, pair.bareSQL)
			explicit, err := ParseOne(context.Background(), pair.explicitSQL, 1)
			require.NoError(t, err, pair.explicitSQL)

			bareRevoke, ok := bare.(*tree.Revoke)
			require.True(t, ok, pair.bareSQL)
			require.Equal(t, tree.RevokeTypePrivilege, bareRevoke.Typ, pair.bareSQL)
			require.Equal(t, tree.OBJECT_TYPE_TABLE, bareRevoke.RevokePrivilege.ObjType, pair.bareSQL)

			require.Equal(t, explicit, bare, pair.bareSQL)
			bareFormatted := tree.String(bare, dialect.MYSQL)
			require.Equal(t, tree.String(explicit, dialect.MYSQL), bareFormatted, pair.bareSQL)
			reparsed, err := ParseOne(context.Background(), bareFormatted, 1)
			require.NoError(t, err, bareFormatted)
			require.Equal(t, bare, reparsed, bareFormatted)
		})
	}

	ifExists, err := ParseOne(context.Background(), "revoke if exists select on db.* from r1", 1)
	require.NoError(t, err)
	revoke, ok := ifExists.(*tree.Revoke)
	require.True(t, ok)
	require.True(t, revoke.RevokePrivilege.IfExists)
}

func TestGrantBareQuotedIdentifiers(t *testing.T) {
	// Identifier spellings that collide with object_type keywords stay usable
	// via quoting in the bare form.
	grant := parseGrantBare(t,
		"grant select on `account` to r1",
		"grant select on table `account` to r1")
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_TABLE, grant.GrantPrivilege.Level.Level)
	require.Equal(t, "account", grant.GrantPrivilege.Level.TabName)

	// Identifiers that require quoting also parse in the bare form and match
	// the explicit keyword form. Format/reparse is not asserted here: the
	// pre-existing tree formatter drops the required backticks when replaying
	// such names (formatter behavior outside this batch's scope, identical
	// for both spellings).
	bare, err := ParseOne(context.Background(), "grant select on `my db`.`my tbl` to r1", 1)
	require.NoError(t, err)
	explicit, err := ParseOne(context.Background(), "grant select on table `my db`.`my tbl` to r1", 1)
	require.NoError(t, err)
	bareGrant, ok := bare.(*tree.Grant)
	require.True(t, ok)
	require.Equal(t, tree.GrantTypePrivilege, bareGrant.Typ)
	require.Equal(t, tree.OBJECT_TYPE_TABLE, bareGrant.GrantPrivilege.ObjType)
	require.Equal(t, explicit, bare)
	require.Equal(t, tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE, bareGrant.GrantPrivilege.Level.Level)
	require.Equal(t, "my db", bareGrant.GrantPrivilege.Level.DbName)
	require.Equal(t, "my tbl", bareGrant.GrantPrivilege.Level.TabName)
}

func TestGrantBarePreservesOtherBranches(t *testing.T) {
	t.Run("role grant with admin option unchanged", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "grant r1 to u1 with admin option", 1)
		require.NoError(t, err)
		grant, ok := stmt.(*tree.Grant)
		require.True(t, ok)
		require.Equal(t, tree.GrantTypeRole, grant.Typ)
		require.True(t, grant.GrantRole.GrantOption)
	})

	t.Run("proxy grant unchanged", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "grant proxy on 'p'@'%' to 'u1'@'%'", 1)
		require.NoError(t, err)
		grant, ok := stmt.(*tree.Grant)
		require.True(t, ok)
		require.Equal(t, tree.GrantTypeProxy, grant.Typ)
	})

	t.Run("explicit object types unchanged", func(t *testing.T) {
		for _, tc := range []struct {
			sql    string
			objTyp tree.ObjectType
		}{
			{"grant select on account * to r1", tree.OBJECT_TYPE_ACCOUNT},
			{"grant create on database * to r1", tree.OBJECT_TYPE_DATABASE},
			{"grant execute on function db.f to r1", tree.OBJECT_TYPE_FUNCTION},
			{"grant select on procedure db.p to r1", tree.OBJECT_TYPE_PROCEDURE},
			{"grant select on view db.v to r1", tree.OBJECT_TYPE_VIEW},
		} {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err, tc.sql)
			grant, ok := stmt.(*tree.Grant)
			require.True(t, ok, tc.sql)
			require.Equal(t, tree.GrantTypePrivilege, grant.Typ, tc.sql)
			require.Equal(t, tc.objTyp, grant.GrantPrivilege.ObjType, tc.sql)
		}
	})

	t.Run("revoke role unchanged", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "revoke r1 from u1", 1)
		require.NoError(t, err)
		revoke, ok := stmt.(*tree.Revoke)
		require.True(t, ok)
		require.Equal(t, tree.RevokeTypeRole, revoke.Typ)
	})

	t.Run("admin option still rejected on privilege grants", func(t *testing.T) {
		for _, sql := range []string{
			"grant select on db.* to r1 with admin option",
			"grant select on table db.* to r1 with admin option",
		} {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		}
	})
}
