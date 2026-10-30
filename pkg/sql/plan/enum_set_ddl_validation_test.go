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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func buildEnumSetDDLForTest(t *testing.T, sql string) error {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	return err
}

// A constant ENUM/SET DEFAULT that is not a member is ER_INVALID_DEFAULT
// (1067) at DDL time, like MySQL. It used to be accepted, and every INSERT
// that relied on the default then failed with 1265.
func TestEnumSetDefaultMustBeMember(t *testing.T) {
	for _, def := range []string{
		"e enum('a','b') default 'zz'",
		"e enum('a','b') default ''",
		"e enum('a','b') default 3",
		"e enum('a','b') default 0",
		"e enum('a','b') not null default 'c'",
		"s set('a','b') default 'c'",
		"s set('a','b') default 'a,c'",
		"s set('a','b') default 4",
	} {
		t.Run("reject "+def, func(t *testing.T) {
			err := buildEnumSetDDLForTest(t, "create table dd(id int, "+def+")")
			require.Error(t, err, def)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidDefault), "%s: %v", def, err)
		})
	}
	for _, def := range []string{
		"e enum('a','b') default 'a'",
		"e enum('a','b') default 'B'",
		"e enum('a','b') default 'a '",
		"e enum('a','b') default null",
		"e enum('','a') default ''",
		"e enum('T','E') not null default 2",
		"e enum('a','b') default ('zz')",
		"s set('a','b') default 'b,a'",
		"s set('a','b') default ''",
		"s set('a','b') default 3",
		"s set('a','b') default null",
	} {
		t.Run("accept "+def, func(t *testing.T) {
			require.NoError(t, buildEnumSetDDLForTest(t, "create table dd(id int, "+def+")"), def)
		})
	}
}

// A member listed twice is ER_DUPLICATED_VALUE_IN_TYPE (1291), like MySQL in
// strict mode. The default collation folds case; a binary collation keeps
// case-distinct members. ENUM keeps raw padded labels, so 'a ' and 'a' stay
// distinct members there.
func TestEnumSetDuplicateMembers(t *testing.T) {
	for _, def := range []string{
		"e enum('a','a')",
		"e enum('a','b','A')",
		"s set('a ','a')",
		"s set('a','a')",
		"s set('x','y','X')",
	} {
		t.Run("reject "+def, func(t *testing.T) {
			err := buildEnumSetDDLForTest(t, "create table dd(id int, "+def+")")
			require.Error(t, err, def)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicatedValueInType), "%s: %v", def, err)
			require.Contains(t, err.Error(), "has duplicated value")
		})
	}
	for _, def := range []string{
		"e enum('a','b')",
		"e enum('a','A') collate utf8mb4_bin",
		"e enum('a','ab',' a')",
		"e enum('a ','a')",
		"s set('a','b')",
	} {
		t.Run("accept "+def, func(t *testing.T) {
			require.NoError(t, buildEnumSetDDLForTest(t, "create table dd(id int, "+def+")"), def)
		})
	}
}
