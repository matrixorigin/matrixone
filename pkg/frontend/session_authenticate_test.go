// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
)

func TestResolveImplicitDefaultRole(t *testing.T) {
	const (
		userID   = int64(42)
		readerID = int64(7)
	)
	ctx := context.Background()
	readerSQL := getSqlForRoleNameOfUserRole(userID, readerID)
	publicSQL := getSqlForRoleNameOfUserRole(userID, publicRoleID)

	t.Run("granted default role", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{"reader"}})

		roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.NoError(t, err)
		require.Equal(t, readerID, roleID)
		require.Equal(t, "reader", roleName)
		require.Equal(t, []string{readerSQL}, bh.executedSQLs)
	})

	for _, name := range []string{
		"revoked default role",
		"missing default role metadata",
		"NULL default role name",
		"empty default role name",
	} {
		t.Run(name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			switch name {
			case "NULL default role name":
				bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{nil}})
			case "empty default role name":
				bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{""}})
			default:
				bh.sql2result[readerSQL] = newMrsForRoleName(nil)
			}
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{publicRoleName}})

			roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
			require.NoError(t, err)
			require.Equal(t, int64(publicRoleID), roleID)
			require.Equal(t, publicRoleName, roleName)
			require.Equal(t, []string{readerSQL, publicSQL}, bh.executedSQLs)
		})
	}

	for _, tc := range []struct {
		name  string
		id    int64
		valid bool
	}{
		{name: "NULL catalog default", valid: false},
		{name: "negative catalog default", id: -1, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{publicRoleName}})

			roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, tc.id, tc.valid)
			require.NoError(t, err)
			require.Equal(t, int64(publicRoleID), roleID)
			require.Equal(t, publicRoleName, roleName)
			require.Equal(t, []string{publicSQL}, bh.executedSQLs)
		})
	}

	t.Run("regrant restores stored default", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{"reader"}})

		roleID, roleName, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.NoError(t, err)
		require.Equal(t, readerID, roleID)
		require.Equal(t, "reader", roleName)
	})

	t.Run("missing public grant rejects login", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName(nil)
		bh.sql2result[publicSQL] = newMrsForRoleName(nil)

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorContains(t, err, "get a valid default role")
	})

	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{name: "NULL public role name", value: nil},
		{name: "empty public role name", value: ""},
		{name: "mismatched public role name", value: "reader"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[publicSQL] = newMrsForRoleName([][]interface{}{{tc.value}})

			_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, 0, false)
			require.ErrorContains(t, err, "get a valid default role")
		})
	}

	t.Run("catalog query error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		wantErr := moerr.NewInternalErrorNoCtx("role lookup failed")
		bh.sql2err[readerSQL] = wantErr

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("public fallback query error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName(nil)
		wantErr := moerr.NewInternalErrorNoCtx("public role lookup failed")
		bh.sql2err[publicSQL] = wantErr

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, []string{readerSQL, publicSQL}, bh.executedSQLs)
	})

	t.Run("invalid role name type is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = newMrsForRoleName([][]interface{}{{struct{}{}}})

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.Error(t, err)
	})

	t.Run("role name NULL check error is returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		result := mock_frontend.NewMockExecResult(ctrl)
		result.EXPECT().GetRowCount().Return(uint64(1))
		wantErr := moerr.NewInternalErrorNoCtx("role name NULL check failed")
		result.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(0)).Return(false, wantErr)

		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[readerSQL] = result

		_, _, err := resolveImplicitDefaultRole(ctx, bh, userID, readerID, true)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestReadStoredDefaultRoleID(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		value     interface{}
		wantID    int64
		wantValid bool
	}{
		{name: "valid", value: int64(7), wantID: 7, wantValid: true},
		{name: "NULL", value: nil},
		{name: "negative", value: int64(-1)},
		{name: "out of range", value: uint64(^uint32(0)) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := newMrsForPasswordOfUser([][]interface{}{{int64(42), "password", tc.value}})
			roleID, valid, err := readStoredDefaultRoleID(ctx, result)
			require.NoError(t, err)
			require.Equal(t, tc.wantID, roleID)
			require.Equal(t, tc.wantValid, valid)
		})
	}

	t.Run("missing row returns error", func(t *testing.T) {
		result := newMrsForPasswordOfUser(nil)
		_, _, err := readStoredDefaultRoleID(ctx, result)
		require.Error(t, err)
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		result := newMrsForPasswordOfUser([][]interface{}{{int64(42), "password", struct{}{}}})
		_, _, err := readStoredDefaultRoleID(ctx, result)
		require.Error(t, err)
	})
}
