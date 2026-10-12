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

package frontend

import (
	"context"
	"math/rand"
	"regexp"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBackgroundRestoreAnnotationLanguage(t *testing.T) {
	old := regexp.MustCompile("MO_TS.*=")
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"", false}, {"MO_TS", false}, {"MO_TS=", true}, {"=MO_TS", false},
		{"mo_ts=", false}, {"MO_TS\n=", false}, {"MO_TS\r\n=", false},
		{"MO_TS\r=", true}, {"MO_TS\u2028=", true}, {"MO_TS\u2029=", true},
		{"MO_TS\x00=", true}, {"MO_TS\xff=", true}, {"MO_\xffTS=", false},
		{"MO_\nTS=", false}, {"MO_TS MO_TS =", true},
		{"MO_TS\nMO_TS =", true}, {"MO_TS\n=MO_TS", false},
		{"'MO_TS ='", true}, {"/* MO_TS = */", true},
		{strings.Repeat("MO_TS\n", 1000) + "MO_TS=", true},
	} {
		require.Equal(t, tc.want, old.MatchString(tc.sql), "old language: %q", tc.sql)
		require.Equal(t, tc.want, isBackgroundRestoreSQL(&tree.Insert{}, tc.sql), "%q", tc.sql)
		require.Equal(t, tc.want, isBackgroundRestoreSQL(&tree.CloneTable{}, tc.sql), "%q", tc.sql)
		require.False(t, isBackgroundRestoreSQL(&tree.Select{}, tc.sql), "%q", tc.sql)
	}

	rng := rand.New(rand.NewSource(42))
	parts := []string{"MO_TS", "=", "\n", "\r", "mo_ts", "\u2028", "\xff", "\x00", "xyz", "'", "/*", "*/"}
	for range 500 {
		var sql strings.Builder
		for range rng.Intn(64) {
			sql.WriteString(parts[rng.Intn(len(parts))])
		}
		input := sql.String()
		require.Equal(t, old.MatchString(input), isBackgroundRestoreSQL(&tree.Insert{}, input), "%q", input)
	}
}

func FuzzBackgroundRestoreAnnotationLanguage(f *testing.F) {
	for _, sql := range []string{"", "MO_TS=", "MO_TS\r\n=", "MO_TS\nMO_TS =", "MO_TS\xff=", "MO_TS\u2028="} {
		f.Add(sql)
	}
	old := regexp.MustCompile("MO_TS.*=")
	f.Fuzz(func(t *testing.T, sql string) {
		require.Equal(t, old.MatchString(sql), isBackgroundRestoreSQL(&tree.Insert{}, sql))
	})
}

func TestBackgroundRestoreClassificationUsesParsedStatement(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	fs := getPu(ses.GetService()).FileService
	t.Cleanup(func() { ses.Close(); fs.Close(context.Background()) })
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	back := ses.InitBackExec(nil, "", fakeDataSetFetcher2).(*backExec)
	t.Cleanup(back.Close)
	original := GetComputationWrapperInBack
	t.Cleanup(func() { GetComputationWrapperInBack = original })
	calls := 0
	gotRestore := false
	GetComputationWrapperInBack = func(_ *ExecCtx, _ string, input *UserInput, _ string, _ engine.Engine, _ *process.Process, _ FeSession) ([]ComputationWrapper, error) {
		calls++
		gotRestore = input.isRestore
		return nil, nil
	}
	// The first parser and classification run through the real background owner.
	// Capture the downstream input without opening an unrelated transaction.
	for _, sql := range []string{"", "  ", "/* comment */", "/* MO_TS = 42 */", "-- MO_TS = 42\n"} {
		back.SetLastAffectedRows(17)
		require.NoError(t, back.Exec(ctx, sql))
		require.Equal(t, int64(17), back.GetLastAffectedRows())
		require.False(t, gotRestore)
		require.NoError(t, back.ExecWithSQLMode(ctx, sql, "ANSI_QUOTES"))
		require.False(t, gotRestore)
	}

	for _, tc := range []struct {
		name, sql string
		want      bool
	}{
		{"select", "select 1 /* MO_TS = 42 */", false},
		{"DDL", "create table x(id int) /* MO_TS = 42 */", false},
		{"ordinary insert", "insert into x values (1)", false},
		{"annotated insert", "insert into x values (1) /* MO_TS = 42 */", true},
		{"lowercase", "insert into x values (1) /* mo_ts = 42 */", false},
		{"newline", "insert into x values (1) /* MO_TS\n= 42 */", false},
		{"later marker", "insert into x values (1) /* MO_TS\nMO_TS = 42 */", true},
		{"clone timestamp", restoreTableDataByTsSQL("db", "x", 42), true},
		{"clone snapshot", restoreTableDataByNameSQL("db", "x", "snap"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls
			require.NoError(t, back.Exec(ctx, tc.sql))
			require.Equal(t, before+1, calls)
			require.Equal(t, tc.want, gotRestore)
		})
	}
	for _, sql := range []string{"select (", "select 1; select 2"} {
		before := calls
		require.Error(t, back.Exec(ctx, sql))
		require.Equal(t, before, calls)
	}
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	txn.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	back.backSes.GetTxnHandler().SetShareTxn(txn)
	t.Cleanup(func() {
		if back.backSes != nil {
			back.backSes.GetTxnHandler().SetShareTxn(nil)
		}
	})
	for _, sql := range []string{"begin", "commit", "rollback"} {
		before := calls
		require.ErrorContains(t, back.Exec(ctx, sql), "share transaction")
		require.Equal(t, before, calls)
	}
	// The mock shared transaction has no live resource to roll back on Close.
	back.backSes.GetTxnHandler().SetShareTxn(nil)
}
