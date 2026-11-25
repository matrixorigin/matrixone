// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// The marker-free restored-window inference guard accepts exactly one authored
// ORDER BY — the inner (release-authored window body) or the outer (the
// already accepted projection ordering) — and still rejects duplicate ORDER,
// the inner LIMIT, and the WITH-bearing shapes before any inference runs.
type restoredWindowTestCtx struct{ CompilerContext }

func (restoredWindowTestCtx) GetContext() context.Context {
	return context.WithValue(context.Background(), restoredViewDefinitionKey{}, restoredViewDefinition{
		database:   "db1",
		name:       "v1",
		createSQL:  "create view v1 as (select count(*) as a from t interval(ts, 1, minute)) interval(ts, 2, minute) limit 5",
		definition: `{"viewsql":"create view v1 as (select count(*) as a from t interval(ts, 1, minute)) interval(ts, 2, minute) limit 5"}`,
	})
}

func restoredGuardSelect(t *testing.T, body string) (*tree.Select, bool) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, body, 1)
	require.NoError(t, err)
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, "expected *tree.Select for: %s", body)
	return restoredLegacyInnerWindow(restoredWindowTestCtx{}, "db1", "v1", sel)
}

func TestRestoredInnerWindowOrderGuardAdmitsSingleInnerOrOuterOnly(t *testing.T) {
	// the release-authored inner ORDER shape (no outer ORDER): admitted
	inner, ok := restoredGuardSelect(t, "(select count(*) as a from t interval(ts, 1, minute) order by a) interval(ts, 2, minute) limit 5")
	require.True(t, ok)
	require.NotNil(t, inner)
	// the already-accepted sole outer ORDER shape: admitted
	_, ok = restoredGuardSelect(t, "(select count(*) as a from t interval(ts, 1, minute)) interval(ts, 2, minute) order by a limit 5")
	require.True(t, ok)
	// no ORDER at all: admitted
	_, ok = restoredGuardSelect(t, "(select count(*) as a from t interval(ts, 1, minute)) interval(ts, 2, minute) limit 5")
	require.True(t, ok)
	// duplicate ORDER (inner+outer): rejected before any inference
	_, ok = restoredGuardSelect(t, "(select count(*) as a from t interval(ts, 1, minute) order by a) interval(ts, 2, minute) order by a limit 5")
	require.False(t, ok)
	// inner LIMIT: still vetoed
	_, ok = restoredGuardSelect(t, "(select count(*) as a from t interval(ts, 1, minute) order by a limit 3) interval(ts, 2, minute) limit 5")
	require.False(t, ok)
	// an inner WITH: still rejected
	_, ok = restoredGuardSelect(t, "(with x as (select 1 as b) select count(*) from x, t interval(ts, 1, minute) order by a) interval(ts, 2, minute) limit 5")
	require.False(t, ok)
	// a shape outside the restored-window family entirely: no gate effect
	_, ok = restoredGuardSelect(t, "select a from t")
	require.False(t, ok)
}
