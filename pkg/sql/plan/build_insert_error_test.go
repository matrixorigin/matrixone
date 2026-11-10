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

package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type insertResolveErrorContext struct {
	*MockCompilerContext
	err   error
	calls int
}

func (ctx *insertResolveErrorContext) Resolve(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	ctx.calls++
	return nil, nil, ctx.err
}

func TestInsertBindingPropagatesResolveErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		onDuplicate bool
		calls       int
	}{
		{name: "deadline", err: context.DeadlineExceeded, calls: 1},
		{name: "cancel", err: context.Canceled, calls: 1},
		{name: "plain error", err: errors.New("catalog unavailable"), calls: 1},
		{name: "moerr", err: moerr.NewInternalErrorNoCtx("catalog unavailable"), calls: 1},
		{name: "unsupported plain insert falls back", err: moerr.NewUnsupportedDML(context.Background(), "test"), calls: 2},
		{name: "unsupported upsert does not fall back", err: moerr.NewUnsupportedDML(context.Background(), "test"), onDuplicate: true, calls: 1},
		{name: "no key upsert falls back", err: moerr.NewUnsupportedDML(context.Background(), noPkOnDupUpdateCause), onDuplicate: true, calls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &insertResolveErrorContext{MockCompilerContext: NewMockCompilerContext(true), err: tc.err}
			sql := "insert into t values (1)"
			if tc.onDuplicate {
				sql += " on duplicate key update a=1"
			}
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			actual, err := bindAndOptimizeInsertQuery(ctx, stmt.(*tree.Insert), false, false)
			require.Nil(t, actual)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.calls, ctx.calls, "保留原有 UnsupportedDML fallback 边界")
		})
	}
}
