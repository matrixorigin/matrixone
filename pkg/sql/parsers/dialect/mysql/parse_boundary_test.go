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

package mysql

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

type foreignParsePanicContext struct {
	context.Context
	failure any
}

func (c foreignParsePanicContext) Err() error { panic(c.failure) }

func TestParseBudgetRethrowsForeignPanics(t *testing.T) {
	for _, failure := range []any{"foreign panic", parseAbort{control: &parseControl{}, err: ErrParseLimit}} {
		ctx := WithParseLimits(foreignParsePanicContext{Context: context.Background(), failure: failure}, ParseLimits{Input: 100, Tokens: 100, Work: 1000})
		require.PanicsWithValue(t, failure, func() { _, _ = ParseWithSQLMode(ctx, "SELECT 1", 0, "") })
	}
	// A failed owner must not contaminate the scanner pool or the next parse.
	statements, err := ParseWithSQLMode(t.Context(), "SELECT 1", 0, "")
	require.NoError(t, err)
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()
	require.Len(t, statements, 1)
}

func TestStatementDigestIndependentPassBudgets(t *testing.T) {
	ctx := WithParseLimits(t.Context(), ParseLimits{Input: 100, Tokens: 3, Work: 100})
	text, hash, err := StatementDigest(ctx, "SELECT 1", "")
	require.NoError(t, err)
	require.Equal(t, "SELECT ?", text)
	require.Equal(t, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae", hash)
}
