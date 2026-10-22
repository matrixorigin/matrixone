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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

type hnswRetryCompilerContext struct {
	plan.CompilerContext
	observed bool
}

func (c *hnswRetryCompilerContext) GetContext() context.Context {
	ctx := c.CompilerContext.GetContext()
	if ctx != nil && ctx.Value(defines.HnswSnapshotFallbackKey{}) == true {
		c.observed = true
	}
	return ctx
}
func TestHnswSnapshotRetryPlanningContext(t *testing.T) {
	for _, sql := range []string{"select 1", "select missing_column"} {
		t.Run(sql, func(t *testing.T) {
			original := defines.AttachAccountId(context.Background(), catalog.System_Account)
			ctx := context.WithValue(original, defines.HnswSnapshotFallbackKey{}, true)
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			compiler := &hnswRetryCompilerContext{CompilerContext: plan.NewEmptyCompilerContext(newPlanTestProcess(t))}
			compiler.SetContext(original)
			_, err = buildPlanForCompileRetry(ctx, nil, compiler, stmt, false, nil)
			if sql == "select 1" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.True(t, compiler.observed, "actual planning must receive the source-scan policy")
			require.Same(t, original, compiler.GetContext(), "success and failure must restore the reusable compiler context")
		})
	}
}
