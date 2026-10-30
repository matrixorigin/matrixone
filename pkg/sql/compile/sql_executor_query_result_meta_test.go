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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type queryResultMetaDelegate struct {
	*plan.MockCompilerContext
	uuid string
	cols []*plan.ColDef
	path string
}

func (d *queryResultMetaDelegate) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	d.uuid = uuid
	return d.cols, d.path, nil
}

// Issue #31536: CTAS populates its target through the internal executor, so
// CREATE TABLE ... AS SELECT ... FROM result_scan(...) binds RESULT_SCAN
// against this compiler context. It must read the saved result through the
// issuing session instead of panicking.
func TestCompilerContextQueryResultMetaDelegatesToSession(t *testing.T) {
	delegate := &queryResultMetaDelegate{
		MockCompilerContext: plan.NewMockCompilerContext(false, nil),
		cols:                []*plan.ColDef{{Name: "id"}, {Name: "name"}},
		path:                "query_result/sys_0.blk",
	}
	c := &compilerContext{
		ctx:  attachInternalExecutorCompilerContext(context.Background(), delegate),
		proc: testutil.NewProcess(t),
	}
	var (
		cols []*plan.ColDef
		path string
		err  error
	)
	require.NotPanics(t, func() { cols, path, err = c.GetQueryResultMeta("qid-1") })
	require.NoError(t, err)
	require.Equal(t, "qid-1", delegate.uuid)
	require.Equal(t, delegate.cols, cols)
	require.Equal(t, delegate.path, path)

	// Without a frontend session there is no saved result to read: report an
	// error rather than panicking.
	bare := &compilerContext{ctx: context.Background(), proc: testutil.NewProcess(t)}
	require.NotPanics(t, func() { _, _, err = bare.GetQueryResultMeta("qid-1") })
	require.Error(t, err)
}
