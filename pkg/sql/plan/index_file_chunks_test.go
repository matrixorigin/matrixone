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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestIndexFileChunksInternalOnly(t *testing.T) {
	const sql = "select index_id, chunk_id, data, tag from index_file_chunks('x', 0, 1) as f"

	opt := NewMockOptimizer(false, newPlanTestProcess(t))
	_, err := runOneStmt(opt, t, sql)
	require.ErrorContains(t, err, "not supported", "user SQL cannot call index_file_chunks")

	opt = NewMockOptimizer(false, newPlanTestProcess(t))
	mc := opt.CurrentContext().(*MockCompilerContext)
	mc.SetContext(defines.AttachInternalExecutor(mc.GetContext()))
	p, err := runOneStmt(opt, t, sql)
	require.NoError(t, err)
	var found bool
	for _, node := range p.GetQuery().GetNodes() {
		if node.GetTableDef().GetTblFunc().GetName() == indexFileChunksFuncName {
			found = true
			require.Len(t, node.TableDef.Cols, 4)
		}
	}
	require.True(t, found)
}
