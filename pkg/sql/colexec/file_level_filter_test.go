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

package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestFileLevelClassifierPolicies(t *testing.T) {
	node := &plan.Node{TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: catalog.ExternalQuery, ColId: catalog.ExternalQueryColId}}}, ExternScan: &plan.ExternScan{}}
	col := &plan.ColRef{ColPos: 0}
	require.False(t, FileLevelPlanner.IsFileLevelColumn(node, col))
	require.True(t, FileLevelExecution.IsFileLevelColumn(node, col))
	node.TableDef.Cols[0].ColId++
	require.False(t, FileLevelExecution.IsFileLevelColumn(node, col))
	node.TableDef.Cols[0] = &plan.ColDef{Name: catalog.ExternalFilePath, ColId: catalog.ExternalFilePathColId}
	require.True(t, FileLevelPlanner.IsFileLevelColumn(node, col))
	require.True(t, FileLevelExecution.IsFileLevelColumn(node, col))
	fn, err := function.GetFunctionByName(context.Background(), "last_kafka_message_id", nil)
	require.NoError(t, err)
	ref := &plan.ObjectRef{Obj: fn.GetEncodedOverloadID()}
	require.False(t, FileLevelPlanner.IsSafeFileLevelFunction(ref))
	require.True(t, FileLevelExecution.IsSafeFileLevelFunction(ref))
}
