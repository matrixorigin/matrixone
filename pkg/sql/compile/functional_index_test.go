// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreInsertKeepsHiddenGeneratedValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	node := &plan.Node{Children: []int32{0}, PreInsertCtx: &plan.PreInsertCtx{
		Ref: &plan.ObjectRef{},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: "b"},
			{Name: "__mo_fi_test", Hidden: true, GeneratedCol: &plan.GeneratedCol{}},
			{Name: catalog.CPrimaryKeyColName, Hidden: true},
			{Name: catalog.Row_ID, Hidden: true},
		}},
	}}
	op, err := constructPreInsert([]*plan.Node{{Stats: &plan.Stats{}}}, node, nil, proc)
	require.NoError(t, err)
	defer op.Release()
	require.Equal(t, []string{"a", "b", "__mo_fi_test"}, op.Attrs)
}
