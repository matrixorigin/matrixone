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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func extWriteCreatesql(t *testing.T, pattern string) string {
	opt := []string{"format", "csv"}
	if pattern != "" {
		opt = append(opt, "write_file_pattern", pattern)
	}
	raw, err := json.Marshal(&tree.ExternParam{ExParamConst: tree.ExParamConst{Option: opt}})
	require.NoError(t, err)
	return string(raw)
}

func TestIsExternalWriteInsert(t *testing.T) {
	// nil InsertCtx
	require.False(t, isExternalWriteInsert(&plan.Node{}))

	// nil TableDef
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{}}))

	// regular (non-external) table
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
	}}))

	// external table but read-only (no write_file_pattern)
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, ""),
		},
	}}))

	// external table with malformed Createsql
	require.False(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: "{not json",
		},
	}}))

	// writable external table
	require.True(t, isExternalWriteInsert(&plan.Node{InsertCtx: &plan.InsertCtx{
		TableDef: &plan.TableDef{
			TableType: catalog.SystemExternalRel,
			Createsql: extWriteCreatesql(t, "stage://s/p-%U.csv"),
		},
	}}))
}
