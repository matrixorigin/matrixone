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

package api

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNewRenameColumnReqWithChecks(t *testing.T) {
	checks := []*plan.CheckDef{{OriginSql: "CHECK (`new_col` > 0)"}}
	req := NewRenameColumnReqWithChecks(1, 2, "old_col", "new_col", 3, checks)

	rename := req.GetRenameCol()
	require.Equal(t, "CHECK (`new_col` > 0)", rename.GetChecks()[0].GetOriginSql())
	require.NotSame(t, checks[0], rename.GetChecks()[0])

	checks[0].OriginSql = "mutated"
	require.Equal(t, "CHECK (`new_col` > 0)", rename.GetChecks()[0].GetOriginSql())

	data, err := req.Marshal()
	require.NoError(t, err)
	var decoded AlterTableReq
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, "CHECK (`new_col` > 0)", decoded.GetRenameCol().GetChecks()[0].GetOriginSql())
}
