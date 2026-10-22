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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestLoadSamplesWithStatementFileAccess(t *testing.T) {
	for _, tc := range []struct {
		name, account, role, data, tail string
		denied                          bool
	}{
		{name: "sample", account: "sys", role: "moadmin", data: "1,a,1,b\n2,c,2,d\n"},
		{name: "header", account: "sys", role: "moadmin", data: "id,name,region,comment\n1,a,1,b\n2,c,2,d\n", tail: " ignore 1 lines"},
		{name: "server file denied", account: "tenant", role: "accountadmin", data: "1,a,1,b\n2,c,2,d\n", denied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "sample.csv")
			require.NoError(t, os.WriteFile(file, []byte(tc.data), 0600))
			proc := newPlanTestProcess(t)
			proc.Base.SessionInfo.Account = tc.account
			proc.Base.SessionInfo.Role = tc.role
			ctx := NewMockCompilerContext(true, proc)
			// The compiler context has no ETL policy: LOAD must bind the
			// statement identity for sampling, just as for file preflight.
			ctx.SetContext(t.Context())
			// Reuse catalog-complete mock metadata, with a wide declared payload
			// whose on-disk rows are short.
			table := ctx.tables["nation"]
			table.Cols[table.Name2ColIndex["n_comment"]].Typ = Type{Id: int32(types.T_blob), Width: 65535}
			stmt, err := mysql.ParseOne(t.Context(), fmt.Sprintf("load data infile '%s' into table nation fields terminated by ','%s parallel 'true'", file, tc.tail), 1)
			require.NoError(t, err)
			defer stmt.Free()
			pn, err := BuildPlan(ctx, stmt, false)
			if tc.denied {
				require.Error(t, err)
				require.Contains(t, err.Error(), "moadmin")
				return
			}
			require.NoError(t, err)
			var scan *planpb.Node
			modern := false
			for _, node := range pn.GetQuery().Nodes {
				modern = modern || node.NodeType == planpb.Node_MULTI_UPDATE
				if node.NodeType == planpb.Node_EXTERNAL_SCAN {
					require.Nil(t, scan)
					scan = node
				}
			}
			require.True(t, modern, "exercise the modern LOAD binder")
			require.NotNil(t, scan)
			require.Equal(t, float64(len("1,a,1,b\n")), scan.Stats.Rowsize, "sample actual bytes instead of the declared BLOB capacity")
			require.Equal(t, float64(2), scan.Stats.Outcnt)
		})
	}
}
