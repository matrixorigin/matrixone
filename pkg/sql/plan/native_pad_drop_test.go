// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestNativePADDropRetainsAdmissionMetadata(t *testing.T) {
	ctx, build := nativePADDDLContext(t)
	table, err := build("create table n (s varchar(8) collate utf8mb4_general_ci)")
	require.NoError(t, err)
	view, err := build("create view v as select 'A' collate utf8mb4_general_ci = 'a' as b")
	require.NoError(t, err)
	ctx.tables["n"] = table.GetDdl().GetCreateTable().TableDef
	ctx.tables["v"] = view.GetDdl().GetCreateView().TableDef
	for _, name := range []string{"n", "v"} {
		ctx.objects[name] = &ObjectRef{ObjName: name, SchemaName: "tpch"}
	}
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, sql := range []string{"drop table n", "drop table n, n", "drop view v"} {
		t.Run(sql, func(t *testing.T) {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion130)
			p, err := build(sql)
			require.NoError(t, err)
			drop := p.GetDdl().GetDropTable()
			if len(drop.Tables) != 0 {
				drop = drop.Tables[0]
			}
			require.NotNil(t, drop.TableDef)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion120)
			_, err = build(sql)
			require.ErrorContains(t, err, "protocol version 130")
		})
	}
	_, err = build("drop table if exists absent")
	require.NoError(t, err)
}
