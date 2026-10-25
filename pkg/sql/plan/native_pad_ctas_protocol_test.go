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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestNativePADCTASSourceDependencyDoesNotChangeBinding(t *testing.T) {
	for _, view := range []bool{false, true} {
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("view_%t/prepare_%t", view, prepared), func(t *testing.T) {
				ctx, build := nativePADDDLContext(t)
				sql := "create table native_source (s varchar(8) collate utf8mb4_general_ci)"
				projection := "s='a' as b"
				if view {
					sql = "create view tpch.native_source as select 'A' collate utf8mb4_general_ci='a' as b"
					projection = "b"
				}
				source, err := build(sql)
				require.NoError(t, err)
				var table *TableDef
				if view {
					table = source.GetDdl().GetCreateView().TableDef
					table.TableType = catalog.SystemViewRel
				} else {
					table = source.GetDdl().GetCreateTable().TableDef
				}
				ctx.tables["native_source"] = table
				ctx.objects["native_source"] = &ObjectRef{SchemaName: "tpch", ObjName: "native_source", Obj: 42}
				parent := WithCollationBindingRevision(ctx.GetContext(), 0)
				ctx.SetContext(parent)
				rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
				buildTarget := func() (*Plan, error) {
					stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "create table n as select "+projection+" from tpch.native_source", 1)
					require.NoError(t, err)
					defer stmt.Free()
					return BuildPlan(ctx, stmt, prepared)
				}
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion124)
				p, err := buildTarget()
				require.ErrorContains(t, err, "protocol version 132")
				require.Nil(t, p)
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion132)
				p, err = buildTarget()
				require.NoError(t, err)
				create := p.GetDdl().GetCreateTable()
				require.Equal(t, defines.MORPCVersion132, create.RequiredProtocolVersion)
				require.Zero(t, create.CollationBindingRevision, "来源native类型不是外层显式COLLATE代际")
				require.Same(t, parent, ctx.GetContext())
			})
		}
	}
}

func TestNativePADCTASKeepsExecutionAndAuthoringIndependent(t *testing.T) {
	for _, tc := range []struct {
		name, projection string
		execution        int64
		native           bool
	}{
		{"timestamp_only", "timestamp('2024-02-29 12:34:56') as d", defines.MORPCVersion122, false},
		{"legacy_cast", "cast('2024-02-29' as datetime) as d", 0, false},
		// Field 19 retains the upstream TIMESTAMP/TIME result-contract scope;
		// fractional SECOND execution must not become a native authoring floor.
		{"fractional_second", "timestampadd(second, 0.5, cast('2024-02-29 12:34:56' as datetime)) as d", 0, false},
		{"fractional_second_timestamp", "timestampadd(second, 0.5, timestamp('2024-02-29 12:34:56')) as d", defines.MORPCVersion122, false},
		{"fractional_second_native", "timestampadd(second, 0.5, cast('2024-02-29 12:34:56' as datetime)) as d, 'A' collate utf8mb4_general_ci='a' as b", 0, true},
		{"native_only", "'A' collate utf8mb4_general_ci='a' as b", 0, true},
		{"mixed", "timestamp('2024-02-29 12:34:56') as d, 'A' collate utf8mb4_general_ci='a' as b", defines.MORPCVersion122, true},
	} {
		for _, prepared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prepare_%t", tc.name, prepared), func(t *testing.T) {
				ctx, _ := nativePADDDLContext(t)
				rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
				build := func() (*Plan, error) {
					stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, "create table n as select "+tc.projection, 1)
					require.NoError(t, err)
					defer stmt.Free()
					return BuildPlan(ctx, stmt, prepared)
				}
				// 执行端已支持132；临时执行时间函数不应提高native作者floor。
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion121)
				p, err := build()
				if tc.native {
					require.ErrorContains(t, err, "protocol version 132")
					require.Nil(t, p)
					rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion132)
					p, err = build()
				}
				require.NoError(t, err)
				create := p.GetDdl().GetCreateTable()
				wantExecution := tc.execution
				if prepared {
					wantExecution = 0
					require.NotNil(t, p.GetDdl().Query)
				}
				require.Equal(t, wantExecution, create.CtasExecutionProtocolVersion)
				wantNative := int64(0)
				wantRevision := uint32(0)
				if tc.native {
					wantNative, wantRevision = defines.MORPCVersion132, 1
				}
				require.Equal(t, wantNative, create.RequiredProtocolVersion)
				require.Equal(t, wantRevision, create.CollationBindingRevision)
				data, err := DeepCopyPlan(p).Marshal()
				require.NoError(t, err)
				var decoded planpb.Plan
				require.NoError(t, decoded.Unmarshal(data))
				require.True(t, proto.Equal(create, decoded.GetDdl().GetCreateTable()))
			})
		}
	}
}
