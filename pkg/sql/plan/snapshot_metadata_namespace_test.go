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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestSnapshotMetadataNamespace(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		caller, target              uint32
		obj                         uint64
		named, wantAdapt, wantError bool
	}{
		{"system account snapshot", 0, 17, 17, true, true, false},
		{"same account", 17, 17, 17, true, false, false},
		{"other tenant rejected", 18, 17, 17, true, false, true},
		{"scope mismatch rejected", 0, 17, 18, true, false, true},
		{"timestamp alone does not authorize namespace", 0, 17, 17, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiler := NewMockCompilerContext(true, newPlanTestProcess(t))
			compiler.GetAccountIdFunc = func() (uint32, error) { return tc.caller, nil }
			original := defines.AttachAccountId(t.Context(), tc.caller)
			compiler.SetContext(original)
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, false)
			snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &planpb.SnapshotTenant{TenantID: tc.target}}
			if tc.named {
				snapshot.ExtraInfo = &planpb.SnapshotExtraInfo{Level: "account", ObjId: tc.obj, Name: "history"}
			}
			before := DeepCopySnapshot(snapshot)
			got, err := builder.metadataViewColumnsSQL("information_schema", "columns", sysview.InformationSchemaColumnsDDL, snapshot)
			if tc.wantError {
				require.ErrorContains(t, err, "cross-account snapshot metadata")
			} else {
				require.NoError(t, err)
				if tc.wantAdapt {
					require.Contains(t, got, "WHERE tbl.account_id = 17 AND (current_account_id() = 0")
				} else {
					require.Equal(t, sysview.InformationSchemaColumnsDDL, got)
				}
			}
			require.Same(t, original, compiler.GetContext())
			require.Equal(t, before, snapshot)
		})
	}
}
