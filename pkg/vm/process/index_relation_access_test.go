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

package process

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
)

func TestIndexRelationAccessIdentityAndScope(t *testing.T) {
	name := catalog.SecondaryIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
	for _, tc := range []struct {
		name, account, role, user string
		allowed                   bool
	}{
		{"missing", "", "", "", false},
		{"missing role", "sys", "", "root", false},
		{"sys other role", "sys", "reader", "root", false},
		{"tenant admin", "tenant", "accountadmin", "admin", false},
		{"missing user", "sys", "moadmin", "", false},
		{"sys admin", "sys", "moadmin", "root", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := &Process{Ctx: context.Background(), Base: &BaseProcess{SessionInfo: SessionInfo{Account: tc.account, Role: tc.role, User: tc.user}}}
			err := CheckIndexRelationAccess(proc, name, "", 0)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.NoError(t, CheckIndexRelationAccess(proc, "normal", catalog.SystemOrdinaryRel, 0))
			for _, def := range []*plan.TableDef{
				{Name: "renamed", TableType: catalog.SystemOrdinaryRel, FeatureFlag: features.IndexTable},
				{Name: "renamed", TableType: "unregistered_legacy_index"},
			} {
				err = CheckIndexRelationDefinitionAccess(proc, def.Name, def)
				if tc.allowed {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
			require.Error(t, CheckIndexRelationDefinitionAccess(proc, "normal", nil))
		})
	}

	proc := &Process{Ctx: WithTrustedIndexRelationAccess(context.Background()), Base: &BaseProcess{}}
	require.NoError(t, CheckIndexRelationAccess(proc, name, "", 0))
	proc.Ctx = WithUserIndexRelationAccess(proc.Ctx)
	require.Error(t, CheckIndexRelationAccess(proc, name, "", 0))
	require.Error(t, CheckIndexRelationAccess(nil, name, "", 0))
}

func TestIndexRelationAccessIdentityProcessCodec(t *testing.T) {
	for _, tc := range []struct {
		name, account, role, user string
		allowed                   bool
	}{
		{"sys admin", "sys", "moadmin", "root", true},
		{"tenant admin", "tenant", "accountadmin", "admin", false},
		{"sys reader", "sys", "reader", "root", false},
		{"missing identity", "", "", "", false},
		{"missing user", "sys", "moadmin", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, _ := newCodecTestProcess(t)
			defer proc.Free()
			proc.Base.SessionInfo.Account, proc.Base.SessionInfo.Role, proc.Base.SessionInfo.User = tc.account, tc.role, tc.user
			codec := NewCodecService(fakeCodecTxnClient{op: fakeCodecTxnOperator{}}, nil, nil, nil, nil, nil, nil, nil)
			data, err := codec.Encode(proc, "select 1")
			require.NoError(t, err)
			var info pipeline.ProcessInfo
			require.NoError(t, info.Unmarshal(data))
			remote, err := codec.Decode(context.Background(), info)
			require.NoError(t, err)
			defer remote.Free()
			require.Equal(t, tc.account, remote.Base.SessionInfo.Account)
			require.Equal(t, tc.role, remote.Base.SessionInfo.Role)
			require.Equal(t, tc.user, remote.Base.SessionInfo.User)
			index := catalog.SecondaryIndexTableNamePrefix + "0198fa2b-7cc8-7ed1-b7ae-a3d9c29e75fd"
			err = CheckIndexRelationAccess(remote, index, "", 0)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
