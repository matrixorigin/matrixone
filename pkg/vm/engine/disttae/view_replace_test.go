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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestValidateReplaceDefVersion(t *testing.T) {
	require.NoError(t, validateReplaceDefVersion(7, &api.AlterTableReplaceDef{}))
	require.NoError(t, validateReplaceDefVersion(7, &api.AlterTableReplaceDef{
		CheckVersion: true, ExpectedVersion: 7,
	}))
	err := validateReplaceDefVersion(8, &api.AlterTableReplaceDef{
		CheckVersion: true, ExpectedVersion: 7,
	})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
}

func TestGuardedReplaceDefIsTheOnlyOwnershipPreservingRequest(t *testing.T) {
	ordinary := api.NewReplaceDefReq(1, 2, &planpb.TableDef{})
	require.False(t, ordinary.GetReplaceDef().GetCheckVersion())
	require.False(t, ordinary.GetReplaceDef().GetPreserveOwnership())

	guarded := api.NewGuardedReplaceDefReq(1, 2, 7, 17, 23, &planpb.TableDef{})
	require.True(t, guarded.GetReplaceDef().GetCheckVersion())
	require.Equal(t, uint32(7), guarded.GetReplaceDef().GetExpectedVersion())
	require.True(t, guarded.GetReplaceDef().GetPreserveOwnership())
	require.Equal(t, uint32(17), guarded.GetReplaceDef().GetPreservedCreator())
	require.Equal(t, uint32(23), guarded.GetReplaceDef().GetPreservedOwner())
}
