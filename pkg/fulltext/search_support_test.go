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

package fulltext

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuildMembershipFilter(t *testing.T) {
	proc := testutil.NewProc(t)
	payload, err := BuildMembershipFilter(proc, nil)
	require.NoError(t, err)
	require.Nil(t, payload)

	_, err = BuildMembershipFilter(proc, []byte{1, 2, 3})
	require.Error(t, err, "a malformed key vector is rejected")

	keys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](keys, 7, false, proc.Mp()))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)
	payload, err = BuildMembershipFilter(proc, data)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
}

func TestCheckZeroRelevanceGuard(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, CheckZeroRelevanceGuard(ctx, nil))
	require.NoError(t, CheckZeroRelevanceGuard(ctx, &plan.Literal{Isnull: true}))
	require.NoError(t, CheckZeroRelevanceGuard(ctx, &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}))
	err := CheckZeroRelevanceGuard(ctx, &plan.Literal{Value: &plan.Literal_Bval{Bval: true}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	err = CheckZeroRelevanceGuard(ctx, &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}
