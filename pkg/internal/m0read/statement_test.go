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

package m0read

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

type statementTestView struct{}

func (*statementTestView) Release() {}

func TestStatementViewMasksParentWithoutChangingContext(t *testing.T) {
	require.Nil(t, GetStatementReadView(nil))
	ctx, cancel := context.WithCancel(defines.AttachAccountId(t.Context(), 42))
	parent := WithStatementReadView(ctx, &statementTestView{})
	require.NotNil(t, GetStatementReadView(parent))
	child := WithStatementReadView(parent, nil)
	require.Nil(t, GetStatementReadView(child))
	account, err := defines.GetAccountId(child)
	require.NoError(t, err)
	require.Equal(t, uint32(42), account)
	cancel()
	require.ErrorIs(t, child.Err(), context.Canceled)
}

func TestRemoteMarkersStayLocalAndIndependent(t *testing.T) {
	require.False(t, IsSingleRemote(nil))
	require.False(t, NeedsRemoteMemberValidation(nil))
	ctx, cancel := context.WithCancel(t.Context())
	require.False(t, IsSingleRemote(ctx))
	require.False(t, NeedsRemoteMemberValidation(ctx))
	single := WithSingleRemote(ctx)
	require.True(t, IsSingleRemote(single))
	require.False(t, NeedsRemoteMemberValidation(single))
	member := WithRemoteMemberValidation(ctx)
	require.True(t, NeedsRemoteMemberValidation(member))
	require.False(t, IsSingleRemote(member))
	both := WithRemoteMemberValidation(single)
	require.True(t, IsSingleRemote(both))
	require.True(t, NeedsRemoteMemberValidation(both))
	cancel()
	require.ErrorIs(t, both.Err(), context.Canceled)
	require.False(t, IsSingleRemote(context.Background()))
}
