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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestNewViewBindingProcessOwnsContextAndSessionState(t *testing.T) {
	parentCtx := defines.AttachAccountId(t.Context(), 10)
	parent := NewTopProcess(parentCtx, mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer parent.Free()
	parent.GetSessionInfo().Account = "parent"
	parent.GetSessionInfo().SeqCurValues = map[uint64]string{1: "parent"}
	parent.GetSessionInfo().QueryId = []string{"parent"}

	childCtx := defines.AttachAccountId(parentCtx, 20)
	child := parent.NewViewBindingProcess(childCtx)
	defer child.Free()
	require.NotSame(t, parent.Base, child.Base)
	require.Same(t, childCtx, child.GetTopContext())
	require.Same(t, childCtx, child.Ctx)
	child.ReplaceTopCtx(context.WithValue(childCtx, struct{}{}, "nested"))
	require.Equal(t, "parent", child.GetSessionInfo().Account)
	require.Nil(t, child.GetSessionInfo().SeqCurValues)
	require.Nil(t, child.GetSessionInfo().QueryId)
	child.GetSessionInfo().SeqCurValues = map[uint64]string{1: "child"}
	child.GetSessionInfo().QueryId = []string{"child"}
	require.Same(t, parentCtx, parent.GetTopContext())
	require.Equal(t, "parent", parent.GetSessionInfo().SeqCurValues[1])
	require.Equal(t, "parent", parent.GetSessionInfo().QueryId[0])
}
