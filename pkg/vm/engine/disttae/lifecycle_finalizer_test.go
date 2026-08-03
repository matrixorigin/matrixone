// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestFinalizeLifecycleCommitWritesCatalogThenControlAndCommits(t *testing.T) {
	workspace := &Transaction{}
	operator := &fakeLifecycleFinalizerOperator{workspace: workspace}
	workspace.writes = append(workspace.writes, Entry{
		bat: batch.NewWithSize(1),
	})

	err := finishLifecycleCommit(
		context.Background(),
		operator,
		DNStore{},
		&api.LifecycleCommitEntry{ProtocolVersion: 1},
	)
	require.NoError(t, err)
	require.NotNil(t, workspace.lifecycleCommitControl)
	require.Equal(t, 1, operator.commitCount)
}

func TestFinalizeLifecycleCommitRejectsControlOnly(t *testing.T) {
	workspace := &Transaction{}
	operator := &fakeLifecycleFinalizerOperator{workspace: workspace}
	err := finishLifecycleCommit(
		context.Background(),
		operator,
		DNStore{},
		&api.LifecycleCommitEntry{ProtocolVersion: 1},
	)
	require.Error(t, err)
	require.Nil(t, workspace.lifecycleCommitControl)
	require.Equal(t, 1, operator.rollbackCount)
}

func TestFinalizeLifecycleCommitRollbackIsBoundedAndDetached(t *testing.T) {
	workspace := &Transaction{}
	operator := &fakeLifecycleFinalizerOperator{workspace: workspace}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := finishLifecycleCommit(
		ctx,
		operator,
		DNStore{},
		&api.LifecycleCommitEntry{ProtocolVersion: 1},
	)
	require.Error(t, err)
	require.Equal(t, 1, operator.rollbackCount)
	require.NoError(t, operator.rollbackContextErr)
	require.WithinDuration(
		t,
		time.Now().Add(lifecycleRollbackTimeout),
		operator.rollbackDeadline,
		time.Second,
	)
}

type fakeLifecycleFinalizerOperator struct {
	workspace          *Transaction
	commitCount        int
	rollbackCount      int
	rollbackContextErr error
	rollbackDeadline   time.Time
}

func (operator *fakeLifecycleFinalizerOperator) GetWorkspace() client.Workspace {
	return operator.workspace
}

func (operator *fakeLifecycleFinalizerOperator) Commit(context.Context) error {
	operator.commitCount++
	return nil
}

func (operator *fakeLifecycleFinalizerOperator) Rollback(ctx context.Context) error {
	operator.rollbackCount++
	operator.rollbackContextErr = ctx.Err()
	operator.rollbackDeadline, _ = ctx.Deadline()
	return nil
}
