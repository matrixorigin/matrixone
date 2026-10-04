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

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestReplayRejectsLegacyMultiTNTransaction(t *testing.T) {
	replayer := &WalReplayer{}
	cmd := txnbase.NewTxnCmd()
	cmd.Participants = []uint64{1, 2}

	require.PanicsWithError(t,
		moerr.NewNotSupportedNoCtxf(
			"replay of a transaction spanning %d TN shards", len(cmd.Participants),
		).Error(),
		func() {
			replayer.OnReplayTxn(cmd, 1)
		},
	)
}

type testReplayCommitter struct {
	err error
}

func (c testReplayCommitter) Commit(context.Context) error {
	return c.err
}

func TestCommitReplayTxn(t *testing.T) {
	require.NotPanics(t, func() {
		commitReplayTxn(context.Background(), testReplayCommitter{})
	})

	err := errors.New("commit failed")
	require.PanicsWithError(t, err.Error(), func() {
		commitReplayTxn(context.Background(), testReplayCommitter{err: err})
	})
}
