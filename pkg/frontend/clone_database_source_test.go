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

package frontend

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
)

func TestCloneDatabaseSourceBranchTableCount(t *testing.T) {
	source := cloneDatabaseSource{
		srcTblInfos: []*tableInfo{
			{tblName: "regular"},
			{tblName: "foreign_key"},
			{tblName: "view", typ: view},
		},
	}

	require.Equal(t, int64(2), source.branchTableCount())
}

func TestCloneSnapshotTxnOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn

	t.Run("normal clone keeps frontend transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2)
		require.Same(t, outerTxn, cloneSnapshotTxnOperator(ses, bh))
	})

	t.Run("data branch uses owning background transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
			forcePessimisticRC: true,
		})
		require.Same(t, branchTxn, cloneSnapshotTxnOperator(ses, bh))
	})
}
