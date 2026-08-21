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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestTxnTableExposesLifecycleTableCapability(t *testing.T) {
	var _ LifecycleTable = (*txnTable)(nil)
}

func TestLifecycleTableDelegatesFailClosedBeforePhysicalReadOrRewrite(t *testing.T) {
	origin := &txnTable{}
	delegate := &txnTableDelegate{origin: origin}

	_, err := delegate.LifecycleReadObject(
		context.Background(),
		types.TS{},
		objectio.ObjectStats{},
		0,
		nil,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))

	_, err = delegate.LifecycleRewriteObject(
		context.Background(),
		LifecycleRewriteOptions{},
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestLifecycleCommitStoreUsesOnlyTheExistingTransactionRoute(t *testing.T) {
	ctrl := gomock.NewController(t)
	operator := mock_frontend.NewMockTxnOperator(ctrl)
	transaction := &Transaction{tnStores: []DNStore{{ServiceID: "tn-1"}}}
	origin := &txnTable{db: &txnDatabase{op: operator}}
	delegate := &txnTableDelegate{origin: origin}

	operator.EXPECT().GetWorkspace().Return(transaction).Times(2)
	store, err := origin.LifecycleCommitStore()
	require.NoError(t, err)
	require.Equal(t, "tn-1", store.ServiceID)
	store, err = delegate.LifecycleCommitStore()
	require.NoError(t, err)
	require.Equal(t, "tn-1", store.ServiceID)

	emptyOperator := mock_frontend.NewMockTxnOperator(ctrl)
	emptyOperator.EXPECT().GetWorkspace().Return(&Transaction{})
	_, err = (&txnTable{db: &txnDatabase{op: emptyOperator}}).LifecycleCommitStore()
	require.ErrorContains(t, err, "no TN route")
}
