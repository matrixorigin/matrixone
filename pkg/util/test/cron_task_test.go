// Copyright 2022 Matrix Origin
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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/golang/mock/gomock"
	"github.com/lni/goutils/leaktest"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const defaultTestTimeout = 3 * time.Minute

// TestCalculateStorageUsage
// example like tasks.TestCronTask in task_test.go
func TestCalculateStorageUsage(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	opt := service.DefaultOptions()
	c, err := service.NewCluster(ctx, t, opt.WithLogLevel(zap.ErrorLevel))
	require.NoError(t, err)
	// close the cluster
	defer func(c service.Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitTNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 0)

	ctrl := gomock.NewController(t)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	table := mock_frontend.NewMockRelation(ctrl)
	table.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().TableColumns(gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), txnOperator).Return(db, nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	pu.SV.SetDefaultValues()

	ieFactory := func() ie.InternalExecutor {
		return frontend.NewInternalExecutor()
	}

	err = mometric.CalculateStorageUsage(ctx, ieFactory)
	require.Nil(t, err)

	s := metric.StorageUsage("sys")
	dm := &dto.Metric{}
	s.Write(dm)
	logutil.Infof("size: %f", dm.GetGauge().GetValue())
	t.Logf("size: %f", dm.GetGauge().GetValue())
}

func TestGetTenantInfo(t *testing.T) {
	ctx := context.TODO()
	tenant, err := frontend.GetTenantInfo(ctx, "sys:internal:moadmin")
	require.Nil(t, err)
	require.Equal(t, "sys", tenant.GetTenant())
	require.Equal(t, "internal", tenant.GetUser())
	require.Equal(t, "moadmin", tenant.GetDefaultRole())

	tenant, err = frontend.GetTenantInfo(ctx, "sys:internal:moadmin?k:v")
	require.Nil(t, err)
	require.Equal(t, "sys", tenant.GetTenant())
	require.Equal(t, "internal", tenant.GetUser())
	require.Equal(t, "moadmin", tenant.GetDefaultRole())
}
