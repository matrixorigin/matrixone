// Copyright 2021 - 2024 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
)

type mockHAKeeperClient struct {
	clusterState        pb.CheckerState
	NonVotingReplicaNum uint64
	NonVotingLocality   pb.Locality
}

func newMockHAKeeperClient() *mockHAKeeperClient {
	return &mockHAKeeperClient{
		clusterState: pb.CheckerState{
			LogState: pb.NewLogState(),
		},
	}
}

func (c *mockHAKeeperClient) Close() error {
	return nil
}

func (c *mockHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return 0, nil
}

func (c *mockHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	return pb.ClusterDetails{}, nil
}

func (c *mockHAKeeperClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	return c.clusterState, nil
}

func (c *mockHAKeeperClient) GetBackupData(ctx context.Context) ([]byte, error) {
	return nil, nil
}

func (c *mockHAKeeperClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	return pb.CommandBatch{}, nil
}

func (c *mockHAKeeperClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	c.clusterState.NonVotingReplicaNum = num
	return nil
}

func (c *mockHAKeeperClient) UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	c.clusterState.NonVotingLocality = locality
	return nil
}

func (c *mockHAKeeperClient) addStore(uuid string, storeInfo pb.LogStoreInfo) {
	c.clusterState.LogState.Stores[uuid] = storeInfo
}

func (c *mockHAKeeperClient) addLogShardRecord(rec metadata.LogShardRecord) {
	c.clusterState.ClusterInfo.LogShards = append(c.clusterState.ClusterInfo.LogShards, rec)
}

func TestSortLogserviceReplicaResult(t *testing.T) {
	type testCase struct {
		infoLists [][]logserviceReplicaInfo
	}
	cases := testCase{
		infoLists: [][]logserviceReplicaInfo{
			{
				{index: 0, hasReplica: true, shardID: 0, replicaID: 131074, replicaRole: Leader},
				{index: 3, hasReplica: true, shardID: 1, replicaID: 262147, replicaRole: Leader},
				{index: 1, hasReplica: true, shardID: 0, replicaID: 131072, replicaRole: Follower},
				{index: 2, hasReplica: true, shardID: 0, replicaID: 131073, replicaRole: Follower},
				{index: 4, hasReplica: true, shardID: 1, replicaID: 262145, replicaRole: Follower},
				{index: 5, hasReplica: true, shardID: 1, replicaID: 262146, replicaRole: Follower},
			},
			{
				{index: 0, hasReplica: true, shardID: 0, replicaID: 131074, replicaRole: Leader},
				{index: 6, hasReplica: false},
				{index: 1, hasReplica: true, shardID: 0, replicaID: 131072, replicaRole: Follower},
				{index: 2, hasReplica: true, shardID: 0, replicaID: 131073, replicaRole: Follower},
				{index: 3, hasReplica: true, shardID: 1, replicaID: 262145, replicaRole: Leader},
				{index: 4, hasReplica: true, shardID: 1, replicaID: 262146, replicaRole: Follower},
				{index: 5, hasReplica: true, shardID: 1, replicaID: 262148, replicaRole: Follower},
			},
		},
	}
	for _, c := range cases.infoLists {
		infoList := sortLogserviceReplicasResult(c)
		for i, info := range infoList {
			assert.Equal(t, i, info.index)
		}
	}
}

func TestSortLogserviceStoreResult(t *testing.T) {
	type testCase struct {
		infoLists [][]logserviceStoreInfo
	}
	cases := testCase{
		infoLists: [][]logserviceStoreInfo{
			{
				{index: 0, storeID: "0"},
				{index: 2, storeID: "2"},
				{index: 4, storeID: "4"},
				{index: 8, storeID: "8"},
				{index: 1, storeID: "1"},
				{index: 6, storeID: "6"},
				{index: 5, storeID: "5"},
				{index: 7, storeID: "7"},
				{index: 3, storeID: "3"},
			},
		},
	}
	for _, c := range cases.infoLists {
		infoList := sortLogserviceStoresResult(c)
		for i, info := range infoList {
			assert.Equal(t, i, info.index)
		}
	}
}

func Test_HandleShowLogserviceReplicas(t *testing.T) {
	type args struct {
		ses      *Session
		execCtx  *ExecCtx
		showStmt *tree.ShowLogserviceReplicas
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	_, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql1 := "show logservice replicas"
	mock.ExpectQuery(sql1).WillReturnRows(
		sqlmock.NewRows([]string{"account_id"}).AddRow(uint64(sysAccountID)),
	)

	pu := config.ParameterUnit{}
	hc := newMockHAKeeperClient()
	store1 := pb.LogStoreInfo{
		Replicas: []pb.LogReplicaInfo{
			{
				LogShardInfo: pb.LogShardInfo{
					ShardID: 0,
					Replicas: map[uint64]string{
						101: "r1",
						102: "r2",
						103: "r3",
					},
				},
			},
		},
	}
	hc.addStore("store1", store1)
	pu.HAKeeperClient = hc
	setGlobalPu(&pu)

	showStmt := &tree.ShowLogserviceReplicas{}

	ses.GetTxnCompileCtx().execCtx.stmt = showStmt

	tests := []struct {
		name string
		args args
	}{
		{
			name: "t1",
			args: args{
				ses:      ses,
				execCtx:  ses.GetTxnCompileCtx().execCtx,
				showStmt: showStmt,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := execInFrontend(tt.args.ses, tt.args.execCtx)
			assert.NoError(t, err)
			rs := tt.args.ses.GetMysqlResultSet()
			assert.Equal(t, uint64(1), rs.GetRowCount())
			shardID, err := rs.GetString(tt.args.execCtx.reqCtx, 0, 0)
			assert.NoError(t, err)
			assert.Equal(t, "0", shardID)
		})
	}
}

func Test_HandleShowLogserviceStores(t *testing.T) {
	type args struct {
		ses      *Session
		execCtx  *ExecCtx
		showStmt *tree.ShowLogserviceStores
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	_, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql1 := "show logservice stores"
	mock.ExpectQuery(sql1).WillReturnRows(
		sqlmock.NewRows([]string{"account_id"}).AddRow(uint64(sysAccountID)),
	)

	pu := config.ParameterUnit{}
	hc := newMockHAKeeperClient()
	store1 := pb.LogStoreInfo{
		Replicas: []pb.LogReplicaInfo{
			{
				LogShardInfo: pb.LogShardInfo{
					ShardID: 0,
					Replicas: map[uint64]string{
						101: "r1",
						102: "r2",
						103: "r3",
					},
				},
			},
		},
	}
	hc.addStore("store1", store1)
	pu.HAKeeperClient = hc
	setGlobalPu(&pu)

	showStmt := &tree.ShowLogserviceStores{}

	ses.GetTxnCompileCtx().execCtx.stmt = showStmt

	tests := []struct {
		name string
		args args
	}{
		{
			name: "t1",
			args: args{
				ses:      ses,
				execCtx:  ses.GetTxnCompileCtx().execCtx,
				showStmt: showStmt,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := execInFrontend(tt.args.ses, tt.args.execCtx)
			assert.NoError(t, err)
			rs := tt.args.ses.GetMysqlResultSet()
			assert.Equal(t, uint64(1), rs.GetRowCount())
			storeID, err := rs.GetString(tt.args.execCtx.reqCtx, 0, 0)
			assert.NoError(t, err)
			assert.Equal(t, "store1", storeID)
		})
	}
}

func Test_HandleLogserviceSettings(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(sysAccountID))
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: ctx,
	}

	pu := config.ParameterUnit{
		SV: &config.FrontendParameters{},
	}
	hc := newMockHAKeeperClient()

	store1 := pb.LogStoreInfo{}
	store2 := pb.LogStoreInfo{}
	hc.addStore("store1", store1)
	hc.addStore("store2", store2)
	hc.addLogShardRecord(metadata.LogShardRecord{
		ShardID:          1,
		NumberOfReplicas: 1,
	})
	pu.HAKeeperClient = hc
	setGlobalPu(&pu)
	execCtx := ses.GetTxnCompileCtx().execCtx

	sql := "set logservice settings non_voting_locality='region:beijing'"
	stmts, err := mysql.Parse(ctx, sql, 0)
	assert.NoError(t, err)
	execCtx.stmt = stmts[0]
	err = execInFrontend(ses, execCtx)
	assert.NoError(t, err)

	sql = "set logservice settings non_voting_replica_num=1"
	stmts, err = mysql.Parse(ctx, sql, 0)
	assert.NoError(t, err)
	execCtx.stmt = stmts[0]
	err = execInFrontend(ses, execCtx)
	assert.NoError(t, err)

	ses.SetMysqlResultSet(&MysqlResultSet{})
	sql = "show logservice settings"
	stmts, err = mysql.Parse(ctx, sql, 0)
	assert.NoError(t, err)
	execCtx.stmt = stmts[0]

	err = execInFrontend(ses, execCtx)
	assert.NoError(t, err)

	rs := ses.GetMysqlResultSet()
	assert.Equal(t, uint64(2), rs.GetRowCount())
	assert.Equal(t, uint64(2), rs.GetColumnCount())
	n00, err := rs.GetString(execCtx.reqCtx, 0, 0)
	assert.NoError(t, err)
	assert.Equal(t, "non_voting_replica_num", n00)

	n01, err := rs.GetString(execCtx.reqCtx, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, "1", n01)

	n10, err := rs.GetString(execCtx.reqCtx, 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, "non_voting_locality", n10)

	n11, err := rs.GetString(execCtx.reqCtx, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, "region:beijing;", n11)
}
