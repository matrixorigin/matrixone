// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
)

var _ queryservice.QueryService = &mockQueryService{}

type mockQueryService struct {
}

func (m *mockQueryService) SetReleaseFunc(_ *query.Response, _ func()) {
}

func (m *mockQueryService) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	return nil, nil
}

func (m *mockQueryService) NewRequest(method query.CmdMethod) *query.Request {
	req := &query.Request{
		CmdMethod: method,
	}
	switch method {
	case query.CmdMethod_GetCacheInfo:
		req.GetCacheInfoRequest = &query.GetCacheInfoRequest{}
	case query.CmdMethod_GetTxnInfo:
		req.GetTxnInfoRequest = &query.GetTxnInfoRequest{}
	case query.CmdMethod_GetLockInfo:
		req.GetLockInfoRequest = &query.GetLockInfoRequest{}
	default:
		panic(fmt.Sprintf("usp method:%s", method.String()))
	}
	return req
}

func (m *mockQueryService) Release(response *query.Response) {
}

func (m *mockQueryService) Start() error {
	return nil
}

func (m *mockQueryService) Close() error {
	return nil
}

func (m *mockQueryService) AddHandleFunc(method query.CmdMethod, h func(context.Context, *query.Request, *query.Response, *morpc.Buffer) error, async bool) {
}

func (m *mockQueryService) AddReleaseFunc(method query.CmdMethod, f func()) {
}

func (m *mockQueryService) ServiceID() string {
	return "mock_query_service"
}

func Test_gettingInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wantCacheInfo := &query.CacheInfo{
		NodeType:  "cache_node",
		NodeId:    "cache_node_id",
		CacheType: "mock_cache",
	}

	wantTxnInfo := &query.TxnInfo{
		CreateAt:  time.Time{},
		Meta:      nil,
		UserTxn:   true,
		WaitLocks: nil,
	}

	wantLockInfo := &query.LockInfo{
		TableId:     1000,
		Keys:        nil,
		LockMode:    lock.LockMode_Exclusive,
		IsRangeLock: true,
		Holders:     nil,
		Waiters:     nil,
	}

	selectStubs := gostub.Stub(
		&selectSuperTenant,
		func(
			sid string,
			selector clusterservice.Selector,
			username string,
			filter func(string) bool,
			appendFn func(service *metadata.CNService)) {
			if appendFn != nil {
				appendFn(&metadata.CNService{
					QueryAddress: "127.0.0.1:6060",
				})
			}
		})
	defer selectStubs.Reset()

	listTnStubs := gostub.Stub(
		&listTnService,
		func(
			sid string,
			appendFn func(service *metadata.TNService)) {
		},
	)
	defer listTnStubs.Reset()

	requestMultipleCnStubs := gostub.Stub(&requestMultipleCn,
		func(ctx context.Context,
			nodes []string,
			qt qclient.QueryClient,
			genRequest func() *query.Request,
			handleValidResponse func(string, *query.Response),
			handleInvalidResponse func(string)) error {
			req := genRequest()
			fmt.Println(">>>>>", *req)
			resp := &query.Response{
				CmdMethod: req.CmdMethod,
			}
			switch req.CmdMethod {
			case query.CmdMethod_GetCacheInfo:
				resp.GetCacheInfoResponse = &query.GetCacheInfoResponse{
					CacheInfoList: []*query.CacheInfo{
						wantCacheInfo,
					},
				}
			case query.CmdMethod_GetTxnInfo:
				resp.GetTxnInfoResponse = &query.GetTxnInfoResponse{
					TxnInfoList: []*query.TxnInfo{
						wantTxnInfo,
					},
				}
			case query.CmdMethod_GetLockInfo:
				resp.GetLockInfoResponse = &query.GetLockInfoResponse{
					CnId: "xxx",
					LockInfoList: []*query.LockInfo{
						wantLockInfo,
					},
				}
			default:
				panic(fmt.Sprintf("usp method %v", req.CmdMethod.String()))
			}

			fmt.Println(">>>>>", *resp)
			handleValidResponse("", resp)
			return nil
		})
	defer requestMultipleCnStubs.Reset()

	mp, err := mpool.NewMPool("ut_pool", 0, mpool.NoFixed)
	if err != nil {
		assert.NoError(t, err)
	}
	defer mpool.DeleteMPool(mp)

	testProc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, &mockQueryService{}, nil, nil, nil)

	type args struct {
		proc *process.Process
	}
	//////test get cache status
	tests := []struct {
		name    string
		args    args
		want    []*query.GetCacheInfoResponse
		wantErr bool
	}{
		{
			name: "t1",
			args: args{
				proc: testProc,
			},
			want: []*query.GetCacheInfoResponse{
				{
					CacheInfoList: []*query.CacheInfo{
						wantCacheInfo,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getCacheStats(tt.args.proc)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCacheStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCacheStats() got = %v, want %v", got, tt.want)
			}
		})
	}

	/////test get txns info
	tests1 := []struct {
		name    string
		args    args
		want    []*query.GetTxnInfoResponse
		wantErr bool
	}{
		{
			name: "t2",
			args: args{
				proc: testProc,
			},
			want: []*query.GetTxnInfoResponse{
				{
					TxnInfoList: []*query.TxnInfo{
						wantTxnInfo,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests1 {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getTxns(tt.args.proc)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCacheStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCacheStats() got = %v, want %v", got, tt.want)
			}
		})
	}

	/////test get locks info
	tests2 := []struct {
		name    string
		args    args
		want    []*query.GetLockInfoResponse
		wantErr bool
	}{
		{
			name: "t3",
			args: args{
				proc: testProc,
			},
			want: []*query.GetLockInfoResponse{
				{
					CnId: "xxx",
					LockInfoList: []*query.LockInfo{
						wantLockInfo,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests2 {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getLocks(tt.args.proc)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCacheStats() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getCacheStats() got = %v, want %v", got, tt.want)
			}
		})
	}

	/////test moCacheCall

	type argsx struct {
		in0  int
		proc *process.Process
		arg  *TableFunction
	}
	tests4 := []struct {
		name    string
		args    argsx
		want    bool
		wantErr bool
	}{
		{
			name: "",
			args: argsx{
				proc: testProc,
				arg: &TableFunction{
					Rets: nil,
					Args: nil,
					Attrs: []string{
						"type",
						"used",
						"hit_ratio",
					},
					Params:   nil,
					FuncName: "",
				},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests4 {
		t.Run(tt.name, func(t *testing.T) {
			tvfst, err := moCachePrepare(tt.args.proc, tt.args.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("moCachePrepare() error = %v, wantErr %v", err, tt.wantErr)
			}
			err = tvfst.start(tt.args.arg, tt.args.proc, 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("tvf.start error = %v, wantErr %v", err, tt.wantErr)
			}

			res, err := tvfst.call(tt.args.arg, tt.args.proc)
			if (err != nil) != tt.wantErr {
				t.Errorf("tvf.call error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			bat := res.Batch
			assert.Equal(t, len(bat.Attrs), 3)
			assert.Equal(t, bat.Attrs[0], "type")
			assert.Equal(t, bat.Attrs[1], "used")
			assert.Equal(t, bat.Attrs[2], "hit_ratio")

			// what?
			assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(0))[0], "mock_cache")
			assert.Equal(t, vector.MustFixedColWithTypeCheck[uint64](bat.GetVector(1))[0], uint64(0))
		})
	}

	///// test moTransactionsCall

	tests5 := []struct {
		name    string
		args    argsx
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "",
			args: argsx{
				proc: testProc,
				arg: &TableFunction{
					Rets: nil,
					Args: nil,
					Attrs: []string{
						"user_txn",
					},
					Params:   nil,
					FuncName: "",
				},
			},
			want: false,
			wantErr: func(assert.TestingT, error, ...interface{}) bool {
				// What?
				return true
			},
		},
	}
	for _, tt := range tests5 {
		t.Run(tt.name, func(t *testing.T) {
			tvfst, err := moTransactionsPrepare(tt.args.proc, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("moTransactionsPrepare(%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			err = tvfst.start(tt.args.arg, tt.args.proc, 0)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.start (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			res, err := tvfst.call(tt.args.arg, tt.args.proc)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.call (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			bat := res.Batch
			assert.Equal(t, len(bat.Attrs), 1)
			assert.Equal(t, bat.Attrs[0], "user_txn")

			// WTH is this check?
			assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(0))[0], "true")
		})
	}

	///// test moLocksCall

	tests6 := []struct {
		name    string
		args    argsx
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "",
			args: argsx{
				proc: testProc,
				arg: &TableFunction{
					Rets: nil,
					Args: nil,
					Attrs: []string{
						"table_id",
						"lock_key",
						"lock_mode",
					},
					Params:   nil,
					FuncName: "",
				},
			},
			want: false,
			wantErr: func(assert.TestingT, error, ...interface{}) bool {

				return true
			},
		},
	}
	for _, tt := range tests6 {
		t.Run(tt.name, func(t *testing.T) {
			tvfst, err := moLocksPrepare(tt.args.proc, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("moLocksCall(%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			err = tvfst.start(tt.args.arg, tt.args.proc, 0)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.start (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			res, err := tvfst.call(tt.args.arg, tt.args.proc)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.call (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			bat := res.Batch
			assert.Equal(t, len(bat.Attrs), 3)
			assert.Equal(t, bat.Attrs[0], "table_id")
			assert.Equal(t, bat.Attrs[1], "lock_key")
			assert.Equal(t, bat.Attrs[2], "lock_mode")

			assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(0))[0], "1000")
			assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(1))[0], "range")
			assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(2))[0], "Exclusive")
		})
	}
}

var _ logservice.CNHAKeeperClient = &mockHKClient{}

type mockHKClient struct {
}

func (m *mockHKClient) Close() error {
	return nil
}

func (m *mockHKClient) AllocateID(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (m *mockHKClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}

func (m *mockHKClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	return 0, nil
}

func (m *mockHKClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	cd := pb.ClusterDetails{
		CNStores: []pb.CNStore{
			{
				ConfigData: &pb.ConfigData{
					Content: map[string]*pb.ConfigItem{
						"xxxx": {
							Name:         "xxxx",
							CurrentValue: "123",
							DefaultValue: "0",
						},
					},
				},
			},
		},
		TNStores: []pb.TNStore{
			{
				ConfigData: &pb.ConfigData{
					Content: map[string]*pb.ConfigItem{
						"xxxx": {
							Name:         "xxxx",
							CurrentValue: "123",
							DefaultValue: "0",
						},
					},
				},
			},
		},
		LogStores: []pb.LogStore{
			{
				ConfigData: &pb.ConfigData{
					Content: map[string]*pb.ConfigItem{
						"xxxx": {
							Name:         "xxxx",
							CurrentValue: "123",
							DefaultValue: "0",
						},
					},
				},
			},
		},
		ProxyStores: []pb.ProxyStore{
			{
				ConfigData: &pb.ConfigData{
					Content: map[string]*pb.ConfigItem{
						"xxxx": {
							Name:         "xxxx",
							CurrentValue: "123",
							DefaultValue: "0",
						},
					},
				},
			},
		},
	}

	return cd, nil
}

func (m *mockHKClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	return pb.CheckerState{}, nil
}

func (m *mockHKClient) GetBackupData(ctx context.Context) ([]byte, error) {
	return nil, nil
}

func (m *mockHKClient) UpdateNonVotingReplicaNum(ctx context.Context, num uint64) error {
	return nil
}

func (m *mockHKClient) UpdateNonVotingLocality(ctx context.Context, locality pb.Locality) error {
	return nil
}

func (m *mockHKClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	return pb.CommandBatch{}, nil
}

func Test_moConfigurationsCall(t *testing.T) {
	mp, err := mpool.NewMPool("ut_pool", 0, mpool.NoFixed)
	if err != nil {
		assert.NoError(t, err)
	}
	defer mpool.DeleteMPool(mp)
	testProc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, &mockQueryService{}, &mockHKClient{}, nil, nil)

	type args struct {
		in0  int
		proc *process.Process
		arg  *TableFunction
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				proc: testProc,
				arg: &TableFunction{
					Rets: nil,
					Args: nil,
					Attrs: []string{
						"name",
						"current_value",
						"default_value",
					},
					Params:   nil,
					FuncName: "",
				},
			},
			want: false,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tvfst, err := moConfigurationsPrepare(tt.args.proc, tt.args.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("moConfigurationsPrepare(%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			err = tvfst.start(tt.args.arg, tt.args.proc, 0)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.start (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			res, err := tvfst.call(tt.args.arg, tt.args.proc)
			if !tt.wantErr(t, err, fmt.Sprintf("tvfst.call (%v, %v, %v)", tt.args.in0, tt.args.proc, tt.args.arg)) {
				return
			}

			bat := res.Batch
			assert.Equal(t, len(bat.Attrs), 3)
			assert.Equal(t, bat.Attrs[0], "name")
			assert.Equal(t, bat.Attrs[1], "current_value")
			assert.Equal(t, bat.Attrs[2], "default_value")

			for i := 0; i < 4; i++ {
				assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(0))[i], "xxxx")
				assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(1))[i], "123")
				assert.Equal(t, vector.InefficientMustStrCol(bat.GetVector(2))[i], "0")
			}
		})
	}
}
