// Copyright 2024 Matrix Origin
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

package cnservice

import (
	"context"
	"errors"
	"math"
	goruntime "runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_incr"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_moserver"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_query"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_shard"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_task"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var dummyBadRequestErr = moerr.NewInternalError(context.TODO(), "bad request")
var dummyErr = moerr.NewInternalError(context.TODO(), "dummy error")

type remoteStatsTestEngine struct {
	engine.Engine
	info  *statsinfo.StatsInfo
	key   statsinfo.StatsInfoKey
	calls int
}

func (e *remoteStatsTestEngine) StatsForRemote(
	_ context.Context,
	key statsinfo.StatsInfoKey,
) *statsinfo.StatsInfo {
	e.calls++
	e.key = key
	return e.info
}

func Test_service_handleISCPDrainConsumerRenewFenceOnly(t *testing.T) {
	exec := &iscp.ISCPTaskExecutor{}
	iscp.RegisterExecutorRuntime("runner-cn", exec)
	defer iscp.UnregisterExecutorRuntime("runner-cn", exec)

	s := &service{cfg: &Config{UUID: "runner-cn"}}
	key := iscp.NewJobRuntimeKey(1, 42, "index_idx1", 7)
	defer iscp.RemoveCNJobFence("runner-cn", key)

	renewReq := &query.Request{ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
		AccountID:      key.AccountID,
		TableID:        key.TableID,
		JobName:        key.JobName,
		JobID:          key.JobID,
		RenewFenceOnly: true,
	}}
	resp := &query.Response{}
	require.ErrorContains(t,
		s.handleISCPDrainConsumer(context.Background(), renewReq, resp, nil),
		"cannot renew ISCP consumer quiescence fence",
	)
	require.Nil(t, resp.ISCPDrainConsumerResponse)
	require.False(t, iscp.RenewCNJobFence("runner-cn", key, time.Second))
	require.False(t, exec.IsJobFenced(key))

	resp = &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			AccountID: key.AccountID,
			TableID:   key.TableID,
			JobName:   key.JobName,
			JobID:     key.JobID,
		},
	}, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success)
	require.True(t, iscp.RenewCNJobFence("runner-cn", key, time.Second))
	require.True(t, exec.IsJobFenced(key))

	resp = &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), renewReq, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success)

	resp = &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			AccountID:       key.AccountID,
			TableID:         key.TableID,
			JobName:         key.JobName,
			JobID:           key.JobID,
			RemoveFenceOnly: true,
		},
	}, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success)

	resp = &query.Response{}
	require.ErrorContains(t,
		s.handleISCPDrainConsumer(context.Background(), renewReq, resp, nil),
		"cannot renew ISCP consumer quiescence fence",
		"late renewal must not recreate a fence after removal",
	)
	require.Nil(t, resp.ISCPDrainConsumerResponse)
	require.False(t, iscp.RenewCNJobFence("runner-cn", key, time.Second))
	require.False(t, exec.IsJobFenced(key))
}

func Test_service_handleISCPDrainConsumerWaitsForExecutorRuntime(t *testing.T) {
	oldTimeout := iscpExecutorReadyTimeout
	iscpExecutorReadyTimeout = time.Second
	oldLookup := iscpGetExecutorRuntimeFn
	defer func() {
		iscpExecutorReadyTimeout = oldTimeout
		iscpGetExecutorRuntimeFn = oldLookup
	}()

	const runnerCN = "late-runner-cn"
	key := iscp.NewJobRuntimeKey(1, 42, "index_idx1", 7)
	defer iscp.RemoveCNJobFence(runnerCN, key)
	exec := &iscp.ISCPTaskExecutor{}
	defer iscp.UnregisterExecutorRuntime(runnerCN, exec)
	firstLookup := make(chan struct{})
	var once sync.Once
	iscpGetExecutorRuntimeFn = func(cnUUID string) (*iscp.ISCPTaskExecutor, bool) {
		missing := false
		once.Do(func() {
			missing = true
			close(firstLookup)
		})
		if missing {
			return nil, false
		}
		return iscp.GetExecutorRuntime(cnUUID)
	}
	go func() {
		<-firstLookup
		iscp.RegisterExecutorRuntime(runnerCN, exec)
	}()

	s := &service{cfg: &Config{UUID: runnerCN}}
	resp := &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			AccountID: 1,
			TableID:   42,
			JobName:   "index_idx1",
			JobID:     7,
		},
	}, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success)
}

func Test_service_handleISCPDrainConsumerReturnsRetryableNotReady(t *testing.T) {
	oldTimeout := iscpExecutorReadyTimeout
	iscpExecutorReadyTimeout = time.Millisecond
	defer func() { iscpExecutorReadyTimeout = oldTimeout }()

	const runnerCN = "not-ready-runner-cn"
	key := iscp.NewJobRuntimeKey(0, 42, "index_idx1", 7)
	defer iscp.RemoveCNJobFence(runnerCN, key)
	s := &service{cfg: &Config{UUID: runnerCN}}
	err := s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			TableID: 42,
			JobName: "index_idx1",
			JobID:   7,
		},
	}, &query.Response{}, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart))
	resp := &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			TableID:        key.TableID,
			JobName:        key.JobName,
			JobID:          key.JobID,
			RenewFenceOnly: true,
		},
	}, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success,
		"the pending CN fence must remain renewable before executor publication")
	require.True(t, iscp.RenewCNJobFence(runnerCN, key, time.Second))

	resp = &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			TableID:         key.TableID,
			JobName:         key.JobName,
			JobID:           key.JobID,
			RemoveFenceOnly: true,
		},
	}, resp, nil))
	require.False(t, iscp.RenewCNJobFence(runnerCN, key, time.Second))
}

func Test_service_handleISCPDrainConsumerRetriesInjectedStartupGap(t *testing.T) {
	oldTimeout := iscpExecutorReadyTimeout
	iscpExecutorReadyTimeout = 20 * time.Millisecond
	defer func() { iscpExecutorReadyTimeout = oldTimeout }()

	require.True(t, fault.Enable())
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(
		context.Background(),
		objectio.FJ_ISCPCancelExecutorNotReady,
		"1:1::",
		"sleep",
		1,
		"",
		false,
	))
	defer func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), objectio.FJ_ISCPCancelExecutorNotReady)
	}()

	const runnerCN = "injected-late-runner-cn"
	key := iscp.NewJobRuntimeKey(1, 42, "index_idx1", 7)
	defer iscp.RemoveCNJobFence(runnerCN, key)
	exec := &iscp.ISCPTaskExecutor{}
	iscp.RegisterExecutorRuntime(runnerCN, exec)
	defer iscp.UnregisterExecutorRuntime(runnerCN, exec)

	req := &query.Request{
		ISCPDrainConsumerRequest: &query.ISCPDrainConsumerRequest{
			AccountID: 1,
			TableID:   42,
			JobName:   "index_idx1",
			JobID:     7,
		},
	}
	s := &service{cfg: &Config{UUID: runnerCN}}
	err := s.handleISCPDrainConsumer(context.Background(), req, &query.Response{}, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart))

	resp := &query.Response{}
	require.NoError(t, s.handleISCPDrainConsumer(context.Background(), req, resp, nil))
	require.True(t, resp.ISCPDrainConsumerResponse.Success)
}

func Test_service_handleGoMaxProcs(t *testing.T) {
	ctx := context.Background()
	type fields struct{}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoMaxProcsRequest: query.GoMaxProcsRequest{MaxProcs: 0}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoMaxProcsResponse: query.GoMaxProcsResponse{MaxProcs: int32(goruntime.GOMAXPROCS(0))}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleGoMaxProcs(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGoMaxProcs(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGoMemLimit(t *testing.T) {
	ctx := context.Background()
	// set no limit
	_ = debug.SetMemoryLimit(-1)
	type fields struct{}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:   "set_4Gi",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoMemLimitRequest: query.GoMemLimitRequest{MemLimitBytes: 4 << 30}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoMemLimitResponse: query.GoMemLimitResponse{MemLimitBytes: math.MaxInt64}},
		},
		{
			name:   "set_no_limit",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoMemLimitRequest: query.GoMemLimitRequest{MemLimitBytes: -1}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoMemLimitResponse: query.GoMemLimitResponse{MemLimitBytes: 4 << 30}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleGoMemLimit(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGoMemLimit(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGoGCPercent(t *testing.T) {
	ctx := context.Background()
	// reset GCPercent
	_ = debug.SetGCPercent(100)
	type fields struct{}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:   "disable_gc",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoGCPercentRequest: query.GoGCPercentRequest{Percent: -1}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoGCPercentResponse: query.GoGCPercentResponse{Percent: 100}},
		},
		{
			name:   "set_90",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoGCPercentRequest: query.GoGCPercentRequest{Percent: 90}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoGCPercentResponse: query.GoGCPercentResponse{Percent: -1}},
		},
		{
			name:   "set_100",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GoGCPercentRequest: query.GoGCPercentRequest{Percent: 100}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GoGCPercentResponse: query.GoGCPercentResponse{Percent: 90}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleGoGCPercent(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGoGCPercent(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleFileServiceCacheRequest(t *testing.T) {
	ctx := context.Background()
	oldMPoolCap := mpool.GlobalCap()
	oldMemoryCacheSize := fileservice.GlobalMemoryCacheSizeHint.Load()
	defer func() {
		mpool.InitCap(oldMPoolCap)
		fileservice.GlobalMemoryCacheSizeHint.Store(oldMemoryCacheSize)
	}()
	type fields struct{}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{FileServiceCacheRequest: query.FileServiceCacheRequest{
					Type:      query.FileServiceCacheType_Disk,
					CacheSize: 0,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
		},
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{FileServiceCacheRequest: query.FileServiceCacheRequest{
					Type:      query.FileServiceCacheType_Memory,
					CacheSize: 0,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
		},
		{
			name:   "memory cache size does not change global mpool cap",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{FileServiceCacheRequest: query.FileServiceCacheRequest{
					Type:      query.FileServiceCacheType_Memory,
					CacheSize: 2 * mpool.GB,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleFileServiceCacheRequest(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			if tt.args.req.FileServiceCacheRequest.Type == query.FileServiceCacheType_Memory &&
				tt.args.req.FileServiceCacheRequest.CacheSize > 0 {
				require.Equal(t, tt.args.req.FileServiceCacheRequest.CacheSize, fileservice.GlobalMemoryCacheSizeHint.Load())
				require.Equal(t, oldMPoolCap, mpool.GlobalCap())
			}
		})
	}
}

func Test_service_handleFileServiceCacheEvictRequest(t *testing.T) {
	ctx := context.Background()
	type fields struct{}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{FileServiceCacheEvictRequest: query.FileServiceCacheEvictRequest{
					Type: query.FileServiceCacheType_Disk,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
		},
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{FileServiceCacheEvictRequest: query.FileServiceCacheEvictRequest{
					Type: query.FileServiceCacheType_Memory,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleFileServiceCacheEvictRequest(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_service_handleReloadAutoIncrementCache(t *testing.T) {

	ctl := gomock.NewController(t)
	incSvc := mock_incr.NewMockAutoIncrementService(ctl)
	incSvc.EXPECT().Reload(gomock.Any(), gomock.Any()).AnyTimes()

	ctx := context.Background()
	type fields struct {
		incrservice incrservice.AutoIncrementService
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		/*{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},*/
		{
			name: "normal",
			fields: fields{
				incrservice: incSvc,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{ReloadAutoIncrementCache: &query.ReloadAutoIncrementCacheRequest{TableID: 0}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{ReloadAutoIncrementCache: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{
				incrservice: tt.fields.incrservice,
			}
			err := s.handleReloadAutoIncrementCache(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleReloadAutoIncrementCache(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGetPipelineInfo(t *testing.T) {

	ctx := context.Background()

	type fields struct {
		counterVal int64
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name:    "nil stats key",
			fields:  fields{},
			args:    args{req: &query.Request{GetStatsInfoRequest: &query.GetStatsInfoRequest{}}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GetPipelineInfoRequest: &query.GetPipelineInfoRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GetPipelineInfoResponse: &query.GetPipelineInfoResponse{Count: 0}},
		},
		{
			name: "val_2346",
			fields: fields{
				counterVal: 2346,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GetPipelineInfoRequest: &query.GetPipelineInfoRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GetPipelineInfoResponse: &query.GetPipelineInfoResponse{Count: 2346}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			s.pipelines.counter.Store(tt.fields.counterVal)
			err := s.handleGetPipelineInfo(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetPipelineInfo(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleRemoveRemoteLockTable(t *testing.T) {

	ctx := context.TODO()
	err := dummyErr
	ctl := gomock.NewController(t)
	lockSvc := mock_lock.NewMockLockService(ctl)
	lockSvc.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	lockSvcRemoved := mock_lock.NewMockLockService(ctl)
	lockSvcRemoved.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	lockSvcErr := mock_lock.NewMockLockService(ctl)
	lockSvcErr.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, err).AnyTimes()

	type fields struct {
		lockService lockservice.LockService
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		/*{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},*/
		{
			name: "!remote",
			fields: fields{
				lockService: lockSvc,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{RemoveRemoteLockTable: &query.RemoveRemoteLockTableRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{RemoveRemoteLockTable: &query.RemoveRemoteLockTableResponse{}},
		},
		{
			name: "remote",
			fields: fields{
				lockService: lockSvcRemoved,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{RemoveRemoteLockTable: &query.RemoveRemoteLockTableRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{RemoveRemoteLockTable: &query.RemoveRemoteLockTableResponse{Count: 1}},
		},
		{
			name: "error",
			fields: fields{
				lockService: lockSvcErr,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{RemoveRemoteLockTable: &query.RemoveRemoteLockTableRequest{}},
				resp: &query.Response{},
			},
			wantErr: err,
			want:    &query.Response{RemoveRemoteLockTable: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{
				lockService: tt.fields.lockService,
			}
			err := s.handleRemoveRemoteLockTable(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleRemoveRemoteLockTable(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleUnsubscribeTable(t *testing.T) {

	err := dummyErr
	ctl := gomock.NewController(t)
	mockEng := mock_frontend.NewMockEngine(ctl)
	mockEng.EXPECT().UnsubscribeTable(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockEngErr := mock_frontend.NewMockEngine(ctl)
	mockEngErr.EXPECT().UnsubscribeTable(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(err).AnyTimes()

	respWithErr := &query.Response{}
	respWithErr.WrapError(err)

	ctx := context.Background()
	type fields struct {
		storeEngine engine.Engine
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name: "success",
			fields: fields{
				storeEngine: mockEng,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{UnsubscribeTable: &query.UnsubscribeTableRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{UnsubscribeTable: &query.UnsubscribeTableResponse{Success: true}},
		},
		{
			name: "error",
			fields: fields{
				storeEngine: mockEngErr,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{UnsubscribeTable: &query.UnsubscribeTableRequest{}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    respWithErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{
				storeEngine: tt.fields.storeEngine,
			}
			err := s.handleUnsubscribeTable(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleUnsubscribeTable(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGetStatsInfo(t *testing.T) {

	ctl := gomock.NewController(t)
	mockEng := mock_frontend.NewMockEngine(ctl)
	mockEng.EXPECT().Stats(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	ctx := context.Background()
	type fields struct {
		storeEngine engine.Engine
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name: "normal",
			fields: fields{
				storeEngine: mockEng,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{GetStatsInfoRequest: &query.GetStatsInfoRequest{StatsInfoKey: &statsinfo.StatsInfoKey{}}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GetStatsInfoResponse: &query.GetStatsInfoResponse{StatsInfo: nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{
				storeEngine: tt.fields.storeEngine,
			}
			err := s.handleGetStatsInfo(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetStatsInfo(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func TestServiceHandleGetStatsInfoUsesRemoteExportBoundary(t *testing.T) {
	key := statsinfo.StatsInfoKey{AccId: 7, DatabaseID: 8, TableID: 9}
	want := &statsinfo.StatsInfo{TableCnt: 42}
	exporter := &remoteStatsTestEngine{info: want}
	s := &service{storeEngine: exporter}
	resp := &query.Response{}

	require.NoError(t, s.handleGetStatsInfo(context.Background(), &query.Request{
		GetStatsInfoRequest: &query.GetStatsInfoRequest{StatsInfoKey: &key},
	}, resp, nil))
	require.Same(t, want, resp.GetStatsInfoResponse.StatsInfo)
	require.Equal(t, 1, exporter.calls)
	require.Equal(t, key, exporter.key)
}

func Test_service_handleTraceSpan(t *testing.T) {

	ctx := context.Background()
	type fields struct {
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		/*{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},*/
		{
			name:   "enable",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{TraceSpanRequest: &query.TraceSpanRequest{
					Cmd:       "enable",
					Spans:     "s3,span2",
					Threshold: 123,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{TraceSpanResponse: &query.TraceSpanResponse{Resp: ctl.TraceSpanRetiredResponse}},
		},
		{
			name:   "cmd_unknown",
			fields: fields{},
			args: args{
				ctx: ctx,
				req: &query.Request{TraceSpanRequest: &query.TraceSpanRequest{
					Cmd:       "unknown",
					Spans:     "span1,span2",
					Threshold: 123,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{TraceSpanResponse: &query.TraceSpanResponse{Resp: ctl.TraceSpanRetiredResponse}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleTraceSpan(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleTraceSpan(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleCtlReader(t *testing.T) {

	ctx := context.Background()
	type fields struct {
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:   "enable",
			fields: fields{},
			args: args{
				ctx: ctx,
				// more details in pkg/sql/plan/function/ctl/reader.go::handleCtlReader
				req: &query.Request{CtlReaderRequest: &query.CtlReaderRequest{
					Cmd:   "enable",
					Cfg:   "force_shuffle",
					Extra: types.EncodeStringSlice([]string{"tid1,tid2:1"}),
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want: &query.Response{CtlReaderResponse: &query.CtlReaderResponse{
				Resp: "successed cmd: enable, cfg: [force_shuffle tid1,tid2 1]",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleCtlReader(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleCtlReader(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleCtlPrefetchOnSubscribed(t *testing.T) {
	ctx := context.Background()

	req := &query.Request{CtlPrefetchOnSubscribedRequest: &query.CtlPrefetchOnSubscribedRequest{
		Patterns: []string{"^foo$"},
	}}
	resp := &query.Response{}
	t.Cleanup(func() {
		require.NoError(t, engine.SetPrefetchOnSubscribed(nil))
	})
	s := &service{}
	err := s.handleCtlPrefetchOnSubscribed(ctx, req, resp, nil)
	require.NoError(t, err)
	require.Equal(t, &query.Response{CtlPrefetchOnSubscribedResponse: &query.CtlPrefetchOnSubscribedResponse{
		Resp: "prefetch_on_subscribed updated, patterns: 1",
	}}, resp)
}

func Test_service_handleRunTask(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)
	mockRunner := mock_task.NewMockTaskRunner(ctl)
	mockRunner.EXPECT().GetExecutor(gomock.Any()).DoAndReturn(func(code task.TaskCode) taskservice.TaskExecutor {
		if code == -1 {
			return nil
		}
		if code == 1 {
			return func(ctx context.Context, task task.Task) error {
				return nil
			}
		}
		return nil
	}).AnyTimes()

	type fields struct {
		runner taskservice.TaskRunner
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name:   "TaskRunnerNotReady",
			fields: fields{},
			args: args{
				ctx: ctx,
				// more details in pkg/sql/plan/function/ctl/reader.go::handleCtlReader
				req: &query.Request{RunTask: &query.RunTaskRequest{
					TaskCode: -1,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{RunTask: &query.RunTaskResponse{Result: "Task Runner Not Ready"}},
		},
		{
			name: "TaskRunnerOK",
			fields: fields{
				runner: mockRunner,
			},
			args: args{
				ctx: ctx,
				// more details in pkg/sql/plan/function/ctl/reader.go::handleCtlReader
				req: &query.Request{RunTask: &query.RunTaskRequest{
					TaskCode: 1,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{RunTask: &query.RunTaskResponse{Result: "OK"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			s.task.runner = tt.fields.runner
			err := s.handleRunTask(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleRunTask(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_queryWorkLifecycleReleasesCompletedTasks(t *testing.T) {
	executorErr := errors.New("executor failed")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "error", err: executorErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lifecycle queryWorkLifecycle
			completed := make(chan struct{})
			relaunched := atomic.Bool{}

			require.True(t, lifecycle.launch(
				func(context.Context, task.Task) error {
					close(completed)
					return tc.err
				},
				&task.AsyncTask{},
			))
			<-completed

			closeCalls := 0
			closeIngress := func() error {
				closeCalls++
				return nil
			}
			require.NoError(t, lifecycle.close(closeIngress))
			require.NoError(t, lifecycle.close(closeIngress))
			require.Equal(t, 1, closeCalls)
			require.False(t, lifecycle.launch(
				func(context.Context, task.Task) error {
					relaunched.Store(true)
					return nil
				},
				&task.AsyncTask{},
			))
			require.False(t, relaunched.Load())
		})
	}
}

func Test_service_closeQueryServiceCancelsAndDrainsRunTask(t *testing.T) {
	ctl := gomock.NewController(t)
	runner := mock_task.NewMockTaskRunner(ctl)
	queryService := &closeRecordingQueryService{
		handlers: make(map[query.CmdMethod]func(context.Context, *query.Request, *query.Response, *morpc.Buffer) error),
		closed:   make(chan struct{}),
	}
	executorStarted := make(chan struct{})
	executorCanceled := make(chan struct{})
	releaseExecutor := make(chan struct{})
	var executorCalls atomic.Int32
	runner.EXPECT().GetExecutor(task.TaskCode(1)).Return(
		func(ctx context.Context, _ task.Task) error {
			executorCalls.Add(1)
			close(executorStarted)
			<-ctx.Done()
			close(executorCanceled)
			<-releaseExecutor
			return ctx.Err()
		},
	).Times(1)

	s := &service{queryService: queryService}
	s.task.runner = runner
	s.initQueryCommandHandler()
	runTask := queryService.handlers[query.CmdMethod_RunTask]
	require.NotNil(t, runTask)
	resp := &query.Response{}
	require.NoError(t, runTask(
		context.Background(),
		&query.Request{RunTask: &query.RunTaskRequest{TaskCode: 1}},
		resp,
		nil,
	))
	require.Equal(t, "OK", resp.RunTask.Result)
	<-executorStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.closeQueryService()
	}()
	<-queryService.closed
	<-executorCanceled
	select {
	case <-closeDone:
		t.Fatal("query service closed before the RunTask executor exited")
	default:
	}

	lateErr := runTask(
		context.Background(),
		&query.Request{RunTask: &query.RunTaskRequest{TaskCode: 1}},
		&query.Response{},
		nil,
	)
	require.True(t, moerr.IsMoErrCode(lateErr, moerr.ErrServiceUnavailable))
	require.Equal(t, int32(1), executorCalls.Load())
	repeatedCloseDone := make(chan error, 1)
	go func() {
		repeatedCloseDone <- s.closeQueryService()
	}()
	select {
	case <-repeatedCloseDone:
		t.Fatal("repeated query service close returned before the RunTask executor exited")
	default:
	}

	close(releaseExecutor)
	require.NoError(t, <-closeDone)
	require.NoError(t, <-repeatedCloseDone)
}

func Test_service_handleRunTaskRejectsLaunchWhenShutdownStartsDuringHandler(t *testing.T) {
	ctl := gomock.NewController(t)
	runner := mock_task.NewMockTaskRunner(ctl)
	queryService := &closeRecordingQueryService{
		handlers: make(map[query.CmdMethod]func(context.Context, *query.Request, *query.Response, *morpc.Buffer) error),
		closed:   make(chan struct{}),
	}
	executorLookupStarted := make(chan struct{})
	releaseExecutorLookup := make(chan struct{})
	var executorCalls atomic.Int32
	runner.EXPECT().GetExecutor(task.TaskCode(1)).DoAndReturn(
		func(task.TaskCode) taskservice.TaskExecutor {
			close(executorLookupStarted)
			<-releaseExecutorLookup
			return func(context.Context, task.Task) error {
				executorCalls.Add(1)
				return nil
			}
		},
	).Times(1)

	s := &service{queryService: queryService}
	s.task.runner = runner
	s.initQueryCommandHandler()
	runTask := queryService.handlers[query.CmdMethod_RunTask]
	handlerDone := make(chan error, 1)
	go func() {
		handlerDone <- runTask(
			context.Background(),
			&query.Request{RunTask: &query.RunTaskRequest{TaskCode: 1}},
			&query.Response{},
			nil,
		)
	}()
	<-executorLookupStarted

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- s.closeQueryService()
	}()
	<-queryService.closed
	close(releaseExecutorLookup)
	require.True(t, moerr.IsMoErrCode(<-handlerDone, moerr.ErrServiceUnavailable))
	require.NoError(t, <-closeDone)
	require.Equal(t, int32(0), executorCalls.Load())
}

func Test_service_handleMigrateConnFrom(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)
	mockServer := mock_moserver.NewMockServer(ctl)
	mockServer.EXPECT().GetRoutineManager().Return(&frontend.RoutineManager{}).AnyTimes()

	type fields struct {
		mo frontend.Server
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name: "notExist_conn_1",
			fields: fields{
				mo: mockServer,
			},
			args: args{
				ctx: ctx,
				req: &query.Request{MigrateConnFromRequest: &query.MigrateConnFromRequest{
					ConnID: 1,
				}},
				resp: &query.Response{},
			},
			wantErr: moerr.NewInternalErrorf(ctx, "cannot get routine to migrate connection %d", 1),
			want:    &query.Response{MigrateConnFromResponse: &query.MigrateConnFromResponse{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{mo: tt.fields.mo}
			err := s.handleMigrateConnFrom(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleMigrateConnFrom(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleMigrateConnTo(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)
	mockServer := mock_moserver.NewMockServer(ctl)
	mockServer.EXPECT().GetRoutineManager().Return(&frontend.RoutineManager{}).AnyTimes()

	type fields struct {
		mo frontend.Server
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name: "notExist_conn_1",
			fields: fields{
				mo: mockServer,
			},
			args: args{
				ctx: ctx,
				req: &query.Request{MigrateConnToRequest: &query.MigrateConnToRequest{
					ConnID: 1,
				}},
				resp: &query.Response{},
			},
			wantErr: moerr.NewInternalErrorf(ctx, "cannot get routine to migrate connection %d", 1),
			want:    &query.Response{MigrateConnToResponse: nil},
		},
		// ignore mockServerSuccess case.
		// tips: success case dependent frontend.Routine{}.
		// tips: frontend.Routine{} only internal api, no mock interface
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{mo: tt.fields.mo}
			err := s.handleMigrateConnTo(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleMigrateConnTo(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGetReplicaCount(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)
	mockReplicaCount := int64(759)
	mockService := mock_shard.NewMockShardService(ctl)
	mockService.EXPECT().ReplicaCount().Return(mockReplicaCount).AnyTimes()

	type fields struct {
		shardService shardservice.ShardService
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name: "normal",
			fields: fields{
				shardService: mockService,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GetReplicaCount: query.GetReplicaCountResponse{Count: mockReplicaCount}},
		},
		{
			name: "disabled",
			fields: fields{
				shardService: nil,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{},
				resp: &query.Response{},
			},
			wantErr: nil,
			want:    &query.Response{GetReplicaCount: query.GetReplicaCountResponse{Count: 0}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{shardService: tt.fields.shardService}
			err := s.handleGetReplicaCount(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetReplicaCount(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleResetSession(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)
	mockServer := mock_moserver.NewMockServer(ctl)
	mockServer.EXPECT().GetRoutineManager().Return(&frontend.RoutineManager{}).AnyTimes()

	type fields struct {
		mo frontend.Server
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:    "nil",
			fields:  fields{},
			args:    args{req: &query.Request{}},
			wantErr: dummyBadRequestErr,
			want:    nil,
		},
		{
			name: "notExist_conn_1",
			fields: fields{
				mo: mockServer,
			},
			args: args{
				ctx: ctx,
				req: &query.Request{ResetSessionRequest: &query.ResetSessionRequest{
					ConnID: 1,
				}},
				resp: &query.Response{},
			},
			wantErr: moerr.NewInternalErrorf(ctx, "cannot get routine to clear session %d", 1),
			want:    &query.Response{ResetSessionResponse: &query.ResetSessionResponse{Success: false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{mo: tt.fields.mo}
			err := s.handleResetSession(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleResetSession(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_handleGetCacheData(t *testing.T) {

	ctx := context.Background()
	ctl := gomock.NewController(t)

	fs, err := fileservice.NewFileServices("dummy", testutil.NewSharedFS())
	require.NoError(t, err)

	mockQuery := mock_query.NewMockQueryService(ctl)

	type fields struct {
		fileService  fileservice.FileService
		queryService queryservice.QueryService
	}
	type args struct {
		ctx  context.Context
		req  *query.Request
		resp *query.Response
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
		want    *query.Response
	}{
		{
			name:   "no_fileService",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{},
				resp: &query.Response{},
			},
			wantErr: moerr.NewNoServiceNoCtx(defines.SharedFileServiceName),
			want:    &query.Response{},
		},
		{
			name: "no_fileService_nil_req",
			fields: fields{
				fileService: fs,
			},
			args: args{
				ctx:  ctx,
				req:  &query.Request{},
				resp: &query.Response{},
			},
			wantErr: dummyBadRequestErr,
			want:    &query.Response{},
		},
		{
			name: "missing_cache_key",
			fields: fields{
				fileService:  fs,
				queryService: mockQuery,
			},
			args: args{
				ctx: ctx,
				req: &query.Request{
					GetCacheDataRequest: &query.GetCacheDataRequest{
						RequestCacheKey: []*query.RequestCacheKey{
							{
								Index:    1,
								CacheKey: nil,
							},
						},
					},
				},
				resp: &query.Response{},
			},
			wantErr: dummyBadRequestErr,
			want:    &query.Response{GetCacheDataResponse: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{
				fileService:  tt.fields.fileService,
				queryService: tt.fields.queryService,
			}
			err := s.handleGetCacheData(tt.args.ctx, tt.args.req, tt.args.resp, nil)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetCacheData(%v, %v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp, nil)
		})
	}
}

func Test_service_copy(t *testing.T) {
	srcOpt := lock.LockOptions{
		Granularity: 0,
		Mode:        0,
	}
	gotOpt := copyLockOptions(srcOpt)
	require.Falsef(t, unsafe.Pointer(&srcOpt) == unsafe.Pointer(gotOpt), "copyLockOptions should diff. src: %p, got: %p", &srcOpt, gotOpt)

	srcLock := client.Lock{
		TableID: 1,
		Rows:    [][]byte{[]byte("123"), []byte("345")},
		Options: lock.LockOptions{},
	}
	got := copyTxnInfo(srcLock)
	require.Falsef(t, unsafe.Pointer(&srcLock.Rows) == unsafe.Pointer(&got.Rows), "copyTxnInfo Rows should diff. src: %p, got: %p", srcLock.Rows, got.Rows)
	require.Falsef(t, unsafe.Pointer(&srcLock.Options) == unsafe.Pointer(got.Options), "copyTxnInfo Options should diff. src: %p, got: %p", &srcLock.Options, got.Options)
}

func Test_service_handleMetadataCacheRequest(t *testing.T) {
	ctx := context.Background()
	s := &service{}
	var resp query.Response
	err := s.handleMetadataCacheRequest(ctx, &query.Request{
		MetadataCacheRequest: query.MetadataCacheRequest{
			CacheSize: 42,
		},
	}, &resp, nil)
	require.Nil(t, err)
	if resp.MetadataCacheResponse.CacheCapacity != 42 {
		t.Fatal()
	}
}

func Test_service_handleIcebergCacheInvalidate(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	handler := &fakeIcebergCacheInvalidationHandler{removed: 2}
	rt.SetGlobalVariables(api.CacheInvalidatorRuntimeKey, handler)
	defer rt.SetGlobalVariables(api.CacheInvalidatorRuntimeKey, nil)

	s := &service{}
	var resp query.Response
	err := s.handleIcebergCacheInvalidate(context.Background(), &query.Request{
		IcebergCacheInvalidateRequest: query.IcebergCacheInvalidateRequest{
			AccountID:            7,
			CatalogID:            42,
			Namespace:            "sales",
			Table:                "orders",
			SnapshotID:           200,
			MetadataLocationHash: "hash-200",
			CommitID:             "commit-200",
		},
	}, &resp, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), resp.IcebergCacheInvalidateResponse.RemovedEntries)
	require.Equal(t, api.CacheInvalidationRequest{
		AccountID:            7,
		CatalogID:            42,
		Namespace:            "sales",
		Table:                "orders",
		SnapshotID:           200,
		MetadataLocationHash: "hash-200",
		CommitID:             "commit-200",
	}, handler.req)
}

func Test_service_handleMongoDBClientRetire(t *testing.T) {
	const serviceID = "mongodb-retire-handler"
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	factory := &cnMongoDBClientFactory{}
	pool := sqlmongodb.NewClientPool(factory)
	t.Cleanup(func() { require.NoError(t, pool.Close(context.Background())) })
	rt.SetGlobalVariables(sqlmongodb.RuntimeDependenciesKey, &sqlmongodb.RuntimeDependencies{Pool: pool})
	defer rt.SetGlobalVariables(sqlmongodb.RuntimeDependenciesKey, nil)

	lease, err := pool.Acquire(t.Context(), sqlmongodb.Connection{
		AccountID: 7, ConnectionID: 9, Version: 3,
	}, sqlmongodb.Credentials{}, sqlmongodb.RuntimeConfig{})
	require.NoError(t, err)
	require.NoError(t, lease.Release(t.Context()))

	s := &service{cfg: &Config{UUID: serviceID}}
	var resp query.Response
	err = s.handleMongoDBClientRetire(t.Context(), &query.Request{
		MongoDBClientRetireRequest: query.MongoDBClientRetireRequest{AccountID: 7},
	}, &resp, nil)
	require.NoError(t, err)
	require.True(t, resp.MongoDBClientRetireResponse.Success)
	require.Equal(t, 1, factory.client.disconnects)
}

type cnMongoDBClientFactory struct {
	client *cnMongoDBClient
}

func (f *cnMongoDBClientFactory) Connect(
	context.Context, sqlmongodb.Connection, sqlmongodb.Credentials, sqlmongodb.RuntimeConfig,
) (sqlmongodb.Client, error) {
	f.client = &cnMongoDBClient{}
	return f.client, nil
}

type cnMongoDBClient struct {
	disconnects int
}

func (*cnMongoDBClient) Collection(string, string) sqlmongodb.Collection { return nil }
func (*cnMongoDBClient) Ping(context.Context) error                      { return nil }
func (c *cnMongoDBClient) Disconnect(context.Context) error {
	c.disconnects++
	return nil
}

type fakeIcebergCacheInvalidationHandler struct {
	req     api.CacheInvalidationRequest
	removed int
}

func (h *fakeIcebergCacheInvalidationHandler) InvalidateIcebergCache(ctx context.Context, req api.CacheInvalidationRequest) (int, error) {
	h.req = req
	return h.removed, nil
}
