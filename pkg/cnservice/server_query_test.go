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
	"fmt"
	"math"
	goruntime "runtime"
	"runtime/debug"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_incr"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_moserver"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var dummyBadRequestErr = moerr.NewInternalError(context.TODO(), "bad request")
var dummyErr = moerr.NewInternalError(context.TODO(), "dummy error")

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
			want:    &query.Response{GoMaxProcsResponse: query.GoMaxProcsResponse{MaxProcs: int32(goruntime.NumCPU())}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleGoMaxProcs(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGoMaxProcs(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			err := s.handleGoMemLimit(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGoMemLimit(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
}

func Test_service_handleFileServiceCacheRequest(t *testing.T) {
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
				ctx: ctx,
				req: &query.Request{FileServiceCacheRequest: query.FileServiceCacheRequest{
					Type:      0,
					CacheSize: 0,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want: &query.Response{FileServiceCacheResponse: query.FileServiceCacheResponse{
				CacheSize:     0,
				CacheCapacity: 0,
				Message:       "Not Implemented",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleFileServiceCacheRequest(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleFileServiceCacheRequest(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
		want    *query.Response
	}{
		{
			name:   "normal",
			fields: fields{},
			args: args{
				ctx:  ctx,
				req:  &query.Request{FileServiceCacheEvictRequest: query.FileServiceCacheEvictRequest{Type: 0}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want: &query.Response{FileServiceCacheEvictResponse: query.FileServiceCacheEvictResponse{
				CacheSize:     0,
				CacheCapacity: 0,
				Message:       "Not Implemented",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleFileServiceCacheEvictRequest(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleFileServiceCacheEvictRequest(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			err := s.handleReloadAutoIncrementCache(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleReloadAutoIncrementCache(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			err := s.handleGetPipelineInfo(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetPipelineInfo(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			err := s.handleRemoveRemoteLockTable(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleRemoveRemoteLockTable(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
}

func Test_service_handleUnsubscribeTable(t *testing.T) {

	err := dummyErr
	ctl := gomock.NewController(t)
	mockEng := mock_frontend.NewMockEngine(ctl)
	mockEng.EXPECT().UnsubscribeTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockEngErr := mock_frontend.NewMockEngine(ctl)
	mockEngErr.EXPECT().UnsubscribeTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(err).AnyTimes()

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
			err := s.handleUnsubscribeTable(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleUnsubscribeTable(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			err := s.handleGetStatsInfo(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetStatsInfo(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
}

func Test_service_handleTraceSpan(t *testing.T) {

	trace.InitMOCtledSpan()
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
			want:    &query.Response{TraceSpanResponse: &query.TraceSpanResponse{Resp: fmt.Sprintf("%v %sd, %v failed", []string{"s3"}, "enable", []string{"span2"})}},
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
			want:    &query.Response{TraceSpanResponse: &query.TraceSpanResponse{Resp: fmt.Sprintf("%v %sd, %v failed", []string{}, "unknown", []string{"span1", "span2"})}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{}
			err := s.handleTraceSpan(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleTraceSpan(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
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
			wantErr: moerr.NewInternalError(ctx, "cannot get routine to migrate connection %d", 1),
			want:    &query.Response{MigrateConnFromResponse: &query.MigrateConnFromResponse{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{mo: tt.fields.mo}
			err := s.handleMigrateConnFrom(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleMigrateConnFrom(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
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
			wantErr: moerr.NewInternalError(ctx, "cannot get routine to migrate connection %d", 1),
			want:    &query.Response{MigrateConnToResponse: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &service{mo: tt.fields.mo}
			err := s.handleMigrateConnTo(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleMigrateConnTo(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
}
