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
	goruntime "runtime"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var dummyBadRequestErr = moerr.NewInternalError(context.TODO(), "bad request")

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
				req:  &query.Request{GoMaxProcsRequest: &query.GoMaxProcsRequest{MaxProcs: 0}},
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
				ctx: ctx,
				req: &query.Request{FileServiceCacheRequest: &query.FileServiceCacheRequest{
					Type:      0,
					CacheSize: 0,
				}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want: &query.Response{FileServiceCacheResponse: &query.FileServiceCacheResponse{
				CacheSize:     0,
				CacheCapacity: 0,
				Message:       "",
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
				req:  &query.Request{FileServiceCacheEvictRequest: &query.FileServiceCacheEvictRequest{Type: 0}},
				resp: &query.Response{},
			},
			wantErr: nil,
			want: &query.Response{FileServiceCacheEvictResponse: &query.FileServiceCacheEvictResponse{
				CacheSize:     0,
				CacheCapacity: 0,
				Message:       "",
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
	incSvc := mock_frontend.NewMockAutoIncrementService(ctl)
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
	var counterVal2346 atomic.Int64
	counterVal2346.Store(2346)

	type fields struct {
		counter atomic.Int64
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
				counter: counterVal2346,
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
			s := &service{
				pipelines: struct{ counter atomic.Int64 }{counter: tt.fields.counter},
			}
			err := s.handleGetPipelineInfo(tt.args.ctx, tt.args.req, tt.args.resp)
			require.Equal(t, tt.wantErr, err)
			require.Equalf(t, tt.want, tt.args.resp,
				"handleGetPipelineInfo(%v, %v, %v)", tt.args.ctx, tt.args.req, tt.args.resp)
		})
	}
}

func Test_service_handleRemoveRemoteLockTable(t *testing.T) {

	err := fmt.Errorf("dummy error")
	ctl := gomock.NewController(t)
	lockSvc := mock_frontend.NewMockLockService(ctl)
	lockSvc.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

	lockSvcRemoved := mock_frontend.NewMockLockService(ctl)
	lockSvcRemoved.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	lockSvcErr := mock_frontend.NewMockLockService(ctl)
	lockSvcErr.EXPECT().CloseRemoteLockTable(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, err).AnyTimes()

	ctx := context.Background()
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

	err := fmt.Errorf("dummy error")
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
