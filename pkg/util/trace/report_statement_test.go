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

package trace

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatementInfo_Report_EndStatement(t *testing.T) {
	type fields struct {
		StatementID          [16]byte
		TransactionID        [16]byte
		SessionID            [16]byte
		Account              string
		User                 string
		Host                 string
		Database             string
		Statement            string
		StatementFingerprint string
		StatementTag         string
		RequestAt            util.TimeNano
		ExecPlan             any
		Status               StatementInfoStatus
		Error                error
		ResponseAt           util.TimeNano
		Duration             uint64
		ExecPlanStats        any

		doReport bool
		doExport bool
	}
	type args struct {
		ctx context.Context
		err error
	}

	tests := []struct {
		name          string
		fields        fields
		args          args
		wantReportCnt int
	}{
		{
			name: "Report_Export_EndStatement",
			fields: fields{
				doReport: true,
				doExport: true,
			},
			args: args{
				ctx: context.Background(),
				err: nil,
			},
			wantReportCnt: 2,
		},
		{
			name: "Report_EndStatement",
			fields: fields{
				doReport: true,
				doExport: false,
			},
			args: args{
				ctx: context.Background(),
				err: nil,
			},
			wantReportCnt: 1,
		},
		{
			name: "just_EndStatement",
			fields: fields{
				doReport: false,
				doExport: false,
			},
			args: args{
				ctx: context.Background(),
				err: nil,
			},
			wantReportCnt: 1,
		},
	}

	gotCnt := 0
	dummyReportStmFunc := func(ctx context.Context, s *StatementInfo) error {
		gotCnt++
		return nil
	}
	s := gostub.Stub(&ReportStatement, dummyReportStmFunc)
	defer s.Reset()

	dummyExport := func(s *StatementInfo) {
		s.mux.Lock()
		s.exported = true
		s.mux.Unlock()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCnt = 0
			s := &StatementInfo{
				StatementID:          tt.fields.StatementID,
				TransactionID:        tt.fields.TransactionID,
				SessionID:            tt.fields.SessionID,
				Account:              tt.fields.Account,
				User:                 tt.fields.User,
				Host:                 tt.fields.Host,
				Database:             tt.fields.Database,
				Statement:            tt.fields.Statement,
				StatementFingerprint: tt.fields.StatementFingerprint,
				StatementTag:         tt.fields.StatementTag,
				RequestAt:            tt.fields.RequestAt,
				ExecPlan:             tt.fields.ExecPlan,
				Status:               tt.fields.Status,
				Error:                tt.fields.Error,
				ResponseAt:           tt.fields.ResponseAt,
				Duration:             tt.fields.Duration,
				ExecPlanStats:        tt.fields.ExecPlanStats,
			}
			if tt.fields.doExport && !tt.fields.doReport {
				t.Errorf("export(%v) need report(%v) first.", tt.fields.doExport, tt.fields.doReport)
			}
			if tt.fields.doReport {
				s.Report(tt.args.ctx)
			}
			if tt.fields.doExport {
				dummyExport(s)
			}
			require.Equal(t, tt.fields.doReport, s.reported)
			require.Equal(t, tt.fields.doExport, s.exported)

			stmCtx := ContextWithStatement(tt.args.ctx, s)
			EndStatement(stmCtx, tt.args.err)
			require.Equal(t, tt.wantReportCnt, gotCnt)
		})
	}
}
