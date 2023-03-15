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

package motrace

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
		RequestAt            time.Time
		ExecPlan             any
		Status               StatementInfoStatus
		Error                error
		ResponseAt           time.Time
		Duration             time.Duration

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
			EndStatement(stmCtx, tt.args.err, 0)
			require.Equal(t, tt.wantReportCnt, gotCnt)
		})
	}
}

var realNoExecPlanJsonResult = `{"code":200,"message":"NO ExecPlan Serialize function","steps":null,"success":false,"uuid":"00000000-0000-0000-0000-000000000000"}`
var dummyNoExecPlanJsonResult = `{"code":200,"message":"no exec plan"}`
var dummyNoExecPlanJsonResult2 = `{"func":"dummy2","code":200,"message":"no exec plan"}`

var dummySerializeExecPlan = func(_ context.Context, plan any, _ uuid.UUID) ([]byte, []byte, Statistic) {
	if plan == nil {
		return []byte(dummyNoExecPlanJsonResult), []byte{}, Statistic{}
	}
	json, err := json.Marshal(plan)
	if err != nil {
		return []byte(fmt.Sprintf(`{"err": %q}`, err.Error())), []byte{}, Statistic{}
	}
	return json, []byte{}, Statistic{RowsRead: 1, BytesScan: 1}
}

var dummySerializeExecPlan2 = func(_ context.Context, plan any, _ uuid.UUID) ([]byte, []byte, Statistic) {
	if plan == nil {
		return []byte(dummyNoExecPlanJsonResult2), []byte{}, Statistic{}
	}
	json, err := json.Marshal(plan)
	if err != nil {
		return []byte(fmt.Sprintf(`{"func":"dymmy2","err": %q}`, err.Error())), []byte{}, Statistic{}
	}
	val := fmt.Sprintf(`{"func":"dummy2","result":%s}`, json)
	return []byte(val), []byte{}, Statistic{}
}

func TestStatementInfo_ExecPlan2Json(t *testing.T) {
	type args struct {
		setDefault        func()
		ExecPlan          any
		SerializeExecPlan SerializeExecPlanFunc
	}

	dummyExecPlan := map[string]any{"key": "val", "int": 1}
	defaultEPJson := `{"int":1,"key":"val"}`
	dummyEPJson := `{"func":"dummy2","result":{"int":1,"key":"val"}}`

	var setDefaultNil = func() {
		SetDefaultSerializeExecPlan(nil)
	}
	var setDefaultDummy = func() {
		SetDefaultSerializeExecPlan(dummySerializeExecPlan)
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "nil",
			args: args{
				setDefault:        setDefaultNil,
				ExecPlan:          nil,
				SerializeExecPlan: nil,
			},
			want: realNoExecPlanJsonResult,
		},
		{
			name: "nil_ep_nil",
			args: args{
				setDefault:        setDefaultNil,
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: nil,
			},
			want: realNoExecPlanJsonResult,
		},
		{
			name: "dummyDefault_nil_nil",
			args: args{
				setDefault:        setDefaultDummy,
				ExecPlan:          nil,
				SerializeExecPlan: nil,
			},
			want: dummyNoExecPlanJsonResult,
		},
		{
			name: "dummyDefault_ep_nil",
			args: args{
				setDefault:        setDefaultDummy,
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: nil,
			},
			want: defaultEPJson,
		},
		{
			name: "dummyDefault_ep_Serialize",
			args: args{
				setDefault:        setDefaultDummy,
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyEPJson,
		},
		{
			name: "nil_ep_Serialize",
			args: args{
				setDefault:        setDefaultNil,
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyEPJson,
		},
		{
			name: "dummyDefault_nil_Serialize",
			args: args{
				setDefault:        setDefaultDummy,
				ExecPlan:          nil,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyNoExecPlanJsonResult2,
		},
		{
			name: "nil_nil_Serialize",
			args: args{
				setDefault:        setDefaultNil,
				ExecPlan:          nil,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyNoExecPlanJsonResult2,
		},
	}

	ctx := DefaultContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.setDefault()
			s := &StatementInfo{}
			s.SetExecPlan(tt.args.ExecPlan, tt.args.SerializeExecPlan)
			got, _ := s.ExecPlan2Json(ctx)
			assert.Equalf(t, tt.want, got, "ExecPlan2Json()")

			mapper := new(map[string]any)
			err := json.Unmarshal([]byte(got), mapper)
			require.Nil(t, err, "jons.Unmarshal failed")
		})
	}
}
