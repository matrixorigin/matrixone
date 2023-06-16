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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		ExecPlan             SerializableExecPlan
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
		ExecPlan          any
		SerializeExecPlan SerializeExecPlanFunc
	}

	dummyExecPlan := map[string]any{"key": "val", "int": 1}
	dummyEPJson := `{"func":"dummy2","result":{"int":1,"key":"val"}}`

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "dummyDefault_ep_Serialize",
			args: args{
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyEPJson,
		},
		{
			name: "nil_ep_Serialize",
			args: args{
				ExecPlan:          dummyExecPlan,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyEPJson,
		},
		{
			name: "dummyDefault_nil_Serialize",
			args: args{
				ExecPlan:          nil,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyNoExecPlanJsonResult2,
		},
		{
			name: "nil_nil_Serialize",
			args: args{
				ExecPlan:          nil,
				SerializeExecPlan: dummySerializeExecPlan2,
			},
			want: dummyNoExecPlanJsonResult2,
		},
	}

	ctx := DefaultContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatementInfo{}
			p := &dummySerializableExecPlan{
				plan: tt.args.ExecPlan,
				f:    tt.args.SerializeExecPlan,
			}
			s.SetSerializableExecPlan(p)
			got, stats := s.ExecPlan2Json(ctx)
			assert.Equalf(t, tt.want, util.UnsafeBytesToString(got), "ExecPlan2Json()")
			assert.Equalf(t, []byte("[]"), stats, "stats")
			mapper := new(map[string]any)
			err := json.Unmarshal([]byte(got), mapper)
			require.Nil(t, err, "jons.Unmarshal failed")
		})
	}
}

type dummySerializableExecPlan struct {
	plan any
	f    SerializeExecPlanFunc
	uuid uuid.UUID
}

func NewDummySerializableExecPlan(plan any, f SerializeExecPlanFunc, uuid2 uuid.UUID) *dummySerializableExecPlan {
	return &dummySerializableExecPlan{
		plan: plan,
		f:    f,
		uuid: uuid2,
	}
}

func (p *dummySerializableExecPlan) Marshal(ctx context.Context) ([]byte, []byte, Statistic) {
	return p.f(ctx, p.plan, p.uuid)
}
func (p *dummySerializableExecPlan) Free() {}

func TestGetStatsValues(t *testing.T) {
	jsonData := []byte(`{
		"statistics":{
			"Time":[{"name":"Time Consumed","value":95834,"unit":"ns"},{"name":"Wait Time","value":442250,"unit":"ns"}],
			"Memory":[{"name":"Memory Size","value":69,"unit":"byte"}],
			"Throughput":[{"name":"Input Rows","value":2,"unit":"count"},{"name":"Output Rows","value":2,"unit":"count"},{"name":"Input Size","value":136,"unit":"byte"},{"name":"Output Size","value":136,"unit":"byte"}],
			"IO":[{"name":"Disk IO","value":0,"unit":"byte"},{"name":"S3 IO Byte","value":68,"unit":"byte"},{"name":"S3 IO Input Count","value":0,"unit":"count"},{"name":"S3 IO Output Count","value":0,"unit":"count"}],
			"Network":[{"name":"Network","value":0,"unit":"byte"}]
		},
		"totalStats":{"name":"Time spent","value":95834,"unit":"ns"}
	}`)

	res, err := getStatsValues(jsonData, 2)
	if err != nil {
		t.Fatalf("Error getting stats values: %v", err)
	}

	assert.Equal(t, 5, len(res), "Expected 5 stats values")

	assert.Equal(t, uint64(1), res[0], "version should be 1")

	if res[1] != 95834 {
		t.Errorf("Expected timeConsumed to be 95834, got %d", res[1])
	}

	// multiple of 2, duration
	if res[2] != 138 {
		t.Errorf("Expected memorySize to be 69, got %d", res[2])
	}

	if res[3] != 0 {
		t.Errorf("Expected s3IOInputCount to be 0, got %d", res[3])
	}

	if res[4] != 0 {
		t.Errorf("Expected s3IOOutputCount to be 0, got %d", res[4])
	}
}

func TestMergeStats(t *testing.T) {
	e := &StatementInfo{
		statsJsonByte: []byte("[1, 80335, 1800, 1, 0]"),
	}

	n := &StatementInfo{
		statsJsonByte: []byte("[1, 147960, 1800, 0, 0]"),
	}

	err := mergeStats(e, n)
	if err != nil {
		t.Fatalf("mergeStats failed: %v", err)
	}

	if string(e.statsJsonByte) != "[1, 228295, 3600, 1, 0]" {
		t.Errorf("Expected '[1, 228295, 3600, 0, 0]', got '%s'", string(e.statsJsonByte))
	}

	n = &StatementInfo{
		statsJsonByte: []byte("[1, 1, 1, 0, 0]"),
	}

	err = mergeStats(e, n)
	if err != nil {
		t.Fatalf("mergeStats failed: %v", err)
	}

	if string(e.statsJsonByte) != "[1, 228296, 3601, 1, 0]" {
		t.Errorf("Expected '[1, 228296, 3601, 0, 0]', got '%s'", string(e.statsJsonByte))
	}
}
