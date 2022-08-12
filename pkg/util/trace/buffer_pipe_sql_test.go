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
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/require"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

var buf = new(bytes.Buffer)
var err1 = errors.WithStack(errors.New("test1"))
var err2 = errors.Wrapf(err1, "test2")
var testBaseBuffer2SqlOption = []buffer2SqlOption{bufferWithSizeThreshold(1 * KB)}
var nodeStateSpanIdStr string

func noopReportLog(context.Context, zapcore.Level, int, string, ...any) {}
func noopReportError(context.Context, error, int)                       {}

func init() {
	setup()
}

func setup() {
	if _, err := Init(
		context.Background(),
		EnableTracer(true),
		WithMOVersion("v0.test.0"),
		WithNode(0, NodeTypeNode),
		WithSQLExecutor(func() internalExecutor.InternalExecutor {
			return nil
		}),
		DebugMode(true),
	); err != nil {
		panic(err)
	}
	logutil.SetLogReporter(&logutil.TraceReporter{ReportLog: noopReportLog, LevelSignal: SetLogLevel})
	errors.SetErrorReporter(noopReportError)

	sc := SpanFromContext(DefaultContext()).SpanContext()
	nodeStateSpanIdStr = fmt.Sprintf("%d, %d", sc.TraceID, sc.SpanID)

	if err := agent.Listen(agent.Options{}); err != nil {
		_ = fmt.Errorf("listen gops agent failed: %s", err)
		panic(err)
	}
	fmt.Println("Finish tests init.")
}

func Test_newBuffer2Sql_base(t *testing.T) {

	buf := newBuffer2Sql()
	byteBuf := new(bytes.Buffer)
	assert.Equal(t, true, buf.IsEmpty())
	buf.Add(&MOSpan{})
	assert.Equal(t, false, buf.IsEmpty())
	assert.Equal(t, false, buf.ShouldFlush())
	assert.Equal(t, "", buf.GetBatch(byteBuf))
	buf.Reset()
	assert.Equal(t, true, buf.IsEmpty())
}

func TestNewSpanBufferPipeWorker(t *testing.T) {
	type args struct {
		opt []buffer2SqlOption
	}
	opts := testBaseBuffer2SqlOption[:]
	tests := []struct {
		name string
		args args
		want batchpipe.PipeImpl[batchpipe.HasName, any]
	}{
		{
			name: "basic",
			args: args{
				opt: opts,
			},
			want: &batchSqlHandler{defaultOpts: opts},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBufferPipe2SqlWorker(tt.args.opt...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBufferPipe2SqlWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_NewItemBuffer_Check_genBatchFunc(t1 *testing.T) {
	type args struct {
		opt  []buffer2SqlOption
		name string
	}
	opts := testBaseBuffer2SqlOption[:]
	tests := []struct {
		name string
		args args
		want genBatchFunc
	}{
		{name: "span_type", args: args{opt: opts, name: MOSpanType}, want: genSpanBatchSql},
		{name: "log_type", args: args{opt: opts, name: MOLogType}, want: genLogBatchSql},
		{name: "statement_type", args: args{opt: opts, name: MOStatementType},
			want: genStatementBatchSql},
		{name: "error_type", args: args{opt: opts, name: MOErrorType},
			want: genErrorBatchSql},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := batchSqlHandler{
				defaultOpts: opts,
			}
			if got := t.NewItemBuffer(tt.args.name); reflect.ValueOf(got.(*buffer2Sql).genBatchFunc).Pointer() != reflect.ValueOf(tt.want).Pointer() {
				t1.Errorf("NewItemBuffer()'s genBatchFunc = %v, want %v", got.(*buffer2Sql).genBatchFunc, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genErrorBatchSql(t1 *testing.T) {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	buf := new(bytes.Buffer)
	newCtx, span := Start(DefaultContext(), "Test_batchSqlHandler_genErrorBatchSql")
	defer span.End()
	sc := SpanFromContext(newCtx).SpanContext()
	newStatementSpanIdStr := fmt.Sprintf("%d, %d", sc.TraceID, sc.SpanID)
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
				},
				buf: buf,
			},
			want: `insert into system.error_info (` +
				"`statement_id`, `span_id`, `node_id`, `node_type`, `err_code`, `stack`, `timestamp`" +
				`) values (` + nodeStateSpanIdStr + `, 0, "Node", "test1", "test1", "1970-01-01 00:00:00.000000")`,
		},
		{
			name: "multi_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
					&MOErrorHolder{Error: err2, Timestamp: uint64(time.Millisecond + time.Microsecond)},
					&MOErrorHolder{Error: errors.WithContext(newCtx, err2),
						Timestamp: uint64(time.Millisecond + time.Microsecond)},
				},
				buf: buf,
			},
			want: `insert into system.error_info (` +
				"`statement_id`, `span_id`, `node_id`, `node_type`, `err_code`, `stack`, `timestamp`" +
				`) values (` + nodeStateSpanIdStr + `, 0, "Node", "test1", "test1", "1970-01-01 00:00:00.000000")` +
				`,(` + nodeStateSpanIdStr + `, 0, "Node", "test2: test1", "test2: test1", "1970-01-01 00:00:00.001001")` +
				`,(` + newStatementSpanIdStr + `, 0, "Node", "test2: test1", "test2: test1", "1970-01-01 00:00:00.001001")`,
		},
	}
	errorFormatter.Store("%v")
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genErrorBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genErrorBatchSql() = %v,\n want %v", got, tt.want)
			} else {
				t1.Logf("SQL: %s", got)
			}
		})
	}
}

func Test_batchSqlHandler_genZapBatchSql(t1 *testing.T) {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	buf := new(bytes.Buffer)
	sc := SpanContextWithIDs(1, 1)
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZap{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   time.Unix(0, 0),
						Caller:      "trace/buffer_pipe_sql_test.go:100",
						Message:     "info message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "trace/buffer_pipe_sql_test.go:100", "info message", "{}")`,
		},
		{
			name: "multi",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZap{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   time.Unix(0, 0),
						Caller:      "trace/buffer_pipe_sql_test.go:100",
						Message:     "info message",
						Extra:       "{}",
					},
					&MOZap{
						Level:       zapcore.DebugLevel,
						SpanContext: &sc,
						Timestamp:   time.Unix(0, int64(time.Microsecond+time.Millisecond)),
						Caller:      "trace/buffer_pipe_sql_test.go:100",
						Message:     "debug message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "trace/buffer_pipe_sql_test.go:100", "info message", "{}")` +
				`,(1, 1, 0, "Node", "1970-01-01 00:00:00.001001", "", "debug", "trace/buffer_pipe_sql_test.go:100", "debug message", "{}")`,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genZapLogBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genZapLogBatchSql() = %v,\n want %v", got, tt.want)
			} else {
				t1.Logf("SQL: %s", got)
			}
		})
	}
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
}

func Test_batchSqlHandler_genLogBatchSql(t1 *testing.T) {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	buf := new(bytes.Buffer)
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single",
			args: args{
				in: []IBuffer2SqlItem{
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(0),
						Level:       zapcore.InfoLevel,
						Caller:      util.Caller(0),
						Message:     "info message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "Test_batchSqlHandler_genLogBatchSql", "info message", "{}")`,
		},
		{
			name: "multi",
			args: args{
				in: []IBuffer2SqlItem{
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(0),
						Level:       zapcore.InfoLevel,
						Caller:      util.Caller(0),
						Message:     "info message",
						Extra:       "{}",
					},
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(time.Millisecond + time.Microsecond),
						Level:       zapcore.DebugLevel,
						Caller:      util.Caller(0),
						Message:     "debug message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "Test_batchSqlHandler_genLogBatchSql", "info message", "{}")` +
				`,(1, 1, 0, "Node", "1970-01-01 00:00:00.001001", "", "debug", "Test_batchSqlHandler_genLogBatchSql", "debug message", "{}")`,
		},
	}
	logStackFormatter.Store("%n")
	t1.Logf("%%n : %n", tests[0].args.in[0].(*MOLog).Caller)
	t1.Logf("%%s : %s", tests[0].args.in[0].(*MOLog).Caller)
	t1.Logf("%%+s: %+s", tests[0].args.in[0].(*MOLog).Caller)
	t1.Logf("%%v : %v", tests[0].args.in[0].(*MOLog).Caller)
	t1.Logf("%%+v: %+v", tests[0].args.in[0].(*MOLog).Caller)
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genLogBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genLogBatchSql() = %v, want %v", got, tt.want)
			} else {
				t1.Logf("SQL: %s", got)
			}
		})
	}
}

func Test_batchSqlHandler_genSpanBatchSql(t1 *testing.T) {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 1}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span1")),
						StartTimeNS: util.TimeNano(0),
						EndTimeNS:   util.TimeNano(time.Microsecond),
						Duration:    util.TimeNano(time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			want: `insert into system.span_info (` +
				"`span_id`, `statement_id`, `parent_span_id`, `node_id`, `node_type`, `resource`, `name`, `start_time`, `end_time`, `duration`" +
				`) values (1, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span1", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000001", 1000)`,
		},
		{
			name: "multi_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 1}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span1")),
						StartTimeNS: util.TimeNano(0),
						EndTimeNS:   util.TimeNano(time.Microsecond),
						Duration:    util.TimeNano(time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 2}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span2")),
						StartTimeNS: util.TimeNano(time.Microsecond),
						EndTimeNS:   util.TimeNano(time.Millisecond),
						Duration:    util.TimeNano(time.Millisecond - time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			want: `insert into system.span_info (` +
				"`span_id`, `statement_id`, `parent_span_id`, `node_id`, `node_type`, `resource`, `name`, `start_time`, `end_time`, `duration`" +
				`) values (1, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span1", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000001", 1000)` +
				`,(2, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span2", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.001000", 999000)`,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			got := genSpanBatchSql(tt.args.in, tt.args.buf)
			require.Equal(t1, tt.want, got)
		})
	}
}

func Test_batchSqlHandler_genStatementBatchSql(t1 *testing.T) {
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          1,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						RequestAt:            util.TimeNano(0),
						Status:               StatementStatusRunning,
						ExecPlan:             "",
					},
				},
				buf: buf,
			},
			want: `insert into system.statement_info (` +
				"`statement_id`, `transaction_id`, `session_id`, `account`, `user`, `host`, `database`, `statement`, `statement_tag`, `statement_fingerprint`, `node_id`, `node_type`, `request_at`, `status`, `exec_plan`" +
				`) values (1, 1, 1, "MO", "moroot", "", "system", "show tables", "show tables", "", 0, "Node", "1970-01-01 00:00:00.000000", "Running", "")`,
		},
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          1,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						RequestAt:            util.TimeNano(0),
						Status:               StatementStatusRunning,
						ExecPlan:             "",
					},
					&StatementInfo{
						StatementID:          2,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show databases",
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            util.TimeNano(time.Microsecond),
						Status:               StatementStatusFailed,
						ExecPlan:             "",
					},
				},
				buf: buf,
			},
			want: `insert into system.statement_info (` +
				"`statement_id`, `transaction_id`, `session_id`, `account`, `user`, `host`, `database`, `statement`, `statement_tag`, `statement_fingerprint`, `node_id`, `node_type`, `request_at`, `status`, `exec_plan`" +
				`) values (1, 1, 1, "MO", "moroot", "", "system", "show tables", "show tables", "", 0, "Node", "1970-01-01 00:00:00.000000", "Running", "")` +
				`,(2, 1, 1, "MO", "moroot", "", "system", "show databases", "show databases", "dcl", 0, "Node", "1970-01-01 00:00:00.000001", "Failed", "")`,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genStatementBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genStatementBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_GetBatch_AllType(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		sizeThreshold int64
	}
	defaultFields := fields{
		Reminder:      batchpipe.NewConstantClock(15 * time.Second),
		sizeThreshold: MB,
	}
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantFunc genBatchFunc
		want     string
	}{
		{
			name:   "single_error",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
				},
				buf: buf,
			},
			wantFunc: genErrorBatchSql,
			want: `insert into system.error_info (` +
				"`statement_id`, `span_id`, `node_id`, `node_type`, `err_code`, `stack`, `timestamp`" +
				`) values (` + nodeStateSpanIdStr + `, 0, "Node", "test1", "test1", "1970-01-01 00:00:00.000000")`,
		},
		{
			name:   "multi_error",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
					&MOErrorHolder{Error: err2, Timestamp: uint64(time.Millisecond + time.Microsecond)},
				},
				buf: buf,
			},
			wantFunc: genErrorBatchSql,
			want: `insert into system.error_info (` +
				"`statement_id`, `span_id`, `node_id`, `node_type`, `err_code`, `stack`, `timestamp`" +
				`) values (` + nodeStateSpanIdStr + `, 0, "Node", "test1", "test1", "1970-01-01 00:00:00.000000")` +
				`,(` + nodeStateSpanIdStr + `, 0, "Node", "test2: test1", "test2: test1", "1970-01-01 00:00:00.001001")`,
		},
		{
			name:   "single_log",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(0),
						Level:       zapcore.InfoLevel,
						Caller:      util.Caller(0),
						Message:     "info message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			wantFunc: genLogBatchSql,
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "Test_buffer2Sql_GetBatch_AllType", "info message", "{}")`,
		},
		{
			name:   "multi_log",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(0),
						Level:       zapcore.InfoLevel,
						Caller:      util.Caller(0),
						Message:     "info message",
						Extra:       "{}",
					},
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(time.Millisecond + time.Microsecond),
						Level:       zapcore.DebugLevel,
						Caller:      util.Caller(0),
						Message:     "debug message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			wantFunc: genLogBatchSql,
			want: `insert into system.log_info (` +
				"`span_id`, `statement_id`, `node_id`, `node_type`, `timestamp`, `name`, `level`, `caller`, `message`, `extra`" +
				`) values (1, 1, 0, "Node", "1970-01-01 00:00:00.000000", "", "info", "Test_buffer2Sql_GetBatch_AllType", "info message", "{}")` +
				`,(1, 1, 0, "Node", "1970-01-01 00:00:00.001001", "", "debug", "Test_buffer2Sql_GetBatch_AllType", "debug message", "{}")`,
		},
		{
			name:   "single_span",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 1}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span1")),
						StartTimeNS: util.TimeNano(0),
						EndTimeNS:   util.TimeNano(time.Microsecond),
						Duration:    util.TimeNano(time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			wantFunc: genSpanBatchSql,
			want: `insert into system.span_info (` +
				"`span_id`, `statement_id`, `parent_span_id`, `node_id`, `node_type`, `resource`, `name`, `start_time`, `end_time`, `duration`" +
				`) values (1, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span1", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000001", 1000)`,
		},
		{
			name:   "multi_span",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 1}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span1")),
						StartTimeNS: util.TimeNano(0),
						EndTimeNS:   util.TimeNano(time.Microsecond),
						Duration:    util.TimeNano(time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
					&MOSpan{
						SpanConfig:  SpanConfig{SpanContext: SpanContext{TraceID: 1, SpanID: 2}, parent: noopSpan{}},
						Name:        *bytes.NewBuffer([]byte("span2")),
						StartTimeNS: util.TimeNano(time.Microsecond),
						EndTimeNS:   util.TimeNano(time.Millisecond),
						Duration:    util.TimeNano(time.Millisecond - time.Microsecond),
						tracer:      gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			wantFunc: genSpanBatchSql,
			want: `insert into system.span_info (` +
				"`span_id`, `statement_id`, `parent_span_id`, `node_id`, `node_type`, `resource`, `name`, `start_time`, `end_time`, `duration`" +
				`) values (1, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span1", "1970-01-01 00:00:00.000000", "1970-01-01 00:00:00.000001", 1000)` +
				`,(2, 1, 0, 0, "Node", "{\"Node\":{\"node_id\":0,\"node_type\":0},\"version\":\"v0.test.0\"}", "span2", "1970-01-01 00:00:00.000001", "1970-01-01 00:00:00.001000", 999000)`,
		},
		{
			name:   "single_statement",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          1,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						RequestAt:            util.TimeNano(0),
						Status:               StatementStatusRunning,
						ExecPlan:             "",
					},
				},
				buf: buf,
			},
			wantFunc: genStatementBatchSql,
			want: `insert into system.statement_info (` +
				"`statement_id`, `transaction_id`, `session_id`, `account`, `user`, `host`, `database`, `statement`, `statement_tag`, `statement_fingerprint`, `node_id`, `node_type`, `request_at`, `status`, `exec_plan`" +
				`) values (1, 1, 1, "MO", "moroot", "", "system", "show tables", "show tables", "", 0, "Node", "1970-01-01 00:00:00.000000", "Running", "")`,
		},
		{
			name:   "multi_statement",
			fields: defaultFields,
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          1,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						RequestAt:            util.TimeNano(0),
						Status:               StatementStatusRunning,
						ExecPlan:             "",
					},
					&StatementInfo{
						StatementID:          2,
						TransactionID:        1,
						SessionID:            1,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show databases",
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            util.TimeNano(time.Microsecond),
						Status:               StatementStatusFailed,
						ExecPlan:             "",
					},
				},
				buf: buf,
			},
			wantFunc: genStatementBatchSql,
			want: `insert into system.statement_info (` +
				"`statement_id`, `transaction_id`, `session_id`, `account`, `user`, `host`, `database`, `statement`, `statement_tag`, `statement_fingerprint`, `node_id`, `node_type`, `request_at`, `status`, `exec_plan`" +
				`) values (1, 1, 1, "MO", "moroot", "", "system", "show tables", "show tables", "", 0, "Node", "1970-01-01 00:00:00.000000", "Running", "")` +
				`,(2, 1, 1, "MO", "moroot", "", "system", "show databases", "show databases", "dcl", 0, "Node", "1970-01-01 00:00:00.000001", "Failed", "")`,
		},
	}

	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bpImpl := NewBufferPipe2SqlWorker(
				bufferWithReminder(tt.fields.Reminder),
				bufferWithSizeThreshold(tt.fields.sizeThreshold),
			)
			b := bpImpl.NewItemBuffer(tt.args.in[0].GetName())
			t.Logf("buffer.type: %s", b.(*buffer2Sql).GetBufferType())
			for _, i := range tt.args.in {
				b.Add(i)
			}
			if got := b.(*buffer2Sql).genBatchFunc; reflect.ValueOf(got).Pointer() != reflect.ValueOf(tt.wantFunc).Pointer() {
				t.Errorf("buffer2Sql's genBatchFunc = %v, want %v", got, tt.wantFunc)
			}
			if got := b.GetBatch(tt.args.buf); got.(string) != tt.want {
				t.Errorf("GetBatch() = %v,\n want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_IsEmpty(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOLog{}},
				sizeThreshold: GB,
				batchFunc:     nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_Reset(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOLog{}},
				sizeThreshold: GB,
				batchFunc:     nil,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			b.Reset()
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_ShouldFlush(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	err1 := errors.WithStack(errors.New("test1"))
	err2 := errors.Wrapf(err1, "test2")
	tests := []struct {
		name        string
		fields      fields
		isNilBuffer bool
		want        bool
	}{
		{
			name: "empty/nil",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: KB,
				batchFunc:     nil,
			},
			isNilBuffer: true,
			want:        false,
		},
		{
			name: "empty/normal",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: KB,
				batchFunc:     genErrorBatchSql,
			},
			isNilBuffer: false,
			want:        false,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder: batchpipe.NewConstantClock(time.Hour),
				buf: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
					&MOErrorHolder{Error: err2, Timestamp: uint64(time.Millisecond + time.Microsecond)},
				},
				sizeThreshold: 512 * B,
				batchFunc:     genErrorBatchSql,
			},
			isNilBuffer: false,
			want:        true,
		},
	}
	errorFormatter.Store("%+v")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBuffer2Sql(
				bufferWithReminder(tt.fields.Reminder),
				bufferWithSizeThreshold(tt.fields.sizeThreshold),
				bufferWithGenBatchFunc(tt.fields.batchFunc),
			)
			t.Logf("ShouldFlush() get %p buffer", b)
			if assert.NotEqual(t, nil, b, "ShouldFlush() get %p buffer", b) != tt.isNilBuffer || tt.isNilBuffer {
				return
			}
			//assert.Equal(t, nil, b, "ShouldFlush() get nil Buffer")
			for _, i := range tt.fields.buf {
				b.Add(i)
			}
			if got := b.ShouldFlush(); got != tt.want {
				t.Errorf("ShouldFlush() = %v, want %v, lenght: %d", got, tt.want, b.Size())
			}
		})
	}
}

func Test_nanoSec2Datetime(t *testing.T) {
	type args struct {
		t util.TimeMono
	}
	tests := []struct {
		name string
		args args
		want types.Datetime
	}{
		{
			name: "1 ns",
			args: args{t: util.TimeNano(1)},
			want: types.Datetime(0),
		},
		{
			name: "1 us",
			args: args{t: util.TimeNano(time.Microsecond)},
			want: types.Datetime(1),
		},
		{
			name: "1 ms",
			args: args{t: util.TimeNano(time.Millisecond)},
			want: types.Datetime(1000),
		},
		{
			name: "1 hour + 1ms",
			args: args{t: util.TimeNano(time.Millisecond + time.Hour)},
			want: types.Datetime(((time.Hour / time.Second) << 20) + 1000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nanoSec2Datetime(tt.args.t); got != tt.want {
				t.Errorf("nanoSec2Datetime() = %d, want %d", got, tt.want)
			}
		})
	}
}

func Test_quote(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "'", args: args{value: `'`}, want: "\\'"},
		{name: `"`, args: args{value: `"`}, want: "\\\""},
		{name: `\n`, args: args{value: `\n`}, want: "\\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quote(tt.args.value); got != tt.want {
				t.Errorf("quote() = %v, want %v", got, tt.want)
			}
		})
	}
	var err1 = errors.WithStack(errors.New("test1"))
	t.Logf("show quote(err): \"%s\"", quote(fmt.Sprintf("%+v", err1)))
}

func Test_withGenBatchFunc(t *testing.T) {
	type args struct {
		f genBatchFunc
	}
	tests := []struct {
		name string
		args args
		want genBatchFunc
	}{
		{name: "genSpanBatchSql", args: args{f: genSpanBatchSql}, want: genSpanBatchSql},
		{name: "genLogBatchSql", args: args{f: genLogBatchSql}, want: genLogBatchSql},
		{name: "genStatementBatchSql", args: args{f: genStatementBatchSql}, want: genStatementBatchSql},
		{name: "genErrorBatchSql", args: args{f: genErrorBatchSql}, want: genErrorBatchSql},
	}
	buf := &buffer2Sql{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufferWithGenBatchFunc(tt.args.f).apply(buf)
			got := buf.genBatchFunc
			if reflect.ValueOf(got).Pointer() != reflect.ValueOf(tt.want).Pointer() {
				t.Errorf("bufferWithGenBatchFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withSizeThreshold(t *testing.T) {
	type args struct {
		size int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{name: "1  B", args: args{size: B}, want: 1},
		{name: "1 KB", args: args{size: KB}, want: 1 << 10},
		{name: "1 MB", args: args{size: MB}, want: 1 << 20},
		{name: "1 GB", args: args{size: GB}, want: 1 << 30},
		{name: "1.001 GB", args: args{size: GB + MB}, want: 1<<30 + 1<<20},
	}
	buf := &buffer2Sql{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bufferWithSizeThreshold(tt.args.size).apply(buf)
			if got := buf.sizeThreshold; got != tt.want {
				t.Errorf("bufferWithSizeThreshold() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_NewItemBatchHandler(t1 *testing.T) {
	type fields struct {
		defaultOpts []buffer2SqlOption
		ch          chan string
	}
	type args struct {
		batch string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   func(batch any)
	}{
		{
			name: "nil",
			fields: fields{
				defaultOpts: []buffer2SqlOption{bufferWithSizeThreshold(GB)},
				ch:          make(chan string, 10),
			},
			args: args{
				batch: "batch",
			},
			want: func(batch any) {},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			gTracerProvider = newMOTracerProvider(WithSQLExecutor(newExecutorFactory(tt.fields.ch)))
			t := batchSqlHandler{
				defaultOpts: tt.fields.defaultOpts,
			}

			wg := sync.WaitGroup{}
			startedC := make(chan struct{}, 1)
			wg.Add(1)
			go func() {
				startedC <- struct{}{}
			loop:
				for {
					batch, ok := <-tt.fields.ch
					if ok {
						require.Equal(t1, tt.args.batch, batch)
					} else {
						t1.Log("exec sql Done.")
						break loop
					}
				}
				wg.Done()
			}()
			<-startedC
			got := t.NewItemBatchHandler()
			got(tt.args.batch)
			close(tt.fields.ch)
			wg.Wait()
		})
	}
}
