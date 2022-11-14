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
	"io"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

var buf = new(bytes.Buffer)
var err1 = moerr.NewInternalError("test1")
var err2 = errutil.Wrapf(err1, "test2")
var traceIDSpanIDColumnStr string
var traceIDSpanIDCsvStr string

func noopReportError(context.Context, error, int) {}

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	if _, err := Init(
		context.Background(),
		EnableTracer(true),
		WithMOVersion("v0.test.0"),
		WithNode("node_uuid", NodeTypeStandalone),
		WithBatchProcessMode(FileService),
		WithFSWriterFactory(func(ctx context.Context, _ string, _ batchpipe.HasName, _ ...export.FSWriterOption) io.StringWriter {
			return os.Stdout
		}),
		WithSQLExecutor(func() internalExecutor.InternalExecutor {
			return nil
		}),
		WithExportInterval(15),
		WithLongQueryTime(0),
		DebugMode(true),
	); err != nil {
		panic(err)
	}
	errutil.SetErrorReporter(noopReportError)

	sc := SpanFromContext(DefaultContext()).SpanContext()
	traceIDSpanIDColumnStr = fmt.Sprintf(`"%s", "%s"`, sc.TraceID.String(), sc.SpanID.String())
	traceIDSpanIDCsvStr = fmt.Sprintf(`%s,%s`, sc.TraceID.String(), sc.SpanID.String())

	if err := agent.Listen(agent.Options{}); err != nil {
		_ = moerr.NewInternalError("listen gops agent failed: %s", err)
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
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOZapLog{}},
				sizeThreshold: mpool.GB,
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
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOZapLog{}},
				sizeThreshold: mpool.GB,
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

func Test_withSizeThreshold(t *testing.T) {
	type args struct {
		size int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{name: "1  B", args: args{size: 1}, want: 1},
		{name: "1 KB", args: args{size: mpool.KB}, want: 1 << 10},
		{name: "1 MB", args: args{size: mpool.MB}, want: 1 << 20},
		{name: "1 GB", args: args{size: mpool.GB}, want: 1 << 30},
		{name: "1.001 GB", args: args{size: mpool.GB + mpool.MB}, want: 1<<30 + 1<<20},
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

/*
var gCtrlSqlCh = make(chan struct{}, 1)
func Test_batchSqlHandler_NewItemBatchHandler(t1 *testing.T) {
	gCtrlSqlCh <- struct{}{}
	type fields struct {
		defaultOpts []bufferOption
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
				defaultOpts: []bufferOption{bufferWithSizeThreshold(GB)},
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
			WithSQLExecutor(newDummyExecutorFactory(tt.fields.ch)).apply(&GetTracerProvider().tracerProviderConfig)
			t := batchSqlHandler{
				defaultOpts: tt.fields.defaultOpts,
			}

			got := t.NewItemBatchHandler(context.TODO())
			go got(tt.args.batch)
			batch, ok := <-tt.fields.ch
			if ok {
				require.Equal(t1, tt.args.batch, batch)
			} else {
				t1.Log("exec sql Done.")
			}
			//close(tt.fields.ch)
		})
	}
	WithSQLExecutor(func() internalExecutor.InternalExecutor { return nil }).apply(&GetTracerProvider().tracerProviderConfig)
	<-gCtrlSqlCh
}*/

func Test_genCsvData(t *testing.T) {
	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	sc := SpanContextWithIDs(_1TraceID, _1SpanID)
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "single_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig: SpanConfig{SpanContext: SpanContext{TraceID: _1TraceID, SpanID: _1SpanID}, parent: noopSpan{}},
						Name:       *bytes.NewBuffer([]byte("span1")),
						StartTime:  zeroTime,
						EndTime:    zeroTime.Add(time.Microsecond),
						tracer:     gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			want: `span_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,,,,,{},0,,,span1,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",internal
`,
		},
		{
			name: "multi_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig: SpanConfig{SpanContext: SpanContext{TraceID: _1TraceID, SpanID: _1SpanID, Kind: SpanKindStatement}, parent: noopSpan{}},
						Name:       *bytes.NewBuffer([]byte("span1")),
						StartTime:  zeroTime,
						EndTime:    zeroTime.Add(time.Microsecond),
						tracer:     gTracer.(*MOTracer),
					},
					&MOSpan{
						SpanConfig: SpanConfig{SpanContext: SpanContext{TraceID: _1TraceID, SpanID: _2SpanID, Kind: SpanKindRemote}, parent: noopSpan{}},
						Name:       *bytes.NewBuffer([]byte("span2")),
						StartTime:  zeroTime.Add(time.Microsecond),
						EndTime:    zeroTime.Add(time.Millisecond),
						tracer:     gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			want: `span_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,,,,,{},0,,,span1,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",statement
span_info,node_uuid,Standalone,0000000000000002,00000000-0000-0000-0000-000000000001,,,,,,{},0,,,span2,0,0001-01-01 00:00:00.000001,0001-01-01 00:00:00.001000,999000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",remote
`,
		},
		{
			name: "single_zap",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZapLog{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   zeroTime,
						Caller:      "trace/buffer_pipe_sql_test.go:912",
						Message:     "info message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,0001-01-01 00:00:00.000000,info,trace/buffer_pipe_sql_test.go:912,info message,{},0,,,,0,,,0,{},internal
`,
		},
		{
			name: "multi_zap",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZapLog{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   zeroTime,
						Caller:      "trace/buffer_pipe_sql_test.go:939",
						Message:     "info message",
						Extra:       "{}",
					},
					&MOZapLog{
						Level:       zapcore.DebugLevel,
						SpanContext: &sc,
						Timestamp:   zeroTime.Add(time.Microsecond + time.Millisecond),
						Caller:      "trace/buffer_pipe_sql_test.go:939",
						Message:     "debug message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,0001-01-01 00:00:00.000000,info,trace/buffer_pipe_sql_test.go:939,info message,{},0,,,,0,,,0,{},internal
log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,0001-01-01 00:00:00.001001,debug,trace/buffer_pipe_sql_test.go:939,debug message,{},0,,,,0,,,0,{},internal
`,
		},
		{
			name: "single_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			want: `00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,Running,0,,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000001""}",0,0
`,
		},
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show databases",
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            zeroTime.Add(time.Microsecond),
						ResponseAt:           zeroTime.Add(time.Microsecond + time.Second),
						Duration:             time.Microsecond + time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError("test error"),
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			want: `00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,Running,0,,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000001""}",0,0
00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,0001-01-01 00:00:00.000001,0001-01-01 00:00:01.000001,1000001000,Failed,20101,internal error: test error,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000002""}",0,0
`,
		},
		{
			name: "single_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: zeroTime},
				},
				buf: buf,
			},
			want: `error_info,node_uuid,Standalone,0,0,,0001-01-01 00:00:00.000000,,,,{},20101,internal error: test1,internal error: test1,,0,,,0,{},
`,
		},
		{
			name: "multi_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: zeroTime},
					&MOErrorHolder{Error: err2, Timestamp: zeroTime.Add(time.Millisecond + time.Microsecond)},
				},
				buf: buf,
			},
			want: `error_info,node_uuid,Standalone,0,0,,0001-01-01 00:00:00.000000,,,,{},20101,internal error: test1,internal error: test1,,0,,,0,{},
error_info,node_uuid,Standalone,0,0,,0001-01-01 00:00:00.001001,,,,{},20101,test2: internal error: test1,test2: internal error: test1,,0,,,0,{},
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genCsvData(tt.args.in, tt.args.buf)
			require.NotEqual(t, nil, got)
			req, ok := got.(CSVRequests)
			require.Equal(t, true, ok)
			require.Equal(t, 1, len(req))
			batch := req[0]
			assert.Equalf(t, tt.want, batch.content, "genCsvData(%v, %v)", batch.content, tt.args.buf)
			t.Logf("%s", tt.want)
		})
	}
}

func Test_genCsvData_diffAccount(t *testing.T) {
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "single_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			want: []string{`00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,Running,0,,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000001""}",0,0
`},
		},
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "sys",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show databases",
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            zeroTime.Add(time.Microsecond),
						ResponseAt:           zeroTime.Add(time.Microsecond + time.Second),
						Duration:             time.Microsecond + time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError("test error"),
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			want: []string{`00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,Running,0,,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000001""}",0,0
`, `00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,sys,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,0001-01-01 00:00:00.000001,0001-01-01 00:00:01.000001,1000001000,Failed,20101,internal error: test error,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000002""}",0,0
`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genCsvData(tt.args.in, tt.args.buf)
			require.NotEqual(t, nil, got)
			reqs, ok := got.(CSVRequests)
			require.Equal(t, true, ok)
			require.Equal(t, len(tt.args.in), len(reqs))
			require.Equal(t, len(tt.args.in), len(tt.want))
			for _, req := range reqs {
				found := false
				for idx, w := range tt.want {
					if w == req.content {
						found = true
						t.Logf("idx %d: %s", idx, w)
					}
				}
				assert.Equalf(t, true, found, "genCsvData: %v", req.content)
			}
		})
	}
}

func Test_genCsvData_LongQueryTime(t *testing.T) {
	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	type args struct {
		in     []IBuffer2SqlItem
		buf    *bytes.Buffer
		queryT int64
	}
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						Duration:             time.Second - time.Nanosecond,
					},
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show tables",
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						Duration:             time.Second - time.Nanosecond,
						SerializeExecPlan:    dummySerializeExecPlan,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            "show databases",
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            zeroTime.Add(time.Microsecond),
						ResponseAt:           zeroTime.Add(time.Microsecond + time.Second),
						Duration:             time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError("test error"),
						ExecPlan:             map[string]string{"key": "val"},
						SerializeExecPlan:    dummySerializeExecPlan,
					},
				},
				buf:    buf,
				queryT: int64(time.Second),
			},
			want: `00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,999999999,Running,0,,"{""code"":200,""message"":""NO ExecPlan Serialize function"",""steps"":null,""success"":false,""uuid"":""00000000-0000-0000-0000-000000000001""}",0,0
00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,999999999,Running,0,,"{""code"":200,""message"":""no exec plan""}",0,0
00000000-0000-0000-0000-000000000002,00000000-0000-0000-0000-000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,0001-01-01 00:00:00.000001,0001-01-01 00:00:01.000001,1000000000,Failed,20101,internal error: test error,"{""key"":""val""}",1,1
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetTracerProvider().longQueryTime = tt.args.queryT
			got := genCsvData(tt.args.in, tt.args.buf)
			require.NotEqual(t, nil, got)
			req, ok := got.(CSVRequests)
			require.Equal(t, true, ok)
			require.Equal(t, 1, len(req))
			batch := req[0]
			assert.Equalf(t, tt.want, batch.content, "genCsvData(%v, %v)", batch.content, tt.args.buf)
			t.Logf("%s", tt.want)
			GetTracerProvider().longQueryTime = 0
		})
	}
}
