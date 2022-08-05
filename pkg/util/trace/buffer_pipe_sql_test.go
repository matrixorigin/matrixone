package trace

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/zap/zapcore"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"
)

var _ IBuffer2SqlItem = &Size{}

type Size struct {
	IBuffer2SqlItem
}

func (s Size) GetName() string {
	return "size"
}

func (s Size) Size() int64 {
	return 64
}

func (s Size) Free() {}

func init() {
	setup()
}

var testBaseBuffer2SqlOption = []buffer2SqlOption{bufferWithSizeThreshold(1 * KB)}

func setup() {
	if _, err := Init(
		context.Background(),
		&config.GlobalSystemVariables,
		DebugMode(true),
		EnableTracer(false),
		WithMOVersion("v0.test.0"),
		WithNode(config.GlobalSystemVariables.GetNodeID(), NodeTypeNode),
		WithSQLExecutor(func() ie.InternalExecutor {
			return nil
		}),
	); err != nil {
		panic(err)
	}
	if err := agent.Listen(agent.Options{}); err != nil {
		fmt.Errorf("listen gops agent failed: %s", err)
		panic(err)
	}
	fmt.Println("Finish tests init.")
}

func teardown() {
	agent.Close()
	fmt.Println("After all tests")
}

func Test_newBuffer2Sql_base(t *testing.T) {

	buf := newBuffer2Sql()
	byteBuf := new(bytes.Buffer)
	assert.Equal(t, true, buf.IsEmpty())
	buf.Add(&Size{})
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
			want: &batchSqlHandler{opts: opts},
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

/*
func Test_batchSqlHandler_NewItemBatchHandler(t1 *testing.T) {
	type fields struct {
		opts []buffer2SqlOption
	}
	tests := []struct {
		name   string
		fields fields
		want   func(batch string)
	}{
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := batchSqlHandler{
				opts: tt.fields.opts,
			}
			if got := t.NewItemBatchHandler(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("NewItemBatchHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}*/

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
				opts: opts,
			}
			if got := t.NewItemBuffer(tt.args.name); reflect.ValueOf(got.(*buffer2Sql).genBatchFunc).Pointer() != reflect.ValueOf(tt.want).Pointer() {
				t1.Errorf("NewItemBuffer()'s genBatchFunc = %v, want %v", got.(*buffer2Sql).genBatchFunc, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genErrorBatchSql(t1 *testing.T) {
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	buf := new(bytes.Buffer)
	err1 := errors.WithStack(errors.New("test1"))
	err2 := errors.Wrapf(err1, "test2")
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
			want: `insert into system.error_info (` + "`err_code`" + ", `stack`" + ", `timestamp`" + `) values ("test1", "test1", "0001-01-01 00:00:00.000000")`,
		},
		{
			name: "multi_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: uint64(0)},
					&MOErrorHolder{Error: err2, Timestamp: uint64(time.Millisecond + time.Microsecond)},
				},
				buf: buf,
			},
			want: `insert into system.error_info (` + "`err_code`" + ", `stack`" + ", `timestamp`" + `) values ("test1", "test1", "0001-01-01 00:00:00.000000"),("test2: test1", "test2: test1", "0001-01-01 00:00:00.001001")`,
		},
	}
	errorFormatter.Store("%v")
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genErrorBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genErrorBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genLogBatchSql(t1 *testing.T) {
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
		// TODO: Add test cases.
		{
			name: "single",
			args: args{
				in: []IBuffer2SqlItem{
					&MOLog{
						StatementId: 1,
						SpanId:      1,
						Timestamp:   uint64(time.Millisecond + time.Microsecond),
						Level:       zapcore.InfoLevel,
						CodeLine:    util.Frame(0),
						Message:     "message",
					},
				},
				buf: buf,
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genLogBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genLogBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genSpanBatchSql(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genSpanBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genSpanBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genStatementBatchSql(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			if got := genStatementBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genStatementBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2SqlOptionFunc_apply(t *testing.T) {
	type args struct {
		b *buffer2Sql
	}
	tests := []struct {
		name string
		f    buffer2SqlOptionFunc
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.f.apply(tt.args.b)
		})
	}
}

func Test_buffer2Sql_Add(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	type args struct {
		item IBuffer2SqlItem
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				mux:           tt.fields.mux,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			b.Add(tt.args.item)
		})
	}
}

func Test_buffer2Sql_GetBatch(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	type args struct {
		buf *bytes.Buffer
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				mux:           tt.fields.mux,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			if got := b.GetBatch(tt.args.buf); got != tt.want {
				t.Errorf("GetBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_IsEmpty(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				mux:           tt.fields.mux,
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
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				mux:           tt.fields.mux,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			b.Reset()
		})
	}
}

func Test_buffer2Sql_ShouldFlush(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &buffer2Sql{
				Reminder:      tt.fields.Reminder,
				buf:           tt.fields.buf,
				mux:           tt.fields.mux,
				sizeThreshold: tt.fields.sizeThreshold,
				genBatchFunc:  tt.fields.batchFunc,
			}
			if got := b.ShouldFlush(); got != tt.want {
				t.Errorf("ShouldFlush() = %v, want %v", got, tt.want)
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nanoSec2Datetime(tt.args.t); got != tt.want {
				t.Errorf("nanoSec2Datetime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_newBuffer2Sql(t *testing.T) {
	type args struct {
		opts []buffer2SqlOption
	}
	tests := []struct {
		name string
		args args
		want *buffer2Sql
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newBuffer2Sql(tt.args.opts...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newBuffer2Sql() = %v, want %v", got, tt.want)
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
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quote(tt.args.value); got != tt.want {
				t.Errorf("quote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withGenBatchFunc(t *testing.T) {
	type args struct {
		f genBatchFunc
	}
	tests := []struct {
		name string
		args args
		want buffer2SqlOption
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bufferWithGenBatchFunc(tt.args.f); !reflect.DeepEqual(got, tt.want) {
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
		want buffer2SqlOption
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bufferWithSizeThreshold(tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("bufferWithSizeThreshold() = %v, want %v", got, tt.want)
			}
		})
	}
}
