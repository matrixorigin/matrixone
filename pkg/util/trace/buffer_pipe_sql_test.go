package trace

import (
	"bytes"
	"reflect"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"

	"github.com/stretchr/testify/assert"
)

type Size struct {
	HasItemSize
}

func (s Size) Size() int64 {
	return 64
}

func (s Size) GetName() string {
	return "size"
}

func TestStructIndexes(t *testing.T) {

	_ = NewBufferPipe2SqlWorker().(batchpipe.PipeImpl[batchpipe.HasName, string])

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
	tests := []struct {
		name string
		args args
		want batchpipe.PipeImpl[batchpipe.HasName, any]
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBufferPipe2SqlWorker(tt.args.opt...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewBufferPipe2SqlWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_NewItemBatchHandler(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	tests := []struct {
		name   string
		fields fields
		want   func(batch string)
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.NewItemBatchHandler(); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("NewItemBatchHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_NewItemBuffer(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   batchpipe.ItemBuffer[HasItemSize, any]
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.NewItemBuffer(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t1.Errorf("NewItemBuffer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genErrorBatchSql(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	type args struct {
		in  []HasItemSize
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
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.genErrorBatchSql(tt.args.in, tt.args.buf); got != tt.want {
				t1.Errorf("genErrorBatchSql() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchSqlHandler_genLogBatchSql(t1 *testing.T) {
	type fields struct {
		opt []buffer2SqlOption
	}
	type args struct {
		in  []HasItemSize
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
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.genLogBatchSql(tt.args.in, tt.args.buf); got != tt.want {
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
		in  []HasItemSize
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
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.genSpanBatchSql(tt.args.in, tt.args.buf); got != tt.want {
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
		in  []HasItemSize
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
			t := batchSqlHandler{
				opt: tt.fields.opt,
			}
			if got := t.genStatementBatchSql(tt.args.in, tt.args.buf); got != tt.want {
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
		buf           []HasItemSize
		mux           sync.Mutex
		sizeThreshold int64
		batchFunc     genBatchFunc
	}
	type args struct {
		item HasItemSize
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
				batchFunc:     tt.fields.batchFunc,
			}
			b.Add(tt.args.item)
		})
	}
}

func Test_buffer2Sql_GetBatch(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []HasItemSize
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
				batchFunc:     tt.fields.batchFunc,
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
		buf           []HasItemSize
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
				batchFunc:     tt.fields.batchFunc,
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
		buf           []HasItemSize
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
				batchFunc:     tt.fields.batchFunc,
			}
			b.Reset()
		})
	}
}

func Test_buffer2Sql_ShouldFlush(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []HasItemSize
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
				batchFunc:     tt.fields.batchFunc,
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
			if got := withGenBatchFunc(tt.args.f); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("withGenBatchFunc() = %v, want %v", got, tt.want)
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
			if got := withSizeThreshold(tt.args.size); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("withSizeThreshold() = %v, want %v", got, tt.want)
			}
		})
	}
}
