package errors

import (
	"context"
	goErrors "errors"
	"reflect"
	"testing"
)

var ctx = context.Background()
var testErr = goErrors.New("test error")
var stackErr = WithStack(testErr)
var msgErr = WithMessage(stackErr, "prefix")

func TestGetContextTracer(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want context.Context
	}{
		{
			name: "nil",
			args: args{err: goErrors.New("test error")},
			want: nil,
		},
		{
			name: "context",
			args: args{err: WithContext(context.Background(), goErrors.New("test error"))},
			want: context.Background(),
		},
		{
			name: "stack",
			args: args{err: WithStack(goErrors.New("test error"))},
			want: nil,
		},
		{
			name: "message",
			args: args{err: WithMessagef(goErrors.New("test error"), "prefix")},
			want: nil,
		},
		{
			name: "stack/context",
			args: args{err: WithStack(WithContext(context.Background(), goErrors.New("test error")))},
			want: context.Background(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetContextTracer(tt.args.err); !reflect.DeepEqual(got, tt.want) && !reflect.DeepEqual(got.Context(), tt.want) {
				t.Errorf("GetContextTracer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasContext(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil",
			args: args{err: goErrors.New("test error")},
			want: false,
		},
		{
			name: "context",
			args: args{err: WithContext(context.Background(), goErrors.New("test error"))},
			want: true,
		},
		{
			name: "stack",
			args: args{err: WithStack(goErrors.New("test error"))},
			want: false,
		},
		{
			name: "message",
			args: args{err: WithMessagef(goErrors.New("test error"), "prefix")},
			want: false,
		},
		{
			name: "stack/context",
			args: args{err: WithStack(WithContext(context.Background(), goErrors.New("test error")))},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasContext(tt.args.err); got != tt.want {
				t.Errorf("HasContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Cause(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "normal",
			fields:  fields{cause: testErr, ctx: ctx},
			wantErr: testErr,
		},
		{
			name:    "stack",
			fields:  fields{stackErr, ctx},
			wantErr: stackErr,
		},
		{
			name:    "message",
			fields:  fields{msgErr, ctx},
			wantErr: msgErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Cause(); !reflect.DeepEqual(got, tt.wantErr) {
				t.Errorf("Cause() error = %v, wantErr %v", got, tt.wantErr)
			}
		})
	}
}

func Test_withContext_Context(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name   string
		fields fields
		want   context.Context
	}{
		{
			name:   "normal",
			fields: fields{cause: testErr, ctx: ctx},
			want:   ctx,
		},
		{
			name:   "stack",
			fields: fields{stackErr, ctx},
			want:   ctx,
		},
		{
			name:   "message",
			fields: fields{msgErr, ctx},
			want:   ctx,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Context(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Context() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Error(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "normal",
			fields: fields{cause: testErr, ctx: ctx},
			want:   "test error",
		},
		{
			name:   "stack",
			fields: fields{stackErr, ctx},
			want:   "test error",
		},
		{
			name:   "message",
			fields: fields{msgErr, ctx},
			want:   "prefix: test error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withContext_Unwrap(t *testing.T) {
	type fields struct {
		cause error
		ctx   context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name:    "normal",
			fields:  fields{cause: testErr, ctx: ctx},
			wantErr: testErr,
		},
		{
			name:    "stack",
			fields:  fields{stackErr, ctx},
			wantErr: stackErr,
		},
		{
			name:    "message",
			fields:  fields{msgErr, ctx},
			wantErr: msgErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withContext{
				cause: tt.fields.cause,
				ctx:   tt.fields.ctx,
			}
			if got := w.Unwrap(); !reflect.DeepEqual(got, tt.wantErr) {
				t.Errorf("Unwrap() = %v, want %v", got, tt.wantErr)
			}
		})
	}
}
