package logutil

import (
	"fmt"
	"testing"
)

func TestCronLogger_Error(t *testing.T) {
	type fields struct {
		logInfo bool
	}
	type args struct {
		err           error
		msg           string
		keysAndValues []any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "normal",
			fields: fields{
				logInfo: false,
			},
			args: args{
				err:           fmt.Errorf("test"),
				msg:           "hello world",
				keysAndValues: []any{"int", 1, "key", "val"},
			},
		},
		{
			name: "no_error",
			fields: fields{
				logInfo: true,
			},
			args: args{
				err:           nil,
				msg:           "hello world",
				keysAndValues: []any{"int", 1, "key", "val"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := GetCronLogger(tt.fields.logInfo)
			l.Error(tt.args.err, tt.args.msg, tt.args.keysAndValues...)
		})
	}
}

func TestCronLogger_Info(t *testing.T) {
	type fields struct {
		logInfo bool
	}
	type args struct {
		msg           string
		keysAndValues []any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "normal",
			fields: fields{
				logInfo: true,
			},
			args: args{
				msg:           "hello world",
				keysAndValues: []any{"int", 1, "key", "val"},
			},
		},
		{
			name: "disable",
			fields: fields{
				logInfo: false,
			},
			args: args{
				msg:           "hello world",
				keysAndValues: []any{"int", 1, "key", "val"},
			},
		},
		{
			name: "empty_kv",
			fields: fields{
				logInfo: true,
			},
			args: args{
				msg:           "hello world",
				keysAndValues: []any{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := GetCronLogger(tt.fields.logInfo)
			l.Info(tt.args.msg, tt.args.keysAndValues...)
		})
	}
}
