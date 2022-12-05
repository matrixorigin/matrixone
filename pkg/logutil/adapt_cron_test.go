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

package logutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
				err:           moerr.NewInternalError(context.TODO(), "test"),
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
