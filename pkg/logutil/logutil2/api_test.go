// Copyright 2021 Matrix Origin
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

package logutil2

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func TestLog(t *testing.T) {
	type args struct {
		ctx    context.Context
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				ctx:    context.Background(),
				msg:    "test",
				fields: []zap.Field{zap.Int("int", 0), zap.String("string", "")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debug(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Info(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Warn(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Error(tt.args.ctx, tt.args.msg, tt.args.fields...)
		})
	}
}
func TestLog_panic(t *testing.T) {
	type args struct {
		ctx    context.Context
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				ctx:    context.Background(),
				msg:    "test",
				fields: []zap.Field{zap.Int("int", 0), zap.String("string", "")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				err := recover()
				require.Equal(t, tt.args.msg, fmt.Sprintf("%s", err))
			}()
			Panic(tt.args.ctx, tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogf(t *testing.T) {
	type args struct {
		ctx    context.Context
		msg    string
		fields []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "empty",
			args: args{
				ctx:    context.Background(),
				msg:    "test",
				fields: []any{},
			},
		},
		{
			name: "normal",
			args: args{
				ctx:    context.Background(),
				msg:    "hello %s",
				fields: []any{"world"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debugf(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Infof(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Warnf(tt.args.ctx, tt.args.msg, tt.args.fields...)
			Errorf(tt.args.ctx, tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLogf_panic(t *testing.T) {
	type args struct {
		ctx    context.Context
		msg    string
		fields []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "empty",
			args: args{
				ctx:    context.Background(),
				msg:    "test",
				fields: []any{},
			},
		},
		{
			name: "normal",
			args: args{
				ctx:    context.Background(),
				msg:    "hello %s",
				fields: []any{"world"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				err := recover()
				require.Equal(t, fmt.Sprintf(tt.args.msg, tt.args.fields...), fmt.Sprintf("%s", err))
			}()
			Panicf(tt.args.ctx, tt.args.msg, tt.args.fields...)
		})
	}
}
