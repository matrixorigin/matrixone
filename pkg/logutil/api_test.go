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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLog(t *testing.T) {
	type args struct {
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
				msg:    "test",
				fields: []zap.Field{zap.Int("int", 0), zap.String("string", "")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debug(tt.args.msg, tt.args.fields...)
			Info(tt.args.msg, tt.args.fields...)
			Warn(tt.args.msg, tt.args.fields...)
			Error(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestLog_panic(t *testing.T) {
	type args struct {
		msg    string
		fields []zap.Field
	}
	tests := []struct {
		name   string
		args   args
		printf bool
	}{
		{
			name: "panic",
			args: args{
				msg:    "test",
				fields: []zap.Field{zap.Int("int", 0), zap.String("string", "")},
			},
			printf: false,
		},
		{
			name: "panicF",
			args: args{
				msg:    "test %s",
				fields: []zap.Field{zap.Int("int", 0), zap.String("string", "")},
			},
			printf: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				err := recover()
				switch tt.printf {
				case true:
					require.Equal(t, fmt.Sprintf(tt.args.msg, "test"), fmt.Sprintf("%s", err))
				default:
					require.Equal(t, tt.args.msg, fmt.Sprintf("%s", err))
				}

			}()
			if tt.printf {
				Panicf(tt.args.msg, "test")
			} else {
				Panic(tt.args.msg, tt.args.fields...)
			}
		})
	}
}

func TestLogf(t *testing.T) {
	type args struct {
		msg    string
		fields []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				msg:    "test",
				fields: []any{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Debugf(tt.args.msg, tt.args.fields...)
			Infof(tt.args.msg, tt.args.fields...)
			Warnf(tt.args.msg, tt.args.fields...)
			Errorf(tt.args.msg, tt.args.fields...)
			gl := GoettyLogger{}
			gl.Debugf(tt.args.msg, tt.args.fields...)
			gl.Infof(tt.args.msg, tt.args.fields...)
			gl.Errorf(tt.args.msg, tt.args.fields...)
			//gl.Fatalf(tt.args.msg, tt.args.fields...)
		})
	}
}

func TestAdjust(t *testing.T) {
	type args struct {
		logger  *zap.Logger
		options []zap.Option
	}
	tests := []struct {
		name string
		args args
		want *zap.Logger
	}{
		{
			name: "normal",
			args: args{
				logger:  GetGlobalLogger(),
				options: nil,
			},
			want: GetGlobalLogger(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Adjust(tt.args.logger, tt.args.options...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Adjust() = %v, want %v", got, tt.want)
			}
		})
	}
}
