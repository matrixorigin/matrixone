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
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestReportZap(t *testing.T) {
	type args struct {
		jsonEncoder zapcore.Encoder
		entry       zapcore.Entry
		fields      []zapcore.Field
	}
	spanField := trace.ContextField(trace.ContextWithSpanContext(context.Background(), trace.SpanContext{}))
	entry := zapcore.Entry{
		Level:      zapcore.InfoLevel,
		Time:       time.Unix(0, 0),
		LoggerName: "test",
		Message:    "info message",
		Caller:     zapcore.NewEntryCaller(uintptr(stack.Caller(3)), "file", 123, true),
	}
	encoder := zapcore.NewJSONEncoder(
		zapcore.EncoderConfig{
			StacktraceKey:  "stacktrace",
			SkipLineEnding: true,
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.EpochTimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		})
	intField := zap.Int("key", 1)
	strField := zap.String("str", "1")
	boolField := zap.Bool("bool", true)
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField},
			},
			want: `{"key":1}`,
		},
		{
			name: "remove first span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{spanField, intField, strField},
			},
			want: `{"str":"1","key":1}`,
		},
		{
			name: "remove middle span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, spanField, strField},
			},
			want: `{"key":1,"str":"1"}`,
		},
		{
			name: "remove double middle span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, spanField, spanField, strField, boolField},
			},
			want: `{"key":1,"bool":true,"str":"1"}`,
		},
		{
			name: "remove end span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, strField, spanField, spanField},
			},
			want: `{"key":1,"str":"1"}`,
		},
		{
			name: "remove multi span",
			args: args{
				jsonEncoder: encoder,
				entry:       entry,
				fields:      []zapcore.Field{intField, strField, spanField, boolField, spanField, spanField},
			},
			want: `{"key":1,"str":"1","bool":true}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReportZap(tt.args.jsonEncoder, tt.args.entry, tt.args.fields)
			require.Equal(t, nil, err)
			require.Equal(t, tt.want, got.String())
		})
	}
}
