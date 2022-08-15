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
	"context"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
	"time"
)

func TestReportLog(t *testing.T) {
	type args struct {
		ctx       context.Context
		level     zapcore.Level
		depth     int
		formatter string
		args      []any
	}
	tests := []struct {
		name         string
		enableTracer bool
		args         args
	}{
		{
			name:         "close",
			enableTracer: false,
			args: args{
				ctx:       ContextWithSpanContext(context.Background(), SpanContextWithIDs(0, 0)),
				level:     zapcore.InfoLevel,
				depth:     3,
				formatter: "info message",
				args:      []any{},
			},
		},
		{
			name:         "collect",
			enableTracer: true,
			args: args{
				ctx:       ContextWithSpanContext(context.Background(), SpanContextWithIDs(0, 0)),
				level:     zapcore.InfoLevel,
				depth:     3,
				formatter: "info message",
				args:      []any{},
			},
		},
	}
	for _, tt := range tests {
		gTracerProvider.EnableTracer(tt.enableTracer)
		t.Run(tt.name, func(t *testing.T) {
			ReportLog(tt.args.ctx, tt.args.level, tt.args.depth, tt.args.formatter, tt.args.args...)
		})
	}
}

func TestReportZap(t *testing.T) {
	type args struct {
		jsonEncoder zapcore.Encoder
		entry       zapcore.Entry
		fields      []zapcore.Field
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normal",
			args: args{
				jsonEncoder: zapcore.NewJSONEncoder(
					zapcore.EncoderConfig{
						StacktraceKey:  "stacktrace",
						LineEnding:     zapcore.DefaultLineEnding,
						EncodeLevel:    zapcore.LowercaseLevelEncoder,
						EncodeTime:     zapcore.EpochTimeEncoder,
						EncodeDuration: zapcore.SecondsDurationEncoder,
						EncodeCaller:   zapcore.ShortCallerEncoder,
					}),
				entry: zapcore.Entry{
					Level:      zapcore.InfoLevel,
					Time:       time.Unix(0, 0),
					LoggerName: "test",
					Message:    "info message",
					Caller:     zapcore.NewEntryCaller(uintptr(util.Caller(3)), "file", 123, true),
				},
				fields: []zapcore.Field{zap.Int("key", 1)},
			},
			want: `{"key":1}` + "\n",
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
