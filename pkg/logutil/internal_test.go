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
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestLogConfig_getter(t *testing.T) {
	type fields struct {
		Level      string
		Format     string
		Filename   string
		MaxSize    int
		MaxDays    int
		MaxBackups int

		Entry zapcore.Entry
	}
	tests := []struct {
		name        string
		fields      fields
		wantLevel   zap.AtomicLevel
		wantOpts    []zap.Option
		wantSyncer  zapcore.WriteSyncer
		wantEncoder zapcore.Encoder
		wantSinks   []ZapSink
	}{
		{
			name: "normal",
			fields: fields{
				Level:      "debug",
				Format:     "console",
				Filename:   "",
				MaxSize:    0,
				MaxDays:    0,
				MaxBackups: 0,

				Entry: zapcore.Entry{Level: zapcore.DebugLevel, Message: "console msg"},
			},
			wantLevel:   zap.NewAtomicLevelAt(zap.DebugLevel),
			wantOpts:    []zap.Option{zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller()},
			wantSyncer:  getConsoleSyncer(),
			wantEncoder: getLoggerEncoder("console"),
			wantSinks:   []ZapSink{{getLoggerEncoder("console"), getConsoleSyncer()}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &LogConfig{
				Level:      tt.fields.Level,
				Format:     tt.fields.Format,
				Filename:   tt.fields.Filename,
				MaxSize:    tt.fields.MaxSize,
				MaxDays:    tt.fields.MaxDays,
				MaxBackups: tt.fields.MaxBackups,

				DisableStore: true,
			}
			require.Equal(t, tt.wantLevel, cfg.GetLevel())
			require.Equal(t, len(tt.wantOpts), len(cfg.getOptions()))
			require.Equal(t, tt.wantSyncer, cfg.getSyncer())
			wantMsg, _ := tt.wantEncoder.EncodeEntry(tt.fields.Entry, nil)
			gotMsg, _ := cfg.getEncoder().EncodeEntry(tt.fields.Entry, nil)
			require.Equal(t, wantMsg.String(), gotMsg.String())
			require.Equal(t, len(tt.wantSinks), len(cfg.getSinks()))
		})
	}
}

func TestSetupMOLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type args struct {
		conf *LogConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "console",
			args: args{conf: &LogConfig{
				Level:      zapcore.DebugLevel.String(),
				Format:     "console",
				Filename:   "",
				MaxSize:    512,
				MaxDays:    0,
				MaxBackups: 0,

				DisableStore:    true,
				StacktraceLevel: "panic",
			}},
		},
		{
			name: "json",
			args: args{conf: &LogConfig{
				Level:      zapcore.DebugLevel.String(),
				Format:     "json",
				Filename:   "",
				MaxSize:    512,
				MaxDays:    0,
				MaxBackups: 0,

				DisableStore:    true,
				StacktraceLevel: "error",
			}},
		},
		/*{
		    // fix gopkg.in/natefinch/lumberjack.v2@v2.0.0/lumberjack.go:390 have a background goroutine for rotate-log
			name: "json",
			args: args{conf: &LogConfig{
				Level:      zapcore.DebugLevel.String(),
				Format:     "json",
				Filename:   path.Join(t.TempDir(), "json.log"),
				MaxSize:    0,
				MaxDays:    0,
				MaxBackups: 0,

				DisableStore: true,
			}},
		},*/
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetupMOLogger(tt.args.conf)
		})
	}
}

func TestSetupMOLogger_panic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type args struct {
		conf *LogConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "panic",
			args: args{conf: &LogConfig{
				Level:      zapcore.DebugLevel.String(),
				Format:     "panic",
				Filename:   "",
				MaxSize:    512,
				MaxDays:    0,
				MaxBackups: 0,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					require.Equal(t, moerr.NewInternalErrorf(context.TODO(), "unsupported log format: %s", tt.args.conf.Format), err)
				} else {
					t.Errorf("not receive panic")
				}
			}()
			SetupMOLogger(tt.args.conf)
		})
	}
}

func Test_getLoggerEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	epoch := time.Unix(0, 0).UTC()
	tests := []struct {
		name, format string
		ts           time.Time
		want         string
	}{
		{
			name: "console/UTC", format: "console", ts: epoch,
			want: "1970/01/01 00:00:00.000000 +0000 DEBUG console msg\n",
		},
		{
			name: "console/negative_offset", format: "console", ts: epoch.In(time.FixedZone("west", -5*60*60)),
			want: "1969/12/31 19:00:00.000000 -0500 DEBUG console msg\n",
		},
		{
			name: "console/fractional_offset", format: "console", ts: epoch.In(time.FixedZone("east", (5*60+30)*60)),
			want: "1970/01/01 05:30:00.000000 +0530 DEBUG console msg\n",
		},
		{
			name: "json", format: "json",
			want: `{"level":"DEBUG","msg":"json msg"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLoggerEncoder(tt.format)
			require.NotNil(t, got)
			buf, err := got.EncodeEntry(zapcore.Entry{Time: tt.ts, Level: zapcore.DebugLevel, Message: tt.format + " msg"}, nil)
			require.NoError(t, err)
			defer buf.Free()
			if tt.format == "json" {
				require.JSONEq(t, tt.want, buf.String())
			} else {
				require.Equal(t, tt.want, buf.String())
			}
		})
	}
}

func TestSetupMOLogger_panicDir(t *testing.T) {
	type args struct {
		conf *LogConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{conf: &LogConfig{
				Level:      zapcore.DebugLevel.String(),
				Format:     "json",
				Filename:   t.TempDir(),
				MaxSize:    512,
				MaxDays:    0,
				MaxBackups: 0,

				DisableStore: true,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					require.Equal(t, "log file can't be a directory", err)
				} else {
					t.Errorf("not receive panic")
				}
			}()
			SetupMOLogger(tt.args.conf)
		})
	}
}
