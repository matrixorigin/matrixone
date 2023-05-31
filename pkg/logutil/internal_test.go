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
	"github.com/lni/goutils/leaktest"
	"regexp"
	"testing"

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
			require.Equal(t, tt.wantLevel, cfg.getLevel())
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
					require.Equal(t, moerr.NewInternalError(context.TODO(), "unsupported log format: %s", tt.args.conf.Format), err)
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
	type args struct {
		format string
	}
	type fields struct {
		entry  zapcore.Entry
		fields []zap.Field
	}
	tests := []struct {
		name       string
		args       args
		fields     fields
		wantOutput *regexp.Regexp
		foundCnt   int
	}{
		{
			name: "console",
			args: args{
				format: "console",
			},
			fields: fields{
				entry:  zapcore.Entry{Level: zapcore.DebugLevel, Message: "console msg"},
				fields: []zap.Field{},
			},
			// like: 0001/01/01 00:00:00.000000 +0000 DEBUG console msg
			wantOutput: regexp.MustCompile(`\d{4}/\d{2}/\d{2} (\d{2}:{0,1}){3}\.\d{6} \+\d{4} DEBUG console msg`),
			foundCnt:   1,
		},
		{
			name: "json",
			args: args{
				format: "json",
			},
			fields: fields{
				entry:  zapcore.Entry{Level: zapcore.DebugLevel, Message: "json msg"},
				fields: []zap.Field{},
			},
			// like: 0001/01/01 00:00:00.000000 +0000 DEBUG console msg
			wantOutput: regexp.MustCompile(`\{.*"level":"DEBUG".*"msg":"json msg".*\}`),
			foundCnt:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getLoggerEncoder(tt.args.format)
			require.NotNil(t, got)
			buf, err := got.EncodeEntry(tt.fields.entry, tt.fields.fields)
			require.Nil(t, err)
			t.Logf("encode result: %s", buf.String())
			found := tt.wantOutput.FindAll(buf.Bytes(), -1)
			t.Logf("found: %s", found)
			require.Equal(t, tt.foundCnt, len(found))
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
