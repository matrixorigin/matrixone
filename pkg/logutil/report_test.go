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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNoop(t *testing.T) {
	require.Equal(t, zap.String("span", "{}"), noopContextField(context.Background()))
	buf, err := noopReportZap(nil, zapcore.Entry{}, nil)
	require.Equal(t, nil, err)
	require.Equal(t, "", buf.String())
	require.Equal(t, zap.Bool(MOInternalFiledKeyNoopReport, true), NoReportFiled())
}

func TestReport(t *testing.T) {
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
		name         string
		fields       fields
		wantLevel    zap.AtomicLevel
		wantOpts     []zap.Option
		wantSyncer   zapcore.WriteSyncer
		wantEncoder  zapcore.Encoder
		wantSinksLen int
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
			wantLevel:    zap.NewAtomicLevelAt(zap.DebugLevel),
			wantOpts:     []zap.Option{zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller()},
			wantSyncer:   getConsoleSyncer(),
			wantEncoder:  getLoggerEncoder("console"),
			wantSinksLen: 2,
		},
	}

	//zapReporter.Store(reportZapFunc(func(encoder zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	//}))
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			cfg := &LogConfig{
				Level:      tt.fields.Level,
				Format:     tt.fields.Format,
				Filename:   tt.fields.Filename,
				MaxSize:    tt.fields.MaxSize,
				MaxDays:    tt.fields.MaxDays,
				MaxBackups: tt.fields.MaxBackups,
			}
			require.Equal(t, tt.wantLevel, cfg.getLevel())
			require.Equal(t, len(tt.wantOpts), len(cfg.getOptions()))
			require.Equal(t, tt.wantSyncer, cfg.getSyncer())
			wantMsg, _ := tt.wantEncoder.EncodeEntry(tt.fields.Entry, nil)
			gotMsg, _ := cfg.getEncoder().EncodeEntry(tt.fields.Entry, nil)
			require.Equal(t, wantMsg.String(), gotMsg.String())
			require.Equal(t, 2, len(cfg.getSinks()))
			SetupMOLogger(cfg)

			gotCfg := getGlobalLogConfig()
			require.Equal(t, cfg.DisableStore, gotCfg.DisableStore)

			Info("hello", GetContextFieldFunc()(context.Background()), zap.Int("int", 0))
			Info("hello, noop", NoReportFiled())

		})
	}
}
