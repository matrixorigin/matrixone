// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestUpdateWatermarkOnlyLogging(t *testing.T) {
	// The logger and reporter are process-wide. Keep both subtests serial.
	for _, level := range []zapcore.Level{zap.InfoLevel, zap.DebugLevel} {
		t.Run(level.String(), func(t *testing.T) {
			key := WatermarkKey{AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "table"}
			label := key.String()
			const event = "cdc.watermark.buffer_update"
			core, logs := observer.New(zap.DebugLevel)
			previous := logutil.GetReportZapFunc()
			logutil.SetLogReporter(&logutil.TraceReporter{
				ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
					if entry.Message == event {
						_ = core.Write(entry, fields)
					}
					return previous(enc, entry, fields)
				},
			})
			t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous}) })
			restore := logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: level.String(), DisableLog: true})
			t.Cleanup(func() { require.True(t, restore(), "logger configuration changed during test") })

			updater := NewCDCWatermarkUpdater(t.Name(), nil)
			t.Cleanup(updater.lifecycle.cancel)
			counter := v2.CdcWatermarkUpdateCounter.WithLabelValues(label, "commit")
			t.Cleanup(func() { v2.CdcWatermarkUpdateCounter.DeleteLabelValues(label, "commit") })
			before := testutil.ToFloat64(counter)

			// Two updates distinguish the first insert from an overwrite.
			values := []types.TS{types.BuildTS(100, 1), types.BuildTS(200, 2)}
			for i, value := range values {
				require.NoError(t, updater.UpdateWatermarkOnly(t.Context(), &key, &value))
				buffered, err := updater.GetFromCache(t.Context(), &key)
				require.NoError(t, err)
				require.Equal(t, value, buffered)
				require.Equal(t, before+float64(i+1), testutil.ToFloat64(counter))
			}

			entries := logs.FilterField(zap.String("task-id", key.TaskId)).All()
			if level == zap.InfoLevel {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, len(values))
			var old types.TS
			for i, entry := range entries {
				require.Equal(t, zap.DebugLevel, entry.Level)
				require.Subset(t, entry.ContextMap(), map[string]any{
					"key":           label,
					"old-watermark": old.ToString(),
					"new-watermark": values[i].ToString(),
					"has-old":       i > 0,
				})
				old = values[i]
			}
		})
	}
}
