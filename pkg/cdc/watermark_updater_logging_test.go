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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestCDCWatermarkUpdater_UpdateWatermarkOnlyLogging(t *testing.T) {
	for _, level := range []string{"info", "debug"} {
		t.Run(level, func(t *testing.T) {
			key := WatermarkKey{AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "table"}
			core, observed := observer.New(zap.DebugLevel)
			previous := logutil.GetReportZapFunc()
			logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
				if entry.Message == "cdc.watermark.buffer_update" {
					for _, field := range fields {
						if field.Key == "task-id" && field.String == key.TaskId {
							_ = core.Write(entry, fields)
							break
						}
					}
				}
				return previous(enc, entry, fields)
			}})
			t.Cleanup(func() { logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previous}) })
			restore := logutil.SetupMOLoggerForTest(&logutil.LogConfig{Level: level, DisableLog: true})
			t.Cleanup(func() { require.True(t, restore(), "logger configuration changed during test") })

			updater := NewCDCWatermarkUpdater(t.Name(), nil)
			t.Cleanup(updater.lifecycle.cancel)
			counter := v2.CdcWatermarkUpdateCounter.WithLabelValues(key.String(), "commit")
			t.Cleanup(func() { v2.CdcWatermarkUpdateCounter.DeleteLabelValues(key.String(), "commit") })
			var metric dto.Metric
			require.NoError(t, counter.Write(&metric))
			initialCount := metric.GetCounter().GetValue()

			// Distinct tier sizes make the emitted counts independently observable.
			updater.cacheCommitting[WatermarkKey{TaskId: "committing"}] = types.BuildTS(1, 1)
			updater.cacheCommitted[WatermarkKey{TaskId: "committed-1"}] = types.BuildTS(2, 1)
			updater.cacheCommitted[WatermarkKey{TaskId: "committed-2"}] = types.BuildTS(3, 1)
			values := []types.TS{types.BuildTS(100, 1), types.BuildTS(200, 2)}
			for i := range values {
				require.NoError(t, updater.UpdateWatermarkOnly(context.Background(), &key, &values[i]))
				got, err := updater.GetFromCache(context.Background(), &key)
				require.NoError(t, err)
				require.Equal(t, values[i], got)
				require.NoError(t, counter.Write(&metric))
				require.Equal(t, initialCount+float64(i+1), metric.GetCounter().GetValue())
			}

			entries := observed.All()
			if level == "info" {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, 2)
			for i, entry := range entries {
				require.Equal(t, zap.DebugLevel, entry.Level)
				old := types.TS{}
				if i > 0 {
					old = values[i-1]
				}
				require.Equal(t, map[string]any{
					"task-id":           key.TaskId,
					"key":               key.String(),
					"old-watermark":     old.ToString(),
					"new-watermark":     values[i].ToString(),
					"has-old":           i > 0,
					"uncommitted-count": int64(1),
					"committing-count":  int64(1),
					"committed-count":   int64(2),
				}, entry.ContextMap())
			}
		})
	}
}
