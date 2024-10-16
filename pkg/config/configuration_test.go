// Copyright 2023 Matrix Origin
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

package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

func TestGetUnixSocketAddress(t *testing.T) {
	assert.NoError(t, os.RemoveAll("/tmp/TestGetUnixSocketAddress/"))
	f := &FrontendParameters{UnixSocketAddress: "/tmp/TestGetUnixSocketAddress/1"}
	assert.Equal(t, f.UnixSocketAddress, f.GetUnixSocketAddress())
}

func TestIsFileExist(t *testing.T) {
	existFile := "/tmp/TestIsFileExist/exist"
	notExistFile := "/tmp/TestIsFileExist/not_exist"
	assert.NoError(t, os.MkdirAll("/tmp/TestIsFileExist", 0755))
	defer func() {
		assert.NoError(t, os.RemoveAll("/tmp/TestIsFileExist"))
	}()

	f, err := os.Create(existFile)
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	ok, err := isFileExist(existFile)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = isFileExist(notExistFile)
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestObservabilityParameters_SetDefaultValues1(t *testing.T) {

	tests := []struct {
		name    string
		prepare func() *ObservabilityParameters
		check   func(t *testing.T, cfg *ObservabilityParameters)
	}{
		{
			name: "longQueryTime_0_enableAggr_200ms",
			prepare: func() *ObservabilityParameters {
				cfg := &ObservabilityParameters{
					LongQueryTime:          0.0,
					DisableStmtAggregation: false,
				}
				cfg.SelectAggThreshold.UnmarshalText([]byte("200ms"))
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, false, cfg.DisableStmtAggregation)
				require.Equal(t, 200*time.Millisecond, cfg.SelectAggThreshold.Duration)
				require.Equal(t, 0.2, cfg.LongQueryTime)
				require.Equal(t, false, cfg.DisableMetric)
				require.Equal(t, false, cfg.DisableTrace)
				require.Equal(t, false, cfg.DisableError)
				require.Equal(t, false, cfg.DisableSpan)
			},
		},
		{
			name: "longQueryTime_1.0_enableAggr_200ms",
			prepare: func() *ObservabilityParameters {
				cfg := &ObservabilityParameters{
					LongQueryTime:          1.0,
					DisableStmtAggregation: false,
				}
				cfg.SelectAggThreshold.UnmarshalText([]byte("200ms"))
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, false, cfg.DisableStmtAggregation)
				require.Equal(t, 200*time.Millisecond, cfg.SelectAggThreshold.Duration)
				require.Equal(t, 1.0, cfg.LongQueryTime)
			},
		},
		{
			name: "longQueryTime_0_disable_200ms",
			prepare: func() *ObservabilityParameters {
				cfg := &ObservabilityParameters{
					LongQueryTime:          0.0,
					DisableStmtAggregation: true,
				}
				cfg.SelectAggThreshold.UnmarshalText([]byte("200ms"))
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, true, cfg.DisableStmtAggregation)
				require.Equal(t, 200*time.Millisecond, cfg.SelectAggThreshold.Duration)
				require.Equal(t, 0.0, cfg.LongQueryTime)
			},
		},
		{
			name: "longQueryTime_1.0_disableAggr_200ms",
			prepare: func() *ObservabilityParameters {
				cfg := &ObservabilityParameters{
					LongQueryTime:          1.0,
					DisableStmtAggregation: true,
				}
				cfg.SelectAggThreshold.UnmarshalText([]byte("200ms"))
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, true, cfg.DisableStmtAggregation)
				require.Equal(t, 200*time.Millisecond, cfg.SelectAggThreshold.Duration)
				require.Equal(t, 1.0, cfg.LongQueryTime)
			},
		},
		{
			name: "resetLabelSelectorByOldConfig",
			prepare: func() *ObservabilityParameters {
				cfg := NewObservabilityParameters()
				cfg.ObservabilityOldParameters = ObservabilityOldParameters{
					StatusPortV12:         123,
					EnableMetricToPromV12: true,
					DisableMetricV12:      true,
					DisableTraceV12:       true,
					DisableErrorV12:       true,
					DisableSpanV12:        true,
					// part metric
					MetricUpdateStorageUsageIntervalV12: toml.Duration{Duration: time.Minute},
					// part statement_info
					EnableStmtMergeV12:        true,
					DisableStmtAggregationV12: true,
					AggregationWindowV12:      toml.Duration{Duration: time.Minute},
					SelectAggThresholdV12:     toml.Duration{Duration: time.Minute},
					LongQueryTimeV12:          123.0,
					SkipRunningStmtV12:        true,
					// part labelSelector
					LabelSelectorV12: map[string]string{
						"key1": "setOldVal",
					},
				}
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, 123, cfg.StatusPort)
				require.Equal(t, true, cfg.EnableMetricToProm)
				require.Equal(t, toml.Duration{Duration: time.Minute}, cfg.MetricStorageUsageUpdateInterval)
				require.Equal(t, true, cfg.DisableMetric)
				require.Equal(t, true, cfg.DisableTrace)
				require.Equal(t, true, cfg.DisableError)
				require.Equal(t, true, cfg.DisableSpan)
				require.Equal(t, true, cfg.EnableStmtMerge)
				require.Equal(t, true, cfg.DisableStmtAggregation)
				require.Equal(t, toml.Duration{Duration: time.Minute}, cfg.AggregationWindow)
				require.Equal(t, toml.Duration{Duration: time.Minute}, cfg.SelectAggThreshold)
				require.Equal(t, 123.0, cfg.LongQueryTime)
				require.Equal(t, true, cfg.SkipRunningStmt)
				require.Equal(t, map[string]string{"key1": "setOldVal"}, cfg.LabelSelector)
			},
		},
		{
			name: "resetLabelSelectorByOldConfig_keepSetVal(not default)",
			prepare: func() *ObservabilityParameters {
				cfg := NewObservabilityParameters()
				cfg.StatusPort = 7101
				cfg.EnableMetricToProm = true
				cfg.DisableMetric = true
				cfg.DisableTrace = true
				cfg.DisableError = true
				cfg.DisableSpan = true
				cfg.EnableStmtMerge = true
				cfg.MetricStorageUsageUpdateInterval.UnmarshalText([]byte("5m"))
				cfg.DisableStmtAggregation = true
				cfg.AggregationWindow.UnmarshalText([]byte("6s"))
				cfg.SelectAggThreshold.UnmarshalText([]byte("300ms"))
				cfg.LongQueryTime = 2.0
				cfg.SkipRunningStmt = !defaultSkipRunningStmt
				cfg.LabelSelector = map[string]string{
					"key1": "setVal",
				}
				// old
				cfg.ObservabilityOldParameters = ObservabilityOldParameters{
					StatusPortV12:         123,
					EnableMetricToPromV12: true,
					// part metric
					MetricUpdateStorageUsageIntervalV12: toml.Duration{Duration: time.Minute},
					// part trace
					DisableMetricV12: true,
					DisableTraceV12:  true,
					DisableErrorV12:  true,
					DisableSpanV12:   true,
					// part statement_info
					EnableStmtMergeV12:        true,
					DisableStmtAggregationV12: true,
					AggregationWindowV12:      toml.Duration{Duration: time.Minute},
					SelectAggThresholdV12:     toml.Duration{Duration: time.Minute},
					LongQueryTimeV12:          123.0,
					SkipRunningStmtV12:        true,
					// part labelSelector
					LabelSelectorV12: map[string]string{
						"key1": "setOldVal",
					},
				}
				return cfg
			},
			check: func(t *testing.T, cfg *ObservabilityParameters) {
				require.Equal(t, 7101, cfg.StatusPort)
				require.Equal(t, true, cfg.EnableMetricToProm)
				require.Equal(t, toml.Duration{Duration: 5 * time.Minute}, cfg.MetricStorageUsageUpdateInterval)
				require.Equal(t, true, cfg.DisableMetric)
				require.Equal(t, true, cfg.DisableTrace)
				require.Equal(t, true, cfg.DisableError)
				require.Equal(t, true, cfg.DisableSpan)
				require.Equal(t, true, cfg.EnableStmtMerge)
				require.Equal(t, true, cfg.DisableStmtAggregation)
				require.Equal(t, toml.Duration{Duration: 6 * time.Second}, cfg.AggregationWindow)
				require.Equal(t, toml.Duration{Duration: 300 * time.Millisecond}, cfg.SelectAggThreshold)
				require.Equal(t, 2.0, cfg.LongQueryTime)
				require.Equal(t, !defaultSkipRunningStmt, cfg.SkipRunningStmt)
				require.Equal(t, map[string]string{"key1": "setVal"}, cfg.LabelSelector)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.prepare()
			cfg.SetDefaultValues("test")
			tt.check(t, cfg)
		})
	}
}
