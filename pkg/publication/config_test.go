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

package publication

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultCCPRConfig(t *testing.T) {
	cfg := DefaultCCPRConfig()
	require.NotNil(t, cfg)
	assert.Equal(t, 10, cfg.PublicationWorkerThread)
	assert.Equal(t, 1000, cfg.FilterObjectWorkerThread)
	assert.Equal(t, 10, cfg.GetChunkWorkerThread)
	assert.Equal(t, 100, cfg.WriteObjectWorkerThread)
	assert.Equal(t, int64(3*1024*1024*1024), cfg.GetChunkMaxMemory)
	assert.Equal(t, int64(100*1024*1024), cfg.GetChunkSize)
	assert.Equal(t, 5, cfg.RetryTimes)
	assert.Equal(t, 10, cfg.RetryThreshold)
	assert.Equal(t, 100000, cfg.BloomFilterExpectedItems)
	assert.Equal(t, 0.01, cfg.BloomFilterFalsePositiveRate)
}

func TestCCPRConfig_Validate_Valid(t *testing.T) {
	cfg := DefaultCCPRConfig()
	assert.NoError(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidPublicationWorkerThread(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.PublicationWorkerThread = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidFilterObjectWorkerThread(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.FilterObjectWorkerThread = -1
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidGetChunkWorkerThread(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.GetChunkWorkerThread = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidWriteObjectWorkerThread(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.WriteObjectWorkerThread = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidGetChunkMaxMemory(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.GetChunkMaxMemory = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidGetChunkSize(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.GetChunkSize = -1
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidRetryTimes(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.RetryTimes = -1
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidRetryThreshold(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.RetryThreshold = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidBloomFilterExpectedItems(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.BloomFilterExpectedItems = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidBloomFilterFalsePositiveRate_Zero(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.BloomFilterFalsePositiveRate = 0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_Validate_InvalidBloomFilterFalsePositiveRate_One(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.BloomFilterFalsePositiveRate = 1.0
	assert.Error(t, cfg.Validate())
}

func TestCCPRConfig_GetChunkMaxConcurrent(t *testing.T) {
	cfg := DefaultCCPRConfig()
	// 3GB / 100MB = 30
	assert.Equal(t, 30, cfg.GetChunkMaxConcurrent())
}

func TestCCPRConfig_GetChunkMaxConcurrent_ZeroChunkSize(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.GetChunkSize = 0
	assert.Equal(t, 1, cfg.GetChunkMaxConcurrent())
}

func TestSetGlobalConfig_Nil(t *testing.T) {
	assert.Error(t, SetGlobalConfig(nil))
}

func TestSetGlobalConfig_Invalid(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.PublicationWorkerThread = 0
	assert.Error(t, SetGlobalConfig(cfg))
}

func TestSetGlobalConfig_Valid(t *testing.T) {
	// Ensure globalConfigOnce has fired before testing SetGlobalConfig
	_ = GetGlobalConfig()

	cfg := DefaultCCPRConfig()
	cfg.PublicationWorkerThread = 20
	require.NoError(t, SetGlobalConfig(cfg))
	assert.Equal(t, 20, GetGlobalConfig().PublicationWorkerThread)
	// restore
	_ = SetGlobalConfig(DefaultCCPRConfig())
}

func TestUpdateGlobalConfig(t *testing.T) {
	// ensure default is set
	_ = SetGlobalConfig(DefaultCCPRConfig())
	err := UpdateGlobalConfig(func(c *CCPRConfig) {
		c.RetryTimes = 99
	})
	require.NoError(t, err)
	assert.Equal(t, 99, GetGlobalConfig().RetryTimes)
	// restore
	_ = SetGlobalConfig(DefaultCCPRConfig())
}

func TestUpdateGlobalConfig_InvalidResult(t *testing.T) {
	_ = SetGlobalConfig(DefaultCCPRConfig())
	err := UpdateGlobalConfig(func(c *CCPRConfig) {
		c.PublicationWorkerThread = -1
	})
	assert.Error(t, err)
	// restore
	_ = SetGlobalConfig(DefaultCCPRConfig())
}

func TestGetterFunctions(t *testing.T) {
	cfg := DefaultCCPRConfig()
	_ = SetGlobalConfig(cfg)
	defer func() { _ = SetGlobalConfig(DefaultCCPRConfig()) }()

	assert.Equal(t, cfg.PublicationWorkerThread, GetPublicationWorkerThread())
	assert.Equal(t, cfg.FilterObjectWorkerThread, GetFilterObjectWorkerThread())
	assert.Equal(t, cfg.GetChunkWorkerThread, GetGetChunkWorkerThread())
	assert.Equal(t, cfg.WriteObjectWorkerThread, GetWriteObjectWorkerThread())
	assert.Equal(t, cfg.GetChunkMaxMemory, GetGetChunkMaxMemory())
	assert.Equal(t, cfg.GetChunkSize, GetGetChunkSize())
	assert.Equal(t, cfg.GCInterval.Duration, GetGCInterval())
	assert.Equal(t, cfg.GCTTL.Duration, GetGCTTL())
	assert.Equal(t, cfg.SyncTaskInterval.Duration, GetSyncTaskInterval())
	assert.Equal(t, cfg.RetryTimes, GetRetryTimes())
	assert.Equal(t, cfg.RetryInterval.Duration, GetRetryInterval())
	assert.Equal(t, cfg.SyncProtectionTTLDuration.Duration, GetSyncProtectionTTLDuration())
	assert.Equal(t, cfg.SyncProtectionRenewInterval.Duration, GetSyncProtectionRenewInterval())
	assert.Equal(t, cfg.BloomFilterExpectedItems, GetBloomFilterExpectedItems())
	assert.Equal(t, cfg.BloomFilterFalsePositiveRate, GetBloomFilterFalsePositiveRate())
	assert.Equal(t, cfg.RetryThreshold, GetRetryThreshold())
}

func TestCCPRConfig_Validate_RetryTimesZero(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.RetryTimes = 0 // zero is valid (non-negative)
	assert.NoError(t, cfg.Validate())
}

func TestCCPRConfig_GetChunkMaxConcurrent_NegativeChunkSize(t *testing.T) {
	cfg := DefaultCCPRConfig()
	cfg.GetChunkSize = -1
	assert.Equal(t, 1, cfg.GetChunkMaxConcurrent())
}

func TestSetGlobalConfig_CustomValues(t *testing.T) {
	cfg := &CCPRConfig{
		PublicationWorkerThread:      5,
		FilterObjectWorkerThread:     500,
		GetChunkWorkerThread:         5,
		WriteObjectWorkerThread:      50,
		GetChunkMaxMemory:            1024 * 1024 * 1024,
		GetChunkSize:                 50 * 1024 * 1024,
		GCInterval:                   toml.Duration{Duration: 30 * time.Minute},
		GCTTL:                        toml.Duration{Duration: 24 * time.Hour},
		SyncTaskInterval:             toml.Duration{Duration: 5 * time.Second},
		RetryTimes:                   3,
		RetryInterval:                toml.Duration{Duration: 2 * time.Second},
		SyncProtectionTTLDuration:    toml.Duration{Duration: 10 * time.Minute},
		SyncProtectionRenewInterval:  toml.Duration{Duration: 3 * time.Minute},
		BloomFilterExpectedItems:     50000,
		BloomFilterFalsePositiveRate: 0.001,
		RetryThreshold:               5,
	}
	require.NoError(t, SetGlobalConfig(cfg))
	got := GetGlobalConfig()
	assert.Equal(t, 5, got.PublicationWorkerThread)
	assert.Equal(t, int64(1024*1024*1024), got.GetChunkMaxMemory)
	// restore
	_ = SetGlobalConfig(DefaultCCPRConfig())
}
