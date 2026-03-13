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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

// Constants for backward compatibility (used by frontend/get_object.go)
const (
	// GetChunkSize is the default size of each chunk (100MB)
	GetChunkSize int64 = 100 * 1024 * 1024
	// GetChunkMaxMemory is the default maximum memory for concurrent GetChunkJob operations (3GB)
	GetChunkMaxMemory int64 = 3 * 1024 * 1024 * 1024
)

// CCPRConfig holds all configuration parameters for CCPR (Cross-Cluster Publication Replication)
type CCPRConfig struct {
	// ============================================================================
	// Worker Pool Configuration
	// ============================================================================

	// PublicationWorkerThread is the number of threads for publication worker pool
	PublicationWorkerThread int `toml:"publication-worker-thread"`
	// FilterObjectWorkerThread is the number of threads for filter object worker pool
	FilterObjectWorkerThread int `toml:"filter-object-worker-thread"`
	// GetChunkWorkerThread is the number of threads for get chunk worker pool
	GetChunkWorkerThread int `toml:"get-chunk-worker-thread"`
	// WriteObjectWorkerThread is the number of threads for write object worker pool
	WriteObjectWorkerThread int `toml:"write-object-worker-thread"`

	// ============================================================================
	// Memory and Chunk Configuration
	// ============================================================================

	// GetChunkMaxMemory is the maximum memory for concurrent GetChunkJob operations
	GetChunkMaxMemory int64 `toml:"get-chunk-max-memory"`
	// GetChunkSize is the size of each chunk
	GetChunkSize int64 `toml:"get-chunk-size"`

	// ============================================================================
	// Executor Configuration
	// ============================================================================

	// GCInterval is the interval for garbage collection
	GCInterval toml.Duration `toml:"gc-interval"`
	// GCTTL is the time-to-live for garbage collection items
	GCTTL toml.Duration `toml:"gc-ttl"`
	// SyncTaskInterval is the interval for sync task execution
	SyncTaskInterval toml.Duration `toml:"sync-task-interval"`
	// RetryTimes is the number of retry attempts for failed operations
	RetryTimes int `toml:"retry-times"`
	// RetryInterval is the interval between retries
	RetryInterval toml.Duration `toml:"retry-interval"`

	// ============================================================================
	// Sync Protection Configuration
	// ============================================================================

	// SyncProtectionTTLDuration is the TTL duration for sync protection
	SyncProtectionTTLDuration toml.Duration `toml:"sync-protection-ttl-duration"`
	// SyncProtectionRenewInterval is the interval for renewing sync protection
	SyncProtectionRenewInterval toml.Duration `toml:"sync-protection-renew-interval"`

	// ============================================================================
	// Bloom Filter Configuration
	// ============================================================================

	// BloomFilterExpectedItems is the expected number of items in bloom filter
	BloomFilterExpectedItems int `toml:"bloom-filter-expected-items"`
	// BloomFilterFalsePositiveRate is the false positive rate for bloom filter
	BloomFilterFalsePositiveRate float64 `toml:"bloom-filter-false-positive-rate"`

	// ============================================================================
	// Error Handling Configuration
	// ============================================================================

	// RetryThreshold is the maximum number of retries before stopping
	RetryThreshold int `toml:"retry-threshold"`
}

// DefaultCCPRConfig returns the default CCPR configuration
func DefaultCCPRConfig() *CCPRConfig {
	return &CCPRConfig{
		// Worker Pool defaults
		PublicationWorkerThread:  10,
		FilterObjectWorkerThread: 1000,
		GetChunkWorkerThread:     10,
		WriteObjectWorkerThread:  100,

		// Memory and Chunk defaults
		GetChunkMaxMemory: 3 * 1024 * 1024 * 1024, // 3GB
		GetChunkSize:      100 * 1024 * 1024,      // 100MB

		// Executor defaults
		GCInterval:       toml.Duration{Duration: time.Hour},
		GCTTL:            toml.Duration{Duration: time.Hour * 24 * 7}, // 7 days
		SyncTaskInterval: toml.Duration{Duration: time.Second * 10},
		RetryTimes:       5,
		RetryInterval:    toml.Duration{Duration: time.Second},

		// Sync Protection defaults
		SyncProtectionTTLDuration:   toml.Duration{Duration: 15 * time.Minute},
		SyncProtectionRenewInterval: toml.Duration{Duration: 5 * time.Minute},

		// Bloom Filter defaults
		BloomFilterExpectedItems:     100000,
		BloomFilterFalsePositiveRate: 0.01,

		// Error Handling defaults
		RetryThreshold: 10,
	}
}

// Validate validates the CCPR configuration
func (c *CCPRConfig) Validate() error {
	if c.PublicationWorkerThread <= 0 {
		return moerr.NewInternalErrorNoCtx("publication-worker-thread must be positive")
	}
	if c.FilterObjectWorkerThread <= 0 {
		return moerr.NewInternalErrorNoCtx("filter-object-worker-thread must be positive")
	}
	if c.GetChunkWorkerThread <= 0 {
		return moerr.NewInternalErrorNoCtx("get-chunk-worker-thread must be positive")
	}
	if c.WriteObjectWorkerThread <= 0 {
		return moerr.NewInternalErrorNoCtx("write-object-worker-thread must be positive")
	}
	if c.GetChunkMaxMemory <= 0 {
		return moerr.NewInternalErrorNoCtx("get-chunk-max-memory must be positive")
	}
	if c.GetChunkSize <= 0 {
		return moerr.NewInternalErrorNoCtx("get-chunk-size must be positive")
	}
	if c.RetryTimes < 0 {
		return moerr.NewInternalErrorNoCtx("retry-times must be non-negative")
	}
	if c.RetryThreshold <= 0 {
		return moerr.NewInternalErrorNoCtx("retry-threshold must be positive")
	}
	if c.BloomFilterExpectedItems <= 0 {
		return moerr.NewInternalErrorNoCtx("bloom-filter-expected-items must be positive")
	}
	if c.BloomFilterFalsePositiveRate <= 0 || c.BloomFilterFalsePositiveRate >= 1 {
		return moerr.NewInternalErrorNoCtx("bloom-filter-false-positive-rate must be between 0 and 1")
	}
	return nil
}

// GetChunkMaxConcurrent returns the maximum number of concurrent chunk operations
func (c *CCPRConfig) GetChunkMaxConcurrent() int {
	if c.GetChunkSize <= 0 {
		return 1
	}
	return int(c.GetChunkMaxMemory / c.GetChunkSize)
}

// ============================================================================
// Global Configuration Instance
// ============================================================================

var (
	globalConfig     *CCPRConfig
	globalConfigOnce sync.Once
	globalConfigMu   sync.RWMutex
)

// GetGlobalConfig returns the global CCPR configuration instance
// If not initialized, returns the default configuration
func GetGlobalConfig() *CCPRConfig {
	globalConfigOnce.Do(func() {
		globalConfig = DefaultCCPRConfig()
	})
	globalConfigMu.RLock()
	defer globalConfigMu.RUnlock()
	return globalConfig
}

// SetGlobalConfig sets the global CCPR configuration
// This should be called during service initialization
func SetGlobalConfig(cfg *CCPRConfig) error {
	if cfg == nil {
		return moerr.NewInternalErrorNoCtx("config cannot be nil")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	globalConfigMu.Lock()
	defer globalConfigMu.Unlock()
	globalConfig = cfg
	return nil
}

// UpdateGlobalConfig updates specific fields in the global configuration
// This allows for runtime configuration updates
func UpdateGlobalConfig(updater func(*CCPRConfig)) error {
	globalConfigMu.Lock()
	defer globalConfigMu.Unlock()
	if globalConfig == nil {
		globalConfig = DefaultCCPRConfig()
	}
	updater(globalConfig)
	return globalConfig.Validate()
}

// ============================================================================
// Configuration Accessor Functions
// ============================================================================

// These functions provide convenient access to configuration values
// They use the global configuration instance

// GetPublicationWorkerThread returns the configured publication worker thread count
func GetPublicationWorkerThread() int {
	return GetGlobalConfig().PublicationWorkerThread
}

// GetFilterObjectWorkerThread returns the configured filter object worker thread count
func GetFilterObjectWorkerThread() int {
	return GetGlobalConfig().FilterObjectWorkerThread
}

// GetGetChunkWorkerThread returns the configured get chunk worker thread count
func GetGetChunkWorkerThread() int {
	return GetGlobalConfig().GetChunkWorkerThread
}

// GetWriteObjectWorkerThread returns the configured write object worker thread count
func GetWriteObjectWorkerThread() int {
	return GetGlobalConfig().WriteObjectWorkerThread
}

// GetGetChunkMaxMemory returns the configured max memory for chunk operations
func GetGetChunkMaxMemory() int64 {
	return GetGlobalConfig().GetChunkMaxMemory
}

// GetGetChunkSize returns the configured chunk size
func GetGetChunkSize() int64 {
	return GetGlobalConfig().GetChunkSize
}

// GetGCInterval returns the configured GC interval
func GetGCInterval() time.Duration {
	return GetGlobalConfig().GCInterval.Duration
}

// GetGCTTL returns the configured GC TTL
func GetGCTTL() time.Duration {
	return GetGlobalConfig().GCTTL.Duration
}

// GetSyncTaskInterval returns the configured sync task interval
func GetSyncTaskInterval() time.Duration {
	return GetGlobalConfig().SyncTaskInterval.Duration
}

// GetRetryTimes returns the configured retry times
func GetRetryTimes() int {
	return GetGlobalConfig().RetryTimes
}

// GetRetryInterval returns the configured retry interval
func GetRetryInterval() time.Duration {
	return GetGlobalConfig().RetryInterval.Duration
}

// GetSyncProtectionTTLDuration returns the configured sync protection TTL duration
func GetSyncProtectionTTLDuration() time.Duration {
	return GetGlobalConfig().SyncProtectionTTLDuration.Duration
}

// GetSyncProtectionRenewInterval returns the configured sync protection renew interval
func GetSyncProtectionRenewInterval() time.Duration {
	return GetGlobalConfig().SyncProtectionRenewInterval.Duration
}

// GetBloomFilterExpectedItems returns the configured bloom filter expected items
func GetBloomFilterExpectedItems() int {
	return GetGlobalConfig().BloomFilterExpectedItems
}

// GetBloomFilterFalsePositiveRate returns the configured bloom filter false positive rate
func GetBloomFilterFalsePositiveRate() float64 {
	return GetGlobalConfig().BloomFilterFalsePositiveRate
}

// GetRetryThreshold returns the configured retry threshold
func GetRetryThreshold() int {
	return GetGlobalConfig().RetryThreshold
}
