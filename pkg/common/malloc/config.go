// Copyright 2024 Matrix Origin
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

package malloc

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type Config struct {
	// !! REMEMBER TO CHANGE patchConfig WHEN ADDING FIELDS

	// Allocator controls which allocator to use
	Allocator *string `toml:"allocator"`

	// CheckFraction controls the fraction of checked deallocations
	// On average, 1 / fraction of deallocations will be checked for double free or missing free
	CheckFraction *uint32 `toml:"check-fraction"`

	// FullStackFraction controls the fraction of full stack collecting in profile sampling
	// On average, 1 / fraction of allocations will be sampled with full stack information
	FullStackFraction *uint32 `toml:"full-stack-fraction"`

	// EnableMetrics indicates whether to expose metrics to prometheus
	EnableMetrics *bool `toml:"enable-metrics"`

	HashmapSoftLimit *uint64 `toml:"hashmap-soft-limit"`
	HashmapHardLimit *uint64 `toml:"hashmap-hard-limit"`
}

var defaultConfig = func() *atomic.Pointer[Config] {
	ret := new(atomic.Pointer[Config])

	// default config
	ret.Store(&Config{
		CheckFraction:     ptrTo(uint32(4096)),
		EnableMetrics:     ptrTo(true),
		FullStackFraction: ptrTo(uint32(100)),
		HashmapSoftLimit:  ptrTo(uint64(48 * (1 << 30))),
		HashmapHardLimit:  ptrTo(uint64(64 * (1 << 30))),
	})

	return ret
}()

func patchConfig(config Config, delta Config) Config {
	if delta.CheckFraction != nil {
		config.CheckFraction = delta.CheckFraction
		logutil.Info("malloc set config", zap.Any("CheckFraction", *delta.CheckFraction))
	}
	if delta.EnableMetrics != nil {
		config.EnableMetrics = delta.EnableMetrics
		logutil.Info("malloc set config", zap.Any("EnableMetrics", *delta.EnableMetrics))
	}
	if delta.FullStackFraction != nil {
		config.FullStackFraction = delta.FullStackFraction
		logutil.Info("malloc set config", zap.Any("FullStackFraction", *delta.FullStackFraction))
	}
	if delta.Allocator != nil {
		config.Allocator = delta.Allocator
		logutil.Info("malloc set config", zap.Any("Allocator", *delta.Allocator))
	}
	if delta.HashmapSoftLimit != nil {
		config.HashmapSoftLimit = delta.HashmapSoftLimit
		logutil.Info("malloc set config", zap.Any("HashmapSoftLimit", *delta.HashmapSoftLimit))
	}
	if delta.HashmapHardLimit != nil {
		config.HashmapHardLimit = delta.HashmapHardLimit
		logutil.Info("malloc set config", zap.Any("HashmapHardLimit", *delta.HashmapHardLimit))
	}
	return config
}

func SetDefaultConfig(delta Config) {
	config := *defaultConfig.Load()
	config = patchConfig(config, delta)
	defaultConfig.Store(&config)
	logutil.Info("malloc: set default config", zap.Any("config", delta))
}

func GetDefaultConfig() Config {
	return *defaultConfig.Load()
}

func WithTempDefaultConfig(config Config, fn func()) {
	old := defaultConfig.Swap(&config)
	defer func() {
		defaultConfig.Store(old)
	}()
	fn()
}
