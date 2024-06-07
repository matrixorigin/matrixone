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
	// CheckFraction controls the fraction of checked deallocations
	// On average, 1 / fraction of deallocations will be checked for double free or missing free
	CheckFraction *uint32 `toml:"check-fraction"`

	// FullStackFraction controls the fraction of full stack collecting in profile sampling
	// On average, 1 / fraction of allocations will be sampled with full stack information
	FullStackFraction *uint32 `toml:"full-stack-fraction"`

	// EnableMetrics indicates whether to expose metrics to prometheus
	EnableMetrics *bool `toml:"enable-metrics"`
}

var defaultConfig = func() *atomic.Pointer[Config] {
	ret := new(atomic.Pointer[Config])

	// default config
	ret.Store(&Config{
		CheckFraction:     ptrTo(uint32(65536)),
		EnableMetrics:     ptrTo(true),
		FullStackFraction: ptrTo(uint32(16)),
	})

	return ret
}()

func SetDefaultConfig(delta Config) {

	// read
	config := *defaultConfig.Load()

	// set
	if delta.CheckFraction != nil {
		config.CheckFraction = delta.CheckFraction
	}
	if delta.EnableMetrics != nil {
		config.EnableMetrics = delta.EnableMetrics
	}
	if delta.FullStackFraction != nil {
		config.FullStackFraction = delta.FullStackFraction
	}

	// update
	defaultConfig.Store(&config)
	logutil.Info("malloc: set default config", zap.Any("config", delta))
}
