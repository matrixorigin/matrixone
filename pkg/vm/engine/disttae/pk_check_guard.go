// Copyright 2026 Matrix Origin
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

package disttae

import (
	"runtime"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
)

const (
	PKCheckGuardModeOff      = "off"
	PKCheckGuardModePressure = "pressure"

	maxPKCheckGuardAutoConcurrency       = 64
	minPKCheckGuardAutoConcurrency       = 16
	maxPKCheckGuardConfiguredConcurrency = 512
)

type PKCheckGuardConfig struct {
	Mode        string `toml:"mode"`
	Concurrency int    `toml:"concurrency"`
}

type pkCheckGuard struct {
	mode             string
	sem              chan struct{}
	pressureProvider rscthrottler.MemoryPressureProvider
}

func newPKCheckGuard(
	cfg PKCheckGuardConfig,
	pressureProvider rscthrottler.MemoryPressureProvider,
) *pkCheckGuard {
	cfg = normalizePKCheckGuardConfig(cfg)
	return &pkCheckGuard{
		mode:             cfg.Mode,
		sem:              make(chan struct{}, cfg.Concurrency),
		pressureProvider: pressureProvider,
	}
}

func normalizePKCheckGuardConfig(cfg PKCheckGuardConfig) PKCheckGuardConfig {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	switch cfg.Mode {
	case PKCheckGuardModeOff, PKCheckGuardModePressure:
	default:
		cfg.Mode = PKCheckGuardModePressure
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultPKCheckGuardConcurrency()
	} else if cfg.Concurrency > maxPKCheckGuardConfiguredConcurrency {
		cfg.Concurrency = maxPKCheckGuardConfiguredConcurrency
	}

	return cfg
}

func defaultPKCheckGuardConcurrency() int {
	return min(max(runtime.GOMAXPROCS(0), minPKCheckGuardAutoConcurrency), maxPKCheckGuardAutoConcurrency)
}

func (g *pkCheckGuard) tryAcquire() (func(), bool) {
	if g == nil || g.mode == PKCheckGuardModeOff {
		return nil, true
	}
	if g.mode == PKCheckGuardModePressure && !g.underPressure() {
		return nil, true
	}
	select {
	case g.sem <- struct{}{}:
		return func() { <-g.sem }, true
	default:
		return nil, false
	}
}

func (g *pkCheckGuard) underPressure() bool {
	if g.pressureProvider == nil {
		return false
	}
	return g.pressureProvider.MemoryPressure() != rscthrottler.MemoryPressureNormal
}

func (g *pkCheckGuard) capacity() int {
	if g == nil {
		return 0
	}
	return cap(g.sem)
}
