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

package checkpoint

import "time"

var DefaultLeveledPolicy LeveledPolicy

func init() {
	DefaultLeveledPolicy = newSimpleLeveledPolicy(nil)
}

type PolicyCfg struct {
	Interval int64 // ms
	Levels   int
}

type LeveledPolicy interface {
	TotalLevels() int
	DecideLevel(score int) int
	ScanInterval(level int) time.Duration
}

type simpleLeveledPolicy struct {
	levels   int
	step     float64
	interval int64
}

func newSimpleLeveledPolicy(cfg *PolicyCfg) *simpleLeveledPolicy {
	if cfg == nil {
		cfg = new(PolicyCfg)
		cfg.Levels = 30
		cfg.Interval = 1000
	}
	if cfg.Levels <= 1 {
		panic("too small levels")
	}
	step := float64(70) / (float64(cfg.Levels) - 1)
	return &simpleLeveledPolicy{
		levels:   cfg.Levels,
		step:     step,
		interval: cfg.Interval,
	}
}

func (policy *simpleLeveledPolicy) ScanInterval(level int) time.Duration {
	return time.Duration(policy.levels-level) * time.Millisecond * time.Duration(policy.interval)
}

func (policy *simpleLeveledPolicy) TotalLevels() int { return policy.levels }
func (policy *simpleLeveledPolicy) DecideLevel(score int) int {
	if score < 30 {
		return 0
	}
	level := int((float64(score)-30)/policy.step) + 1
	if level > policy.levels-1 {
		level = policy.levels - 1
	}
	return level
}
