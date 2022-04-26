package checkpoint

import "time"

var DefaultLeveledPolicy LeveledPolicy

func init() {
	DefaultLeveledPolicy = newSimpleLeveledPolicy(30)
}

type LeveledPolicy interface {
	TotalLevels() int
	DecideLevel(score int) int
	ScanInterval(level int) time.Duration
}

type simpleLeveledPolicy struct {
	levels int
	step   float64
}

func newSimpleLeveledPolicy(levels int) *simpleLeveledPolicy {
	if levels <= 1 {
		panic("too small levels")
	}
	step := float64(70) / (float64(levels) - 1)
	return &simpleLeveledPolicy{
		levels: levels,
		step:   step,
	}
}

func (policy *simpleLeveledPolicy) ScanInterval(level int) time.Duration {
	return time.Second * time.Duration(2)
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
