package perfcounter

import "sync/atomic"

// StatsCounter represents a combination of global & local counter.
type StatsCounter struct {
	local  atomic.Int64
	global atomic.Int64
}

// Add adds to local counter
func (c *StatsCounter) Add(delta int64) (new int64) {
	return c.local.Add(delta)
}

// Load return the sum of local and global
func (c *StatsCounter) Load() int64 {
	return c.global.Load() + c.local.Load()
}

// LoadC returns local counter value.
// Right now, only user of LoadC is LogExporter. Hence, MergeAndReset is also embedded here.
func (c *StatsCounter) LoadC() int64 {
	val := c.local.Load()

	{
		// Merge and Reset
		c.global.Add(val)
		c.local.Store(0)
	}

	return val
}
