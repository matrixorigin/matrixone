package local

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
)

type BucketsMetrics struct {
	name        string
	upperBounds []float64
	counts      []uint64
}

func NewBucketMetrics(
	name string,
	start, factor float64,
	count int) *BucketsMetrics {
	if count < 1 {
		panic("BucketsMetrics needs a positive count")
	}
	if start <= 0 {
		panic("BucketsMetrics needs a positive start value")
	}
	if factor <= 1 {
		panic("BucketsMetrics needs a factor greater than 1")
	}
	upperBounds := make([]float64, count+1)
	for i := range upperBounds {
		upperBounds[i] = start
		start *= factor
	}
	upperBounds[count] = math.MaxFloat64
	return &BucketsMetrics{
		name:        name,
		upperBounds: upperBounds,
		counts:      make([]uint64, count+1),
	}
}

func (bm *BucketsMetrics) Add(v float64) {
	i := sort.SearchFloat64s(bm.upperBounds, v)
	atomic.AddUint64(&bm.counts[i], 1)
}

func (bm *BucketsMetrics) Snapshot() BucketsMetrics {
	snap := BucketsMetrics{
		name:        bm.name,
		upperBounds: bm.upperBounds,
		counts:      make([]uint64, len(bm.counts)),
	}
	for i := range bm.counts {
		snap.counts[i] = atomic.LoadUint64(&bm.counts[i])
	}
	return snap
}

func (bm BucketsMetrics) DurationString() string {
	total := uint64(0)
	for _, v := range bm.counts {
		total += v
	}

	n := len(bm.counts)
	var bytes bytes.Buffer
	bytes.WriteString("[")
	for i := range bm.upperBounds {
		v := bm.counts[i]
		rate := float64(v) / float64(total) * 100
		if i < n-1 {
			bytes.WriteString(fmt.Sprintf("%dms: %d(%.2f%%)", int(bm.upperBounds[i]*1000), bm.counts[i], rate))
			bytes.WriteString(", ")
		} else {
			bytes.WriteString(fmt.Sprintf("unlimited: %d(%.2f%%)", bm.counts[i], rate))
		}
	}
	bytes.WriteString("]")
	return bytes.String()
}
