// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diagnostic

import (
	"container/heap"
	"context"
	"math"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/kmeans/balanced"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const NeighborK = 10

var ErrAnalysisLimit = moerr.NewInvalidInputNoCtx("index analysis memory limit")

type neighbor struct {
	Key      string
	Distance float64
}
type neighborHeap []neighbor

func (h neighborHeap) Len() int { return len(h) }
func (h neighborHeap) Less(i, j int) bool {
	if h[i].Distance != h[j].Distance {
		return h[i].Distance > h[j].Distance
	}
	return h[i].Key > h[j].Key
}
func (h neighborHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *neighborHeap) Push(x any)   { *h = append(*h, x.(neighbor)) }
func (h *neighborHeap) Pop() any     { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

type ExactNeighbors struct {
	Queries []Sample
	nearest []neighborHeap
	bytes   int
}

func NewExactNeighbors(queries []Sample) *ExactNeighbors {
	return &ExactNeighbors{Queries: queries, nearest: make([]neighborHeap, len(queries))}
}

// Observe keeps only top-k keys, not a matrix or retained source vectors.
func (a *ExactNeighbors) Observe(ctx context.Context, key string, values []float64) error {
	owned := ""
	for i, q := range a.Queries {
		if i%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if key == q.Key {
			continue
		}
		distance, err := metric.L2DistanceSq(q.Vector, values)
		if err != nil {
			return err
		}
		if math.IsInf(distance, 0) || math.IsNaN(distance) {
			return moerr.NewInvalidInput(ctx, "index analysis distance overflow")
		}
		h := &a.nearest[i]
		if len(*h) == NeighborK && (distance > (*h)[0].Distance || distance == (*h)[0].Distance && key >= (*h)[0].Key) {
			continue
		}
		old := 0
		if len(*h) == NeighborK {
			old = len((*h)[0].Key) + 32
		}
		cost := len(key) + 32
		if a.bytes-old+cost > MaxAnalysisBytes {
			return ErrAnalysisLimit
		}
		if owned == "" {
			owned = strings.Clone(key)
		}
		item := neighbor{Key: owned, Distance: distance}
		if len(*h) < NeighborK {
			heap.Push(h, item)
		} else {
			(*h)[0] = item
			heap.Fix(h, 0)
		}
		a.bytes += cost - old
	}
	return nil
}

func (a *ExactNeighbors) Keys() []string {
	keys := make([]string, 0, len(a.Queries)*NeighborK)
	for _, h := range a.nearest {
		for _, n := range h {
			keys = append(keys, n.Key)
		}
	}
	return keys
}

func SelectQueries(samples []Sample, old map[string]bool, limit int) []Sample {
	if old == nil {
		return samples[:min(limit, len(samples))]
	}
	var newer, before []Sample
	for _, s := range samples {
		if old[s.Key] {
			before = append(before, s)
		} else {
			newer = append(newer, s)
		}
	}
	nold := min(len(before), limit/2)
	nnew := min(len(newer), limit-nold)
	nold = min(len(before), limit-nnew)
	out := make([]Sample, 0, nold+nnew)
	out = append(out, newer[:nnew]...)
	out = append(out, before[:nold]...)
	return out
}

func ProbeCounts(k int) []int {
	if k <= 0 {
		return nil
	}
	seen := map[int]bool{k: true, max(1, k/8): true}
	for p := 1; p < k; {
		seen[p] = true
		if p > k/2 {
			break
		}
		p *= 2
	}
	out := make([]int, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

// Coverage models bucket eligibility of exact neighbors. It intentionally does
// not claim to replay quantized ranking or search-specific pruning.
func (a *ExactNeighbors) Coverage(ctx context.Context, buckets []*Bucket, membership map[string]int64, old map[string]bool, target float64, currentProbe int) (map[string]any, []indexstats.Suggestion, error) {
	out := map[string]any{"kind": "centroid_coverage", "k": NeighborK, "sample_count": len(a.Queries), "target": target}
	advice := make([]indexstats.Suggestion, 0)
	if int64(len(buckets)) > MaxAnalysisBytes/64 {
		out["status"] = "resource_limit"
		return out, advice, nil
	}
	probes := ProbeCounts(len(buckets))
	if len(probes) == 0 || len(a.Queries) == 0 {
		out["status"] = "no_queries"
		return out, advice, nil
	}
	sums, newSums, oldSums := make([]float64, len(probes)), make([]float64, len(probes)), make([]float64, len(probes))
	count, nnew, nold := 0, 0, 0
	type centerDistance struct {
		id       int64
		distance float64
	}
	centers := make([]centerDistance, len(buckets))
	ranks := make(map[int64]int, len(buckets))
	for qi, q := range a.Queries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		h := a.nearest[qi]
		if len(h) == 0 {
			continue
		}
		for i, b := range buckets {
			if i%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, nil, err
				}
			}
			if len(b.Center) == 0 {
				out["status"] = "untrained"
				return out, advice, nil
			}
			d, err := metric.L2DistanceSq(q.Vector, b.Center)
			if err != nil {
				return nil, nil, err
			}
			if math.IsInf(d, 0) || math.IsNaN(d) {
				out["status"] = "distance_unavailable"
				return out, advice, nil
			}
			centers[i] = centerDistance{b.ID, d}
		}
		sort.Slice(centers, func(i, j int) bool {
			if centers[i].distance != centers[j].distance {
				return centers[i].distance < centers[j].distance
			}
			return centers[i].id < centers[j].id
		})
		for i, c := range centers {
			ranks[c.id] = i + 1
		}
		for _, n := range h {
			cid, ok := membership[n.Key]
			if !ok || ranks[cid] == 0 {
				out["status"] = "index_coverage_unavailable"
				return out, advice, nil
			}
		}
		count++
		isNew := old != nil && !old[q.Key]
		if old != nil {
			if isNew {
				nnew++
			} else {
				nold++
			}
		}
		for i, p := range probes {
			hits := 0
			for _, n := range h {
				if ranks[membership[n.Key]] <= p {
					hits++
				}
			}
			recall := float64(hits) / float64(len(h))
			sums[i] += recall
			if old != nil {
				if isNew {
					newSums[i] += recall
				} else {
					oldSums[i] += recall
				}
			}
		}
	}
	if count == 0 {
		out["status"] = "no_neighbors"
		return out, advice, nil
	}
	makeCurve := func(values []float64, n int) []map[string]any {
		curve := make([]map[string]any, 0, len(probes))
		for i, p := range probes {
			curve = append(curve, map[string]any{"nprobe": p, "recall": values[i] / float64(n)})
		}
		return curve
	}
	out["sample_count"] = count
	out["curve"] = makeCurve(sums, count)
	if old != nil {
		out["new_query_count"], out["old_query_count"] = nnew, nold
		if nnew > 0 {
			out["new_curve"] = makeCurve(newSums, nnew)
		}
		if nold > 0 {
			out["old_curve"] = makeCurve(oldSums, nold)
		}
	}
	recommended := 0
	cut := max(1, len(buckets)/8)
	for i, p := range probes {
		if recommended == 0 && sums[i]/float64(count) >= target {
			recommended = p
		}
		if p == cut {
			if sums[i]/float64(count) < target {
				out["status"] = "below_target_at_probe_budget"
				advice = indexstats.AddReindex(advice, "neighbor_coverage_below_target", nil)
			}
			if nnew >= 10 && nold >= 10 && oldSums[i]/float64(nold)-newSums[i]/float64(nnew) >= .1 {
				out["status"] = "new_data_recall_drop"
				advice = indexstats.AddReindex(advice, "new_data_recall_drop", nil)
			}
		}
	}
	if recommended > 0 {
		out["recommended_probe_limit"] = recommended
		if recommended != currentProbe {
			advice = append(advice, indexstats.Suggestion{Action: "SET", Reason: "recall_target", Variable: "probe_limit", Value: recommended})
		}
	}
	return out, advice, nil
}

func Retrain(ctx context.Context, samples []Sample, buckets []*Bucket) (map[string]any, bool, error) {
	out := map[string]any{"sample_count": len(samples), "max_iterations": 10, "approximate": true}
	k := len(buckets)
	if len(samples) < k || k == 0 {
		out["status"] = "sample_limit"
		return out, false, nil
	}
	if err := ctx.Err(); err != nil {
		return out, false, err
	}
	dimension := len(samples[0].Vector)
	estimate := int64(len(samples)+k)*int64(dimension)*8 + int64(len(samples))*128 + int64(k)*64
	if estimate > MaxAnalysisBytes {
		out["status"] = "resource_limit"
		return out, false, nil
	}
	data := make([][]float64, len(samples))
	for i, s := range samples {
		data[i] = s.Vector
	}
	clusterer, err := balanced.NewKMeans(data, k, 10, 1e-4, metric.Metric_L2Distance, false, 1)
	if err != nil {
		return out, false, err
	}
	defer clusterer.Close()
	if err = clusterer.InitCentroids(ctx); err != nil {
		return out, false, err
	}
	result, err := clusterer.Cluster(ctx)
	if err != nil {
		return out, false, err
	}
	centers, ok := result.([][]float64)
	if !ok {
		return out, false, moerr.NewInternalError(ctx, "unexpected kmeans centroid type")
	}
	loss := func(cs [][]float64) (float64, error) {
		total := float64(0)
		for _, s := range samples {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			best := math.Inf(1)
			for _, c := range cs {
				d, err := metric.L2DistanceSq(s.Vector, c)
				if err != nil {
					return 0, err
				}
				best = min(best, d)
			}
			total += best
		}
		return total, nil
	}
	currentCenters := make([][]float64, 0, len(buckets))
	for _, b := range buckets {
		currentCenters = append(currentCenters, b.Center)
	}
	before, err := loss(currentCenters)
	if err != nil {
		return out, false, err
	}
	after, err := loss(centers)
	if err != nil {
		return out, false, err
	}
	if math.IsInf(before, 0) || math.IsInf(after, 0) || math.IsNaN(before) || math.IsNaN(after) {
		out["status"] = "loss_unavailable"
		return out, false, nil
	}
	switch {
	case after > 0:
		ratio := before / after
		if !math.IsInf(ratio, 0) {
			out["sse_ratio"] = ratio
		} else {
			out["status"] = "loss_ratio_overflow"
		}
	case before == 0:
		out["sse_ratio"] = float64(1)
	default:
		out["status"] = "zero_retrained_loss"
	}
	return out, before > 0 && after <= before/1.2, nil
}
