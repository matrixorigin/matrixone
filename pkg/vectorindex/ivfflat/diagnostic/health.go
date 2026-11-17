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
	"encoding/json"
	"math"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/indexstats"
	iscpmeta "github.com/matrixorigin/matrixone/pkg/iscp/metadata"
)

const BuildMetadataKey = "diagnostic_build"
const SampleSize = 100
const MaxAnalysisBytes = 64 << 20

type BuildInfo struct {
	Format          int    `json:"format"`
	IndexVersion    int64  `json:"index_version"`
	BuildTS         string `json:"build_ts"`
	Rows            int64  `json:"nrow"`
	TrainingSamples int64  `json:"train_nsample"`
	SourceTableID   uint64 `json:"source_table_id"`
	Historical      bool   `json:"historical"`
}

func DecodeBuild(raw string, version int64) (*BuildInfo, string) {
	if raw == "" {
		return nil, "baseline_missing"
	}
	var b BuildInfo
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		return nil, "baseline_invalid"
	}
	if b.Format != 1 {
		return nil, "baseline_format_unsupported"
	}
	if b.IndexVersion != version {
		return nil, "baseline_stale"
	}
	if b.Rows < 0 || b.TrainingSamples < 0 || b.TrainingSamples > b.Rows {
		return nil, "baseline_invalid"
	}
	if _, ok := iscpmeta.ParseWatermark(b.BuildTS); !ok {
		return nil, "baseline_invalid"
	}
	return &b, ""
}

func BuildHealth(n, lists int64, base *BuildInfo) (map[string]any, []indexstats.Suggestion) {
	metrics := map[string]any{}
	advice := make([]indexstats.Suggestion, 0)
	if n > 0 && lists > 0 {
		root := math.Sqrt(float64(n))
		target := max(int64(1), int64(math.Ceil(root)))
		if target != lists && (float64(lists) < root/2 || float64(lists) > root*2 || lists == 1 && n > 1) {
			reason := "lists_below_sqrt_n"
			if float64(lists) > root*2 {
				reason = "lists_above_sqrt_n"
			}
			advice = indexstats.AddReindex(advice, reason, map[string]any{"lists": target})
		}
	}
	if base != nil {
		if base.Rows > 0 {
			metrics["growth_ratio"] = float64(n-base.Rows) / float64(base.Rows)
		}
		if lists > 0 {
			target := 30 * float64(lists)
			metrics["training_sample_ratio"] = float64(base.TrainingSamples) / target
			if float64(base.TrainingSamples) < target && float64(n) >= target {
				advice = indexstats.AddReindex(advice, "training_sample_insufficient", nil)
			}
		}
	}
	return metrics, advice
}

type Sample struct {
	Key      string
	Centroid int64
	Vector   []float64
}

type Reservoir struct {
	Samples  []Sample
	capacity int
	seen     uint64
	bytes    int
	rng      *rand.Rand
	Status   string
}

func NewReservoir(capacity int) *Reservoir {
	return &Reservoir{capacity: capacity, rng: rand.New(rand.NewPCG(1, 2))}
}
func (r *Reservoir) Observe(key string, centroid int64, vector []float64) {
	if r.Status != "" || vector == nil || r.capacity <= 0 {
		return
	}
	r.seen++
	slot := len(r.Samples)
	if slot >= r.capacity {
		j := r.rng.Uint64N(r.seen)
		if j >= uint64(r.capacity) {
			return
		}
		slot = int(j)
	}
	cost := len(key) + len(vector)*8 + 64
	old := 0
	if slot < len(r.Samples) {
		old = len(r.Samples[slot].Key) + len(r.Samples[slot].Vector)*8 + 64
	}
	if r.bytes-old+cost > MaxAnalysisBytes {
		r.Status = "sample_resource_limit"
		r.Samples = nil
		r.bytes = 0
		return
	}
	s := Sample{Key: strings.Clone(key), Centroid: centroid, Vector: append([]float64(nil), vector...)}
	if slot == len(r.Samples) {
		r.Samples = append(r.Samples, s)
	} else {
		r.Samples[slot] = s
	}
	r.bytes += cost - old
}

// Drift uses cohort means on a bounded sample. Both counts and sample scope are
// explicit; retained history is supplied by the reader, never assumed here.
func Drift(samples []Sample, old map[string]bool, buckets []*Bucket, approximate bool) map[string]any {
	out := map[string]any{"sample_count": len(samples), "new_sample_count": 0, "old_sample_count": 0}
	if approximate {
		out["approximate"] = true
	}
	centers := make(map[int64][]float64, len(buckets))
	ranked := append([]*Bucket(nil), buckets...)
	for _, b := range buckets {
		centers[b.ID] = b.Center
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Count != ranked[j].Count {
			return ranked[i].Count > ranked[j].Count
		}
		return ranked[i].ID < ranked[j].ID
	})
	hot := map[int64]bool{}
	for _, b := range ranked[:min(len(ranked), (len(ranked)+19)/20)] {
		hot[b.ID] = true
	}
	before, after := NewBucket(0, []float64{0}), NewBucket(1, []float64{0})
	hotNew := 0
	for _, s := range samples {
		c := centers[s.Centroid]
		if len(c) != len(s.Vector) || len(c) == 0 {
			out["status"] = "geometry_unavailable"
			return out
		}
		norm := float64(0)
		for i, v := range s.Vector {
			norm = math.Hypot(norm, v-c[i])
		}
		if math.IsInf(norm, 0) || math.IsNaN(norm) {
			out["status"] = "geometry_unavailable"
			return out
		}
		target := after
		if old[s.Key] {
			target = before
		} else if hot[s.Centroid] {
			hotNew++
		}
		if err := target.Add([]float64{norm}); err != nil {
			out["status"] = "geometry_unavailable"
			return out
		}
	}
	out["new_sample_count"], out["old_sample_count"] = after.Count, before.Count
	if after.Count > 0 {
		out["new_rows_in_largest_5pct_buckets_ratio"] = float64(hotNew) / float64(after.Count)
	}
	switch {
	case after.Count == 0 || before.Count == 0:
		out["status"] = "cohort_unavailable"
	case before.scale == 0:
		out["status"] = "zero_old_residual"
	case after.scale == 0:
		out["new_old_residual_ratio"] = float64(0)
	default:
		logRatio := 2*(math.Log(after.scale)-math.Log(before.scale)) + math.Log(after.squared/float64(after.Count)) - math.Log(before.squared/float64(before.Count))
		ratio := math.Exp(logRatio)
		if math.IsInf(ratio, 0) || math.IsNaN(ratio) {
			out["status"] = "residual_ratio_overflow"
		} else {
			out["new_old_residual_ratio"] = ratio
		}
	}
	return out
}
