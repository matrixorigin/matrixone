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

package ivfflat

// PROTOTYPE (test-only): reader-driven rank-ordered IVF widening search.
//
// Models exactly what the production change does inside the reader path
// (pkg/vm/engine/tae/blockio/read.go:HandleOrderByLimitOnIVFFlatIndex +
//  pkg/vm/engine/readutil/reader.go:Read() block loop):
//
//   1. rank ALL centroids by distance to the query (cheap, in-memory)
//   2. walk centroid buckets in RANK order (nearest first)
//   3. apply the INCLUDE-column filter per row (assume INCLUDE exists)
//   4. feed a bounded top-K heap  (== objectio.IndexReaderTopOp.DistHeap)
//   5. EARLY-STOP once K qualify AND the next centroid cannot beat the K-th dist
//
// One pass. No per-round SQL loop, no cursor, no 32-empty-round cap (the PR's F1).
//
// Run:  go test ./pkg/vectorindex/ivfflat/ -run TestWideningProto -v

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

const protoDim = 32

type protoEntry struct {
	vec []float32
	pk  int
	cat int // stands in for an INCLUDE column we filter on
}

func protoL2(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i] - b[i])
		s += d * d
	}
	return math.Sqrt(s)
}

// clustered data: real embeddings cluster, so IVF locality holds. Far-apart
// cluster centers; each entry = a center + tight noise.
func protoClusterCenters(n int, r *rand.Rand) [][]float32 {
	cs := make([][]float32, n)
	for i := range cs {
		c := make([]float32, protoDim)
		for j := range c {
			c[j] = float32(r.NormFloat64()) * 12
		}
		cs[i] = c
	}
	return cs
}

func protoAround(c []float32, r *rand.Rand) []float32 {
	v := make([]float32, protoDim)
	for i := range v {
		v[i] = c[i] + float32(r.NormFloat64())*0.5
	}
	return v
}

func protoBuildBuckets(entries []protoEntry, centroids [][]float32) [][]protoEntry {
	buckets := make([][]protoEntry, len(centroids))
	for _, e := range entries {
		best, bestD := 0, math.MaxFloat64
		for c := range centroids {
			if d := protoL2(e.vec, centroids[c]); d < bestD {
				best, bestD = c, d
			}
		}
		buckets[best] = append(buckets[best], e)
	}
	return buckets
}

// rankCentroids: all centroid ids ordered nearest->farthest from query.
func protoRankCentroids(q []float32, centroids [][]float32) []int {
	ids := make([]int, len(centroids))
	for i := range ids {
		ids[i] = i
	}
	sort.Slice(ids, func(a, b int) bool {
		return protoL2(q, centroids[ids[a]]) < protoL2(q, centroids[ids[b]])
	})
	return ids
}

type protoCand struct {
	pk   int
	dist float64
}

// ground truth: filter ALL entries, sort by distance, take K.
func protoBrute(q []float32, entries []protoEntry, k, wantCat int) []protoCand {
	var cs []protoCand
	for _, e := range entries {
		if e.cat != wantCat {
			continue
		}
		cs = append(cs, protoCand{e.pk, protoL2(e.vec, q)})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].dist < cs[j].dist })
	if len(cs) > k {
		cs = cs[:k]
	}
	return cs
}

// READER-DRIVEN WIDENING (prototype of the reader-loop change).
// Walks ranked centroid buckets, keeps a K-bounded top set, EARLY-STOPS once it
// holds K qualifying rows and the next centroid is farther than the current K-th
// distance (centroid distance as the pruning key, IVF-standard). Returns
// (topK, bucketsScanned).
func protoReaderWidening(q []float32, buckets [][]protoEntry, centroids [][]float32, ranked []int, k, nprobe, wantCat int, slack float64) ([]protoCand, int) {
	top := make([]protoCand, 0, k+1)
	scanned := 0
	kth := func() float64 {
		if len(top) < k {
			return math.MaxFloat64
		}
		return top[len(top)-1].dist
	}
	for i, cid := range ranked {
		// EARLY-STOP: past the initial nprobe, stop once we have K and the next
		// centroid is farther than the K-th hit by more than the in-cluster
		// radius `slack` -- i.e. no point in that bucket can beat the K-th.
		// (slack=0 reproduces the too-aggressive F1-style premature stop.)
		if i >= nprobe && len(top) >= k && protoL2(q, centroids[cid]) > kth()+slack {
			break
		}
		scanned++
		for _, e := range buckets[cid] {
			if e.cat != wantCat { // INCLUDE filter pushed into the scan
				continue
			}
			d := protoL2(e.vec, q)
			// insert into sorted bounded top-K (models DistHeap)
			pos := sort.Search(len(top), func(j int) bool { return top[j].dist > d })
			top = append(top, protoCand{})
			copy(top[pos+1:], top[pos:])
			top[pos] = protoCand{e.pk, d}
			if len(top) > k {
				top = top[:k]
			}
		}
	}
	return top, scanned
}

// fixed-nprobe (main today): scan only the nprobe nearest buckets, then filter.
func protoFixedNprobe(q []float32, buckets [][]protoEntry, ranked []int, k, nprobe, wantCat int) []protoCand {
	var cs []protoCand
	for i := 0; i < nprobe && i < len(ranked); i++ {
		for _, e := range buckets[ranked[i]] {
			if e.cat != wantCat {
				continue
			}
			cs = append(cs, protoCand{e.pk, protoL2(e.vec, q)})
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].dist < cs[j].dist })
	if len(cs) > k {
		cs = cs[:k]
	}
	return cs
}

func protoRecall(got, truth []protoCand) float64 {
	if len(truth) == 0 {
		return 1
	}
	set := map[int]bool{}
	for _, c := range truth {
		set[c.pk] = true
	}
	hit := 0
	for _, c := range got {
		if set[c.pk] {
			hit++
		}
	}
	return float64(hit) / float64(len(truth))
}

func TestWideningProto(t *testing.T) {
	cases := []struct {
		name    string
		nCats   int // 1/nCats selectivity
		wantCat int
	}{
		{"broad filter (1 of 2)", 2, 0},
		{"selective (1 of 50)", 50, 0},
		{"very selective (1 of 200)", 200, 0},
	}

	const (
		nEntries   = 100000
		nClusters  = 200
		nCentroids = 512
		k          = 10
		nprobe     = 8
	)

	r := rand.New(rand.NewSource(42))
	centers := protoClusterCenters(nClusters, r)
	entries := make([]protoEntry, nEntries)
	for i := range entries {
		c := centers[r.Intn(nClusters)]
		entries[i] = protoEntry{vec: protoAround(c, r), pk: i, cat: r.Intn(1)} // cat set per-case below
	}

	t.Logf("reader-driven rank-ordered IVF widening (single pass, no per-round SQL)")
	t.Logf("entries=%d clusters=%d centroids=%d K=%d nprobe=%d", nEntries, nClusters, nCentroids, k, nprobe)

	for _, tc := range cases {
		// assign cat per case (orthogonal to cluster, so the filter is not local)
		for i := range entries {
			entries[i].cat = r.Intn(tc.nCats)
		}
		centroids := protoSampleCentroids(entries, nCentroids, r)
		buckets := protoBuildBuckets(entries, centroids)
		radius := protoBucketRadius(buckets, centroids) // in-cluster radius for the correct stop bound
		q := protoAround(centers[r.Intn(nClusters)], r) // query near some cluster
		ranked := protoRankCentroids(q, centroids)

		truth := protoBrute(q, entries, k, tc.wantCat)
		fixed := protoFixedNprobe(q, buckets, ranked, k, nprobe, tc.wantCat)
		wideBad, scanBad := protoReaderWidening(q, buckets, centroids, ranked, k, nprobe, tc.wantCat, 0)      // F1-style stop
		wide, scanned := protoReaderWidening(q, buckets, centroids, ranked, k, nprobe, tc.wantCat, radius)    // correct stop

		t.Logf("--- %s (selectivity %.1f%%) ---", tc.name, 100.0/float64(tc.nCats))
		t.Logf("  fixed-nprobe(main)       : %2d rows  recall=%3.0f%%", len(fixed), protoRecall(fixed, truth)*100)
		t.Logf("  widening, slack=0 (F1)   : %2d rows  recall=%3.0f%%  buckets=%d/%d", len(wideBad), protoRecall(wideBad, truth)*100, scanBad, nCentroids)
		t.Logf("  widening, radius-aware   : %2d rows  recall=%3.0f%%  buckets=%d/%d (%.0f%%)",
			len(wide), protoRecall(wide, truth)*100, scanned, nCentroids, 100*float64(scanned)/float64(nCentroids))
	}
}

// centroids sampled from data (dense regions), mimicking k-means init.
func protoSampleCentroids(entries []protoEntry, n int, r *rand.Rand) [][]float32 {
	cs := make([][]float32, n)
	for i := range cs {
		cs[i] = entries[r.Intn(len(entries))].vec
	}
	return cs
}

// protoBucketRadius: 95th-percentile entry-to-centroid distance across buckets,
// i.e. how far inside a bucket a point can sit from its centroid. This is the
// slack a correct early-stop must add to the centroid distance.
func protoBucketRadius(buckets [][]protoEntry, centroids [][]float32) float64 {
	var ds []float64
	for c := range buckets {
		for _, e := range buckets[c] {
			ds = append(ds, protoL2(e.vec, centroids[c]))
		}
	}
	if len(ds) == 0 {
		return 0
	}
	sort.Float64s(ds)
	return ds[int(0.95*float64(len(ds)))]
}
