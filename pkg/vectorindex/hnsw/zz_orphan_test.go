// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	usearch "github.com/unum-cloud/usearch/golang"
)

func zzLoadSift(t *testing.T) (keys []usearch.Key, vecs [][]float32, dim int) {
	f, err := os.Open("../../../test/distributed/resources/vector/sift128_base_10k.csv.gz")
	if err != nil {
		// Reference repro for USearch #735; needs the local SIFT data file.
		t.Skip("sift128_base_10k.csv.gz not available, skipping: ", err)
	}
	defer f.Close()
	gz, _ := gzip.NewReader(f)
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		ci := strings.IndexByte(line, ':')
		if ci < 0 {
			continue
		}
		id, _ := strconv.ParseInt(strings.TrimSpace(line[:ci]), 10, 64)
		vs := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line[ci+1:]), "["), "]")
		parts := strings.Split(vs, ",")
		vec := make([]float32, len(parts))
		for i, p := range parts {
			fv, _ := strconv.ParseFloat(strings.TrimSpace(p), 32)
			vec[i] = float32(fv)
		}
		keys = append(keys, usearch.Key(id))
		vecs = append(vecs, vec)
		dim = len(vec)
	}
	return
}

func zzBuild(keys []usearch.Key, vecs [][]float32, dim int, threads uint) *usearch.Index {
	c := usearch.DefaultConfig(uint(dim))
	c.Quantization = usearch.F32
	c.Metric = usearch.L2sq
	// Match the BVT t2 case (vector_hnsw_async.sql): M 64 EF_CONSTRUCTION 200 EF_SEARCH 200.
	c.Connectivity = 64
	c.ExpansionAdd = 200
	c.ExpansionSearch = 200
	idx, _ := usearch.NewIndex(c)
	idx.Reserve(uint(len(keys)))
	idx.ChangeThreadsAdd(threads)
	n := len(keys)
	if threads <= 1 {
		for i := 0; i < n; i++ {
			idx.Add(keys[i], vecs[i])
		}
		return idx
	}
	var wg sync.WaitGroup
	chunk := (n + int(threads) - 1) / int(threads)
	for tn := 0; tn < int(threads); tn++ {
		s, e := tn*chunk, tn*chunk+chunk
		if s >= n {
			break
		}
		if e > n {
			e = n
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				idx.Add(keys[i], vecs[i])
			}
		}(s, e)
	}
	wg.Wait()
	return idx
}

// TestZZBuildOrphan is a regression guard for USearch #735 (concurrent add()
// orphans nodes): a multi-threaded build used to occasionally leave id 0
// unreachable in search despite contains()==true. Our patched libusearch
// (two-pass add: all forward links before any reverse link) fixes the race, so
// an 8-thread build must now report 0 orphans — this asserts that and fails if a
// future libusearch regresses it. Builds 30x with the same params as the BVT t2
// case (M 64, EF_CONSTRUCTION 200, EF_SEARCH 200). Auto-skips when the SIFT data
// file is absent (see zzLoadSift).
func TestZZBuildOrphan(t *testing.T) {
	keys, vecs, dim := zzLoadSift(t)
	t.Logf("loaded %d vectors dim=%d id0=%d", len(keys), dim, keys[0])
	q := vecs[0]
	const iters = 30
	for _, threads := range []uint{8} {
		notTop1, missing, notContained := 0, 0, 0
		for it := 0; it < iters; it++ {
			// Rotate the insertion order each iteration so a different key lands
			// first and the thread chunks shift — exercises different concurrent
			// add interleavings, like `load data ... parallel 'true'` loading rows
			// in a non-deterministic order. Deterministic (no RNG); keys stay
			// aligned with vecs.
			off := (it * (len(keys) / iters)) % len(keys)
			ik := append(append([]usearch.Key(nil), keys[off:]...), keys[:off]...)
			iv := append(append([][]float32(nil), vecs[off:]...), vecs[:off]...)
			idx := zzBuild(ik, iv, dim, threads)
			contained, _ := idx.Contains(0)
			rk, _, _ := idx.Search(q, 10)
			rank := -1
			for i, k := range rk {
				if k == 0 {
					rank = i
					break
				}
			}
			if !contained {
				notContained++
			}
			if rank != 0 {
				notTop1++
			}
			if rank < 0 {
				missing++
			}
			if rank != 0 {
				top1 := "none"
				if len(rk) > 0 {
					top1 = fmt.Sprint(rk[0])
				}
				t.Logf("  threads=%d iter=%d: id0 rank=%d top1=%s contains0=%v", threads, it, rank, top1, contained)
			}
			idx.Destroy()
		}
		fmt.Printf("\n*** threads=%d : id0_not_top1=%d/%d  id0_missing_top10=%d/%d  id0_not_in_index=%d/%d ***\n", threads, notTop1, iters, missing, iters, notContained, iters)
		// #735 regression guard: with the patched libusearch the build must never
		// orphan id 0, at any thread count. notContained==0 always held (the vector
		// is stored); the race only broke reachability, so missing/notTop1 are the
		// real signal.
		if missing > 0 || notTop1 > 0 || notContained > 0 {
			t.Errorf("USearch #735 regression: threads=%d orphaned id0 — not_top1=%d/%d missing_top10=%d/%d not_in_index=%d/%d",
				threads, notTop1, iters, missing, iters, notContained, iters)
		}
	}
}
