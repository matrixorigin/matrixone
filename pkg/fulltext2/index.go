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

package fulltext2

import (
	"fmt"
	"iter"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2/tokenfilter"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// Index is a queryable fulltext v2 index made of several loaded segments — a
// tag=0 base plus tag=1 CDC tail deltas (§4) — with per-pk liveness and a delete
// set. It resolves the two things a lone Segment cannot:
//
//   - GLOBAL stats: N and avgDocLen span all segments (a term's idf and BM25's
//     length norm must be global, not per-segment), computed once at construction.
//   - Liveness: the same pk may land in several segments (UPDATE / reinsert / a
//     stale base copy); only the highest-Recency copy is live, and a delete at
//     recency d kills copies with recency < d. Dead copies never appear in
//     results and never contribute to a phrase's document frequency.
type Index struct {
	segments []*Segment
	deletes  map[any]int64 // normalizeKey(pk) -> recency at/after which older copies are dead

	globalN         int64
	globalAvgDocLen float64
	// liveOrd[si] is a per-segment ord-indexed liveness bitmap (built once in resolve),
	// so every liveness check — isLive (phrase), livenessMembership (boolean/stream), and
	// ReconstructLiveDocs (compact) — is an O(1) bitmap index, allocation-free, with no
	// per-candidate keyOf + map lookup. A fully-live segment keeps liveOrd[si]==nil ("all
	// live" fast path), so an append-only index costs ZERO resident liveness heap. It is
	// the SOLE resident liveness structure: the pk->loc map that resolve() builds to
	// derive it is local and discarded (it was O(live docs) of Go heap — the dominant
	// load-time floor). Mirrors bm25's ComputeLiveness []Membership.
	liveOrd [][]bool

	// threadsSearch is the degree of parallelism for the multi-segment top-k search
	// (SearchBoolean), resolved from the fulltext2_threads_search session variable and
	// baked here once at Load (single-threaded), so it is read-only during the
	// concurrent search and never races. 0 means use GOMAXPROCS (vectorindex.GetConcurrency).
	// It is a perf-only hint: any value yields identical results.
	threadsSearch int

	// tokenFilters is the token_filter analyzer chain (currently "stop"), baked here
	// once at Load from the index's TableConfig. It is applied ONLY on the BM25
	// bag-of-words query path (SearchBagOfWords/StreamBagOfWords) so the query strips
	// the SAME tokens the build/CDC paths stripped; NL/boolean are rejected at plan
	// time for a token_filter index. Empty ⇒ no filtering. Read-only during search.
	tokenFilters []tokenfilter.TokenFilter
}

type docLoc struct {
	si  int
	ord int64
}

// NewIndex builds an index over the given loaded segments (each carrying its
// Recency) and delete set, computing global stats and the liveness map. deletes
// may be nil.
func NewIndex(segments []*Segment, deletes map[any]int64) *Index {
	idx := &Index{segments: segments, deletes: deletes}
	idx.resolve()
	return idx
}

// resolve computes the live copy of each pk (highest Recency, not delete-shadowed),
// then the global doc count + average document length; each segment's AvgDocLen is
// set to the global value (§4) so BM25 length-normalizes consistently.
func (idx *Index) resolve() {
	type best struct {
		rec int64
		loc docLoc
	}
	top := make(map[any]best)
	for si, seg := range idx.segments {
		nd := seg.numDocs()
		for ord := 0; ord < nd; ord++ {
			key := normalizeKey(seg.pk(int64(ord)))
			cand := best{seg.Recency, docLoc{si, int64(ord)}}
			if cur, ok := top[key]; !ok || cand.rec > cur.rec {
				top[key] = cand
			}
		}
	}

	// liveLoc (pk key -> live loc) is LOCAL: it drives the liveOrd bitmap + global stats
	// below and is then discarded, so it never becomes part of the resident cache
	// footprint (it was O(live docs) of Go-heap map — the biggest load-time allocation).
	liveLoc := make(map[any]docLoc, len(top))
	var sumDocLen int64
	for key, b := range top {
		if d, ok := idx.deletes[key]; ok && b.rec < d {
			continue // shadowed by a later delete
		}
		liveLoc[key] = b.loc
		sumDocLen += int64(idx.segments[b.loc.si].docLen[b.loc.ord])
	}
	idx.globalN = int64(len(liveLoc))
	if idx.globalN > 0 {
		idx.globalAvgDocLen = float64(sumDocLen) / float64(idx.globalN)
	}
	for _, seg := range idx.segments {
		seg.AvgDocLen = idx.globalAvgDocLen
	}

	// Per-segment liveness bitmap (mirrors bm25's ComputeLiveness): a fully-live
	// segment keeps liveOrd[si]==nil (the common single-base / append-only case) so it
	// costs no O(doc-count) allocation; otherwise mark each live ord true. Every query
	// and compact liveness check then indexes this O(1) instead of keyOf+map per candidate.
	idx.liveOrd = make([][]bool, len(idx.segments))
	liveCount := make([]int, len(idx.segments))
	for _, loc := range liveLoc {
		liveCount[loc.si]++
	}
	for si, seg := range idx.segments {
		if liveCount[si] < seg.numDocs() { // some dead ord in this segment → need the bitmap
			idx.liveOrd[si] = make([]bool, seg.numDocs())
		}
	}
	for _, loc := range liveLoc {
		if b := idx.liveOrd[loc.si]; b != nil {
			b[loc.ord] = true
		}
	}
}

// includeTypes returns the index's INCLUDE column types (types.T per column), read from the
// first segment that carries them — all segments of an index share one INCLUDE schema.
// nil when the index has no INCLUDE columns.
func (idx *Index) includeTypes() []int32 {
	for _, s := range idx.segments {
		if len(s.includeTypes) > 0 {
			return s.includeTypes
		}
	}
	return nil
}

// pkType returns the index's primary-key type (types.T), used to type-bind an inline pk
// predicate. All segments share the pk type; 0 when the index has no segments.
func (idx *Index) pkType() int32 {
	if len(idx.segments) > 0 {
		return idx.segments[0].PkType
	}
	return 0
}

// isLive reports whether (si, ord) is the live copy of its pk, via the per-segment
// liveness bitmap (nil => the segment is fully live). Same semantics as
// livenessMembership; no pk key or map lookup needed.
func (idx *Index) isLive(si int, ord int64) bool {
	b := idx.liveOrd[si]
	return b == nil || b[ord]
}

// liveTermDF returns the number of live documents in segment si that contain p.
// A nil liveness bitmap proves that the segment is fully live, so its raw posting
// count is exact without touching compressed blocks. Dirty loaded segments decode
// one docID block at a time and never materialize the full posting list.
func (idx *Index) liveTermDF(si int, p *termPostings) int {
	live := idx.liveOrd[si]
	if live == nil {
		return p.df()
	}

	df := 0
	if p.docIDs != nil {
		for _, ord := range p.docIDs {
			if ord >= 0 && ord < int64(len(live)) && live[ord] {
				df++
			}
		}
		return df
	}

	var docs [BlockSize]int64
	for b := 0; b < p.nblk(); b++ {
		n := p.fillBlockDocs(b, docs[:])
		for _, ord := range docs[:n] {
			if ord >= 0 && ord < int64(len(live)) && live[ord] {
				df++
			}
		}
	}
	return df
}

// SearchPhrase runs an NL exact-phrase query across all segments and returns the
// global top-k (score desc; equal scores are order-unspecified). A single term is the
// degenerate one-term phrase. Scoring uses global N + global avgDocLen, and the
// phrase's document frequency is the number of LIVE documents that match (dead
// and delete-shadowed copies are excluded), so idf is exact.
func (idx *Index) SearchPhrase(slots []phraseSlot, algo ScoreAlgo, k int, filter *prefilter) []Result {
	if k <= 0 || idx.globalN == 0 || len(slots) == 0 {
		return nil
	}
	// A phrase can match at most globalN live docs, so cap k to it: a huge pushed LIMIT
	// (clamped to MaxInt32 upstream) must never size the top-k beyond the live corpus.
	if int64(k) > idx.globalN {
		k = int(idx.globalN)
	}
	// Single pass, O(min(k, #matches)) memory (was O(matches): a matched map + a full results slice,
	// which OOMs on a low-selectivity phrase that hits most of the corpus). A lone
	// phrase has ONE idf² (idfSquared over its live df), and scoreTerm multiplies the
	// per-doc factor by that constant, so idf² does not change the top-k order — rank on
	// the idf²-free partial score in a bounded top-k, count the (filter-independent) live
	// df alongside, and scale the k winners by idf² at the end. isLive is true for exactly
	// one (si,ord) per pk, so counting live hits IS the distinct-live df (no dedup set).
	tk := newBoundedTopK(k)
	df := 0
	for si, seg := range idx.segments {
		allow := mkAllow(seg, filter) // WHERE prefilter over this segment's ords (nil = none)
		for _, h := range seg.matchPhrase(slots) {
			if !idx.isLive(si, h.ord) {
				continue
			}
			df++ // distinct live phrase match (filter-independent, matches the old dfSet)
			if allowed(allow, h.ord) {
				partial := seg.scoreTerm(algo, float64(h.tf), 1.0, h.ord, idx.globalAvgDocLen)
				tk.push(seg.pk(h.ord), partial, seg.decodeInclude(h.ord))
			}
		}
	}
	if tk.len() == 0 {
		return nil
	}
	idf2 := idfSquared(idx.globalN, df) // df = live phrase-matched docs, filter-independent
	return tk.resultsDescScaled(float32(idf2))
}

// boundedTopK keeps only the highest-scoring k (pk, score) pairs seen via push in O(k)
// memory — a binary min-heap by score, so the current k-th best sits at the root and is
// evicted in O(log k) when a better candidate arrives. It replaces "materialize every
// match, then top-k", which was O(matches). Ties at the k-th place are unspecified (as
// before — no arbitrary pk tie-break; see topKResults).
//
// It is deliberately keyed by the resolved `pk any` (NOT the segment `ord`), unlike the
// ord-keyed FastMaxHeap in wand.go: SearchPhrase accumulates across ALL segments into
// ONE heap, and each segment has its own ord space (ord 5 in seg0 != ord 5 in seg1), so
// only the globally-unique pk is a safe key here. It also defers the single idf² scale to
// resultsDescScaled. Those two properties are why it is not the FastMaxHeap used by the
// per-segment WAND paths — do not "unify" them; keying this by ord reintroduces cross-
// segment ord collisions.
type topKItem struct {
	pk      any
	score   float32
	include []any // INCLUDE values carried for a covering query (nil when no INCLUDE columns)
}

type boundedTopK struct {
	k int
	h []topKItem
}

// newBoundedTopK does NOT pre-allocate a cap-k backing array: push grows h lazily and
// never past k items, so the heap is bounded by min(k, #matches) — an absurd pushed
// LIMIT (e.g. 5e8) on a query matching a handful of docs costs a handful of slots, not
// a k-sized (multi-GB) allocation.
func newBoundedTopK(k int) *boundedTopK { return &boundedTopK{k: k} }

func (b *boundedTopK) len() int { return len(b.h) }

func (b *boundedTopK) push(pk any, score float32, include []any) {
	if b.k <= 0 {
		return
	}
	if len(b.h) < b.k {
		b.h = append(b.h, topKItem{pk, score, include})
		for i := len(b.h) - 1; i > 0; {
			p := (i - 1) / 2
			if b.h[p].score <= b.h[i].score {
				break
			}
			b.h[p], b.h[i] = b.h[i], b.h[p]
			i = p
		}
		return
	}
	if score <= b.h[0].score {
		return // not better than the current k-th best
	}
	b.h[0] = topKItem{pk, score, include}
	n := len(b.h)
	for i := 0; ; {
		l, r, s := 2*i+1, 2*i+2, i
		if l < n && b.h[l].score < b.h[s].score {
			s = l
		}
		if r < n && b.h[r].score < b.h[s].score {
			s = r
		}
		if s == i {
			break
		}
		b.h[s], b.h[i] = b.h[i], b.h[s]
		i = s
	}
}

// resultsDescScaled returns the retained items as Results in score-descending order,
// each partial score multiplied by scale (the idf² deferred out of the ranking).
func (b *boundedTopK) resultsDescScaled(scale float32) []Result {
	out := make([]Result, len(b.h))
	for i := range b.h {
		out[i] = Result{Pk: b.h[i].pk, Score: b.h[i].score * scale, Include: b.h[i].include}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

// topKResults returns the top-k results ordered by score descending. Equal scores are
// equally relevant, so their relative order is unspecified (SQL LIMIT over a tied ORDER
// BY key is itself unspecified) — we do NOT impose an arbitrary pk tie-break, which would
// force a sortKey string alloc for every matched doc (the alloc storm that dominated the
// CPU/GC profile of a query matching far more than k docs, e.g. a common single-term NL
// phrase or a boolean union).
//
// Selection is a bounded SoA FastMaxHeap keyed by result index with distance = -score:
// keeping the k smallest distances yields the k highest scores in O(n) with ZERO
// per-candidate allocation. Pop returns largest-distance (lowest-score) first, so
// filling the output back-to-front lands it in score-descending order.
func topKResults(results []Result, k int) []Result {
	if k <= 0 || len(results) == 0 {
		return nil
	}
	if k > len(results) {
		k = len(results)
	}
	keysBuf := make([]int64, k)
	distsBuf := make([]float32, k)
	h := vectorindex.NewFastMaxHeap[float32, int64](k, keysBuf, distsBuf)
	for i := range results {
		h.Push(int64(i), -results[i].Score)
	}
	out := make([]Result, k)
	for i := k - 1; i >= 0; i-- {
		idx, _, ok := h.Pop()
		if !ok { // fewer than k live entries (e.g. duplicate pushes were bounded out)
			return out[i+1:]
		}
		out[i] = results[idx]
	}
	return out
}

// SearchText tokenizes query with tok and runs an NL exact-phrase search across
// the index.
func (idx *Index) SearchText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int, filter *prefilter) ([]Result, error) {
	return idx.SearchPhrase(tokenizePhraseSlots(tok, query), algo, k, filter), nil
}

// globalStats carries the corpus-global scoring inputs (N, average doc length, and
// per-term document frequency) so a boolean query scores every segment on the SAME
// scale. Without it each segment used its LOCAL N/df, so appending a CDC tail shifted
// a doc's score and could drop a globally-top-k doc at the merge. df is summed across
// segments (like bm25's gdf) and memoized per query; it is built fresh per query. The
// parallel multi-segment search shares one gs across worker goroutines, so the df caches
// are guarded by mu (below) and minScore is atomic.
type globalStats struct {
	n         int64
	avgDocLen float64
	idx       *Index
	// mu guards dfCache/phraseDfCache. The serial search path never contends; the
	// parallel multi-segment search (SearchBoolean with threadsSearch) shares ONE gs
	// across worker goroutines, so every cache read/write is taken under mu. Each df/
	// phraseDf is still computed at most once (the lock is held across the compute).
	mu      sync.Mutex
	dfCache map[string]int
	// phraseDfCache memoizes only the corpus-global phrase COUNT per phrase (an int),
	// never the per-segment hit slices — see phraseDf. Retaining the slices would defeat
	// the no-LIMIT streaming O(one segment) bound: a low-selectivity phrase over many
	// capacity-sized segments would hold O(global matching corpus) before the first
	// segment could finish emitting.
	phraseDfCache map[string]int
	// minScore is the shared global minimum competitive score (BM25 k-th-best floor),
	// as float32 bits, seeded -Inf. Each segment's WAND raises it to its local k-th best
	// (raise); every segment reads it (floor) as an extra pruning floor so a strong
	// segment lets weaker ones skip globally-uncompetitive docs. It is a results-neutral
	// pruning hint — the block-skip against it is STRICT, so no doc that could tie the
	// global k-th is ever dropped and the merged top-k is identical with or without it
	// (and across worker counts). Atomic because the parallel search shares one gs.
	minScore atomic.Uint32
}

func (idx *Index) newGlobalStats() *globalStats {
	gs := &globalStats{n: idx.globalN, avgDocLen: idx.globalAvgDocLen, idx: idx,
		dfCache: make(map[string]int), phraseDfCache: make(map[string]int)}
	gs.minScore.Store(math.Float32bits(float32(math.Inf(-1))))
	return gs
}

// floor returns the shared global minimum competitive score, or -Inf when gs is nil
// (a direct single-segment query) or no segment heap has filled yet.
func (gs *globalStats) floor() float32 {
	if gs == nil {
		return float32(math.Inf(-1))
	}
	return math.Float32frombits(gs.minScore.Load())
}

// raise publishes a segment's local k-th-best score to the shared floor, keeping the
// running maximum. Concurrency-safe (CAS loop); a no-op when gs is nil or v does not
// exceed the current floor.
func (gs *globalStats) raise(v float32) {
	if gs == nil {
		return
	}
	for {
		old := gs.minScore.Load()
		if v <= math.Float32frombits(old) {
			return
		}
		if gs.minScore.CompareAndSwap(old, math.Float32bits(v)) {
			return
		}
	}
}

// phraseHits returns seg's matchPhrase hits WITHOUT retaining them across segments: the
// caller (evalClause) uses the slice transiently to score one segment and drops it, so
// only O(one segment) of phrase hits is live at a time. phraseDf recomputes-and-discards
// its own cross-segment scan for the same reason; the two intentionally do NOT share a
// cache (that sharing is what broke the streaming memory bound). The doubled matchPhrase
// work is the accepted cost of bounded memory on the no-LIMIT streaming path.
func (gs *globalStats) phraseHits(seg *Segment, slots []phraseSlot) []docTf {
	return seg.matchPhrase(slots)
}

// df returns term's corpus-global LIVE document frequency: only ords not superseded by a
// newer copy (recency) or a tombstone are counted. df feeds idf against the LIVE globalN, so
// counting dead PHYSICAL copies (an UPDATE leaves the old copy in a lower-recency segment
// until MERGE reclaims it) would inflate df past N and make a dirty base+tail rank a query
// DIFFERENTLY from the identical corpus after compaction. Dirty segments stream one posting
// block at a time while fully-live segments use raw df directly; the result is memoized so the
// scan is paid once per query term.
func (gs *globalStats) df(term string) int {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if d, ok := gs.dfCache[term]; ok {
		return d
	}
	d := 0
	for si, seg := range gs.idx.segments {
		// A loaded segment with no liveness bitmap is fully live. Its raw df is
		// already exact, so read only the header varint at the FST offset and
		// avoid allocating the block-directory metadata that LookupLoaded builds.
		// Missing or malformed headers stay a miss. Segment search still performs
		// its independent full lookup before any posting can produce a hit.
		if seg.dict != nil && gs.idx.liveOrd[si] == nil {
			if df, ok := seg.lookupLoadedDF(term); ok {
				d += df
			}
			continue
		}
		if pl, ok := seg.lookup(term); ok {
			d += gs.idx.liveTermDF(si, pl)
		}
	}
	gs.dfCache[term] = d
	return d
}

// idfFor is the term's idf² under the global corpus stats, or the segment-local stats
// when gs is nil (a direct Segment query / single-segment test, where local == global).
func (gs *globalStats) idfFor(seg *Segment, term string, pl *termPostings) float64 {
	if gs == nil {
		return idfSquared(seg.N, pl.df())
	}
	return idfSquared(gs.n, gs.df(term))
}

// phraseDf returns the corpus-global LIVE document frequency of a phrase (the number of LIVE
// docs matching the contiguous phrase, summed over segments), memoizing only the COUNT. Each
// segment's matchPhrase slice is counted and immediately discarded (count-and-discard), so the
// df scan holds at most one segment's hits at a time — never the whole corpus. Only live ords
// are counted (mirrors the live term df above and streamPhrase's pass 1), so a dirty tail does
// not inflate the phrase idf relative to the compacted corpus.
func (gs *globalStats) phraseDf(slots []phraseSlot) int {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	key := slotsKey(slots)
	if d, ok := gs.phraseDfCache[key]; ok {
		return d
	}
	d := 0
	for si, seg := range gs.idx.segments {
		for _, h := range seg.matchPhrase(slots) { // counted, then the slice is released this iteration
			if gs.idx.isLive(si, h.ord) {
				d++
			}
		}
	}
	gs.phraseDfCache[key] = d
	return d
}

// phraseIdfFor is a phrase clause's idf² under the global corpus stats (global N +
// cross-segment phrase df), or the segment-local stats when gs is nil — so a phrase
// clause ranks consistently across a base + CDC tail, like the term/prefix clauses.
func (gs *globalStats) phraseIdfFor(seg *Segment, slots []phraseSlot, localHits int) float64 {
	if gs == nil {
		return idfSquared(seg.N, localHits)
	}
	return idfSquared(gs.n, gs.phraseDf(slots))
}

// avgdl is the global average doc length, or the segment's own when gs is nil.
func (gs *globalStats) avgdl(seg *Segment) float64 {
	if gs == nil {
		return seg.avgDocLenOrMean()
	}
	return gs.avgDocLen
}

// wandPrepass computes the segment visit order and, when active, per-segment query
// score ceilings for the WAND pre-pass. With the pre-pass off, the query not a pure
// disjunction of terms, or a single segment, it returns the natural index order and a
// nil ceiling slice (no reordering, no whole-segment skip — behaviour unchanged). On a
// disjunctive (bag-of-words) query with the pre-pass on, it computes each segment's
// ceiling (queryCeilingWAND, dict lookups only) and orders segments by ceiling
// descending so the strongest segment raises the shared floor first; the returned
// ceilings then drive the whole-segment skip in the search loop. Reordering never
// changes results (the merge is order-independent) and the skip is strict, so the
// top-k is identical with the pre-pass on or off.
// ceilPrepassMinSegPerWorker is the segments-per-worker threshold at or above which the
// ceiling pass fans out instead of running serially. The ceiling pass is nseg dict
// lookups (no posting walk), so serially it is a small fraction of the search it orders
// and skips — and at a few segments per worker, goroutine-launch overhead costs more
// than the lookups save. A segment sweep (wiki_en_3f, 12 cores) showed the serial pass
// only became a measurable tax at 8 segments per worker (96 segments), so that is the
// crossover; below it serial wins.
const ceilPrepassMinSegPerWorker = 8

// fanOut runs fn(si) for every si in order on at most workers goroutines and
// re-raises the first worker panic on the calling goroutine after all workers end.
func fanOut(order []int, workers int, fn func(si int)) {
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var once sync.Once
	var panicked any
	for _, si := range order {
		wg.Add(1)
		sem <- struct{}{}
		go func(si int) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					once.Do(func() { panicked = r })
				}
			}()
			fn(si)
		}(si)
	}
	wg.Wait()
	if panicked != nil {
		panic(panicked)
	}
}

func (idx *Index) wandPrepass(q BoolQuery, algo ScoreAlgo, gs *globalStats, workers int) (order []int, ceil []float32) {
	nseg := len(idx.segments)
	order = make([]int, nseg)
	for i := range order {
		order[i] = i
	}
	clauses, ok := disjunctiveTerms(q)
	// The pre-pass is unconditional (like the shared floor — no toggle), but only pays off
	// when an earlier segment's floor is established before a later one is searched:
	// serial (workers==1), or a multi-wave fan-out (nseg > workers) where later segments
	// wait on the semaphore. It applies only to the disjunctive (BM25 bag-of-words) shape.
	// Otherwise — a single parallel wave (nseg <= workers, the common post-MERGE steady
	// state of ~cores segments, where every segment starts together with floor=-Inf so no
	// whole-segment skip can fire), a non-disjunctive query, or a lone segment — it is a
	// no-op (natural order, nil ceilings) and the always-on block-level floor does the work.
	if !ok || nseg <= 1 || (workers > 1 && nseg <= workers) {
		return order, nil
	}
	ceil = make([]float32, nseg)
	computeCeil := func(si int) {
		seg := idx.segments[si]
		ceil[si] = seg.queryCeilingWAND(clauses, algo, gs, gs.avgdl(seg))
	}
	// Serial unless there are enough segments per worker to amortize goroutine overhead;
	// then fan out over the same worker budget as the search. Each worker writes ONLY its
	// own ceil[si]; gs.df is mutex-guarded and segment lookups are read-only.
	if workers <= 1 || nseg < ceilPrepassMinSegPerWorker*workers {
		for si := range idx.segments {
			computeCeil(si)
		}
	} else {
		fanOut(order, workers, computeCeil)
	}
	sort.SliceStable(order, func(a, b int) bool { return ceil[order[a]] > ceil[order[b]] })
	return order, ceil
}

// SearchBoolean runs a parsed boolean-mode query across the index and returns the
// global top-k (score desc; equal scores are order-unspecified). Each segment is scored on the
// GLOBAL corpus stats (globalStats), and its walk admits only the LIVE copy of a pk
// (liveness ANDed into the WHERE prefilter), so a segment's top-k is k live docs on
// the global scale — the merge below is then the exact global top-k even across a
// base + CDC tail. All clause types (term, prefix, phrase, and the disjunctive WAND
// path) score on global stats — a phrase clause uses the cross-segment phrase df.
// highDFKept returns clauses with ranked high-df terms removed: a single-term clause whose
// global df/N reaches cutoff is dropped. It returns the input UNCHANGED when the cutoff is
// off (<=0 or >1), when there is at most one clause, when nothing crosses the cutoff, or
// when no kept clause can match (every kept term has df 0 — never empty the query). Only
// atomic terms are considered; phrase/prefix/group clauses are always kept. gs.df memoizes
// the df lookups, so the later idf pass reuses them. A dropped term matches nearly every
// doc with idf≈0, so removing it cannot displace a top-k doc that matches any surviving
// term; a doc matching only dropped terms scored ≈0 and was never competitive.
func (idx *Index) highDFKept(clauses []clause, gs *globalStats, cutoff float64) []clause {
	if cutoff <= 0 || cutoff > 1 || gs.n <= 0 || len(clauses) <= 1 {
		return clauses
	}
	thresh := cutoff * float64(gs.n)
	kept := make([]clause, 0, len(clauses))
	matchable := 0
	for _, c := range clauses {
		if c.kind != clauseTerm {
			kept = append(kept, c)
			matchable++
			continue
		}
		df := gs.df(c.terms[0])
		if float64(df) >= thresh {
			continue
		}
		kept = append(kept, c)
		if df > 0 {
			matchable++
		}
	}
	if matchable == 0 || len(kept) == len(clauses) {
		return clauses
	}
	return kept
}

func (idx *Index) SearchBoolean(q BoolQuery, algo ScoreAlgo, k int, filter *prefilter) ([]Result, error) {
	if k <= 0 || idx.globalN == 0 {
		return nil, nil
	}
	gs := idx.newGlobalStats()
	// Ranked bag-of-words high-df cutoff (fulltext2_search_df_cutoff, carried on the query):
	// drop pure-disjunction SHOULD terms whose global df reaches the cutoff before the WAND
	// search. No-op (q.dfCutoff==0) for every non-bag-of-words shape.
	if clauses, ok := disjunctiveTerms(q); ok {
		q.should = idx.highDFKept(clauses, gs, q.dfCutoff)
	}
	nseg := len(idx.segments)
	// Each segment is a fully independent top-k search (own dense ord space, own WAND
	// threshold, own per-segment heap), so the segment loop fans out across
	// threadsSearch workers. The shared state is gs: its df caches are mutex-guarded,
	// and its minScore floor is an atomic results-neutral pruning hint (a segment's WAND
	// skips docs that cannot beat the best segment's k-th score). Each worker writes ONLY
	// its own perSeg[si] slot (no shared result map during the walk). The merge below runs
	// serially in segment order, so the winning live copy per pk is chosen exactly as the
	// serial walk did -- parallelism is a perf hint that yields an identical top-k.
	perSeg := make([][]Result, nseg)
	errs := make([]error, nseg)
	workers := int(vectorindex.GetConcurrency(int64(idx.threadsSearch)))
	if workers < 1 {
		workers = 1
	}
	if workers > nseg {
		workers = nseg
	}
	// Keep the DENSE full-boolean evaluator serial. The bounded per-segment searches —
	// WAND (O(k) heap) and conjunctive (O(unique terms · BlockSize + k)) — parallelize
	// safely, but searchBooleanFull allocates O(segment doc count) score/hit scratch per
	// segment, so fanning it out would hold workers × O(N) live at once (not the
	// documented O(segments × k)). Only the bag-of-words/ranked path, the one this feature
	// targets, needs the parallelism; the dense path runs one segment at a time.
	if _, conj := conjunctiveTerms(q); !conj {
		if _, disj := disjunctiveTerms(q); !disj {
			workers = 1
		}
	}
	// order is the segment visit order; ceil[si] (when non-nil) is seg si's query-score
	// ceiling. With the WAND pre-pass on a disjunctive (bag-of-words) query, segments are
	// visited highest-ceiling first so the shared floor climbs fastest, and a segment
	// whose ceiling is below the floor is skipped whole (its docs cannot enter the top-k).
	order, ceil := idx.wandPrepass(q, algo, gs, workers)
	searchSeg := func(si int) {
		if ceil != nil && ceil[si] < gs.floor() {
			return // whole-segment skip: ceiling below the global floor → no top-k doc here
		}
		seg := idx.segments[si]
		allow := andAllow(mkAllow(seg, filter), &livenessMembership{idx: idx, si: si})
		res, err := seg.SearchBoolean(q, algo, k, allow, gs)
		if err != nil {
			errs[si] = err
			return
		}
		perSeg[si] = res
	}
	if workers <= 1 || nseg <= 1 {
		for _, si := range order {
			searchSeg(si)
		}
	} else {
		fanOut(order, workers, searchSeg)
	}
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	matched := make(map[any]Result)
	for _, res := range perSeg {
		for _, r := range res {
			matched[normalizeKey(r.Pk)] = r // liveness inside the walk → one live copy per pk
		}
	}
	if len(matched) == 0 {
		return nil, nil
	}
	results := make([]Result, 0, len(matched))
	for _, r := range matched {
		results = append(results, r)
	}
	return topKResults(results, k), nil
}

// streamBoolean emits every LIVE, WHERE-admitted boolean match (pk, score) through sink
// WITHOUT a global top-K heap or an O(corpus) matched map — the streaming analogue of
// SearchBoolean for the no-LIMIT MUST / MUST-NOT / mixed-phrase path. Liveness is ANDed into
// each segment's prefilter exactly as SearchBoolean does, so a pk that survives in exactly
// one live segment is emitted once. Each segment's evalBoolean is O(segment N) (bounded by
// the segment capacity) and freed before the next, so peak Go heap is O(one segment), NOT
// O(globalN) — this is what makes the streamed boolean shape genuinely bounded rather than
// the old SearchQuery(globalN) fallback that materialized the whole live corpus. Ranking is
// the upstream ORDER BY score node's job (same contract as the other streaming paths).
func (idx *Index) streamBoolean(q BoolQuery, algo ScoreAlgo, filter *prefilter, sink *streamSink) error {
	if idx.globalN == 0 {
		return nil
	}
	gs := idx.newGlobalStats()
	for si, seg := range idx.segments {
		allow := andAllow(mkAllow(seg, filter), &livenessMembership{idx: idx, si: si})
		err := seg.evalBoolean(q, algo, allow, gs, func(ord int, score float32) {
			sink.pushPk(seg, int64(ord), score) // pushPk no-ops once the sink has stopped
		})
		if err != nil {
			return err
		}
		// Bail before the NEXT segment on an emit error (the expensive per-segment work is
		// bounded to one segment). Unlike streamWAND's `for !sink.stopped`, evalBoolean is
		// NOT short-circuited mid-segment on abort: a boolean query must fully score every
		// clause into the dense array before it can admit any candidate, so that work is
		// already done by the time an emit can fail — the only remaining cost is the tail of
		// the candidate forEach running as no-op pushes (a cheap bitset walk, no scoring).
		// Threading a stop signal through the sink-agnostic evalBoolean to save that tail
		// isn't worth complicating the helper it shares with searchBooleanFull.
		if sink.err != nil {
			return sink.err
		}
	}
	sink.flush()
	return sink.err
}

// SearchBooleanText tokenizes+parses query in boolean mode with tok, then
// evaluates it across the index — the convenience entry mirroring
// MATCH(col) AGAINST('query' IN BOOLEAN MODE) with a fixed tokenizer.
func (idx *Index) SearchBooleanText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	q, err := ParseBoolean(query, tok)
	if err != nil {
		return nil, err
	}
	return idx.SearchBoolean(q, algo, k, nil) // convenience entry: no WHERE prefilter
}

// ReconstructLiveDocs yields each LIVE document as (pk, ordered terms) from the
// positional postings across all loaded segments — the input to a MERGE compaction
// that folds base + tail into a fresh dead-doc-free base WITHOUT re-tokenizing the
// source. Per-doc term order is recovered from token positions.
//
// Reconstruction is done ONE SEGMENT AT A TIME: a live doc's postings live entirely
// within its own (live) segment, so `buckets` only ever holds the CURRENT segment's
// live docs and is freed before the next — peak Go heap is O(one segment's postings)
// (≤ max_index_capacity), NOT O(the whole live corpus). This matters because MERGE
// (CompactSegments) is deliberately exempt from checkBaseLoadBudget, so an all-at-once
// reconstruction would OOM-kill the CN on exactly the operation meant to reclaim it.
// Docs are yielded in load-order across segments, pk-sorted WITHIN each segment (so the
// output is still deterministic; the fresh base's per-segment doc grouping differs but
// its contents and the resulting query answers do not). On a posting-decode error the
// iterator yields once with a non-nil error and stops.
func (idx *Index) ReconstructLiveDocs(positionFree bool) iter.Seq2[TokenizedDoc, error] {
	return func(yield func(TokenizedDoc, error) bool) {
		type posTerm struct {
			pos  int32
			term string
		}
		for si, seg := range idx.segments {
			live := idx.liveOrd[si]
			nd := seg.numDocs()
			// Bucket ONLY this segment's live ords (keyed by ord, unique within the segment).
			buckets := make(map[int64][]posTerm)
			for ord := 0; ord < nd; ord++ {
				if live == nil || live[ord] {
					buckets[int64(ord)] = nil
				}
			}
			if len(buckets) == 0 {
				continue
			}
			err := seg.forEachPosting(func(term string, tp *termPostings) {
				docs := tp.materializeDocIDs() // docIDs (block-compressed on load)
				if positionFree {
					// A position-free segment has no positions: reconstruct each doc's terms
					// from tf — emit the term tf times with a synthetic ascending position.
					// Order is irrelevant (the position-free rebuild drops positions again),
					// but tf is preserved so BM25 ranking is unchanged.
					tfs := tp.materializeTfs()
					for i, ord := range docs {
						if _, ok := buckets[ord]; !ok {
							continue
						}
						for t := int32(0); t < int32(tfs[i]); t++ {
							buckets[ord] = append(buckets[ord], posTerm{t, term})
						}
					}
					return
				}
				mat := tp.materializePositions() // decode this term's positions once
				for i, ord := range docs {
					if _, ok := buckets[ord]; !ok {
						continue
					}
					for _, pos := range mat[i] {
						buckets[ord] = append(buckets[ord], posTerm{pos, term})
					}
				}
			})
			if err != nil {
				yield(TokenizedDoc{}, err)
				return
			}

			// Yield this segment's live docs pk-sorted (sortKey computed once each).
			type keyed struct {
				sk  string
				ord int64
			}
			keys := make([]keyed, 0, len(buckets))
			for ord := range buckets {
				keys = append(keys, keyed{sortKey(normalizeKey(seg.pk(ord))), ord})
			}
			sort.Slice(keys, func(i, j int) bool { return keys[i].sk < keys[j].sk })
			for _, kd := range keys {
				pl := buckets[kd.ord]
				sort.SliceStable(pl, func(i, j int) bool { return pl[i].pos < pl[j].pos })
				terms := make([]string, len(pl))
				positions := make([]int32, len(pl))
				for i, pt := range pl {
					terms[i] = pt.term
					positions[i] = pt.pos // byte position, carried through MERGE
				}
				buckets[kd.ord] = nil // copied out — let GC reclaim it
				// Carry the source doc's INCLUDE values through MERGE so the rebuilt base
				// keeps them (a compaction that dropped them would break covering/prefilter).
				var include []any
				if nIncl := seg.nIncludeCols(); nIncl > 0 {
					include = make([]any, nIncl)
					for c := 0; c < nIncl; c++ {
						v, isNull, ierr := seg.includeVal(kd.ord, c)
						if ierr != nil {
							yield(TokenizedDoc{}, ierr)
							return
						}
						if !isNull {
							include[c] = v
						}
					}
				}
				if !yield(TokenizedDoc{Pk: seg.pk(kd.ord), Terms: terms, Positions: positions, Include: include}, nil) {
					return
				}
			}
			// buckets goes out of scope → freed before the next segment.
		}
	}
}

// NumDocs returns the number of live documents in the index.
func (idx *Index) NumDocs() int64 { return idx.globalN }

// Free releases the off-heap posting buffers of every segment the index owns.
// Called on cache eviction (Fulltext2Search.Destroy) and by single-shot loaders
// (compact) once done. Build-side segments free nothing (no-op).
func (idx *Index) Free() {
	if idx != nil {
		freeSegs(idx.segments)
	}
}

// normalizeKey converts a pk to a comparable map key ([]byte -> string) and is the
// key for every pk-keyed map (liveness, deletes, dedup). Keying by the pk VALUE (not
// its String()) makes it INJECTIVE for every comparable pk type via Go == : a
// microsecond-precision DATETIME/TIME/TIMESTAMP (whose String() truncates to whole
// seconds), a raw uuid ([16]byte), a decimal128 struct — all key by their exact value
// and can never collide, so no distinct live document is dropped. Mirrors bm25's
// normalizeKey exactly (the complete port; a %v/String() key was lossy).
func normalizeKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

// sortKey is a deterministic INJECTIVE string projection of a pk, used to order
// equal-score results (and MERGE output) reproducibly — never as a map key. The
// temporal types are keyed by their underlying int64 because %v/String() truncates
// them to whole seconds, which would make two equal-score DATETIME(6) rows sharing a
// second sort nondeterministically at a LIMIT boundary; every other scalar pk has an
// injective %v (int/decimal exact, uuid canonical, date a distinct day).
func sortKey(pk any) string {
	switch v := pk.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case types.Datetime:
		return strconv.FormatInt(int64(v), 10)
	case types.Time:
		return strconv.FormatInt(int64(v), 10)
	case types.Timestamp:
		return strconv.FormatInt(int64(v), 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}
