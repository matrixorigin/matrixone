# Morsel-Driven Pipeline for MatrixOne

## 1. Execution Model Overview

```
                    Plan Tree (from optimizer)
                           │
                           ▼
              ┌────────────────────────┐
              │   Pipeline Compiler     │
              │   (cuts at breakers)    │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │    Execution DAG        │
              │                        │
              │  Fragment₀ ──▶ Barrier₀ │
              │       ╲                │
              │  Fragment₁ ──▶ Barrier₁ ──▶ Fragment₂ ──▶ Sink │
              │       ╱                │
              │  Fragment₃ ──▶ Barrier₀ │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │      Scheduler          │
              │  (workers = GOMAXPROCS) │
              └────────────────────────┘
```

## 2. Core Data Structures

### 2.1 Morsel

A morsel is not a batch — it's a **work ticket** describing what to read.

```go
package morsel

// Morsel describes a unit of work for a source operator.
// It is NOT materialized data — it's a pointer to data.
type Morsel struct {
    // For table scan: block range to read.
    Blocks []objectio.BlockInfo
    
    // For barrier output: materialized batch (already in memory).
    Batch *batch.Batch
    
    // For exchange receiver: a received batch from RPC.
    // Reuses Batch field.
    
    // Provenance for debugging/tracing.
    FragmentID int
    Seq        uint64
}

// MorselSize is adaptive per-source.
// Initial: 16 blocks (~1.3M rows). Shrinks near tail.
const (
    InitialMorselBlocks = 16
    MinMorselBlocks     = 2
)
```

### 2.2 Fragment (Pipeline Segment)

```go
package pipeline

// Fragment is an executable pipeline segment between two breakers.
// Stateless except for per-worker operator states.
type Fragment struct {
    ID          int
    Ops         []Operator           // push-based operators in chain order
    Source      Source               // morsel provider
    Sink        Sink                 // where output goes
    
    // Per-worker state. Indexed by workerID.
    // Allocated during Init, freed during Close.
    States      [][]OperatorState    // [workerID][opIndex]
    
    // Lifecycle.
    status      atomic.Int32         // inactive → active → draining → done
    inflight    atomic.Int64         // morsels currently being processed
    
    // Memory budget for this fragment (shared across workers).
    memBudget   *MemBudget
    
    // Parent query context.
    ctx         context.Context
    cancel      context.CancelCauseFunc
}

const (
    FragInactive = iota
    FragActive
    FragDraining  // source exhausted, waiting for inflight to finish
    FragDone
)

// Activate is called by scheduler when upstream barrier finalizes.
func (f *Fragment) Activate(nWorkers int) {
    f.States = make([][]OperatorState, nWorkers)
    for w := 0; w < nWorkers; w++ {
        f.States[w] = make([]OperatorState, len(f.Ops))
        for i, op := range f.Ops {
            f.States[w][i] = op.InitState(w, f.memBudget)
        }
    }
    f.status.Store(FragActive)
}

func (f *Fragment) TryGetMorsel() (*Morsel, bool) {
    if f.status.Load() != FragActive {
        return nil, false
    }
    m := f.Source.Next()
    if m == nil {
        f.status.CompareAndSwap(FragActive, FragDraining)
        return nil, false
    }
    f.inflight.Add(1)
    return m, true
}

func (f *Fragment) MorselDone() {
    if f.inflight.Add(-1) == 0 && f.status.Load() == FragDraining {
        f.status.Store(FragDone)
    }
}
```

### 2.3 Push Operator Interface

```go
package pipeline

// Operator is the push-based operator interface.
// Process is called per-morsel. No recursion, no stack, no channel.
type Operator interface {
    // InitState creates per-worker state. Called once per worker.
    InitState(workerID int, mem *MemBudget) OperatorState
    
    // Process pushes input through the operator.
    // Writes output rows into out. Returns rows written.
    // May write 0 rows (fully filtered) — caller skips downstream.
    //
    // MUST NOT hold references to `in` after returning.
    // `out` is pre-allocated by the caller, reused across morsels.
    Process(state OperatorState, in, out *batch.Batch) (int, error)
    
    // Drain is called once when fragment transitions to done.
    // For operators with buffered state (none in a correct fragment, 
    // since buffering operators are barriers — but needed for window funcs).
    Drain(state OperatorState, out *batch.Batch) (int, error)
    
    // Close releases per-worker state.
    Close(state OperatorState)
}

// OperatorState is opaque per-operator per-worker state.
// No locking needed — exclusively owned by one worker.
type OperatorState interface{}
```

## 3. Sources

### 3.1 Table Scan Source (the Phase 0 win)

```go
package source

// ScanSource serves morsels from a table's block list.
// Thread-safe: multiple workers call Next() concurrently.
type ScanSource struct {
    mu          sync.Mutex
    blocks      []objectio.BlockInfo
    cursor      int
    morselSize  int           // blocks per morsel (adaptive)
    
    // Prefetch state: issue async reads ahead of cursor.
    prefetcher  *Prefetcher
    
    // Zone-map filter: skip entire blocks before serving.
    filter      *ZoneMapFilter
    
    // Memory budget: throttle if downstream is slower than scan.
    mem         *MemBudget
}

func NewScanSource(
    blocks []objectio.BlockInfo,
    filter *ZoneMapFilter,
    prefetcher *Prefetcher,
    mem *MemBudget,
) *ScanSource {
    // Pre-filter blocks by zone map (bulk elimination).
    // This is done ONCE up front, not per-morsel.
    filtered := make([]objectio.BlockInfo, 0, len(blocks)/4)
    for _, b := range blocks {
        if !filter.CanSkip(b) {
            filtered = append(filtered, b)
        }
    }
    
    return &ScanSource{
        blocks:     filtered,
        morselSize: InitialMorselBlocks,
        prefetcher: prefetcher,
        filter:     filter,
        mem:        mem,
    }
}

func (s *ScanSource) Next() *Morsel {
    s.mu.Lock()
    if s.cursor >= len(s.blocks) {
        s.mu.Unlock()
        return nil
    }
    
    // Adaptive sizing: shrink near tail for better load balancing.
    size := s.morselSize
    remaining := len(s.blocks) - s.cursor
    if remaining < size*4 {
        size = max(MinMorselBlocks, remaining/4)
    }
    
    end := min(s.cursor+size, len(s.blocks))
    blocks := s.blocks[s.cursor:end]
    s.cursor = end
    
    // Issue prefetch for next morsel while current is being processed.
    if s.cursor < len(s.blocks) {
        prefEnd := min(s.cursor+size, len(s.blocks))
        s.prefetcher.Prefetch(s.blocks[s.cursor:prefEnd])
    }
    s.mu.Unlock()
    
    return &Morsel{Blocks: blocks}
}

func (s *ScanSource) Remaining() int {
    s.mu.Lock()
    r := len(s.blocks) - s.cursor
    s.mu.Unlock()
    return r
}
```

### 3.2 Prefetcher (critical for S3)

```go
// Prefetcher issues async block reads to hide S3 latency.
// It maintains a window of in-flight reads ahead of the scan cursor.
type Prefetcher struct {
    fs       fileservice.FileService
    inflight chan struct{}  // semaphore: max concurrent prefetches
    cache    *objectio.BlockReadCache
}

func NewPrefetcher(fs fileservice.FileService, concurrency int) *Prefetcher {
    return &Prefetcher{
        fs:       fs,
        inflight: make(chan struct{}, concurrency),
    }
}

func (p *Prefetcher) Prefetch(blocks []objectio.BlockInfo) {
    for _, b := range blocks {
        if p.cache.Has(b.BlockID) {
            continue
        }
        select {
        case p.inflight <- struct{}{}:
            go func(blk objectio.BlockInfo) {
                defer func() { <-p.inflight }()
                _ = objectio.PrefetchBlock(p.fs, blk)
            }(b)
        default:
            // Prefetch buffer full — skip, will read synchronously later.
            return
        }
    }
}
```

### 3.3 Barrier Output Source

```go
// BarrierSource serves materialized batches from a completed barrier.
type BarrierSource struct {
    batches []*batch.Batch
    cursor  atomic.Int64
}

func (s *BarrierSource) Next() *Morsel {
    idx := s.cursor.Add(1) - 1
    if int(idx) >= len(s.batches) {
        return nil
    }
    return &Morsel{Batch: s.batches[idx]}
}
```

### 3.4 Exchange Receive Source (from remote CN)

```go
// ExchangeSource receives batches from remote CNs.
// Replaces current MergeReceiver operator.
type ExchangeSource struct {
    ch      chan *batch.Batch  // fed by RPC handler goroutine
    closed  atomic.Bool
}

func (s *ExchangeSource) Next() *Morsel {
    b, ok := <-s.ch
    if !ok || b == nil {
        return nil
    }
    return &Morsel{Batch: b}
}
```

## 4. Sinks & Barriers

### 4.1 Sink Interface

```go
// Sink receives output from a fragment's operator chain.
type Sink interface {
    // Contribute deposits a result batch. Thread-safe.
    Contribute(workerID int, batch *batch.Batch) error
    
    // Finalize is called once when the fragment is done.
    // Returns the source for the downstream fragment (if any).
    Finalize() (Source, error)
    
    // Close releases resources.
    Close()
}
```

### 4.2 Hash Join Barrier

Two fragments feed this: build side (small) and probe side (large).
Build completes first → finalize → probe fragment activates.

```go
// HashBuildBarrier: concurrent hash table construction.
type HashBuildBarrier struct {
    nWorkers    int
    keyColumns  []int
    buildTypes  []types.Type
    
    // Per-worker local hash tables. Zero contention during build.
    locals      []*LocalHashTable
    
    // Merged read-only table for probe. Populated during Finalize.
    merged      *SharedHashTable
    
    mem         *MemBudget
}

// LocalHashTable: worker-private during build phase.
type LocalHashTable struct {
    // Open-addressing with Robin Hood hashing.
    entries  []hashEntry
    payloads []batch.Batch   // stored rows, chunked
    mask     uint64
    count    int
}

func (b *HashBuildBarrier) Contribute(workerID int, bat *batch.Batch) error {
    local := b.locals[workerID]
    
    // Hash each key, insert into worker-local table.
    keys := hashBatchKeys(bat, b.keyColumns)
    for i, h := range keys {
        local.Insert(h, bat, i)
    }
    return nil
}

func (b *HashBuildBarrier) Finalize() (Source, error) {
    // Merge local tables into shared read-only table.
    // Strategy: compute total size, allocate once, scatter.
    totalRows := 0
    for _, l := range b.locals {
        totalRows += l.count
    }
    
    b.merged = NewSharedHashTable(totalRows, b.buildTypes)
    for _, l := range b.locals {
        b.merged.Absorb(l)
    }
    
    // Probe source is the scan source for the probe-side table.
    // It's set externally by the DAG compiler.
    return nil, nil
}

// SharedHashTable: immutable after Finalize. Concurrent reads, no locking.
type SharedHashTable struct {
    entries  []hashEntry     // open-addressing, immutable
    payloads []byte          // flat row storage
    mask     uint64
    rowSize  int
}

// Probe is called by hash-probe operator per input row. Lock-free.
func (t *SharedHashTable) Probe(hash uint64, key []byte) ([]Match, bool) {
    pos := hash & t.mask
    for {
        e := t.entries[pos]
        if e.isEmpty() {
            return nil, false
        }
        if e.hash == hash && t.keyEquals(e.rowPtr, key) {
            return t.collectChain(e), true
        }
        pos = (pos + 1) & t.mask
    }
}
```

### 4.3 Aggregation Barrier

Directly generalizes the local accumulator pattern into an architectural primitive.

```go
// AggBarrier: morsel-parallel aggregation.
// Each worker maintains a complete local aggregation state.
type AggBarrier struct {
    nWorkers   int
    groupExprs []plan.Expr
    aggDescs   []AggDesc
    
    // Per-worker aggregation state.
    // This IS the local accumulator pattern, formalized.
    locals     []*AggState
    
    mem        *MemBudget
}

// AggState: one per worker. Owns a complete hash table of groups.
type AggState struct {
    // Hash table: group key → slot index.
    ht       aggHashTable
    
    // Per-aggregate accumulator vectors (same as current aggExec.state).
    accs     []AggFuncExec
    
    // Group count.
    nGroups  int
}

func (b *AggBarrier) Contribute(workerID int, bat *batch.Batch) error {
    state := b.locals[workerID]
    
    // Compute group keys.
    keys, hashes := computeGroupKeys(bat, b.groupExprs)
    
    // For each row: find-or-create group in local table, accumulate.
    groups := state.ht.FindOrInsertBatch(keys, hashes)
    
    // Feed into accumulators. This is BatchFill with the local accumulator.
    for _, acc := range state.accs {
        if err := acc.BatchFill(0, groups, bat.Vecs); err != nil {
            return err
        }
    }
    return nil
}

func (b *AggBarrier) Finalize() (Source, error) {
    // Two strategies based on group count:
    
    totalGroups := 0
    for _, l := range b.locals {
        totalGroups += l.nGroups
    }
    
    if totalGroups < 100_000 {
        // Strategy A: sequential merge (small number of groups).
        return b.mergeSequential()
    }
    
    // Strategy B: partition-parallel merge (many groups).
    return b.mergePartitioned()
}

func (b *AggBarrier) mergeSequential() (Source, error) {
    target := b.locals[0]
    for w := 1; w < b.nWorkers; w++ {
        source := b.locals[w]
        for _, acc := range target.accs {
            if err := acc.BatchMerge(source.accs[acc.ID()], 0, source.groupMapping()); err != nil {
                return nil, err
            }
        }
    }
    
    results, err := target.Flush()
    if err != nil {
        return nil, err
    }
    return &BarrierSource{batches: results}, nil
}

func (b *AggBarrier) mergePartitioned() (Source, error) {
    // Partition local states by hash(group_key) % nPartitions.
    // Each partition can be merged independently → parallelize merge.
    const nPartitions = 64
    
    partitions := make([][]*AggState, nPartitions)
    for w := 0; w < b.nWorkers; w++ {
        split := b.locals[w].SplitByPartition(nPartitions)
        for p := 0; p < nPartitions; p++ {
            partitions[p] = append(partitions[p], split[p])
        }
    }
    
    results := make([]*batch.Batch, nPartitions)
    var wg sync.WaitGroup
    for p := 0; p < nPartitions; p++ {
        wg.Add(1)
        go func(pid int) {
            defer wg.Done()
            merged := mergePartitionStates(partitions[pid])
            results[pid], _ = merged.Flush()
        }(p)
    }
    wg.Wait()
    
    return &BarrierSource{batches: results}, nil
}
```

### 4.4 Sort Barrier

```go
// SortBarrier: concurrent external sort with morsel-sized runs.
type SortBarrier struct {
    nWorkers   int
    orderExprs []plan.OrderBySpec
    limit      int64           // -1 for no limit (LIMIT pushdown)
    
    // Per-worker sorted runs.
    locals     [][]*batch.Batch  // [workerID][]sorted_run
    
    // Spill support.
    spiller    *DiskSpiller
    mem        *MemBudget
}

func (b *SortBarrier) Contribute(workerID int, bat *batch.Batch) error {
    if b.limit > 0 {
        return b.contributeTopN(workerID, bat)
    }
    
    sorted := sortBatch(bat, b.orderExprs)
    
    if !b.mem.TryReserve(sorted.Size()) {
        b.spillRuns(workerID)
    }
    b.locals[workerID] = append(b.locals[workerID], sorted)
    return nil
}

func (b *SortBarrier) Finalize() (Source, error) {
    // K-way merge of all sorted runs.
    var allRuns []*batch.Batch
    for _, runs := range b.locals {
        allRuns = append(allRuns, runs...)
    }
    
    return &KWayMergeSource{
        runs:    allRuns,
        order:   b.orderExprs,
        limit:   b.limit,
    }, nil
}

// KWayMergeSource lazily produces sorted morsels via heap-merge.
// Single-threaded (output must maintain global order).
type KWayMergeSource struct {
    runs  []*batch.Batch
    heap  *mergeHeap
    order []plan.OrderBySpec
    limit int64
    emitted int64
}

func (s *KWayMergeSource) Next() *Morsel {
    out := batch.New(targetMorselRows)
    for out.RowCount() < targetMorselRows {
        if s.limit > 0 && s.emitted >= s.limit {
            break
        }
        row, ok := s.heap.Pop()
        if !ok {
            break
        }
        out.AppendRow(row)
        s.emitted++
    }
    if out.RowCount() == 0 {
        return nil
    }
    return &Morsel{Batch: out}
}
```

### 4.5 Exchange Sink (Dispatch to remote CN)

```go
// ExchangeSink sends batches to remote CNs.
// Replaces current Dispatch operator.
type ExchangeSink struct {
    mode       ExchangeMode  // Broadcast, Hash, RoundRobin
    targets    []*RemoteTarget
    hashCols   []int
    
    // Serialization buffer per worker (avoid allocation).
    marshalBuf []bytes.Buffer  // [workerID]
}

type ExchangeMode int
const (
    ExchangeBroadcast ExchangeMode = iota
    ExchangeHash
    ExchangeRoundRobin
)

func (s *ExchangeSink) Contribute(workerID int, bat *batch.Batch) error {
    switch s.mode {
    case ExchangeBroadcast:
        data, err := bat.MarshalBinaryWithBuffer(&s.marshalBuf[workerID], true)
        if err != nil {
            return err
        }
        for _, t := range s.targets {
            if err := t.Send(data); err != nil {
                return err
            }
        }
        
    case ExchangeHash:
        partitions := partitionBatch(bat, s.hashCols, len(s.targets))
        for i, p := range partitions {
            if p.RowCount() == 0 {
                continue
            }
            data, err := p.MarshalBinaryWithBuffer(&s.marshalBuf[workerID], true)
            if err != nil {
                return err
            }
            if err := s.targets[i].Send(data); err != nil {
                return err
            }
        }
        
    case ExchangeRoundRobin:
        target := s.targets[workerID%len(s.targets)]
        data, err := bat.MarshalBinaryWithBuffer(&s.marshalBuf[workerID], true)
        if err != nil {
            return err
        }
        return target.Send(data)
    }
    return nil
}

func (s *ExchangeSink) Finalize() (Source, error) {
    for _, t := range s.targets {
        t.SendEOF()
    }
    return nil, nil
}
```

## 5. Scheduler

### 5.1 Worker Pool

```go
package scheduler

type Scheduler struct {
    nWorkers   int
    fragments  []*Fragment        // topological order
    
    // Ready queue: fragments that are Active and have morsels.
    ready      []*Fragment        // lock-free scanning
    
    // Completion.
    allDone    chan struct{}
    queryCtx   context.Context
    cancel     context.CancelCauseFunc
    
    // Stats for adaptive decisions.
    stats      []*WorkerStats    // [workerID]
}

type WorkerStats struct {
    MorselsProcessed atomic.Int64
    RowsProcessed    atomic.Int64
    StallNanos       atomic.Int64  // time spent waiting for morsel
}

func NewScheduler(ctx context.Context, dag *PipelineDAG) *Scheduler {
    nWorkers := runtime.GOMAXPROCS(0)
    
    s := &Scheduler{
        nWorkers:  nWorkers,
        fragments: dag.Fragments,
        allDone:   make(chan struct{}),
        queryCtx:  ctx,
        stats:     make([]*WorkerStats, nWorkers),
    }
    for i := range s.stats {
        s.stats[i] = &WorkerStats{}
    }
    
    // Activate root fragments (those with no upstream dependency).
    for _, f := range dag.RootFragments() {
        f.Activate(nWorkers)
    }
    
    return s
}

func (s *Scheduler) Run() error {
    var wg sync.WaitGroup
    errs := make(chan error, s.nWorkers)
    
    for w := 0; w < s.nWorkers; w++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            if err := s.workerLoop(id); err != nil {
                errs <- err
            }
        }(w)
    }
    
    wg.Wait()
    close(errs)
    
    for err := range errs {
        if err != nil {
            return err
        }
    }
    return nil
}

func (s *Scheduler) workerLoop(workerID int) error {
    for {
        select {
        case <-s.queryCtx.Done():
            return s.queryCtx.Err()
        default:
        }
        
        frag, morsel := s.pickTask(workerID)
        if frag == nil {
            if s.allFragmentsDone() {
                return nil
            }
            runtime.Gosched()
            continue
        }
        
        if err := s.executeMorsel(workerID, frag, morsel); err != nil {
            return err
        }
    }
}
```

### 5.2 Task Selection (Priority)

```go
func (s *Scheduler) pickTask(workerID int) (*Fragment, *Morsel) {
    // Priority order:
    // 1. Fragments closest to completion (tail latency)
    // 2. Downstream fragments (reduce buffered data, memory pressure)
    // 3. Upstream fragments (keep pipeline flowing)
    
    var bestFrag *Fragment
    bestPriority := -1
    
    for i := len(s.fragments) - 1; i >= 0; i-- {
        f := s.fragments[i]
        if f.status.Load() != FragActive {
            continue
        }
        
        remaining := f.Source.Remaining()
        if remaining == 0 {
            continue
        }
        
        // Priority: lower remaining = higher priority (finish it off).
        // Tiebreak: prefer downstream (higher fragment ID in topo order).
        priority := 1000000 - remaining + i*1000
        if priority > bestPriority {
            bestFrag = f
            bestPriority = priority
        }
    }
    
    if bestFrag == nil {
        return nil, nil
    }
    
    morsel, ok := bestFrag.TryGetMorsel()
    if !ok {
        return nil, nil
    }
    return bestFrag, morsel
}
```

### 5.3 Morsel Execution

```go
func (s *Scheduler) executeMorsel(workerID int, frag *Fragment, morsel *Morsel) error {
    defer frag.MorselDone()
    
    // Materialize morsel into a batch (reads from S3/cache).
    in, err := materializeMorsel(morsel, frag, workerID)
    if err != nil {
        return err
    }
    
    // Push through operator chain.
    out := batchPool.Get()
    defer batchPool.Put(out)
    
    current := in
    for i, op := range frag.Ops {
        state := frag.States[workerID][i]
        n, err := op.Process(state, current, out)
        if err != nil {
            return err
        }
        if n == 0 {
            goto done
        }
        out.SetRowCount(n)
        current, out = out, current
    }
    
    // Deposit result into sink.
    if err := frag.Sink.Contribute(workerID, current); err != nil {
        return err
    }
    
done:
    s.stats[workerID].MorselsProcessed.Add(1)
    s.stats[workerID].RowsProcessed.Add(int64(in.RowCount()))
    
    // Check if fragment is complete → trigger barrier finalization.
    if frag.status.Load() == FragDone {
        s.onFragmentDone(frag)
    }
    return nil
}

func (s *Scheduler) onFragmentDone(frag *Fragment) {
    source, err := frag.Sink.Finalize()
    if err != nil {
        s.cancelQuery(err)
        return
    }
    
    // Find downstream fragment and activate it.
    for _, downstream := range s.fragments {
        if downstream.DependsOn(frag.ID) && downstream.status.Load() == FragInactive {
            if probe, ok := downstream.Ops[0].(*HashProbeOp); ok {
                probe.SetHashTable(frag.Sink.(*HashBuildBarrier).merged)
            }
            downstream.Source = source
            downstream.Activate(s.nWorkers)
            break
        }
    }
}
```

## 6. Memory Management

### 6.1 Memory Budget

```go
// MemBudget tracks memory usage for a fragment.
// When exceeded, triggers spill or backpressure.
type MemBudget struct {
    limit     int64
    used      atomic.Int64
    spill     SpillCallback
    
    // Backpressure: if downstream is full, slow down source.
    pressure  atomic.Int32  // 0=normal, 1=yield, 2=spill
}

func (m *MemBudget) TryReserve(bytes int64) bool {
    for {
        cur := m.used.Load()
        if cur+bytes > m.limit {
            return false
        }
        if m.used.CompareAndSwap(cur, cur+bytes) {
            return true
        }
    }
}

func (m *MemBudget) Release(bytes int64) {
    m.used.Add(-bytes)
}
```

### 6.2 Batch Ownership Rules

```
1. Source → Worker: source creates/provides batch, worker BORROWS it.
   Worker must not retain references after Process() returns.

2. Worker → Sink: sink TAKES ownership. Worker must not touch batch after.
   Exception: if sink copies (e.g., exchange serializes), batch returns to pool.

3. Pool: working batches (in/out) are pooled per-worker.
   Allocated once, reused across morsels. Freed on fragment close.
```

## 7. Spill-to-Disk Integration

```go
// SpillableBarrier: barrier that can spill partitions when memory is tight.
type SpillableBarrier struct {
    barrier   Barrier
    spiller   *DiskSpiller
    mem       *MemBudget
    spilled   bool
}

func (b *SpillableBarrier) Contribute(workerID int, bat *batch.Batch) error {
    if !b.mem.TryReserve(int64(bat.Size())) {
        b.spillColdest(workerID)
        b.spilled = true
    }
    return b.barrier.Contribute(workerID, bat)
}

func (b *SpillableBarrier) Finalize() (Source, error) {
    if !b.spilled {
        return b.barrier.Finalize()
    }
    // Spilled: re-read spilled partitions, process per-partition.
    // Recursively spill if a partition still doesn't fit.
    return b.finalizeWithSpill()
}
```

## 8. Pipeline DAG Compiler

```go
package compiler

func CompileDAG(plan *plan.Plan, ctx *CompileContext) (*PipelineDAG, error) {
    dag := &PipelineDAG{}
    root := plan.GetQuery().GetNodes()[plan.GetQuery().Steps[0]]
    compileNode(root, dag, ctx)
    dag.TopologicalSort()
    dag.AssignMemory(ctx.QueryMemLimit)
    return dag, nil
}

func compileNode(node *plan.Node, dag *PipelineDAG, ctx *CompileContext) FragmentRef {
    switch node.NodeType {
    case plan.Node_TABLE_SCAN:
        frag := dag.NewFragment()
        frag.Source = compileScanSource(node, ctx)
        frag.Ops = compileFilterProject(node)
        return FragmentRef{frag: frag, needsSink: true}
        
    case plan.Node_JOIN:
        // Build side: separate fragment → HashBuildBarrier.
        buildRef := compileNode(node.Children[0], dag, ctx)
        barrier := &HashBuildBarrier{
            nWorkers:   ctx.Workers,
            keyColumns: extractJoinKeys(node, 0),
        }
        buildRef.frag.Sink = barrier
        dag.AddBarrier(barrier)
        
        // Probe side: separate fragment, depends on build completion.
        probeRef := compileNode(node.Children[1], dag, ctx)
        probeFrag := dag.NewFragment()
        probeFrag.Source = probeRef.frag.Source
        probeFrag.Ops = append(probeRef.frag.Ops, &HashProbeOp{
            keyColumns: extractJoinKeys(node, 1),
            joinType:   node.JoinType,
        })
        probeFrag.DependsOnBarrier = barrier
        return FragmentRef{frag: probeFrag, needsSink: true}
        
    case plan.Node_AGG:
        childRef := compileNode(node.Children[0], dag, ctx)
        barrier := &AggBarrier{
            nWorkers:   ctx.Workers,
            groupExprs: node.GroupBy,
            aggDescs:   compileAggDescs(node.AggList),
        }
        childRef.frag.Sink = barrier
        dag.AddBarrier(barrier)
        
        outFrag := dag.NewFragment()
        outFrag.DependsOnBarrier = barrier
        outFrag.Ops = compileFilterProject(node) // HAVING, final projection
        return FragmentRef{frag: outFrag, needsSink: true}
        
    case plan.Node_SORT:
        childRef := compileNode(node.Children[0], dag, ctx)
        barrier := &SortBarrier{
            nWorkers:   ctx.Workers,
            orderExprs: node.OrderBy,
            limit:      extractLimit(node),
        }
        childRef.frag.Sink = barrier
        dag.AddBarrier(barrier)
        
        outFrag := dag.NewFragment()
        outFrag.DependsOnBarrier = barrier
        return FragmentRef{frag: outFrag, needsSink: true}
        
    case plan.Node_PROJECT, plan.Node_FILTER:
        childRef := compileNode(node.Children[0], dag, ctx)
        childRef.frag.Ops = append(childRef.frag.Ops, compilePushOp(node))
        return childRef
    }
    
    panic("unhandled node type")
}
```

## 9. Distributed Execution

### 9.1 Two-Phase Pattern

```
                        CN-0 (coordinator)
                ┌────────────────────────────────┐
                │  ExchangeSource (from CN-1,2)  │
                │         │                      │
                │    MergeAgg Fragment            │
                │         │                      │
                │    Result Sink                  │
                └────────────────────────────────┘
                        ▲         ▲
              partial aggs    partial aggs
                        │         │
    ┌───────────────────┘         └───────────────────┐
    │ CN-1                               CN-2         │
    │ ┌──────────────────────┐   ┌──────────────────────┐
    │ │ ScanSource (morsel)  │   │ ScanSource (morsel)  │
    │ │     │                │   │     │                │
    │ │ Filter/Project       │   │ Filter/Project       │
    │ │     │                │   │     │                │
    │ │ AggBarrier (local)   │   │ AggBarrier (local)   │
    │ │     │                │   │     │                │
    │ │ ExchangeSink(hash)   │   │ ExchangeSink(hash)   │
    │ └──────────────────────┘   └──────────────────────┘
    └─────────────────────────────────────────────────────┘
```

### 9.2 Remote Fragment Placement Decision

```go
func DecideDistribution(node *plan.Node, stats *TableStats) ExchangeMode {
    buildRows := stats.EstimatedRows(node.Children[0])
    probeRows := stats.EstimatedRows(node.Children[1])
    
    if buildRows < BroadcastThreshold {
        // Small build: broadcast to all CNs. Pure morsel-parallel probe.
        return ExchangeBroadcast
    }
    
    if buildRows > probeRows/10 {
        // Large both sides: must repartition by key.
        return ExchangeHash
    }
    
    perCNMemory := stats.AvailableMemPerCN()
    buildSize := stats.EstimatedSize(node.Children[0])
    if buildSize < perCNMemory/4 {
        return ExchangeBroadcast
    }
    return ExchangeHash
}

const BroadcastThreshold = 1_000_000 // rows
```

## 10. Cancellation & Error Propagation

```
Worker encounters error → cancels query context → all workers drain.
No partial results escape. Clean shutdown guaranteed.

Each worker's loop checks ctx.Done() between morsels.
Within a morsel (single operator chain invocation), errors return immediately.
No goroutine leak: workers are bounded (= GOMAXPROCS), not per-morsel.

Barrier finalization errors also cancel the query.
```

## 11. Comparison: Current vs Morsel

```
Current: SELECT sum(l_extendedprice) FROM lineitem WHERE l_shipdate < '1995-01-01'

  Scope 0: [blocks 0-233]   ──volcano──▶ MergeGroup ──▶ Result
  Scope 1: [blocks 234-467]  ──volcano──▶ ╱
  Scope 2: [blocks 468-701]  ──volcano──▶╱
  
  - 3 goroutines, each with full operator stack
  - Scope 2 takes 3x longer (hot range) → 2 goroutines idle
  - Channel sync at MergeGroup
  - Total: 3 goroutines + 3 channels + 1 merge goroutine = 7 goroutines

Morsel:

  Fragment 0: ScanSource ──▶ Filter ──▶ Project ──▶ AggBarrier
  Fragment 1: (agg results) ──▶ Result Sink
  
  Worker 0: pulls morsel, push filter→project→agg, pulls next morsel...
  Worker 1: pulls morsel, push filter→project→agg, pulls next morsel...
  Worker 2: pulls morsel, push filter→project→agg, pulls next morsel...
  
  - 3 OS threads (no goroutine creation/destruction)
  - Self-balancing: workers that get skip-heavy morsels immediately pull more
  - No channel: agg barrier is per-worker arrays + final merge
  - Total: 3 fixed threads + 0 channels = minimal scheduling overhead
```

## 12. Relationship to Shuffle

Morsel-driven execution **eliminates intra-CN shuffle entirely**:

- Intra-CN GROUP BY: each worker accumulates all keys locally → merge
- Intra-CN hash join: workers build locally, probe shared read-only table
- No data copying between goroutines within the same CN

Inter-CN exchange remains but shrinks:
- Exchange partial aggregates instead of raw rows
- Small build sides: broadcast instead of shuffle
- Only large-both-sides joins still need full hash repartition

## 13. Implementation Phases

| Phase | Scope | Effort |
|-------|-------|--------|
| **0a** | Shared `ScanSource` for existing scope goroutines | ~300 LOC |
| **0b** | Adaptive morsel sizing + S3 prefetcher | ~500 LOC |
| **1** | Worker pool + push operators for scan+filter+agg (no join) | ~2000 LOC |
| **2** | Hash join barrier (partitioned build + concurrent probe) | ~1500 LOC |
| **3** | Sort barrier + spill integration | ~1000 LOC |
| **4** | Exchange integration (ExchangeSink/Source replace Dispatch) | ~1000 LOC |
| **5** | Full DAG compiler, deprecate Scope tree | ~3000 LOC |

Each phase is independently shippable and benchmarkable.
Phase 0a alone gives the self-balancing scan benefit with minimal risk.
