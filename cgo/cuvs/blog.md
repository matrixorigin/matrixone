# Scaling 88 Million Vectors on Modest Hardware: How MatrixOne Leverages cuVS for Extreme IVF Performance

As AI applications proliferate, efficient vector search at scale has moved from a "nice-to-have" to a core database requirement. At MatrixOrigin, we recently faced a significant engineering challenge: **How do we build and search an IVF index over tens of millions of high-dimensional vectors without renting an entire data center?**

For our benchmarks we used NVIDIA's open **`wiki_all` dataset** (768-dimensional vectors derived from Wikipedia passage embeddings), scaling up to **88 million vectors**. Traditional CPU-based approaches were hitting a wall: index builds took hours — sometimes days — and search latency was inconsistent under concurrency. By integrating NVIDIA's **cuVS** and **RAFT** libraries into our architecture, we transformed our performance profile. Here is the step-by-step story of how we did it, and the head-to-head numbers that prove it.

## The Challenge: The "Giant Index" Problem

Our target was an IVF index with thousands of clusters holding tens of millions of 768-D vectors. On modest CPU hardware we encountered three primary bottlenecks:

1. **Clustering Latency**: Standard K-Means was slow and often produced unbalanced clusters, leading to "hotspots" that slowed down search.
2. **Assignment Overhead**: Mapping 50M+ vectors to their nearest centroids is computationally expensive. On CPUs, this task competed for resources with data loading and decompression, dragging the process out to a full day.
3. **Filtered Search Penalty**: Real SQL workloads rarely query a vector index in isolation — they look like *"top-20 nearest passages **where `file_attribute = X`**"*. The straightforward implementation is **file-based filtering**: for every incoming query, re-read the filter columns from object storage to evaluate the predicate. At tens of millions of rows, that turns each query into a storage-bound job — disk/network I/O dominates and GPU search throughput collapses long before the index itself is the bottleneck. Compounding this, the "search first, filter later" pattern wastes GPU cycles ranking rows the predicate will discard and forces deeper `nprobe` sweeps to refill `top-k` after filtering.

## Hardware & Methodology

Before walking through the engineering, here is the explicit setup so the numbers later in the post have context. The headline is that this is **modest, off-the-shelf hardware** — one or eight commodity GPUs on stock AWS instances, not a custom training cluster.

**Hardware (all benchmarks run on AWS `g6e`, NVIDIA L40S):**

| | IVF-Flat (all scales) | IVF-PQ (1M, 10M) | IVF-PQ (88M) |
|---|---|---|---|
| AWS instance | `g6e.16xlarge` | `g6e.16xlarge` | `g6e.48xlarge` |
| vCPU / host RAM | 64 vCPU / 512 GB | 64 vCPU / 512 GB | 192 vCPU / 1536 GB |
| GPU | 1× L40S — *build only* | 1× L40S (48 GB) | 8× L40S (sharded) |
| Search runs on | **CPU** | **GPU** | **GPU** |

For every IVF-Flat search number we report, **search runs on the CPU** even when the index was *built* on the GPU — that is the apples-to-apples "CPU search" baseline against which the GPU IVF-PQ numbers should be read. IVF-PQ runs **end-to-end on the GPU**.

## Step 1: Solving Clustering with Balanced K-Means

Standard K-Means often results in some clusters having thousands of vectors while others have almost none. In an IVF index, this leads to unpredictable IO and search times.

We initially implemented our own balanced K-Means, which brought clustering time down from 30 minutes to 5 minutes. By switching to the **cuVS Balanced K-Means algorithm**, we tapped into full GPU parallelism.

* **Result**: Clustering time dropped from **5 minutes to just 5 seconds**.

## Step 2: Offloading Assignment to Brute-Force GPU Kernels

Once the centroids are defined, every vector must be assigned to its closest cluster. Doing this on a 16-core CPU is a nightmare of cache misses and thread contention.

By using the **cuVS Brute-Force index** to offload distance computation to the GPU, we eliminated the CPU bottleneck entirely.

* **Result**: The assignment phase dropped from **24 hours to 30 minutes**.

## Step 3: The Architecture — `cuvs_worker_t`

To bridge Go and CUDA cleanly, we built the `cuvs_worker_t`: a persistent C++ worker that owns long-lived GPU resources on behalf of the Go-based engine.

### RAFT Resource Management

We leverage the **RAFT** library to manage long-lived `raft::resources`. By caching CUDA streams and handles within persistent C++ threads, we ensure that our Go-based engine can interact with the GPU with near-zero per-request initialization overhead.

## Step 4: Staying Within Memory Budget with Auto-Quantization

50M+ 768-D vectors in `float32` require well over 100 GB — far exceeding a typical RAM budget. To solve this, we implemented **Automatic Type Quantization** directly on the GPU using the cuVS quantization library.

* **FP16 (Half Precision)**: Reduces memory by 2x with almost zero recall loss.
* **8-Bit Integer (int8/uint8)**: Uses a learned Scalar Quantizer to compress vectors by 4x.
* Because conversion happens on the GPU, we avoid taxing the CPU and minimize PCIe bus traffic.

## Step 5: Pushing SQL Predicates Down — Filtered Search Inside cuVS

A vector index is rarely queried in isolation. Real workloads look like *"find the top-20 nearest passages **where `file_attribute = X`**"*. The naive approach — search first, filter later — wastes GPU cycles ranking candidates that the optimizer is about to throw away, and forces deep `nprobe` sweeps just to backfill `top-k` after filtering.

cuVS supports **pre-filtering via a predicate bitset**, and we wired it directly into the MatrixOne query pipeline:

1. We keep the **filter columns resident in RAM** (e.g., `file_attribute`, score columns referenced by predicates), avoiding per-query disk reads.
2. The SQL planner extracts the predicate (e.g., `file_attribute = 20000007`) and the **CPU computes a packed bitset** in RAM — 1 bit per indexed vector — by scanning the in-RAM column. This is dramatically cheaper than the alternative of file-based filtering, where each query would re-read the column from storage.
3. The bitset is handed to cuVS, which consults it during graph traversal / list scanning so the GPU skips disqualified vectors before distance computation.
4. The CAGRA / IVF-PQ kernel returns only `top-k` results that already satisfy the predicate — no post-hoc reranking pass.

The bitset itself stays in host RAM; only the index data lives on the GPU. The combination — RAM-resident filter columns plus CPU-computed bitsets driving GPU pre-filtering — sidesteps both disk I/O and wasted GPU work. The 88M numbers below show the payoff concretely: with bitset pre-filtering, **IVF-PQ is the only index in the sweep that holds recall ≥ 0.80** under the predicate (0.81 @ 80 QPS), while IVF-Flat trades recall for throughput (peaking at 273 QPS at recall 0.59) but cannot reach the 0.80 bar at any tested `nprobe`.

## Head-to-Head: CPU IVF-Flat vs. Pure-GPU IVF-PQ

To quantify the value of pushing *both build and search* onto the GPU (IVF-PQ) versus only accelerating the build pipeline (IVF-Flat with CPU-side search), we benchmarked both on AWS `g6e` instances using NVIDIA L40S GPUs across three scales of the `wiki_all` dataset (1M, 10M, 88M @ 768-D, top-20, concurrency = 100, n = 10000 queries).

### Parameter Tuning: How We Chose `nprobe` and `pq_bits`

Before showing the head-to-head numbers, it's worth explaining how the parameters in those tables were picked. Two questions need answers for each index family: *how do we pick `nprobe`*, and (for IVF-PQ) *how aggressive can the quantization be*? We tuned on the 10M slice — large enough to be representative, cheap enough to sweep — targeting **recall ≈ 0.80 @ top-20**, then validated the chosen setting at 88M.

#### IVF-PQ: `pq_bits = 8`, `nprobe = 16`

We ran a Pareto sweep over `nprobe ∈ {1, 8, 16, 32, 64, 128, 256}`:

![10M IVF-PQ: Recall vs. Latency across nprobe](pareto_10m.png)

The curve shows a classic IVF knee: recall climbs steeply until `nprobe = 16` (0.79), then flattens — beyond that, each doubling of `nprobe` adds at most ~1 point of recall but latency starts to drift up. **`nprobe = 16` is the Pareto-optimal point** for our 0.80 recall target.

Next, can more aggressive PQ compression hold that target? We swept `pq_bits ∈ {8, 7, 6}` at the same `nprobe` ladder (10M, top-20, concurrency=100, n=10000):

| `nprobe` | `pq_bits=8` Recall | `pq_bits=7` Recall | `pq_bits=6` Recall |
|---|---|---|---|
| 1   | 0.39 | 0.38 | 0.37 |
| 8   | 0.74 | 0.71 | 0.68 |
| **16**  | **0.79** | 0.76 | 0.72 |
| 32  | 0.82 | 0.79 | 0.74 |
| 64  | 0.83 | 0.80 | 0.75 |
| 128 | 0.84 | 0.81 | 0.76 |
| 256 | 0.84 | 0.81 | 0.76 |

Only `pq_bits = 8` clears 0.80 at `nprobe = 16`. `pq_bits = 7` needs `nprobe ≥ 64` to get there (4× more probes for the same recall), and `pq_bits = 6` never reaches 0.80 in this sweep — its asymptotic ceiling is ~0.76. Since dropping from 8 → 7 bits saves only ~12% on stored vector bytes, trading recall headroom for a fractional storage win is the wrong call.

We then validated at 88M:

![88M IVF-PQ: Recall vs. Latency across nprobe](pareto_88m.png)

The 88M curve shows the same knee: recall hits 0.83 at `nprobe = 16` (~125 ms), and only creeps to 0.88 by `nprobe = 256` — but latency triples to ~380 ms once `nprobe ≥ 32`, where the per-probe cost stops fitting in the device-memory working set. The 10M-tuned setting (`pq_bits = 8, nprobe = 16`) holds at scale, which is the whole point of doing the Pareto on the smaller dataset.

#### IVF-Flat: `lists = 10000`, `nprobe = 8` to match the recall target

IVF-Flat has no quantization knob — vectors are stored uncompressed in `float32` — so the only tunables are cluster count (`lists`) and `nprobe`. We set `lists = 10000` for the 88M index (≈ √N, the standard heuristic) and tuned `nprobe` to hit the same recall target as IVF-PQ (~0.8 @ top-20). On the 10M slice this lands at **`nprobe = 8`** (recall 0.82) — half the probes IVF-PQ needs for the same recall, since IVF-Flat keeps full-precision vectors. We use the same setting at 88M and report two higher points to span the curve:

| `nprobe` | Recall@20 | QPS |
|---|---|---|
| **8**   | **0.77** | **188** |
| 16  | 0.89 | 129 |
| 32  | 0.92 | 114 |

(88M `wiki_all`, no filter, top-20, concurrency=100, n=10000.)

Unlike IVF-PQ there is no flat region: every additional probe pulls more uncompressed `float32` cluster pages into the CPU search loop — served either from the in-RAM cache or the local SSD (~2 GB/s) — and QPS drops monotonically as `nprobe` grows. At 88M the recall target slips slightly: `nprobe = 8` matches IVF-PQ at smaller scales but only reaches 0.77 recall here, because each cluster gets fewer probes relative to the index size. Pushing `nprobe` higher recovers recall, but throughput keeps falling — there is no sweet spot, only a recall-vs-throughput dial. The head-to-head below uses `nprobe ∈ {8, 16, 32}` for IVF-Flat to span the full curve.

### Build Time

| Dataset | IVF-Flat (CPU build) | IVF-Flat (GPU build) | IVF-PQ (GPU build) | Speedup (CPU vs IVF-PQ) |
|---|---|---|---|---|
| 1M | 36 s | 15 s | 47 s | 0.8× |
| 10M | 1 min 32 s | 2 min 12 s | 7 min 32 s | 0.2× |
| **88M** | **6 h 23 min** | **20 min** | **50 min** | **~7.7×** |

At 1M and 10M, the CPU build is competitive (or faster) — IVF-PQ pays a fixed PQ-codebook training cost that only amortizes once the dataset is large. At **88M vectors the picture inverts decisively: GPU IVF-PQ builds ~7–8× faster than CPU IVF-Flat, and GPU-built IVF-Flat is ~19× faster** — turning an overnight job into a coffee break.

### Search Throughput (no filter, top-20)

**Recall-matched headline** — IVF-Flat `nprobe=8` vs IVF-PQ `nprobe=16`, both tuned to land near the same recall target (~0.8 on the 10M slice):

| Dataset | IVF-Flat (`nprobe=8`) | IVF-PQ (`nprobe=16`) | IVF-PQ vs IVF-Flat |
|---|---|---|---|
| 1M | 768 QPS, recall 0.86 | 904 QPS, recall 0.82 | 1.2× |
| 10M | 645 QPS, recall 0.82 | 1066 QPS, recall 0.79 | 1.7× |
| **88M** | **188 QPS, recall 0.77** | **759 QPS, recall 0.83** | **~4.0×** |

**Full `nprobe` sweep** — same setup, each index across `nprobe ∈ {8, 16, 32}`:

| Dataset | Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|---|
| 1M | 8 | 0.86 | 768 | 0.78 | 1060 |
| 1M | 16 | 0.93 | 860 | 0.82 | 904 |
| 1M | 32 | 0.97 | 705 | 0.84 | 889 |
| 10M | 8 | 0.82 | 645 | 0.74 | 1099 |
| 10M | 16 | 0.90 | 461 | 0.79 | 1066 |
| 10M | 32 | 0.95 | 313 | 0.82 | 756 |
| 88M | 8 | 0.77 | 188 | 0.79 | 776 |
| 88M | 16 | 0.89 | 129 | 0.83 | 759 |
| 88M | 32 | 0.92 | 114 | 0.85 | 260 |

### Search Throughput Under SQL Pre-Filter (top-20)

All three datasets, `file_attribute = 20000007` evaluated as a bitset before distance computation, top-20, concurrency=100, n=10000.

**1M:**

| Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|
| 8 | 0.68 | **1220** | 0.70 | 864 |
| 16 | 0.76 | 1033 | 0.78 | 684 |
| 32 | 0.82 | 779 | **0.83** | 649 |

**10M:**

| Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|
| 8 | 0.64 | **853** | 0.76 | 341 |
| 16 | 0.74 | 532 | 0.74 | 356 |
| 32 | **0.80** | 230 | **0.80** | 330 |

**88M:**

| Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|
| 8 | 0.59 | **273** | 0.69 | 97 |
| 16 | 0.68 | 170 | 0.77 | 98 |
| 32 | 0.75 | 111 | **0.81** | 80 |

The trade-off is scale-dependent:

* **At 1M**, the dataset is small enough that the CPU IVF-Flat search path is plenty fast — IVF-Flat dominates on raw QPS (1220 vs 864 at `nprobe=8`) while staying within ~2 points of IVF-PQ on recall. If your dataset is small, IVF-Flat is the clear pick under filters.
* **At 10M**, the lines cross. IVF-Flat is faster at low `nprobe` but cannot reach recall 0.80 without going to `nprobe=32`, where IVF-PQ catches and overtakes it (330 vs 230 QPS at matched recall 0.80).
* **At 88M**, the trade-off shifts: IVF-Flat is faster per probe (273 → 111 QPS) because the predicate cuts the number of `float32` vectors the CPU has to touch, but it cannot reach 0.80 recall in the tested sweep. IVF-PQ holds **0.69 → 0.77 → 0.81 recall at a steady ~80–98 QPS** and is the only index that clears the recall bar.

So the recommendation under filters: **IVF-Flat below ~10M, IVF-PQ above** — or wherever the workload demands recall ≥ 0.80.

### What the Numbers Tell Us

* **At 88M vectors and recall ~0.8, pure-GPU IVF-PQ is ~4× faster than CPU IVF-Flat** at the recall-matched setting (759 QPS @ `nprobe=16`, recall 0.83 vs 188 QPS @ `nprobe=8`, recall 0.77). The gap widens as IVF-Flat is pushed for higher recall — IVF-PQ stays at ~750 QPS while IVF-Flat falls to 129 QPS at `nprobe=16` (~5.9×) and 114 QPS at `nprobe=32` (~2.3× before IVF-PQ also drops at `nprobe=32` once it falls off its flat region).
* **Recall stability**: IVF-PQ recall climbs gently with `nprobe` (0.79 → 0.85 from 8 to 32 at 88M), while IVF-Flat moves through a wider band (0.77 → 0.92) but pays a ~40% QPS penalty to reach the top of it. IVF-PQ is the easier dial to set for production SLOs because it holds throughput nearly flat across a 4× range of `nprobe`.
* **Filtered queries: scale flips the answer.** With bitset pre-filtering inside cuVS, IVF-Flat dominates QPS at 1M and 10M (1220 / 853 QPS at `nprobe=8`) and only narrowly trails IVF-PQ on recall there. By 88M the lines invert: IVF-Flat is still faster per probe (273 → 111 QPS) but cannot reach 0.80 recall in the tested sweep, while IVF-PQ holds 0.69 → 0.77 → 0.81 recall at a steady ~80–98 QPS. **Below ~10M, IVF-Flat is the right filtered index; above, IVF-PQ is the only one that clears recall ≥ 0.80.**
* **Why IVF-Flat's 88M ceiling sits where it does — uncompressed footprint**: at 768-D `float32`, 88M vectors are **~270 GB** of raw vector data. The CPU search path serves cluster pages from the in-RAM cache when they're hot and from the local SSD (~2 GB/s) when they're not, so as `nprobe` grows the per-query bytes touched scale linearly and throughput falls. IVF-PQ sidesteps this entirely: with `M=192, bits=8`, the 88M index is ~17 GB compressed and fits comfortably across 8 sharded GPUs at ~3.5 GB VRAM each — every probe is served from on-device memory at GPU bandwidth.
* **IVF-Flat still wins for small datasets and recall-critical workloads** (e.g., 1M with `nprobe=32` reaches 0.97 recall at 705 QPS, vs IVF-PQ's 0.84 at 889 QPS). IVF-PQ trades a few points of recall for steady throughput at scale; IVF-Flat trades steady throughput for the option of pushing recall to the limit.

### Setup

| | IVF-PQ (1M / 10M) | IVF-PQ (88M) | IVF-Flat (88M) |
|---|---|---|---|
| Lists | 1000 / 4096 | 6000 | 10000 |
| PQ Params | BITS_PER_CODE 8, M 192 | BITS_PER_CODE 8, M 192 | — |
| Quantization | f32 / f16 | f16 | f32 |
| GPU | 1× L40S (48 GB) — *build + search* | 8× L40S (sharded) — *build + search* | 1× L40S (48 GB) — *build only* |
| Instance | `g6e.16xlarge` | `g6e.48xlarge` | `g6e.16xlarge` |
| Host RAM | 512 GB | 1536 GB | 512 GB |
| Storage | local SSD (~2 GB/s read) | local SSD (~2 GB/s read) | local SSD (~2 GB/s read) |

The 88M IVF-PQ deployment runs **sharded across 8 GPUs at ~3.5 GB VRAM each** — well under the 48 GB per-GPU budget — leaving headroom for concurrent workloads.

**Why L40S and not A10?** Training requires the input vectors to be uploaded as a *single contiguous* GPU allocation. Even at `float16`, 10M × 768-D is ~15 GB for the input alone — and once the training working set (centroids, intermediate buffers, PQ codebook fitting) is layered on top, the allocation exceeds A10's 24 GB VRAM and OOMs, even though the final compressed index would fit comfortably. L40S's 48 GB is large enough to hold the contiguous training input plus working set, which is why it's the smallest GPU we can build on at this scale.

## Supported Indexes

Our architecture now supports a suite of high-performance indexes, each with a clear sweet spot:

* **CAGRA**: Hardware-accelerated graph index for state-of-the-art search latency, with native bitset-pre-filter support.
* **IVF-Flat**: High-accuracy general-purpose search; best when the dataset fits comfortably in memory.
* **IVF-PQ**: For extreme compression and the best throughput-per-dollar at billion-scale, with bitset pre-filtering for SQL predicates.
* **K-Means**: High-speed data partitioning as a primitive for downstream pipelines.

## Conclusion

By shifting clustering, assignment, quantization — *and search, including SQL predicate evaluation* — onto the GPU through cuVS, MatrixOne handles massive vector datasets on surprisingly modest hardware. What once took a full day now takes well under an hour, with search latencies that remain low under heavy concurrency.

The benchmark on `wiki_all` is unambiguous: at 88M vectors, **pure-GPU IVF-PQ builds ~7–8× faster than CPU IVF-Flat, delivers ~4–6× higher unfiltered QPS than CPU IVF-Flat at recall ~0.8, and is the only index that holds recall ≥ 0.80 once SQL pre-filtering is in play at that scale**. Below ~10M, IVF-Flat keeps the throughput crown for both filtered and unfiltered workloads. Combined with `cuvs_worker_t` and bitset pre-filtering, this is not just a "fast index" but a **production-ready database engine** that scales with the demands of modern AI.
