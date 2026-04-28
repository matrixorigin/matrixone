# Scaling 88 Million Vectors on Modest Hardware: How MatrixOne Leverages cuVS for Extreme IVF Performance

As AI applications proliferate, efficient vector search at scale has moved from a "nice-to-have" to a core database requirement. At MatrixOrigin, we recently faced a significant engineering challenge: **How do we build and search an IVF index over tens of millions of high-dimensional vectors without renting an entire data center?**

For our benchmarks we used NVIDIA's open **`wiki_all` dataset** (768-dimensional vectors derived from Wikipedia passage embeddings), scaling up to **88 million vectors**. Traditional CPU-based approaches were hitting a wall: index builds took hours — sometimes days — and search latency was inconsistent under concurrency. By integrating NVIDIA's **cuVS** and **RAFT** libraries into our architecture, we transformed our performance profile. Here is the step-by-step story of how we did it, and the head-to-head numbers that prove it.

## The Challenge: The "Giant Index" Problem

Our target was an IVF index with thousands of clusters holding tens of millions of 768-D vectors. On modest CPU hardware we encountered three primary bottlenecks:

1. **Clustering Latency**: Standard K-Means was slow and often produced unbalanced clusters, leading to "hotspots" that slowed down search.
2. **Assignment Overhead**: Mapping 50M+ vectors to their nearest centroids is computationally expensive. On CPUs, this task competed for resources with data loading and decompression, dragging the process out to a full day.
3. **The GPU "Single Query" Trap**: Databases typically process one query at a time. GPUs, however, only show their true strength when processing large batches.

## Step 1: Solving Clustering with Balanced K-Means

Standard K-Means often results in some clusters having thousands of vectors while others have almost none. In an IVF index, this leads to unpredictable IO and search times.

We initially implemented our own balanced K-Means, which brought clustering time down from 30 minutes to 5 minutes. By switching to the **cuVS Balanced K-Means algorithm**, we tapped into full GPU parallelism.

* **Result**: Clustering time dropped from **5 minutes to just 5 seconds**.

## Step 2: Offloading Assignment to Brute-Force GPU Kernels

Once the centroids are defined, every vector must be assigned to its closest cluster. Doing this on a 16-core CPU is a nightmare of cache misses and thread contention.

By using the **cuVS Brute-Force index** to offload distance computation to the GPU, we eliminated the CPU bottleneck entirely.

* **Result**: The assignment phase dropped from **24 hours to 30 minutes**.

## Step 3: The Architecture — `cuvs_worker_t` and Dynamic Batching

To solve the "Single Query" problem, we designed a bridge between Go and CUDA: the `cuvs_worker_t`.

### Dynamic Batching: The Secret Sauce

Instead of launching a new CUDA kernel for every incoming request, our worker implements **Dynamic Batching**. It holds incoming queries for a tiny microsecond window, consolidates them into a single matrix, and executes one large GPU search.

* This maximizes warp utilization and reduces kernel launch overhead.
* **Performance Gain**: Provides a **5x–10x throughput boost** in high-concurrency environments.

### RAFT Resource Management

We leverage the **RAFT** library to manage long-lived `raft::resources`. By caching CUDA streams and handles within persistent C++ threads, we ensure that our Go-based engine can interact with the GPU with near-zero per-request initialization overhead.

## Step 4: Staying Within Memory Budget with Auto-Quantization

50M+ 768-D vectors in `float32` require well over 100 GB — far exceeding a typical RAM budget. To solve this, we implemented **Automatic Type Quantization** directly on the GPU using the cuVS quantization library.

* **FP16 (Half Precision)**: Reduces memory by 2x with almost zero recall loss.
* **8-Bit Integer (int8/uint8)**: Uses a learned Scalar Quantizer to compress vectors by 4x.
* Because conversion happens on the GPU, we avoid taxing the CPU and minimize PCIe bus traffic.

## Step 5: Pushing SQL Predicates Down — Filtered Search Inside cuVS

A vector index is rarely queried in isolation. Real workloads look like *"find the top-10 nearest passages **where `file_id = X`**"*. The naive approach — search first, filter later — wastes GPU cycles ranking candidates that the optimizer is about to throw away, and forces deep `nprobe` sweeps just to backfill `top-k` after filtering.

cuVS supports **pre-filtering via a predicate bitset**, and we wired it directly into the MatrixOne query pipeline:

1. We keep the **filter columns resident in RAM** (e.g., `file_id`, score columns referenced by predicates), avoiding per-query disk reads.
2. The SQL planner extracts the predicate (e.g., `file_id = 20000007`) and the **CPU computes a packed bitset** in RAM — 1 bit per indexed vector — by scanning the in-RAM column. This is dramatically cheaper than the alternative of file-based filtering, where each query would re-read the column from storage.
3. The bitset is handed to cuVS, which consults it during graph traversal / list scanning so the GPU skips disqualified vectors before distance computation.
4. The CAGRA / IVF-PQ kernel returns only `top-k` results that already satisfy the predicate — no post-hoc reranking pass.

The bitset itself stays in host RAM; only the index data lives on the GPU. The combination — RAM-resident filter columns plus CPU-computed bitsets driving GPU pre-filtering — sidesteps both disk I/O and wasted GPU work. The 88M numbers below show the payoff concretely: under SQL pre-filtering, GPU-enhanced IVF-Flat (which has to filter on the CPU *after* search) drops to **~3 QPS at high recall (0.97)** and tops out at ~12 QPS at lower recall, while pure-GPU IVF-PQ with bitset pre-filtering holds **~67 QPS** essentially flat across `nprobe`.

## Head-to-Head: CPU IVF-Flat vs. GPU-Enhanced IVF-Flat vs. Pure-GPU IVF-PQ

To quantify the value of pushing *both build and search* onto the GPU (IVF-PQ) versus only accelerating the build pipeline (IVF-Flat with CPU-side search), we benchmarked both on AWS `g6e` instances using NVIDIA L40S GPUs across three scales of the `wiki_all` dataset (1M, 10M, 88M @ 768-D, top-10, concurrency = 100, n = 10000 queries).

### Build Time

| Dataset | IVF-Flat (CPU build) | IVF-Flat (GPU build, CPU search) | IVF-PQ (Pure GPU) | Speedup |
|---|---|---|---|---|
| 1M | 58 s | 29 s | 45 s | 0.6x |
| 10M | 19 min | 4 min 26s | 4 min 21 s | 4.2x |
| **88M** | **4 h 8 min** | **62 min** | **32 min 8 s** | **~7.7x** |

At small scale, IVF-Flat's simpler build wins. At **88M vectors, IVF-PQ builds nearly 8× faster** — turning an overnight job into a coffee break.

### Search Throughput (no filter, top-10)

| Dataset | Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|---|
| 1M | 5 | 0.86 | 415 | 0.84 | **884** |
| 1M | 20 | 0.97 | 411 | 0.84 | 781 |
| 1M | 100 | 0.99 | 384 | 0.84 | 755 |
| 10M | 5 | 0.75 | 200 | 0.84 | **837** |
| 10M | 20 | 0.91 | 71 | 0.84 | 713 |
| 10M | 100 | 0.98 | 28 | 0.84 | 661 |
| 88M | 5 | 0.70 | 22 | 0.87 | **278** |
| 88M | 20 | 0.91 | 10 | 0.87 | 233 |
| 88M | 100 | 0.96 | 4 | 0.87 | 230 |

### Search Throughput Under SQL Pre-Filter (88M, top-10)

| Nprobe | IVF-Flat Recall | IVF-Flat QPS | IVF-PQ Recall | IVF-PQ QPS |
|---|---|---|---|---|
| 5 | 0.61 | 11.9 | 0.86 | **66.9** |
| 20 | 0.82 | 12 | 0.86 | 65.4 |
| 100 | 0.97 | 3 | 0.86 | 66.4 |

This is the bitset pre-filtering payoff: IVF-PQ stays flat at ~67 QPS across `nprobe`, while IVF-Flat must push `nprobe` up to recover the recall it loses to post-search filtering — and pays for it. At the high-recall setting (`nprobe=100`, recall 0.97), pure-GPU IVF-PQ delivers **~22× higher QPS** because the GPU never spends a cycle on rows the SQL predicate already rejected.

### What the Numbers Tell Us

* **At 88M vectors, pure-GPU IVF-PQ is ~12× faster than GPU-enhanced IVF-Flat** at comparable recall, and the gap widens dramatically as `nprobe` grows — at `nprobe=100` it reaches **~57×** — because each additional probe sends IVF-Flat further into uncached cluster pages on disk (see cache-miss analysis below).
* **Recall stability**: IVF-PQ recall is largely invariant to `nprobe`, while IVF-Flat needs aggressive `nprobe` (and therefore more CPU work) to reach high recall. That makes IVF-PQ much easier to tune for production SLOs.
* **Filtered queries are where IVF-Flat collapses**: with bitset pre-filtering inside cuVS, IVF-PQ keeps throughput essentially flat under predicates (~67 QPS regardless of `nprobe`); IVF-Flat's CPU-side filter pass forces the index deeper to refill `top-k`, dragging QPS from ~12 down to ~3 as `nprobe` climbs from 5 to 100.
* **Why IVF-Flat's 88M numbers are so low — memory cache misses**: at 768-D `float32`, 88M vectors are **~270 GB** of raw vector data. The host has 512 GB RAM, but after the OS and the database engine itself, only **~256 GB is actually free for the data cache** — so the working set doesn't fit. As `nprobe` grows, IVF-Flat touches more cluster lists per query, the cache miss rate climbs, and search degrades from a memory-bound workload into a **disk-IO-bound** one — which is why QPS drops from 22 → 10 → 4 (no filter) and 12 → 3 (filtered) as `nprobe` goes from 5 → 100. IVF-PQ avoids this entirely: with `M=192, bits=8`, the 88M index is ~17 GB compressed and fits comfortably across 8 sharded GPUs at ~3.5 GB VRAM each — every probe is served from on-device memory, never the disk.
* **IVF-Flat still wins for small datasets and recall-critical workloads** (e.g., 1M with `nprobe=100` reaches 0.99 recall). IVF-PQ trades ~10–15 points of recall for an order of magnitude of throughput at scale.

### Setup

| | IVF-PQ (1M / 10M) | IVF-PQ (88M) | IVF-Flat (88M) |
|---|---|---|---|
| Lists | 1000 / 4096 | 6000 | 10000 |
| PQ Params | BITS_PER_CODE 8, M 192 | BITS_PER_CODE 8, M 192 | — |
| Quantization | f32 / f16 | f16 | f32 |
| GPU | 1× L40S (48 GB) | 8× L40S (sharded) | 1× L40S (48 GB) |
| Instance | `g6e.12xlarge` | `g6e.48xlarge` | `g6e.16xlarge` |
| Host RAM / usable DB cache | — | — | 512 GB / ~256 GB |

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

The benchmark on `wiki_all` is unambiguous: at 88M vectors, **pure-GPU IVF-PQ delivers ~8× faster builds, ~12–57× higher unfiltered QPS (depending on `nprobe`), and up to ~22× higher filtered QPS at high recall** than GPU-assisted IVF-Flat with CPU search — at recall levels production workloads can ship with. Combined with `cuvs_worker_t`, dynamic batching, and bitset pre-filtering, this is not just a "fast index" but a **production-ready database engine** that scales with the demands of modern AI.
