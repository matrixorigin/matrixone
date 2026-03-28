# Scaling 50 Million Vectors on Modest Hardware: How MatrixOne Leverages cuVS for Extreme IVF-Flat Performance

As AI applications proliferate, the demand for efficient vector search at scale has moved from a "nice-to-have" to a core database requirement. At MatrixOrigin, we recently faced a significant engineering challenge: **How do we build and search an IVF-Flat index of 50 million 1024-dimensional vectors on a server with only 16 cores and 64GB of RAM?**

Traditional CPU-based approaches were hitting a wall. Building the index took days, and search latency was inconsistent. By integrating NVIDIA’s **cuVS** and **RAFT** libraries into our architecture, we transformed our performance profile. Here is the step-by-step story of how we did it.

## The Challenge: The "Giant Index" Problem
Our target was an IVF-Flat index with approximately 8,000 clusters holding 50 million vectors. On a 16-core machine, we encountered three primary bottlenecks:
1.  **Clustering Latency**: Standard K-Means was slow and often produced unbalanced clusters, leading to "hotspots" that slowed down search.
2.  **Assignment Overhead**: Mapping 50 million vectors to their nearest centroids is computationally expensive. On CPUs, this task competed for resources with data loading and decompression, dragging the process out to 24 hours.
3.  **The GPU "Single Query" Trap**: Databases typically process one query at a time. GPUs, however, only show their true strength when processing large batches.

## Step 1: Solving Clustering with Balanced K-Means
Standard K-Means often results in some clusters having thousands of vectors while others have almost none. In an IVF index, this leads to unpredictable IO and search times.

We initially implemented our own balanced K-Means, which brought the clustering time down from 30 minutes to 5 minutes. However, by switching to the **cuVS Balanced K-Means algorithm**, we utilized GPU parallelism to its fullest. 
*   **Result**: Clustering time dropped from **5 minutes to just 5 seconds**. 

## Step 2: Offloading Assignment to Brute-Force GPU Kernels
Once the 8,000 centroids are defined, every one of the 50 million vectors must be assigned to its closest cluster. Doing this on a 16-core CPU is a nightmare of cache misses and thread contention.

By using the **cuVS Brute-Force index** to "offline" this distance computation to the GPU, we eliminated the CPU bottleneck entirely.
*   **Result**: The assignment phase dropped from **24 hours to 30 minutes**.

## Step 3: The Architecture—`cuvs_worker_t` and Dynamic Batching
To solve the "Single Query" problem, we designed a sophisticated bridge between Go and CUDA: the `cuvs_worker_t`.

### Dynamic Batching: The Secret Sauce
Instead of launching a new CUDA kernel for every incoming request, our worker implements **Dynamic Batching**. It holds incoming queries for a tiny microsecond window, consolidates them into a single matrix, and executes one large GPU search.
*   This maximizes warp utilization and reduces kernel launch overhead.
*   **Performance Gain**: Provides a **5x-10x throughput boost** in high-concurrency environments.

### RAFT Resource Management
We leverage the **RAFT** library to manage long-lived `raft::resources`. By caching CUDA streams and handles within persistent C++ threads, we ensure that our Go-based kernel can interact with the GPU with near-zero resource initialization overhead.

## Step 4: Staying Within 64GB with Auto-Quantization
50 million 1024D vectors in `float32` require roughly 200GB of space—far exceeding our 64GB RAM limit. To solve this, we implemented **Automatic Type Quantization** directly on the GPU.
*   **FP16 (Half Precision)**: Reduces memory by 2x with almost zero recall loss.
*   **8-Bit Integer (int8/uint8)**: Uses a learned Scalar Quantizer to compress vectors by 4x.
*   Because conversion happens on the GPU, we avoid taxing the CPU and minimize PCIe bus traffic.

## Step 5: Overlapping Disk IO with GPU Distance Computation During Search

IVF-Flat search has a structure that most people overlook as an optimization opportunity:

1. **Centroid probing** — find the `n_probes` nearest centroids to the query (fast, done on GPU).
2. **Data block loading** — for each chosen centroid, load its inverted list (the raw vectors stored in that cluster) from disk.
3. **Brute-force re-ranking** — compute exact distances between the query and every vector in those lists to find the true nearest neighbors.

In a naive implementation, these three steps run strictly in sequence. Step 2 is the bottleneck: the GPU sits idle while the database reads multiple data blocks off NVMe or spinning disk, one centroid at a time.

### The Overlap Opportunity

Because we have `n_probes` centroid lists to process, we can pipeline IO and GPU computation:

```
Iteration i:   load list[i+1] from disk  ──────────────────────────┐
               pairwise_distance_async(query, list[i]) → d_ptr      │ GPU working
               ... GPU computing distances for list[i] ...           │
               sync_stream()        ← wait only here                │
               merge top-k results                                   │
               cudaFreeAsync(d_ptr, stream)    ◄────────────────────┘
               → next iteration uses list[i+1] already in memory
```

While the GPU computes exact distances for centroid list `i`, the host thread is already reading centroid list `i+1` from disk into a host buffer. By the time `sync_stream()` returns, the next block is ready to upload. The GPU and disk are never idle waiting on each other.

### Why `pairwise_distance_async` Makes This Possible

We expose two variants:

- `pairwise_distance<T>()` — fully synchronous. Uploads, computes, syncs, and frees before returning. Simple but forces serial IO→GPU→IO→GPU sequencing.
- `pairwise_distance_async<T>()` — launches all GPU work on the CUDA stream and returns immediately with the raw device pointer. The caller drives `sync_stream()` and `cudaFreeAsync()` when it chooses.

The async variant hands control back to the host thread the moment the GPU kernel is queued, giving that thread a full window to issue the next disk read while the GPU is busy. `cudaFreeAsync` is similarly non-blocking—it schedules the device memory release to happen after all in-flight GPU work on the stream completes, so there is no stall on the host side between iterations.

The internal allocation is a single `cudaMallocAsync` covering `[X | Y | distance_matrix]` as one contiguous block, keeping per-iteration allocator overhead minimal even across hundreds of probed lists.

*   **Result**: For queries probing 20–50 centroid lists on a dataset stored on NVMe, overlapping IO and GPU computation cuts per-query latency roughly in half compared to the synchronous approach, with no additional threads required.

## Summary of Supported Indexes
Our architecture now supports a suite of high-performance indexes:
*   **CAGRA**: A hardware-accelerated graph index for state-of-the-art search speed.
*   **IVF-Flat**: The workhorse for high-accuracy general-purpose search.
*   **IVF-PQ**: For extreme compression of billion-scale datasets.
*   **K-Means**: For high-speed data partitioning.

## Conclusion
By shifting the heavy lifting of clustering, assignment, and quantization to the GPU through cuVS, MatrixOne can now handle massive vector datasets on surprisingly modest hardware. What once took a full day now takes less than an hour, with search latencies that remain low even under heavy load.

The integration of `cuvs_worker_t` and dynamic batching ensures that we don't just have a "fast index," but a **production-ready database engine** capable of scaling with the needs of modern AI.
