✦ Architecture Design: cuVS-Accelerated Vector Indexing

  1. Overview
  The MatrixOne cuvs package provides a high-performance, GPU-accelerated vector search and clustering infrastructure. It acts as
  a bridge between the Go-based database kernel and NVIDIA's cuVS and RAFT libraries. The architecture is designed to solve three
  primary challenges:
   1. Impedance Mismatch: Reconciling Go’s concurrent goroutine scheduler with CUDA’s thread-specific resource requirements.
   2. Scalability: Supporting datasets that exceed single-GPU memory (Sharding) or high-concurrency search requirements
      (Replicated).
   3. Efficiency: Minimizing CUDA kernel launch overhead via dynamic query batching.

  ---

  2. Core Component: cuvs_worker_t
  The cuvs_worker_t is the foundational engine of the architecture.

  Implementation Details:
   * Persistent C++ Thread Pool: Instead of executing CUDA calls directly from CGO (which could be scheduled on any OS thread),
     the worker maintains a dedicated pool of long-lived C++ threads. Each thread is pinned to a specific GPU device.
   * Job Queuing: Requests from the Go layer are submitted as "Jobs" to an internal thread-safe queue. The worker returns a
     std::future, allowing the Go layer to perform other tasks while the GPU processes the request.
   * Context Stability: By using dedicated threads, we ensure that CUDA context and RAFT resource handles remain stable and
     cached, avoiding the expensive overhead of context creation or handle re-initialization.

  ---

  3. Distribution Modes
  The system supports three distinct modes to leverage multi-GPU hardware:

  A. Single GPU Mode
   * Design: The index resides entirely on one device.
   * Use Case: Small to medium datasets where latency is the priority.

  B. Replicated Mode (Scaling Throughput)
   * Design: The full index is loaded onto multiple GPUs simultaneously.
   * Mechanism: The cuvs_worker implements a load-balancing strategy (typically round-robin). Incoming queries are dispatched to
     the next available GPU.
   * Benefit: Linearly scales the Queries Per Second (QPS) by utilizing the compute power of all available GPUs.

  C. Sharded Mode (Scaling Capacity)
   * Design: The dataset is partitioned into $N$ shards across $N$ GPUs.
   * Mechanism:
       1. Broadcast: A search request is sent to all GPUs.
       2. Local Search: Each GPU searches its local shard independently using RAFT resources.
       3. Top-K Merge: The worker aggregates the results ($N \times K$ candidates) and performs a final merge-sort (often on the
          CPU or via a fast GPU kernel) to return the global top-K.
   * Benefit: Enables indexing of massive datasets (e.g., 100M+ vectors) that would not fit in the memory of a single GPU.

  ---

  4. RAFT Resource Management
  The package relies on RAFT (raft::resources) for all CUDA-accelerated operations.

   * Resource Caching: raft::resources objects (containing CUDA streams, cuBLAS handles, and workspace memory) are held within the
     cuvs_worker threads. They are created once at Start() and reused for the lifetime of the index.
   * Stream-Based Parallelism: Every index operation is executed asynchronously on a RAFT-managed CUDA stream. This allows the
     system to overlap data transfers (Host-to-Device) with kernel execution, maximizing hardware utilization.
   * Memory Layout: Leveraging raft::mdspan and raft::mdarray ensures that memory is handled in a layout-aware manner
     (C-contiguous or Fortran-contiguous), matching the requirements of optimized BLAS and LAPACK kernels.

  ---

  5. Dynamic Batching: The Throughput Key
  In a database environment, queries often arrive one by one from different users. Processing these as individual CUDA kernels is
  inefficient due to launch overhead and under-utilization of GPU warps.

  The Dynamic Batching Mechanism:
   * Aggregation Window: When multiple search requests arrive at the worker within a small time window (microseconds), the worker
     stalls briefly to aggregate them.
   * Matrix Consolidation: Individual query vectors are packed into a single large query matrix.
   * Consolidated Search: A single cuvs::neighbors::search call is made. GPUs are significantly more efficient at processing one
     $64 \times D$ matrix than 64 individual $1 \times D$ vectors.
   * Automatic Fulfilling: Once the batch search completes, the worker de-multiplexes the results and fulfills the specific
     std::future for each individual Go request.

  ---

  6. Currently Supported Indexes
  The architecture is extensible, currently supporting the following index types:


  ┌────────────┬────────────────────────┬────────────────────────────────────────────────────────────────────────┐
  │ Index Type │ Internal Algorithm     │ Primary Strength                                                       │
  ├────────────┼────────────────────────┼────────────────────────────────────────────────────────────────────────┤
  │ CAGRA      │ Graph-based            │ State-of-the-art search speed and high recall. Optimized for GPU graph │
  │            │ (Hardware-accelerated) │ traversal.                                                             │
  │ IVF-Flat   │ Inverted File Index    │ High accuracy, good balance between build time and search speed.       │
  │ IVF-PQ     │ Product Quantization   │ Massive compression. Can store billions of vectors by compressing them │
  │            │                        │ into codes.                                                            │
  │ Brute      │ Exact Flat Search      │ 100% recall. Used for ground-truth verification and small datasets.    │
  │ Force      │                        │                                                                        │
  │ K-Means    │ Clustering             │ High-performance centroid calculation for data partitioning and        │
  │            │                        │ unsupervised learning.                                                 │
  └────────────┴────────────────────────┴────────────────────────────────────────────────────────────────────────┘

  ---

  7. Operational Metadata (Info())
  Every index supports a JSON-formatted Info() method. This provides structured telemetry including:
   * Shared Specs: Element size, vector dimension, distance metric, and current capacity.
   * Topology: The list of active GPU device IDs.
   * Algorithm Specifics: Graph degrees (CAGRA), Number of lists (IVF), or PQ bits (IVF-PQ).
   * Status: Loading state and current vector count.
                                                                      
