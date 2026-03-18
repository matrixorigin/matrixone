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

 6. Automatic Type Quantization
  To optimize memory footprint and search speed, the architecture features an automated quantization pipeline that converts
  high-precision float32 vectors into compressed formats.

   * Transparent Conversion: The Go layer can consistently provide float32 data. The system automatically handles the conversion
     to the index's internal type (half, int8, or uint8) directly on the GPU.
   * FP16 (Half Precision):
       * Mechanism: Uses raft::copy to perform bit-level conversion from 32-bit to 16-bit floating point.
       * Benefit: 2x memory reduction with negligible impact on search recall.
   * 8-Bit Integer (int8/uint8):
       * Mechanism: Implements a learned Scalar Quantizer. The system samples the dataset to determine optimal min and max
         clipping bounds.
       * Training: Before building, the quantizer is "trained" on a subset of the data to ensure the 256 available integer levels
         are mapped to the most significant range of the distribution.
       * Benefit: 4x memory reduction, enabling massive datasets to reside in VRAM.
   * GPU-Accelerated: All quantization kernels are executed on the device. This minimizes CPU usage and avoids the latency of
     converting data before sending it over the PCIe bus.

  7. Supported Index Types
  The following indexes are fully integrated into the MatrixOne GPU architecture:


  ┌──────────┬──────────────────────┬───────────────────────────────────────────────────────────────────────────────┐
  │ Index    │ Algorithm            │ Strengths                                                                     │
  ├──────────┼──────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
  │ CAGRA    │ Hardware-accelerated │ Best-in-class search speed and high recall. Optimized for hardware graph      │
  │          │ Graph                │ traversal.                                                                    │
  │ IVF-Flat │ Inverted File Index  │ High accuracy and fast search. Excellent for general-purpose use.             │
  │ IVF-PQ   │ Product Quantization │ Extreme compression. Supports billions of vectors via lossy code compression. │
  │ Brute    │ Exact Flat Search    │ 100% recall. Ideal for small datasets or generating ground-truth for          │
  │ Force    │                      │ benchmarks.                                                                   │
  │ K-Means  │ Clustering           │ High-performance centroid calculation for data partitioning and unsupervised  │
  │          │                      │ learning.                                                                     │
  └──────────┴──────────────────────┴───────────────────────────────────────────────────────────────────────────────┘


  8. Operational Telemetry
  All indexes implement a unified Info() method that returns a JSON-formatted string. This allows the database to programmatically
  verify:
   * Hardware Mapping: Which GPU devices are holding which shards.
   * Data Layout: Element sizes, dimensions, and current vector counts.
   * Hyper-parameters: Internal tuning values like NLists, GraphDegree, or PQBits.
