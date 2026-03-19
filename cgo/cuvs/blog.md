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

## Summary of Supported Indexes
Our architecture now supports a suite of high-performance indexes:
*   **CAGRA**: A hardware-accelerated graph index for state-of-the-art search speed.
*   **IVF-Flat**: The workhorse for high-accuracy general-purpose search.
*   **IVF-PQ**: For extreme compression of billion-scale datasets.
*   **K-Means**: For high-speed data partitioning.

## Conclusion
By shifting the heavy lifting of clustering, assignment, and quantization to the GPU through cuVS, MatrixOne can now handle massive vector datasets on surprisingly modest hardware. What once took a full day now takes less than an hour, with search latencies that remain low even under heavy load.

The integration of `cuvs_worker_t` and dynamic batching ensures that we don't just have a "fast index," but a **production-ready database engine** capable of scaling with the needs of modern AI.
