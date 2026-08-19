/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// SPDX-License-Identifier: Apache-2.0
// -----------------------------------------------------------------------------
// bench_upload_cost.cu — what does uploading the dataset cost CAGRA and IVF-Flat?
//
// bench_hostview.cu answered this for IVF-PQ. The answer does not carry over,
// because the three algorithms relate to their input differently:
//
//   IVF-PQ    encodes to PQ codes; the input is never referenced again.
//   IVF-Flat  copies the vectors INTO its lists (interleaved). The input is also
//             never referenced again -- so uploading it and holding it meant
//             carrying the dataset TWICE on the device, for the index's life.
//   CAGRA     searches by reading the actual vectors, so a dataset must stay
//             resident. Only the OWNER changes: with a device view cuVS stores a
//             reference to the caller's buffer when rows are 16B-aligned
//             (cagra.hpp:524-527); with a host view it makes and owns a copy
//             (:556-558). Steady state is identical. The win is that cuVS attaches
//             the dataset LAST, after optimize (cagra_build.cuh:2264, :2289), so
//             with a host view the raw vectors are not resident during the
//             optimize step -- which is the device peak.
//
// So the prediction is: IVF-Flat should show a large peak drop (a whole redundant
// copy), CAGRA a smaller one (peak goes from dataset+optimize to
// max(optimize, dataset+graph)), and neither should lose recall.
//
// Also checks the failure mode CAGRA hides: cuVS catches std::bad_alloc while
// attaching and returns a GRAPH-ONLY index with a warning, and size() does not
// reveal it -- only dataset().extent(0) does.
//
// Standalone: cuVS / RAFT / RMM only.
// Build: make -C cgo/cuvs bench_upload_cost      Run: ./bench_upload_cost [rows]
// -----------------------------------------------------------------------------

#include <cuvs/neighbors/cagra.hpp>
#include <cuvs/neighbors/ivf_flat.hpp>

#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_resources.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <rmm/device_uvector.hpp>

#include <cuda_runtime.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define CUDA_CHECK(c) do { cudaError_t e=(c); if(e!=cudaSuccess){ \
    std::fprintf(stderr,"CUDA %s @%d\n",cudaGetErrorString(e),__LINE__); std::exit(1);} } while(0)

namespace {
constexpr int64_t kDim = 768, kQueries = 100;
constexpr uint32_t kTopK = 10;

size_t free_bytes() { size_t f=0,t=0; CUDA_CHECK(cudaMemGetInfo(&f,&t)); return f; }

class Peak {
 public:
  Peak() : lo_(free_bytes()), stop_(false) {
    th_ = std::thread([this]{ CUDA_CHECK(cudaSetDevice(0));
      while(!stop_.load(std::memory_order_relaxed)) {
        size_t f=free_bytes(), c=lo_.load(std::memory_order_relaxed);
        while(f<c && !lo_.compare_exchange_weak(c,f)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); } });
  }
  size_t stop(size_t base){ stop_.store(true); th_.join();
    size_t l=lo_.load(); return l<base?base-l:0; }
 private:
  std::atomic<size_t> lo_; std::atomic<bool> stop_; std::thread th_;
};

struct R { double sec; size_t peak; double recall; };

// Each query perturbs a known row, so the correct top-1 is that row's id.
template <typename Search>
double recall_of(Search search, const std::vector<int64_t>& seeds) {
  auto got = search();
  int hit = 0;
  for (int64_t q = 0; q < kQueries; ++q)
    for (uint32_t j = 0; j < kTopK; ++j)
      if (got[q*kTopK+j] == seeds[q]) { ++hit; break; }
  return double(hit)/double(kQueries);
}
}  // namespace

int main(int argc, char** argv) {
  int64_t n = (argc>1) ? std::atoll(argv[1]) : 200000;
  CUDA_CHECK(cudaSetDevice(0));
  cudaDeviceProp prop{}; CUDA_CHECK(cudaGetDeviceProperties(&prop,0));
  std::printf("GPU: %s  free %.2f GB   n=%ld dim=%ld  dataset=%.2f GB (float32)\n\n",
              prop.name, free_bytes()/1e9, (long)n, (long)kDim, n*kDim*4/1e9);

  std::mt19937 rng(42);
  std::uniform_real_distribution<float> uni(-1.f,1.f);
  // Clusters must OVERLAP. At sigma 0.10 the 1000 clusters are isolated islands and
  // CAGRA's greedy graph walk cannot cross between them -- recall collapsed to 0.09
  // while IVF-Flat scored 1.000, because IVF picks the cluster by centroid distance
  // and never has to navigate. That measures the corpus, not the change. sigma 0.50
  // against centers in [-1,1] leaves the graph connected.
  std::normal_distribution<float> jit(0.f,0.50f), qj(0.f,0.01f);
  constexpr int64_t kC = 1000;
  std::vector<float> ctr(kC*kDim); for(auto&v:ctr) v=uni(rng);
  std::vector<float> data((size_t)n*kDim);
  for (int64_t i=0;i<n;++i){ const float* c=ctr.data()+(i%kC)*kDim;
    for (int64_t j=0;j<kDim;++j) data[i*kDim+j]=c[j]+jit(rng); }
  std::uniform_int_distribution<int64_t> pick(0,n-1);
  std::vector<int64_t> seeds(kQueries); std::vector<float> qs(kQueries*kDim);
  for (int64_t q=0;q<kQueries;++q){ seeds[q]=pick(rng);
    for (int64_t j=0;j<kDim;++j) qs[q*kDim+j]=data[seeds[q]*kDim+j]+qj(rng); }

  auto hv = raft::make_host_matrix_view<const float,int64_t>(data.data(), n, kDim);

  std::printf("%-10s %-14s | %9s %9s | %9s\n","algo","dataset via","build(s)","peak","recall@10");
  std::printf("%s\n", std::string(60,'-').c_str());

  auto upload = [&](raft::device_resources& res, rmm::device_uvector<float>& d){
    raft::copy(d.data(), data.data(), d.size(), raft::resource::get_cuda_stream(res));
    res.sync_stream();
    return raft::make_device_matrix_view<const float,int64_t>(d.data(), n, kDim);
  };

  // ---------------- CAGRA ----------------
  cuvs::neighbors::cagra::index_params cp;
  // The wiki_all template values. At 64/32 with itopk 64 recall sat near 0.07 on
  // 768-dim data -- too thin a graph for this dimensionality, not a build-path issue.
  cp.intermediate_graph_degree = 256; cp.graph_degree = 64;
  cuvs::neighbors::cagra::search_params csp; csp.itopk_size = 256;

  for (int host = 0; host < 2; ++host) {
    raft::device_resources res;
    auto stream = raft::resource::get_cuda_stream(res);
    size_t base = free_bytes(); Peak w; auto t0=std::chrono::steady_clock::now();
    std::unique_ptr<cuvs::neighbors::cagra::index<float,uint32_t>> idx;
    // MUST outlive idx on the device path. update_dataset stores only a REFERENCE
    // when rows are 16B-aligned (cagra.hpp:524-527), so letting this go out of
    // scope dangles the index -- the first search then dies with
    // cudaErrorIllegalAddress. That is precisely why MO retained the buffer in
    // dataset_device_ptr_, and precisely what the host view removes the need for:
    // with a host view cuVS makes and owns the copy itself.
    std::unique_ptr<rmm::device_uvector<float>> held;
    if (host) {
      idx.reset(new cuvs::neighbors::cagra::index<float,uint32_t>(
          cuvs::neighbors::cagra::build(res, cp, hv)));
    } else {
      held.reset(new rmm::device_uvector<float>((size_t)n*kDim, stream));
      auto dv = upload(res, *held);
      idx.reset(new cuvs::neighbors::cagra::index<float,uint32_t>(
          cuvs::neighbors::cagra::build(res, cp, dv)));
      res.sync_stream();
    }
    res.sync_stream();
    auto t1=std::chrono::steady_clock::now(); size_t pk=w.stop(base);

    // The degradation cuVS hides: a graph-only index still reports size().
    if (idx->dataset().extent(0) != n)
      std::printf("  !! cagra attached %ld of %ld rows -- graph-only index\n",
                  (long)idx->dataset().extent(0), (long)n);

    double rc = recall_of([&]{
      rmm::device_uvector<float> dq(kQueries*kDim, stream);
      raft::copy(dq.data(), qs.data(), dq.size(), stream);
      auto nb = raft::make_device_matrix<uint32_t,int64_t>(res,kQueries,kTopK);
      auto ds = raft::make_device_matrix<float,int64_t>(res,kQueries,kTopK);
      cuvs::neighbors::cagra::search(res, csp, *idx,
        raft::make_device_matrix_view<const float,int64_t>(dq.data(),kQueries,kDim),
        nb.view(), ds.view());
      std::vector<uint32_t> h(kQueries*kTopK);
      raft::copy(h.data(), nb.data_handle(), h.size(), stream); res.sync_stream();
      return std::vector<int64_t>(h.begin(), h.end());
    }, seeds);

    std::printf("%-10s %-14s | %9.2f %8.2fGB | %9.3f\n", "cagra",
                host?"host view":"device (old)",
                std::chrono::duration<double>(t1-t0).count(), pk/1e9, rc);
    std::fflush(stdout);
  }

  // ---------------- IVF-Flat ----------------
  cuvs::neighbors::ivf_flat::index_params ip; ip.n_lists = 1024;
  cuvs::neighbors::ivf_flat::search_params isp; isp.n_probes = 32;

  for (int host = 0; host < 2; ++host) {
    raft::device_resources res;
    auto stream = raft::resource::get_cuda_stream(res);
    size_t base = free_bytes(); Peak w; auto t0=std::chrono::steady_clock::now();
    std::unique_ptr<cuvs::neighbors::ivf_flat::index<float,int64_t>> idx;
    std::unique_ptr<rmm::device_uvector<float>> held;   // the old code held this FOREVER
    if (host) {
      idx.reset(new cuvs::neighbors::ivf_flat::index<float,int64_t>(
          cuvs::neighbors::ivf_flat::build(res, ip, hv)));
    } else {
      held.reset(new rmm::device_uvector<float>((size_t)n*kDim, stream));
      auto dv = upload(res, *held);
      idx.reset(new cuvs::neighbors::ivf_flat::index<float,int64_t>(
          cuvs::neighbors::ivf_flat::build(res, ip, dv)));
      res.sync_stream();
      // held stays alive here on purpose: that is what MO used to do, and it is
      // the second copy this change removes.
    }
    res.sync_stream();
    auto t1=std::chrono::steady_clock::now(); size_t pk=w.stop(base);

    double rc = recall_of([&]{
      rmm::device_uvector<float> dq(kQueries*kDim, stream);
      raft::copy(dq.data(), qs.data(), dq.size(), stream);
      auto nb = raft::make_device_matrix<int64_t,int64_t>(res,kQueries,kTopK);
      auto ds = raft::make_device_matrix<float,int64_t>(res,kQueries,kTopK);
      cuvs::neighbors::ivf_flat::search(res, isp, *idx,
        raft::make_device_matrix_view<const float,int64_t>(dq.data(),kQueries,kDim),
        nb.view(), ds.view());
      std::vector<int64_t> h(kQueries*kTopK);
      raft::copy(h.data(), nb.data_handle(), h.size(), stream); res.sync_stream();
      return h;
    }, seeds);

    std::printf("%-10s %-14s | %9.2f %8.2fGB | %9.3f\n", "ivfflat",
                host?"host view":"device (old)",
                std::chrono::duration<double>(t1-t0).count(), pk/1e9, rc);
    std::fflush(stdout);
  }
  return 0;
}
