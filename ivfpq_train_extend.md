# IVF-PQ on a single GPU: what landed, and what the AWS run has to decide

Target is **SINGLE**, not sharded. The 88M template says `distribution_mode: "sharded"`,
but that was written around a build that could not fit otherwise; the point of the work
below is to make one card viable. Whether it actually is comes down to one number that
cannot be measured on an 8 GB dev box — see "The one open question".

Supersedes the earlier draft, which proposed hand-writing a train/extend split in MO.
cuVS already implements that split; MO was opting out of it.

## Landed on `gpu_single_mode` (pushed)

| commit | |
|---|---|
| `2d880bc0c0` | `bench_hostview.cu` — device upload vs host view |
| `91e5d1f9a8` | build IVF-PQ from a `host_matrix_view`, all three distribution modes |
| `71812cbcd2` | `bench_wiki88.cu` — 88M projection at the real tuning |
| `72f99d4930` | size the build against the index, not the dataset |
| `241f7004fb` | cagra rotation BVT expectations |
| `2e4ab2f2ad` | (unrelated) stale INCLUDE-column error expectation |
| `dd6892c6af` | this document |
| `13bf8459e4` | bound capacity by host memory, not only VRAM |

Verified: `test_cuvs_worker` 162/162, GPU vector BVT 955/955, 12 new tag-free unit tests,
`go vet` clean under default and `gpu` tags.

## Three resources, three mechanisms

The build was failing for one reason but being sized for another. Separating them:

| resource | 88M, dim 768, f16 | bounded by | state |
|---|---|---|---|
| dataset | 96.8 GB host, per sub-index | *nothing* on device — cuVS streams it from a host view; host bounded by `capacity` | **done** |
| k-means trainset | `train_rows · 768 · 6` | clamping `kmeans_trainset_fraction` | **done** |
| the index | `88M · (m + 8)` | `m`, and nothing else | **open** |
| host build buffer | `capacity · 768 · 2` | `hostRowsFit`, 60% of `MemAvailable` | **done** |

The dataset pressure was self-inflicted: MO allocated `count × dim` device memory and
copied the host buffer up before calling the *device* `build` overload. cuVS's build is
templated on the mdspan accessor (`ivf_pq_build.cuh:1223`), so a host view runs the same
code while gathering the trainset on the host (`sample_rows.cuh:47-63`) and streaming the
encode through `batch_load_iterator` at a batch size that halves until it fits
(`:1050-1097`). Measured at dim 960: within noise while the dataset fits (+0.8% at 1M),
2.0× faster once it does not, peak VRAM 4.94 → 1.35 GB at 1M and near flat as rows grow,
recall identical at 1.000 from 250k to 2.5M.

This is the intended path, not an implementation detail we are leaning on. cuVS built
this dataset to force it: `docs/source/cuvs_bench/wiki_all_dataset.rst` says wiki-all's
~251 GB is "intentionally larger than the typical memory of GPUs ... to promote the use
of compression and efficient out-of-core methods for both indexing and search." The
`host_matrix_view` overloads are that mechanism, and the header's note about setting a
stream pool for "kernel and copy overlapping" only means anything for host input.

The index-size formula capacity is now sized against is the documented one rather than
something reverse-engineered — `docs/source/neighbors/ivfpq.rst`, Index (device memory):
`n_vectors * (pq_dim * pq_bits/8 + sizeof_idx) + n_clusters`. The same page's "Build peak
memory usage (device)" is trainset + labels + centroids, with the index absent, which
independently confirms the two phases do not overlap.

**The two remaining resources never coexist.** The trainset and its k-means scratch are
scoped to a block closing at `ivf_pq_build.cuh:1369`, before `detail::extend` at `:1374`
allocates the list data. Peak is `max(train, encode)`, not the sum — which is why the
fraction is clamped rather than the capacity. Capacity is bounded by the index, which
nothing can shrink; the sample can simply be made smaller.

Two arithmetic corrections worth keeping visible, both of which were wrong in every
earlier estimate here:

- **`m` unset is not free.** cuVS `calculate_pq_dim` (`ivf_pq_index.cu:611`) picks
  `dim/2` rounded down to a multiple of 32 — **384** at dim 768, twice the 192 the
  template configures, so a default-`m` index costs twice the memory.
- **Narrow storage makes the trainset bigger.** cuVS keeps it in float32 whatever the
  storage type, plus a second copy in `T` for non-float `T` (`:1288-1307`), both live at
  the peak. f16 is **6** bytes/element against f32's 4.

## SINGLE at 88M: the arithmetic

dim 768, f16, `lists=6000`, `train=2%` → 1.76M training rows.

```
trainset  = 1.76M · 768 · 6                    =  8.1 GB     (fixed by train %)
index     = 88M · (m + 8)                       =  varies     (fixed by m)
peak      = max(trainset, index)
```

| m | index | peak | of 20 GB |
|---|---|---|---|
| 192 (template) | 17.6 GB | 17.6 GB | 88% |
| 160 | 14.8 GB | 14.8 GB | 74% |
| 128 | 12.0 GB | 12.0 GB | 60% |
| 96 | 9.2 GB | 9.2 GB | 46% |

At every `m` in that table the trainset is not the constraint. **`m` alone decides
whether SINGLE works.** Note this is search residency, not just build: a query reaches
every list, so sub-index rotation does not reduce it — splitting an index does not shrink
the sum of its parts.

## The one open question

The `N·(m+8)` figure is a floor. Measured peak runs above it, and the shape of the gap
decides whether SINGLE works:

| rows | raw codes+ids | measured peak | overhead |
|---|---|---|---|
| 1M | 0.20 GB | 0.75 GB | 0.55 GB |
| 2.5M | 0.50 GB | 1.05 GB | 0.55 GB |
| 5M | 1.00 GB | 1.57 GB | 0.57 GB |
| 8M | 1.60 GB | 2.62 GB | **1.02 GB** |

Flat at ~0.56 GB through 5M, then nearly doubling at 8M. A linear fit projects 24.1 GB at
88M, which would sink SINGLE at `m=192`.

**That fit is probably wrong, and the cuVS docs say why.** `docs/source/neighbors/ivfpq.rst`:

> Workspace size is not trivial, a heuristic controls the batch size to make sure the
> workspace fits `raft::resource::get_workspace_free_bytes(res)`.

The gap is not a fixed cost scaling with rows — it is cuVS **expanding to fill available
workspace**. On the 7.34 GB dev card, with most of it idle, the heuristic took more; on a
20 GB card carrying a 17.6 GB index there is little free, so it takes less. That also
explains the 8M jump, which `conservative_memory_allocation=true` could not (2.62 vs
2.63 GB) — it was never IVF list over-growth.

So the overhead is self-limiting rather than additive, and the honest projection at
`m=192` is "the 17.6 GB floor plus whatever workspace is left", not 24.1 GB. Still worth
measuring, but the prior now favours SINGLE working on the template's own tuning.

The itemised extras from the same page are negligible and already inside the floor:
codebook `4·pq_dim·pq_len·2^pq_bits` = 0.79 MB, extras `n_clusters·(20 + 8·dim)` = 37 MB,
list pointers 24 KB.

## What to run on AWS

**Experiment 1 — is the overhead constant?** This decides the hardware question.

```
make -C cgo/cuvs bench_wiki88
./bench_wiki88 88000000            # ceiling, not target: runs 1M..88M
```

It already reports the train and encode phases separately at the real tuning, so the
overhead column above extends directly. Host RAM is the limit on how far it goes: f16 at
dim 768 is 1536 B/row, so a 40M single-buffer run needs ~61 GB. Points at 16M
and 32M are enough to tell a constant from a slope; the 8M jump is the thing to confirm
or dismiss first, since a single anomalous point is the whole ambiguity.

**Experiment 2 — what does `m` cost in recall?** Only needed if experiment 1 says the
overhead scales. On the 1M set against the existing groundtruth:

```
python run_matrix.py --phase matrix --scale 1M --algo ivfpq     # ivfpq_m 192 / 160 / 128
```

`ivfpq_m` is read from `cfg/templates/<scale>.json` via `tun.get("ivfpq_m", 192)`.

**Watch during both:** `[IVFPQ build_internal]` log lines carry `cudaMemGetInfo` per
phase; `GPU_free_MB` at `SINGLE_GPU:before-cuvs-build` against sub-index number should be
flat now that retired sub-indexes are freed.

## Notes and residual risk

- **Host RAM is bounded by `capacity`, not by `N`.** An earlier version of this document
  said 88M in f16 needs 135 GB of host RAM. That was wrong: `flattened_host_dataset` is
  sized `capacity · dim · sizeof(Q)`, not `N · dim · sizeof(Q)`, and it is cleared after
  each sub-index builds (`ivf_pq.hpp:418-419`, after all ranks join). Only one sub-index
  buffer is live at a time, so 135 GB would require `capacity == N`.
  What is true is that sizing capacity against the PQ codes made this buffer ~7.7x
  bigger than the old dataset-based bound did (12.1 GB → 96.8 GB at dim 768 / f16 /
  m 192), because the dataset term in the old per-row cost had been bounding the host
  side for free. `planCapacity` now takes an explicit `hostRowsFit` from 60% of
  `MemAvailable` — the same fraction as the device rule, deliberately not a second
  heuristic. Under SHARDED every rank views a disjoint slice of that one buffer
  (`ivf_pq.hpp:527`), so sharding divides the work but not the allocation.
- **~~`large_workspace_resource` is unconfigured~~ — resolved by the cuVS docs.**
  `docs/source/neighbors/ivfpq.rst`: "if there's not enough space left in the workspace
  memory resource, IVF-PQ build automatically switches to the managed memory for the
  training set and labels." An oversized trainset degrades to paging rather than
  throwing, and needs no configuration from MO.
- **Thin centroids warn, they do not refuse.** The ~39 points-per-centroid floor is a
  rule of thumb, not a cuVS constraint (`validate_build_params` only checks
  `rows >= n_lists`), so refusing would break configurations that work today.
- **CAGRA is a different problem and is out of scope.** It reads actual vectors while
  walking the graph, so its dataset is resident for the index's life — and unlike ivfpq
  that residency is `N`, not `capacity`, since every sub-index must be loaded to search:
  135 GB in f16 at 88M, whatever the build does. `extend` also throws for float16 and is unsupported under
  sharded (`cagra.hpp:711,718`). The only lever is a compressed (VPQ) dataset, a separate
  feature.
