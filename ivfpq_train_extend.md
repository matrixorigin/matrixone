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

Verified: `test_cuvs_worker` 162/162, GPU vector BVT 955/955, 12 new tag-free unit tests,
`go vet` clean under default and `gpu` tags.

## Three resources, three mechanisms

The build was failing for one reason but being sized for another. Separating them:

| resource | 88M, dim 768, f16 | bounded by | state |
|---|---|---|---|
| dataset | 135 GB host | *nothing* — cuVS streams it from a host view | **done** |
| k-means trainset | `train_rows · 768 · 6` | clamping `kmeans_trainset_fraction` | **done** |
| the index | `88M · (m + 8)` | `m`, and nothing else | **open** |

The dataset pressure was self-inflicted: MO allocated `count × dim` device memory and
copied the host buffer up before calling the *device* `build` overload. cuVS's build is
templated on the mdspan accessor (`ivf_pq_build.cuh:1223`), so a host view runs the same
code while gathering the trainset on the host (`sample_rows.cuh:47-63`) and streaming the
encode through `batch_load_iterator` at a batch size that halves until it fits
(`:1050-1097`). Measured at dim 960: within noise while the dataset fits (+0.8% at 1M),
2.0× faster once it does not, peak VRAM 4.94 → 1.35 GB at 1M and near flat as rows grow,
recall identical at 1.000 from 250k to 2.5M.

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

The table above is the *floor*. Measured peak runs above it, and the gap is not yet
characterised:

| rows | raw codes+ids | measured peak | overhead |
|---|---|---|---|
| 1M | 0.20 GB | 0.75 GB | 0.55 GB |
| 2.5M | 0.50 GB | 1.05 GB | 0.55 GB |
| 5M | 1.00 GB | 1.57 GB | 0.57 GB |
| 8M | 1.60 GB | 2.62 GB | **1.02 GB** |

Constant at ~0.56 GB through 5M, then nearly doubling at 8M. Everything turns on which
of those is the trend:

- **overhead constant (~0.6 GB)** → 88M at `m=192` is 18.2 GB. SINGLE works on the
  template's own tuning, no change needed.
- **overhead proportional (~1.64× the floor)** → 88M at `m=192` is 28.9 GB. SINGLE needs
  `m≤128`, and the recall cost of that has to be paid.

An 8 GB card cannot separate these, and `conservative_memory_allocation=true` was tested
and is not the cause (8M: 2.62 vs 2.63 GB). This is the measurement the AWS box exists
for.

## What to run on AWS

**Experiment 1 — is the overhead constant?** This decides the hardware question.

```
make -C cgo/cuvs bench_wiki88
./bench_wiki88 40000000            # or as high as host RAM allows
```

It already reports the train and encode phases separately at the real tuning, so the
overhead column above extends directly. Host RAM is the limit on how far it goes: f16 at
dim 768 is 1536 B/row, so 40M needs ~61 GB and the full 88M needs ~135 GB. Points at 16M
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

- **Host RAM is the binding resource and is not solved here.** `flattened_host_dataset`
  is still `capacity · dim · sizeof(Q)` — 135 GB for 88M in f16. Under SHARDED every rank
  slices that one buffer, undivided by `G` (`ivf_pq.hpp:527`). Stated as a precondition.
- **`large_workspace_resource` is unconfigured.** cuVS falls back to it when the trainset
  does not fit comfortably (`kTolerableRatio = 4`, `:1266-1272`). Whether that spills to
  managed memory or throws is unspecified in MO. Given the RMM history on this branch,
  configure it deliberately rather than discovering the default at 88M.
- **Thin centroids warn, they do not refuse.** The ~39 points-per-centroid floor is a
  rule of thumb, not a cuVS constraint (`validate_build_params` only checks
  `rows >= n_lists`), so refusing would break configurations that work today.
- **CAGRA is a different problem and is out of scope.** It reads actual vectors while
  walking the graph, so its dataset is resident for the index's life — 135 GB in f16 at
  88M, whatever the build does. `extend` also throws for float16 and is unsupported under
  sharded (`cagra.hpp:711,718`). The only lever is a compressed (VPQ) dataset, a separate
  feature.
