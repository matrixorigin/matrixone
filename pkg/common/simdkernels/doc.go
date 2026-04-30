// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package simdkernels hosts hand-tuned SIMD/assembly kernels used by the
// vectorized executors. Currently focused on Decimal64/Decimal128 hot paths
// (predicate scans, sign extension, int64-fast-path multiply). Per-arch
// build tags select between assembly, archsimd, and pure-Go fallbacks.
package simdkernels
