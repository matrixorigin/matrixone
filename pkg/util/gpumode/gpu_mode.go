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

// Package gpumode declares a process-wide GpuMode flag and a session-
// sysvar–aware EffectiveGpuMode resolver, used by vector-index
// dispatch sites (brute force, kmeans, adhoc brute force, pairwise
// distance) to decide between GPU (cuvs) and CPU implementations at
// runtime.
//
// The build tag drives the default:
//   - `-tags gpu` build → GpuMode = true (init() in gpu_mode_gpu.go).
//   - default build      → GpuMode = false (Go zero value).
//
// An operator on a gpu-tag binary can flip the dispatch off per
// session by `SET gpu_mode = 0`, exercising the CPU paths for
// debugging or benchmarking.
package gpumode

// GpuMode is the process-wide default for whether vector-index
// dispatch routes to GPU paths. Flipped to true at init() by
// gpu_mode_gpu.go in -tags gpu builds; defaults false otherwise.
// Read directly by call sites that have no proc in scope; sites with
// a proc should consult EffectiveGpuMode().
var GpuMode bool

// EffectiveGpuMode returns the per-call gpu_mode decision. Session
// sysvar override (set via `SET gpu_mode = 0/1`) wins over the build-
// tag default. Pass proc.GetResolveVariableFunc() — nil is safe and
// falls back to GpuMode.
//
// Bool sysvars come back from the resolver as int8 (matches
// gSysVarsDefs convention); anything else (nil, error, unexpected
// type) falls back to GpuMode.
func EffectiveGpuMode(resolver func(string, bool, bool) (any, error)) bool {
	if resolver == nil {
		return GpuMode
	}
	v, err := resolver("gpu_mode", true, false)
	if err != nil || v == nil {
		return GpuMode
	}
	if b, ok := v.(int8); ok {
		return b != 0
	}
	return GpuMode
}

// GpuModeDefaultInt8 returns GpuMode as int8(0) or int8(1) for use as
// the `Default` field of the gpu_mode entry in
// pkg/frontend/variables.go's gSysVarsDefs map. Lives alongside
// GpuMode so the bool→int8 conversion isn't duplicated at the
// registration site.
func GpuModeDefaultInt8() int8 {
	if GpuMode {
		return 1
	}
	return 0
}
