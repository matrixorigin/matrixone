// Copyright 2021 - 2022 Matrix Origin
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

//go:build !mpoolprofile

package mpool

// No-op stubs when mpool profiling is disabled (default).
// Build with -tags mpoolprofile to enable per-allocation stack tracking.

func profileRecordAlloc(_ int, _ uintptr, _ int64) {}

func profileRecordFree(_ uintptr, _ int64) {}

func profileRecordRealloc(_ int, _, _ uintptr, _, _ int64) {}

func profileTrackedCount() int { return 0 }
