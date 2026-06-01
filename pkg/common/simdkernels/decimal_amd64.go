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

package simdkernels

import "unsafe"

// Decimal64SignExtend widens n Decimal64 (uint64) values to Decimal128 via
// branchless sign extension. dst points to the Decimal128 output array,
// src points to the Decimal64 input array.
// Uses 4× loop unrolling and PREFETCHT0 to hide L2 latency on cold
// destination cache lines that would otherwise stall the store pipeline.
//
//go:noescape
func Decimal64SignExtend(dst, src unsafe.Pointer, n int)
