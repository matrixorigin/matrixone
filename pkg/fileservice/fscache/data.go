// Copyright 2024 Matrix Origin
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

package fscache

type Data interface {
	// Size is the logical payload length visible to cache consumers.
	Size() int64
	// Capacity is the allocator-backed capacity retained while the data is live.
	// Cache admission and physical-memory metrics use this value.
	Capacity() int64
	Bytes() []byte
	Slice(length int) Data
	Retain()
	Release()
}
