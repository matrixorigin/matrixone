// Copyright 2022 Matrix Origin
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

package hashmap

// Map is a  robinhashmap implementation
type Map[K comparable, V any] struct {
	count int32
	size  uint32
	// https://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/
	shift   uint32
	maxDist uint32
	buckets []bucket[K, V]
}

type bucket[K comparable, V any] struct {
	key K
	h   uint64
	// The distance the entry is from its desired position.
	dist uint32
	val  *V
}
