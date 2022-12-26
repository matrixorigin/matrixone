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

package memorytable

type KV[
	K Ordered[K],
	V any,
] interface {
	Copy() KV[K, V]
	Get(KVPair[K, V]) (KVPair[K, V], bool)
	Set(KVPair[K, V]) (KVPair[K, V], bool)
	Delete(KVPair[K, V])
	Iter() KVIter[K, V]
}

type KVIter[
	K Ordered[K],
	V any,
] interface {
	SeekIter[KVPair[K, V]]
}
