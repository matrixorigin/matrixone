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

import "fmt"

type KVPair[
	K Ordered[K],
	V any,
] struct {
	Key K

	ID      int64
	Value   V
	Indexes []Tuple
}

var nextKVPairID = int64(1 << 32)

func compareKVPair[
	K Ordered[K],
	V any,
](a, b *KVPair[K, V]) bool {
	return a.Key.Less(b.Key)
}

func (k *KVPair[K, V]) String() string {
	return fmt.Sprintf("kv pair, id %v, key %v, value %v", k.ID, k.Key, k.Value)
}
