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

// KVPair represents a physical row in Table
type KVPair[
	K Ordered[K],
	V any,
] struct {
	Key K
	*KVValue[K, V]
}

// KVValue represents the value of a KVPair
type KVValue[
	K Ordered[K],
	V any,
] struct {
	ID      int64
	Value   V
	Indexes []Tuple
}

func (k KVPair[K, V]) Valid() bool {
	return k.KVValue != nil
}

var nextKVPairID = int64(1 << 32)

func compareKVPair[
	K Ordered[K],
	V any,
](a, b KVPair[K, V]) bool {
	return a.Key.Less(b.Key)
}

// String returns text form of KVPair
func (k *KVPair[K, V]) String() string {
	if k.KVValue != nil {
		return fmt.Sprintf("kv pair, key %v, value %v", k.Key, k.KVValue)
	}
	return fmt.Sprintf("kv pair, key %v", k.Key)
}
