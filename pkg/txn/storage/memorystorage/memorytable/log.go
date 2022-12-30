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

type log[
	K Ordered[K],
	V any,
] struct {
	serial  int64
	key     K
	pair    *KVPair[K, V]
	oldPair *KVPair[K, V]
}

var nextLogSerial = int64(1 << 48)

func compareLog[
	K Ordered[K],
	V any,
](a, b *log[K, V]) bool {
	if a.key.Less(b.key) {
		return true
	}
	if b.key.Less(a.key) {
		return false
	}
	return a.serial < b.serial
}

func (l *log[K, V]) String() string {
	return fmt.Sprintf(
		"log: serial %v, pair %v, old pair %v",
		l.serial,
		l.pair,
		l.oldPair,
	)
}
