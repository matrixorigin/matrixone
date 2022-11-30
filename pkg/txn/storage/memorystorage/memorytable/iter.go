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

// Iter represents a generic iterator
type Iter[T any] interface {
	First() bool
	Next() bool
	Close() error
	Read() (T, error)
}

// KVIter represents a key-value iterator
type KVIter[K any, V any] interface {
	First() bool
	Seek(K) bool
	Next() bool
	Close() error
	Read() (K, V, error)
}
