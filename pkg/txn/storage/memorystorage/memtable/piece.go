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

package memtable

// Piece represents a piece of data
// it may be an in-memory b-tree, or a TAE or parquet file
type Piece[
	K Ordered[K],
	V any,
] interface {
	Copy() Piece[K, V]
	Get(tx *Transaction, key K) (value V, err error)
	NewIter(tx *Transaction) Iter[K, V]
}

type Iter[
	K Ordered[K],
	V any,
] interface {
	First() bool
	Next() bool
	Seek(key K) bool
	Read() (key K, value V, err error)
	Item() any // implementation specific
	Close() error
}

// IndexedPiece extends the Piece interface to support index lookup
type IndexedPiece[
	K Ordered[K],
	V any,
] interface {
	Piece[K, V]

	Index(tx *Transaction, index Tuple) (entries []*IndexEntry[K, V], err error)
}

// MutablePiece extends the Piece interface to support mutation
type MutablePiece[
	K Ordered[K],
	V any,
	R Row[K, V],
] interface {
	Piece[K, V]

	TxCommitter
	Insert(tx *Transaction, row R) error
	Update(tx *Transaction, row R) error
	Delete(tx *Transaction, key K) error
}

// DiffIterPiece  extends the Piece interface to support iterate diffs
type DiffIterPiece[
	K Ordered[K],
	V any,
] interface {
	Piece[K, V]

	NewDiffIter(fromTime, toTime *Time) DiffIter[K, V]
}

type DiffIter[
	K Ordered[K],
	V any,
] interface {
	First() bool
	Next() bool
	Seek(key K) bool
	Read() (key K, value V, bornTime Time, lockTime *Time, err error)
	Close() error
}
