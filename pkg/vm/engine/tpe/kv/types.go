// Copyright 2021 Matrix Origin
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

package kv

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"

type KVHandler interface {
	// NextID gets the next id of the type
	NextID(typ string) (uint64,error)

	// Set writes the key-value (overwrite)
	Set(key tuplecodec.TupleKey,value tuplecodec.TupleValue) error

	// SetBatch writes the batch of key-value (overwrite)
	SetBatch(keys []tuplecodec.TupleKey,values []tuplecodec.TupleValue) []error

	// DedupSet writes the key-value. It will fail if the key exists
	DedupSet(key tuplecodec.TupleKey, value tuplecodec.TupleValue) error

	// DedupSetBatch writes the batch of keys-values. It will fail if there is one key exists
	DedupSetBatch(keys []tuplecodec.TupleKey, values []tuplecodec.TupleValue) []error

	// Get gets the value of the key
	Get(key tuplecodec.TupleKey)(tuplecodec.TupleValue, error)

	// GetBatch gets the values of the keys
	GetBatch(keys []tuplecodec.TupleKey)([]tuplecodec.TupleValue, error)

	// GetRange gets the values among the range [startKey,endKey).
	GetRange(startKey tuplecodec.TupleKey, endKey tuplecodec.TupleKey) ([]tuplecodec.TupleValue, error)

	// GetRange gets the values from the startKey with limit
	GetRangeWithLimit(startKey tuplecodec.TupleKey, limit uint64) ([]tuplecodec.TupleKey,[]tuplecodec.TupleValue, error)

	// GetWithPrefix gets the values of the prefix with limit
	GetWithPrefix(prefix tuplecodec.TupleKey, limit uint64) ([]tuplecodec.TupleKey, []tuplecodec.TupleValue, error)

	// GetShardsWithRange get the shards that holds the range [startKey,endKey)
	GetShardsWithRange(startKey tuplecodec.TupleKey, endKey tuplecodec.TupleKey) (interface{}, error)

	// GetShardsWithPrefix get the shards that holds the keys with prefix
	GetShardsWithPrefix(prefix tuplecodec.TupleKey) (interface{}, error)
}