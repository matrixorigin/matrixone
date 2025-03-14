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

package vectorindex

import (
	"runtime"

	usearch "github.com/unum-cloud/usearch/golang"
)

/*
  HNSW vector index using usearch

  Multiple mini-indexes with max size 10K vectors will be build and search will run usearch.Search concurrently with all mini-indexes.
  Single model file will be splited into chunks and each chunk size is 64K maximum.
*/

const (
	MaxChunkSize = 65536
)

// HNSW have two secondary index tables, metadata and index storage.  For new vector index algorithm that share the same secondary tables,
// can use the same IndexTableConfig struct
type IndexTableConfig struct {
	DbName        string `json:"db"`
	SrcTable      string `json:"src"`
	MetadataTable string `json:"metadata"`
	IndexTable    string `json:"index"`
	PKey          string `json:"pkey"`
	KeyPart       string `json:"part"`
	ThreadsBuild  int64  `json:"threads_build"`
	ThreadsSearch int64  `json:"threads_search"`
	IndexCapacity int64  `json:"index_capacity"`

	// IVF related
	EntriesTable string `json:"entries"`
	DataSize     int64  `json:"datasize"`
	Nprobe       uint   `json:"nprobe"`
	PKeyType     int32  `json:"pktype"`
	KeyPartType  int32  `json:"parttype"`
}

// HNSW specified parameters
type HnswParam struct {
	M              string `json:"m"`
	EfConstruction string `json:"ef_construction"`
	Quantization   string `json:"quantization"`
	OpType         string `json:"op_type"`
	EfSearch       string `json:"ef_search"`
}

// IVF specified parameters
type IvfParam struct {
	Lists  string `json:"lists"`
	OpType string `json:"op_type"`
}

type IvfflatIndexConfig struct {
	Lists      uint
	Metric     uint16
	InitType   uint16
	Dimensions uint
	Normalize  bool
	Version    int64
	VectorType int32
}

// This is generalized index config and able to share between various algorithm types.  Simply add your new configuration such as usearch.IndexConfig
type IndexConfig struct {
	Type    string
	Usearch usearch.IndexConfig
	Ivfflat IvfflatIndexConfig
}

type RuntimeConfig struct {
	Limit uint
	Probe uint
}

// nthread == 0, result will return NumCPU - 1
func GetConcurrency(nthread int64) int64 {
	if nthread > 0 {
		return nthread
	}
	ncpu := runtime.NumCPU()
	return int64(ncpu)
}

// nthread == 0, result will return NumCPU
func GetConcurrencyForBuild(nthread int64) int64 {
	if nthread > 0 {
		return nthread
	}
	return int64(runtime.NumCPU())
}
