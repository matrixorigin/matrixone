//go:build !gpu

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

package cagra

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

var errGPURequired = moerr.NewInternalErrorNoCtx("CAGRA requires a GPU build (build tag: gpu)")

// CagraModel is a dummy placeholder for non-GPU builds.
// All methods return an error indicating that GPU support is required.
type CagraModel[T cuvs.VectorType] struct {
	Id          string
	Path        string
	FileSize    int64
	MaxCapacity uint64
	Timestamp   int64
	Checksum    string
	Dirty       bool
	View        bool
	Len         int64
}

func NewCagraModelForBuild[T cuvs.VectorType](id string, cfg vectorindex.IndexConfig, nthread uint32, devices []int) (*CagraModel[T], error) {
	return nil, errGPURequired
}

func LoadMetadata[T cuvs.VectorType](sqlproc *sqlexec.SqlProcess, dbname string, metatbl string) ([]*CagraModel[T], error) {
	return nil, errGPURequired
}

func (idx *CagraModel[T]) InitEmpty(totalCount uint64) error {
	return errGPURequired
}

func (idx *CagraModel[T]) AddChunk(chunk []T, chunkCount uint64, ids []uint32) error {
	return errGPURequired
}

func (idx *CagraModel[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []uint32) error {
	return errGPURequired
}

func (idx *CagraModel[T]) Build() error {
	return errGPURequired
}

func (idx *CagraModel[T]) Destroy() error {
	return errGPURequired
}

func (idx *CagraModel[T]) ToSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	return nil, errGPURequired
}

func (idx *CagraModel[T]) ToDeleteSql(cfg vectorindex.IndexTableConfig) ([]string, error) {
	return nil, errGPURequired
}

func (idx *CagraModel[T]) Empty() bool {
	return true
}

func (idx *CagraModel[T]) Full() bool {
	return false
}

func (idx *CagraModel[T]) Search(query []T, limit uint32) (keys []int64, distances []float32, err error) {
	return nil, nil, errGPURequired
}

func (idx *CagraModel[T]) LoadIndex(
	sqlproc *sqlexec.SqlProcess,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread int64,
	view bool) error {
	return errGPURequired
}

func (idx *CagraModel[T]) Unload() error {
	return errGPURequired
}
