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
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// CagraSearch is a dummy placeholder for non-GPU builds.
type CagraSearch[T cuvs.VectorType] struct {
	Idxcfg  vectorindex.IndexConfig
	Tblcfg  vectorindex.IndexTableConfig
	Devices []int
}

func NewCagraSearch[T cuvs.VectorType](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, devices []int) *CagraSearch[T] {
	return &CagraSearch[T]{Idxcfg: idxcfg, Tblcfg: tblcfg, Devices: devices}
}

func (s *CagraSearch[T]) Search(sqlproc *sqlexec.SqlProcess, anyquery any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, errGPURequired
}

func (s *CagraSearch[T]) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	return errGPURequired
}

func (s *CagraSearch[T]) Load(sqlproc *sqlexec.SqlProcess) error {
	return errGPURequired
}

func (s *CagraSearch[T]) Destroy() {}

func (s *CagraSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {
	return errGPURequired
}
