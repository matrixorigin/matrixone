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

package ivfpq

import (
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// IvfpqBuild is a dummy placeholder for non-GPU builds.
type IvfpqBuild[T cuvs.VectorType] struct{}

func NewIvfpqBuild[T cuvs.VectorType](
	uid string,
	idxcfg vectorindex.IndexConfig,
	tblcfg vectorindex.IndexTableConfig,
	nthread uint32,
	devices []int,
) (*IvfpqBuild[T], error) {
	return nil, errGPURequired
}

func (b *IvfpqBuild[T]) AddFloat(id int64, vec []float32) error {
	return errGPURequired
}

func (b *IvfpqBuild[T]) ToInsertSql(ts int64) ([]string, error) {
	return nil, errGPURequired
}

func (b *IvfpqBuild[T]) Destroy() error {
	return errGPURequired
}

func (b *IvfpqBuild[T]) GetIndexes() []*IvfpqModel[T] {
	return nil
}
