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

package wrapper

import (
	"io"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
)

type batchWrapper2 struct {
	closer  io.Closer
	wrapped batch.IBatch
}

func NewBatch2(closer io.Closer, wrapped batch.IBatch) *batchWrapper2 {
	w := &batchWrapper2{
		closer:  closer,
		wrapped: wrapped,
	}
	return w
}

func (bat *batchWrapper2) IsReadonly() bool             { return bat.wrapped.IsReadonly() }
func (bat *batchWrapper2) Length() int                  { return bat.wrapped.Length() }
func (bat *batchWrapper2) GetAttrs() []int              { return bat.wrapped.GetAttrs() }
func (bat *batchWrapper2) CloseVector(attr int) error   { return nil }
func (bat *batchWrapper2) IsVectorClosed(attr int) (bool, error) { return false, nil }

// TODO: Should return a vector wrapper that cannot be closed
func (bat *batchWrapper2) GetReaderByAttr(attr int) (dbi.IVectorReader, error) {
	reader, err := bat.wrapped.GetReaderByAttr(attr)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (bat *batchWrapper2) Close() error { return bat.closer.Close() }
