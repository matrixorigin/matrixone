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

package moengine

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

func newBlock(h handle.Block) *txnBlock {
	return &txnBlock{
		handle: h,
	}
}

func (blk *txnBlock) Read(attrs []string, compressed []*bytes.Buffer, deCompressed []*bytes.Buffer) (*batch.Batch, error) {
	var view *model.ColumnView
	var err error
	bat := batch.New(true, attrs)
	bat.Vecs = make([]*vector.Vector, len(attrs))
	for i, attr := range attrs {
		view, err = blk.handle.GetColumnDataByName(attr, deCompressed[i])
		if err != nil {
			if view != nil {
				view.Close()
			}
			return nil, err
		}
		view.ApplyDeletes()
		if view.GetData().Allocated() > 0 {
			bat.Vecs[i] = containers.CopyToMoVec(view.GetData())
		} else {
			bat.Vecs[i] = containers.UnmarshalToMoVec(view.GetData())
		}
		view.Close()
	}
	return bat, nil
}
