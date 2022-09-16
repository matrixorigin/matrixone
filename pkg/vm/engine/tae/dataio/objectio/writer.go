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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
)

type Writer struct {
	writer objectio.Writer
	fs     *ObjectFS
}

func NewWriter(fs *ObjectFS) *Writer {
	return &Writer{
		fs: fs,
	}
}

func (w *Writer) WriteBlock(
	id *common.ID,
	columns *containers.Batch) (block objectio.BlockObject, err error) {
	name := EncodeBlkName(id)
	writer, err := objectio.NewObjectWriter(name, w.fs.service)
	if err != nil {
		return
	}
	w.writer = writer
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = moengine.CopyToMoVectors(columns.Vecs)
	block, err = w.writer.Write(bat)
	return
}

func (w *Writer) WriteIndex(
	block objectio.BlockObject,
	index objectio.IndexData) (err error) {
	w.writer.WriteIndex(block, index)
	return
}
