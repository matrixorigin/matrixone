// Copyright 2023 Matrix Origin
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

package etl

import (
	"bytes"
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// var _ table.RowWriter = (*BufWriter)(nil)
var _ io.WriteCloser = (*BufWriter)(nil)

type BufWriter struct {
	ctx    context.Context
	writer io.Writer
	buf    *bytes.Buffer
}

func NewBufWriter(ctx context.Context, writer io.Writer) *BufWriter {
	w := &BufWriter{
		ctx:    ctx,
		writer: writer,
		buf:    bytes.NewBuffer(make([]byte, 0, mpool.MB)),
	}
	return w
}

func (w *BufWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *BufWriter) Close() error {
	_, err := w.writer.Write(w.buf.Next(w.buf.Len()))
	w.buf = nil
	return err
}
