package etl

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"io"
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
