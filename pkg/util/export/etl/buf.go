package etl

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
)

// var _ table.RowWriter = (*BufWriter)(nil)
var _ io.WriteCloser = (*BufWriter)(nil)

type BufWriter struct {
	ctx    context.Context
	writer io.Writer

	buf       *bytes.Buffer
	formatter *csv.Writer
}

func NewBufWriter(ctx context.Context, writer io.Writer) *BufWriter {
	w := &BufWriter{
		ctx:       ctx,
		writer:    writer,
		buf:       nil,
		formatter: nil,
	}
	return w
}

func (w *BufWriter) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *BufWriter) Close() error {
	_, err := w.writer.Write(w.buf.Next(w.buf.Len()))
	return err
}
