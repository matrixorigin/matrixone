package wrapper

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
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
func (bat *batchWrapper2) IsVectorClosed(attr int) bool { return false }

// TODO: Should return a vector wrapper that cannot be closed
func (bat *batchWrapper2) GetReaderByAttr(attr int) dbi.IVectorReader {
	return bat.wrapped.GetReaderByAttr(attr)
}

func (bat *batchWrapper2) Close() error { return bat.closer.Close() }
