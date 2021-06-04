package dio

import (
	"context"
	base "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	// log "github.com/sirupsen/logrus"
)

type DefaultIOFactory struct {
	base.ICleanerFactory
	base.IReaderFactory
	base.IWriterFactory
}

type DefaultIO struct {
	base.Writer
	base.Reader
	base.Cleaner
}

func NewIOFactory(w base.IWriterFactory, r base.IReaderFactory, c base.ICleanerFactory) base.IOFactory {
	f := &DefaultIOFactory{
		ICleanerFactory: c,
		IWriterFactory:  w,
		IReaderFactory:  r,
	}
	return f
}

func (f *DefaultIOFactory) MakeIO(writerName, readerName, cleanerName string, ctx context.Context) base.IO {
	w := f.MakeWriter(writerName, ctx)
	if w == nil {
		return nil
	}
	r := f.MakeReader(readerName, ctx)
	if r == nil {
		return nil
	}
	c := f.MakeCleaner(cleanerName, ctx)
	if c == nil {
		return nil
	}

	dio := &DefaultIO{
		Writer:  w,
		Reader:  r,
		Cleaner: c,
	}
	return dio
}
