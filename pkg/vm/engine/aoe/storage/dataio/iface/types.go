package iface

import (
	"context"
	e "matrixone/pkg/vm/engine/aoe/storage"
)

type Reader interface {
	Load() error
}

type Writer interface {
	Flush() error
}

type Cleaner interface {
	Clean() error
}

type IReaderFactory interface {
	Init(opts *e.Options, dirname string)
	RegisterBuilder(name string, wb ReaderBuilder)
	MakeReader(name string, ctx context.Context) Reader
	GetOpts() *e.Options
	GetDir() string
}

type IWriterFactory interface {
	Init(opts *e.Options, dirname string)
	RegisterBuilder(name string, wb WriterBuilder)
	MakeWriter(name string, ctx context.Context) Writer
	GetOpts() *e.Options
	GetDir() string
}

type ICleanerFactory interface {
	RegisterBuilder(name string, cb CleanerBuilder)
	MakeCleaner(name string, ctx context.Context) Cleaner
}

type ReaderBuilder interface {
	Build(rf IReaderFactory, ctx context.Context) Reader
}

type WriterBuilder interface {
	Build(wf IWriterFactory, ctx context.Context) Writer
}

type CleanerBuilder interface {
	Build(cf ICleanerFactory, ctx context.Context) Cleaner
}

type IO interface {
	Writer
	Reader
	Cleaner
}

type IOFactory interface {
	MakeIO(writerName, readerName, cleanerName string, ctx context.Context) IO
}
