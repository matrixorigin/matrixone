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
