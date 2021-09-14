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
