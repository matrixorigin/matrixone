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
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage"
	base "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
)

var (
	WRITER_FACTORY = &WriterFactory{
		Builders: make(map[string]base.WriterBuilder),
	}
)

type WriterFactory struct {
	Opts     *storage.Options
	Dirname  string
	Builders map[string]base.WriterBuilder
}

func (wf *WriterFactory) Init(opts *storage.Options, dirname string) {
	wf.Opts = opts
	wf.Dirname = dirname
}

func (wf *WriterFactory) GetOpts() *storage.Options {
	return wf.Opts
}

func (wf *WriterFactory) GetDir() string {
	return wf.Dirname
}

func (wf *WriterFactory) RegisterBuilder(name string, wb base.WriterBuilder) {
	_, ok := wf.Builders[name]
	if ok {
		panic(fmt.Sprintf("Duplicate write %s found", name))
	}
	wf.Builders[name] = wb
}

func (wf *WriterFactory) MakeWriter(name string, ctx context.Context) base.Writer {
	wb, ok := wf.Builders[name]
	if !ok {
		return nil
	}
	return wb.Build(wf, ctx)
}
