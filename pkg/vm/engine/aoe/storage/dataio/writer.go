package dio

import (
	"context"
	"fmt"
	e "matrixone/pkg/vm/engine/aoe/storage"
	base "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	// log "github.com/sirupsen/logrus"
)

var (
	WRITER_FACTORY = &WriterFactory{
		Builders: make(map[string]base.WriterBuilder),
	}
)

type WriterFactory struct {
	Opts     *e.Options
	Dirname  string
	Builders map[string]base.WriterBuilder
}

func (wf *WriterFactory) Init(opts *e.Options, dirname string) {
	wf.Opts = opts
	wf.Dirname = dirname
}

func (wf *WriterFactory) GetOpts() *e.Options {
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
