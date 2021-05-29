package dio

import (
	// e "matrixone/pkg/vm/engine/aoe/storage"
	"context"
	"fmt"
	base "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	// log "github.com/sirupsen/logrus"
)

type emptyCleaner int

func (*emptyCleaner) Clean() error {
	return nil
}

var (
	cleaner = new(emptyCleaner)
)

var (
	CLEANER_FACTORY = &CleanerFactory{
		Builders: make(map[string]base.CleanerBuilder),
	}
)

type CleanerFactory struct {
	// Opts     *e.Options
	// Dirname  string
	Builders map[string]base.CleanerBuilder
}

func (cf *CleanerFactory) RegisterBuilder(name string, cb base.CleanerBuilder) {
	_, ok := cf.Builders[name]
	if ok {
		panic(fmt.Sprintf("Duplicate reader %s found", name))
	}
	cf.Builders[name] = cb
}

func (cf *CleanerFactory) MakeCleaner(name string, ctx context.Context) base.Cleaner {
	cb, ok := cf.Builders[name]
	if !ok {
		return nil
	}
	return cb.Build(cf, ctx)
}
