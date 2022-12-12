package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type GcTask struct {
	object []string
	fs     *objectio.ObjectFS
}

func NewGcTask(fs *objectio.ObjectFS, names []string) GcTask {
	return GcTask{
		object: names,
		fs:     fs,
	}
}

func (g *GcTask) ExecDelete() error {
	if len(g.object) == 0 {
		return nil
	}

	err := g.fs.DelFiles(context.Background(), g.object)
	if err != nil {
		return err
	}

	return nil
}
