package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type GCTask struct {
	object []string
	fs     *objectio.ObjectFS
}

func NewGCTask(fs *objectio.ObjectFS, names []string) GCTask {
	return GCTask{
		object: names,
		fs:     fs,
	}
}

func (g *GCTask) ExecDelete() error {
	if len(g.object) == 0 {
		return nil
	}

	err := g.fs.DelFiles(context.Background(), g.object)
	if err != nil {
		return err
	}

	return nil
}
