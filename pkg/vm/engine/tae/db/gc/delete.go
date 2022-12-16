package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"os"
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
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
