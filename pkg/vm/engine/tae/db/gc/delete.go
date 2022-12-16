package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"sync"
)

type GCTask struct {
	sync.RWMutex
	objects []string
	state   CleanerState
	fs      *objectio.ObjectFS
}

func NewGCTask(fs *objectio.ObjectFS) *GCTask {
	return &GCTask{
		state: Idle,
		fs:    fs,
	}
}

func (g *GCTask) GetState() CleanerState {
	g.RLock()
	defer g.RUnlock()
	return g.state
}

func (g *GCTask) resetObjects() {
	g.objects = make([]string, 0)
}

func (g *GCTask) ExecDelete(names []string) error {
	g.Lock()
	g.state = Running
	g.objects = append(g.objects, names...)
	g.Unlock()
	if len(g.objects) == 0 {
		return nil
	}

	err := g.fs.DelFiles(context.Background(), g.objects)
	g.Lock()
	defer g.Unlock()
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		g.state = Idle
		return err
	}
	g.resetObjects()
	g.state = Idle
	return nil
}
