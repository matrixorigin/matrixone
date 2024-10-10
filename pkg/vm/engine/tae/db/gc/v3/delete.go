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

package gc

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type GCWorker struct {
	sync.RWMutex
	// objects is list of files that can be GC
	objects []string

	// The status of GCWorker, only one delete worker can be running
	state CleanerState

	cleaner *checkpointCleaner
	fs      *objectio.ObjectFS
}

func NewGCWorker(fs *objectio.ObjectFS, cleaner *checkpointCleaner) *GCWorker {
	return &GCWorker{
		state:   Idle,
		fs:      fs,
		cleaner: cleaner,
	}
}

func (g *GCWorker) Start() bool {
	g.Lock()
	defer g.Unlock()
	if g.state == Running {
		return false
	}
	g.state = Running
	return true
}

func (g *GCWorker) Idle() {
	g.Lock()
	defer g.Unlock()
	g.state = Idle
}

func (g *GCWorker) resetObjects() {
	g.objects = make([]string, 0)
}

func (g *GCWorker) ExecDelete(ctx context.Context, names []string) error {
	g.Lock()
	g.objects = append(g.objects, names...)
	if len(g.objects) == 0 {
		g.state = Idle
		g.Unlock()
		return nil
	}
	deleteCount := len(g.objects)
	g.Unlock()
	now := time.Now()
	defer func() {
		logutil.Info("[DB GC] exec delete files",
			zap.Int("file count", deleteCount),
			zap.String("time cost", time.Since(now).String()))
	}()
	logutil.Infof("[DB GC] files to delete: %v", g.objects)
	err := g.fs.DelFiles(ctx, g.objects)
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
