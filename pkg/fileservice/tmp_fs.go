// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var (
	tmpService *TmpFileService

	appConfigs = map[string]*AppConfig{}
)

func init() {
	appConfigs = make(map[string]*AppConfig)
}

func RegisterAppConfig(appConfig *AppConfig) {
	appConfigs[appConfig.Name] = appConfig
}

type TmpFileService struct {
	closed atomic.Bool
	FileService
	apps   map[string]*AppFS
	appsMu sync.RWMutex

	gcInterval time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

const (
	TmpFileGCInterval = time.Hour
)

func NewTmpFileService(name, rootPath string, gcInterval time.Duration) (*TmpFileService, error) {
	if tmpService != nil {
		return tmpService, nil
	}
	var etlfs FileService
	var err error
	if etlfs, err = NewLocalETLFS(name, rootPath); err != nil {
		return nil, err
	}

	service := &TmpFileService{
		FileService: etlfs,
		gcInterval:  gcInterval,
		apps:        make(map[string]*AppFS),
		appsMu:      sync.RWMutex{},
		wg:          sync.WaitGroup{},
	}
	var ctx context.Context
	ctx, service.cancel = context.WithCancel(context.Background())
	go service.tmpFileServiceGCTicker(ctx)
	tmpService = service
	service.init()
	return service, nil
}

func NewTestTmpFileService(name, rootPath string, gcInterval time.Duration) (*TmpFileService, error) {
	var etlfs FileService
	var err error
	if etlfs, err = NewLocalETLFS(name, rootPath); err != nil {
		return nil, err
	}

	service := &TmpFileService{
		FileService: etlfs,
		gcInterval:  gcInterval,
		apps:        make(map[string]*AppFS),
		appsMu:      sync.RWMutex{},
		wg:          sync.WaitGroup{},
	}
	var ctx context.Context
	ctx, service.cancel = context.WithCancel(context.Background())
	go service.tmpFileServiceGCTicker(ctx)
	tmpService = service
	service.init()
	return service, nil
}

func (fs *TmpFileService) GetOrCreateApp(appConfig *AppConfig) (*AppFS, error) {
	fs.appsMu.RLock()
	app, ok := fs.apps[appConfig.Name]
	fs.appsMu.RUnlock()
	if ok {
		return app, nil
	}
	fs.appsMu.Lock()
	defer fs.appsMu.Unlock()
	app, ok = fs.apps[appConfig.Name]
	if ok {
		return app, nil
	}
	logutil.Info(
		"fs.tmp.create.app",
		zap.String("name", appConfig.Name),
	)
	app = &AppFS{
		tmpFS:     fs,
		appConfig: appConfig,
	}
	fs.apps[appConfig.Name] = app
	return app, nil
}

func (fs *TmpFileService) getAllApps() []*AppFS {
	fs.appsMu.RLock()
	defer fs.appsMu.RUnlock()
	apps := make([]*AppFS, 0, len(fs.apps))
	for _, app := range fs.apps {
		apps = append(apps, app)
	}
	return apps
}
func (fs *TmpFileService) gc(ctx context.Context) {
	apps := fs.getAllApps()
	for _, appFS := range apps {
		appConfig := appFS.appConfig
		appPath := appConfig.Name
		entries := fs.FileService.List(ctx, appPath)
		gcedFiles := make([]string, 0)
		for entry, err := range entries {
			if err != nil {
				logutil.Warn(
					"fs.tmp.gc.failed",
					zap.String("app", appConfig.Name),
					zap.String("file", path.Join(appPath, entry.Name)),
					zap.Error(err),
				)
				continue
			}
			needGC, err := appFS.appConfig.GCFn(entry.Name, appFS)
			if err != nil {
				logutil.Warn(
					"fs.tmp.gc.failed",
					zap.String("app", appConfig.Name),
					zap.String("file", path.Join(appPath, entry.Name)),
					zap.Error(err),
				)
				continue
			}
			if needGC {
				gcedFiles = append(gcedFiles, path.Join(appPath, entry.Name))
			}
		}
		logutil.Info(
			"fs.tmp.gc",
			zap.String("app", appConfig.Name),
			zap.Strings("files", gcedFiles),
		)
	}
}

func (fs *TmpFileService) tmpFileServiceGCTicker(ctx context.Context) {
	logutil.Info(
		"fs.tmp.gc.start",
		zap.String("interval", fs.gcInterval.String()),
	)
	fs.wg.Add(1)
	defer fs.wg.Done()
	ticker := time.NewTicker(fs.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logutil.Info("fs.tmp.gc.stop")
			return

		case <-ticker.C:
			fs.gc(ctx)
		}
	}
}

func (fs *TmpFileService) Close(ctx context.Context) {
	if fs.closed.Load() {
		return
	}
	defer logutil.Info("fs.tmp.close")
	fs.closed.Store(true)
	fs.cancel()
	fs.wg.Wait()
	fs.FileService.Close(ctx)
}

func (fs *TmpFileService) init() {
	entries := fs.List(context.Background(), "")
	for entry, err := range entries {
		if err != nil {
			logutil.Warn(
				"fs.tmp.init.failed",
				zap.String("name", entry.Name),
				zap.Error(err),
			)
			continue
		}
		config, ok := appConfigs[entry.Name]
		if !ok {
			logutil.Warn(
				"fs.tmp.init.not.found",
				zap.String("name", entry.Name),
			)
			continue
		}
		fs.GetOrCreateApp(config)
	}
}
