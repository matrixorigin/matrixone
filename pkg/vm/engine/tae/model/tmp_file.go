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

package ioutil

import (
	"context"
	"go.uber.org/zap"
	"path"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

const (
	TmpFileDir = "tmp"

	TmpFileGCInterval = time.Hour
)

type TmpFileService struct {
	fs   fileservice.FileService
	apps map[string]*AppConfig

	gcJob *tasks.CancelableJob
}

type AppConfig struct {
	name           string
	gcFunc         func(filePath string, fs fileservice.FileService)
	TTL            time.Duration
	nameFunc       func(createTime time.Time) string
	decodeNameFunc func(name string) (createTime time.Time, err error)
}

func NewTmpFileService(fs fileservice.FileService, gcInterval time.Duration) *TmpFileService {
	service := &TmpFileService{
		fs:   fs,
		apps: make(map[string]*AppConfig),
	}
	service.gcJob = tasks.NewCancelableCronJob(
		"TMP-FILE-GC",
		gcInterval,
		service.gc,
		true,
		1,
	)
	return service
}
func (fs *TmpFileService) Start() {
	fs.gcJob.Start()
}
func (fs *TmpFileService) Stop() {
	fs.gcJob.Stop()
}
func (fs *TmpFileService) Write(
	ctx context.Context, appName string, ioVector fileservice.IOVector,
) (fullName string, err error) {
	createTime := time.Now()
	fileName := fs.apps[appName].nameFunc(createTime)
	appPath := path.Join(TmpFileDir, appName)
	fullName = path.Join(appPath, fileName)
	ioVector.FilePath = fullName
	if err = fs.fs.Write(ctx, ioVector); err != nil {
		return
	}
	return
}

func (fs *TmpFileService) AddApp(appConfig *AppConfig) error {
	fs.apps[appConfig.name] = appConfig
	return nil
}

func (fs *TmpFileService) ListFiles(ctx context.Context, appName string) ([]string, error) {
	appPath := path.Join(TmpFileDir, appName)
	entries := fs.fs.List(ctx, appPath)
	files := make([]string, 0)
	for entry, err := range entries {
		if err != nil {
			return nil, err
		}
		files = append(files, path.Join(appPath, entry.Name))
	}
	return files, nil
}

func (fs *TmpFileService) gc(ctx context.Context) {
	for _, appConfig := range fs.apps {
		appPath := path.Join(TmpFileDir, appConfig.name)
		entries := fs.fs.List(ctx, appPath)
		gcedFiles := make([]string, 0)
		for entry, err := range entries {
			if err != nil {
				logutil.Warnf("TMP-FILE-GC failed, err: %v", err)
				continue
			}
			createTime, err := appConfig.decodeNameFunc(entry.Name)
			if err != nil {
				logutil.Warnf("TMP-FILE-GC gc failed, err: %v", err)
				continue
			}
			if time.Since(createTime) > appConfig.TTL {
				filePath := path.Join(appPath, entry.Name)
				appConfig.gcFunc(filePath, fs.fs)
				gcedFiles = append(gcedFiles, filePath)
			}
		}
		logutil.Info(
			"TMP-FILE-GC",
			zap.String("app", appConfig.name),
			zap.Int("gced_files", len(gcedFiles)),
		)
	}
}
