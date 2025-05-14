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

package model

import (
	"context"
	"path"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type CancelableJob interface {
	Start()
	Stop()
}

const (
	TmpFileDir = "tmp"

	TmpFileGCInterval = time.Hour
)

type TmpFileService struct {
	fs   fileservice.FileService
	apps map[string]*AppConfig

	gcJob CancelableJob
}

type AppConfig struct {
	Name           string
	GCFn         func(filePath string, fs fileservice.FileService)
	TTL            time.Duration
	NameFunc       func(createTime time.Time) string
	DecodeNameFunc func(name string) (createTime time.Time, err error)
}

func NewTmpFileService(fs fileservice.FileService, jobFactory func(fn func(context.Context)) CancelableJob) *TmpFileService {
	service := &TmpFileService{
		fs:   fs,
		apps: make(map[string]*AppConfig),
	}
	service.gcJob = jobFactory(service.gc)
	return service
}
func (fs *TmpFileService) Start() {
	fs.gcJob.Start()
}
func (fs *TmpFileService) Stop() {
	fs.gcJob.Stop()
}
func (fs *TmpFileService) AddApp(appConfig *AppConfig) error {
	fs.apps[appConfig.Name] = appConfig
	return nil
}


func (fs *TmpFileService) gc(ctx context.Context) {
	for _, appConfig := range fs.apps {
		appPath := path.Join(TmpFileDir, appConfig.Name)
		entries := fs.fs.List(ctx, appPath)
		gcedFiles := make([]string, 0)
		for entry, err := range entries {
			if err != nil {
				logutil.Warnf("TMP-FILE-GC failed, err: %v", err)
				continue
			}
			createTime, err := appConfig.DecodeNameFunc(entry.Name)
			if err != nil {
				logutil.Warnf("TMP-FILE-GC gc failed, err: %v", err)
				continue
			}
			if time.Since(createTime) > appConfig.TTL {
				filePath := path.Join(appPath, entry.Name)
				appConfig.GCFn(filePath, fs.fs)
				gcedFiles = append(gcedFiles, filePath)
			}
		}
		logutil.Info(
			"TMP-FILE-GC",
			zap.String("app", appConfig.Name),
			zap.String("gced_files", strings.Join(gcedFiles, ",")),
		)
	}
}

func (fs *TmpFileService) Write(
	ctx context.Context, appName string, ioVector fileservice.IOVector,
) (fullName string, err error) {
	createTime := time.Now()
	fileName := fs.apps[appName].NameFunc(createTime)
	appPath := path.Join(TmpFileDir, appName)
	fullName = path.Join(appPath, fileName)
	ioVector.FilePath = fullName
	if err = fs.fs.Write(ctx, ioVector); err != nil {
		return
	}
	return
}
func (fs *TmpFileService) Read(ctx context.Context, appName string, ioVector *fileservice.IOVector) (err error) {
	return fs.fs.Read(ctx, ioVector)
}
func (fs *TmpFileService) Delete(ctx context.Context,filePath string) (err error) {
	return fs.fs.Delete(ctx, filePath)
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