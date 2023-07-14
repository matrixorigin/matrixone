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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

const (
	memFileServiceBackend     = "MEM"
	diskFileServiceBackend    = "DISK"
	diskETLFileServiceBackend = "DISK-ETL"
	s3FileServiceBackend      = "S3"
	minioFileServiceBackend   = "MINIO"
)

func GetDefaultBackend() string { return diskFileServiceBackend }
func GetRawBackendByBackend(src string) string {
	if src == diskFileServiceBackend {
		return diskETLFileServiceBackend
	}
	return src
}

// Config fileService config
type Config struct {
	// Name name of fileservice, describe what an instance of fileservice is used for
	Name string `toml:"name"`
	// Backend fileservice backend. [MEM|DISK|DISK-ETL|S3|MINIO]
	Backend string `toml:"backend"`
	// S3 used to create fileservice using s3 as the backend
	S3 S3Config `toml:"s3"`
	// Cache specifies configs for cache
	Cache CacheConfig `toml:"cache"`
	// DataDir used to create fileservice using DISK as the backend
	DataDir string `toml:"data-dir"`
}

// NewFileServicesFunc creates a new *FileServices
type NewFileServicesFunc = func(defaultName string) (*FileServices, error)

// NewFileService create file service from config
func NewFileService(ctx context.Context, cfg Config, perfCounterSets []*perfcounter.CounterSet) (FileService, error) {
	if cfg.Name == "" {
		panic("empty name")
	}
	switch strings.ToUpper(cfg.Backend) {
	case memFileServiceBackend:
		return newMemFileService(cfg, perfCounterSets)
	case diskFileServiceBackend:
		return newDiskFileService(ctx, cfg, perfCounterSets)
	case diskETLFileServiceBackend:
		return newDiskETLFileService(cfg, perfCounterSets)
	case minioFileServiceBackend:
		return newMinioFileService(ctx, cfg, perfCounterSets)
	case s3FileServiceBackend:
		return newS3FileService(ctx, cfg, perfCounterSets)
	default:
		return nil, moerr.NewInternalErrorNoCtx("file service backend %s not implemented", cfg.Backend)
	}
}

func newMemFileService(cfg Config, perfCounters []*perfcounter.CounterSet) (FileService, error) {
	fs, err := NewMemoryFS(
		cfg.Name,
		cfg.Cache,
		perfCounters,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newDiskFileService(ctx context.Context, cfg Config, perfCounters []*perfcounter.CounterSet) (FileService, error) {
	if cfg.DataDir == "" {
		panic(fmt.Sprintf("empty data dir: %+v", cfg))
	}
	fs, err := NewLocalFS(
		ctx,
		cfg.Name,
		cfg.DataDir,
		cfg.Cache,
		perfCounters,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newDiskETLFileService(cfg Config, _ []*perfcounter.CounterSet) (FileService, error) {
	fs, err := NewLocalETLFS(
		cfg.Name,
		cfg.DataDir,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newMinioFileService(ctx context.Context, cfg Config, perfCounters []*perfcounter.CounterSet) (FileService, error) {
	fs, err := NewS3FSOnMinio(
		ctx,
		cfg.S3.SharedConfigProfile,
		cfg.Name,
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
		cfg.Cache,
		perfCounters,
		false,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newS3FileService(ctx context.Context, cfg Config, perfCounters []*perfcounter.CounterSet) (FileService, error) {
	fs, err := NewS3FS(
		ctx,
		cfg.S3.SharedConfigProfile,
		cfg.Name,
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
		cfg.Cache,
		perfCounters,
		false,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}
