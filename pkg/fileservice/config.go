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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	memFileServiceBackend     = "MEM"
	diskFileServiceBackend    = "DISK"
	diskETLFileServiceBackend = "DISK-ETL"
	s3FileServiceBackend      = "S3"
	minioFileServiceBackend   = "MINIO"
)

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
func NewFileService(cfg Config) (FileService, error) {
	switch strings.ToUpper(cfg.Backend) {
	case memFileServiceBackend:
		return newMemFileService(cfg)
	case diskFileServiceBackend:
		return newDiskFileService(cfg)
	case diskETLFileServiceBackend:
		return newDiskETLFileService(cfg)
	case minioFileServiceBackend:
		return newMinioFileService(cfg)
	case s3FileServiceBackend:
		return newS3FileService(cfg)
	default:
		return nil, moerr.NewInternalError("file service backend %s not implemented", cfg.Backend)
	}
}

func newMemFileService(cfg Config) (FileService, error) {
	fs, err := NewMemoryFS(cfg.Name)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newDiskFileService(cfg Config) (FileService, error) {
	fs, err := NewLocalFS(
		cfg.Name,
		cfg.DataDir,
		int64(cfg.Cache.MemoryCapacity),
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newDiskETLFileService(cfg Config) (FileService, error) {
	fs, err := NewLocalETLFS(
		cfg.Name,
		cfg.DataDir,
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newMinioFileService(cfg Config) (FileService, error) {
	fs, err := NewS3FSOnMinio(
		cfg.S3.SharedConfigProfile,
		cfg.Name,
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
		int64(cfg.Cache.MemoryCapacity),
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func newS3FileService(cfg Config) (FileService, error) {
	fs, err := NewS3FS(
		cfg.S3.SharedConfigProfile,
		cfg.Name,
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
		int64(cfg.Cache.MemoryCapacity),
	)
	if err != nil {
		return nil, err
	}
	return fs, nil
}
