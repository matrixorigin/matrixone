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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	memFileServiceBackend   = "MEM"
	diskFileServiceBackend  = "DISK"
	s3FileServiceBackend    = "S3"
	minioFileServiceBackend = "MINIO"
)

// Config fileService config
type Config struct {
	// Name name of fileservice, describe what an instance of fileservice is used for
	Name string `toml:"name"`
	// Backend fileservice backend. [MEM|DISK|S3|MINIO]
	Backend string `toml:"backend"`
	// CacheMemCapacityBytes cache memory capacity bytes
	CacheMemCapacityBytes toml.ByteSize `toml:"cache-mem-capacity-bytes"`
	// S3 used to create fileservice using s3 as the backend
	S3 S3Config `toml:"s3"`
	// DataDir used to create fileservice using DISK as the backend
	DataDir string `toml:"data-dir"`
}

// FileServiceFactory returns an instance of fileservice by name. When starting a MO node, multiple
// FileServiceConfig configurations are specified in the configuration file for use in specific scenarios.
// The corresponding instance is obtained according to Name.
type FileServiceFactory func(name string) (FileService, error)

// NewFileService create file service from config
func NewFileService(cfg Config) (FileService, error) {
	switch strings.ToUpper(cfg.Backend) {
	case memFileServiceBackend:
		return newMemFileService(cfg)
	case diskFileServiceBackend:
		return newDiskFileService(cfg)
	case minioFileServiceBackend:
		return newMinioFileService(cfg)
	case s3FileServiceBackend:
		return newS3FileService(cfg)
	default:
		return nil, fmt.Errorf("file service backend %s not implemented", cfg.Backend)
	}
}

func newMemFileService(cfg Config) (FileService, error) {
	fs, err := NewMemoryFS()
	if err != nil {
		return nil, err
	}
	return NewCacheFS(fs, int(cfg.CacheMemCapacityBytes))
}

func newDiskFileService(cfg Config) (FileService, error) {
	fs, err := NewLocalFS(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return NewCacheFS(fs, int(cfg.CacheMemCapacityBytes))
}

func newMinioFileService(cfg Config) (FileService, error) {
	fs, err := NewS3FSOnMinio(
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
	)
	if err != nil {
		return nil, err
	}
	return NewCacheFS(fs, int(cfg.CacheMemCapacityBytes))
}

func newS3FileService(cfg Config) (FileService, error) {
	fs, err := NewS3FS(
		cfg.S3.Endpoint,
		cfg.S3.Bucket,
		cfg.S3.KeyPrefix,
	)
	if err != nil {
		return nil, err
	}
	return NewCacheFS(fs, int(cfg.CacheMemCapacityBytes))
}
