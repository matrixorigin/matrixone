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
)

var (
	memFileServiceBackend   = "MEM"
	diskFileServiceBackend  = "DISK"
	s3FileServiceBackend    = "S3"
	minioFileServiceBackend = "MINIO"
)

// Config config to create fileservice
type Config struct {
	// Backend file service backend implementation. [Mem|DISK|S3|MINIO]. Default is DISK.
	Backend string `toml:"backend"`
	// S3 used to create fileservice using s3 as the backend
	S3 S3Config `toml:"s3"`
	// DataDir used to create fileservice using DISK as the backend
	DataDir string `toml:"data-dir"`
}

// NewService create fileservice by config
func NewService(cfg Config) (FileService, error) {
	switch cfg.Backend {
	case memFileServiceBackend:
		return newMemFileService()
	case diskFileServiceBackend:
		return newDiskFileService(cfg.Backend)
	case minioFileServiceBackend:
		return newMinioFileService(cfg.S3)
	case s3FileServiceBackend:
		return newS3FileService(cfg.S3)
	default:
		return nil, fmt.Errorf("not implment for %s", cfg.Backend)
	}
}

func newMemFileService() (FileService, error) {
	return NewMemoryFS()
}

func newDiskFileService(dir string) (FileService, error) {
	return NewLocalFS(dir)
}

func newMinioFileService(s3 S3Config) (FileService, error) {
	return NewS3FSOnMinio(
		s3.Endpoint,
		s3.Bucket,
		s3.KeyPrefix,
	)
}

func newS3FileService(s3 S3Config) (FileService, error) {
	return NewS3FS(
		s3.Endpoint,
		s3.Bucket,
		s3.KeyPrefix,
	)
}
