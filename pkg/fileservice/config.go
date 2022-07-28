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
	s3Backend    = "S3"
	minioBackend = "MINIO"
)

// Config config to create fileservice
type Config struct {
	// S3 used to create fileservice using s3 as the backend
	S3 S3Config `toml:"s3"`
	// DataDir used to create fileservice using DISK as the backend
	DataDir string `toml:"data-dir"`
}

// NewService create DN used fileservice
func NewServiceForDN(cfg Config) (FileService, error) {
	return newS3BasedService(cfg.S3)
}

// NewService create CN used fileservice
func NewServiceForCN(cfg Config) (FileService, error) {
	return newS3BasedService(cfg.S3)
}

func newS3BasedService(s3 S3Config) (FileService, error) {
	switch s3.Backend {
	case minioBackend:
		return newMinioFileService(s3)
	case s3Backend:
		return newS3FileService(s3)
	default:
		return nil, fmt.Errorf("not implment for %s", s3.Backend)
	}
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
