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
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	const text = `
bucket = "mo-test"
endpoint = "http://minio:9000"
key-prefix = "server/data"
cert-files = ['/etc/ssl/cert.pem']
  `
	var config Config
	_, err := toml.Decode(text, &config.S3)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(config.S3.CertFiles))
}

func TestNewFileservice(t *testing.T) {
	for _, backend := range []string{
		memFileServiceBackend,
		diskFileServiceBackend,
		diskETLFileServiceBackend,
		minioFileServiceBackend,
		s3FileServiceBackend,
	} {
		fs, err := NewFileService(
			context.Background(),
			Config{
				Name:    "test",
				Backend: backend,
				DataDir: t.TempDir(),
				S3: ObjectStorageArguments{
					Endpoint: "disk",
					Bucket:   t.TempDir(),
				},
			},
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		fs.Close()
	}
}
