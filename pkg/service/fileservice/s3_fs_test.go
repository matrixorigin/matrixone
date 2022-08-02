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
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestS3FS(t *testing.T) {

	var sharedConfig struct {
		Endpoint  string
		Region    string
		APIKey    string
		APISecret string
		Bucket    string
		KeyPrefix string
	}
	content, err := os.ReadFile("s3.json")
	if os.IsNotExist(err) {
		// no s3.json, skip tests
		return
	}
	assert.Nil(t, err)
	err = json.Unmarshal(content, &sharedConfig)
	assert.Nil(t, err)

	os.Setenv("AWS_REGION", sharedConfig.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", sharedConfig.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", sharedConfig.APISecret)

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func() FileService {

			config := sharedConfig
			config.KeyPrefix = time.Now().Format("2006-01-02.15:04:05.000000")

			fs, err := NewS3FS(
				config.Endpoint,
				config.Bucket,
				config.KeyPrefix,
			)
			assert.Nil(t, err)

			return fs
		})
	})

	t.Run("cache", func(t *testing.T) {
		testCache(t, func() FileService {

			config := sharedConfig
			config.KeyPrefix = time.Now().Format("2006-01-02.15:04:05.000000")

			fs, err := NewS3FS(
				config.Endpoint,
				config.Bucket,
				config.KeyPrefix,
			)
			assert.Nil(t, err)

			return fs
		})
	})

	t.Run("list root", func(t *testing.T) {
		fs, err := NewS3FS(
			sharedConfig.Endpoint,
			sharedConfig.Bucket,
			"",
		)
		assert.Nil(t, err)
		ctx := context.Background()
		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.True(t, len(entries) > 0)
	})

}

func TestS3FSMinioServer(t *testing.T) {
	t.Skip() //TODO

	exePath, err := exec.LookPath("minio")
	if errors.Is(err, exec.ErrNotFound) {
		// minio not found in machine
		return
	}

	// start minio
	cmd := exec.Command(exePath,
		"server",
		t.TempDir(),
	)
	cmd.Env = append(os.Environ(),
		"MINIO_ROOT_USER=test",
		"MINIO_ROOT_PASSWORD=test",
		"MINIO_SITE_NAME=test",
		"MINIO_SITE_REGION=test",
	)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err = cmd.Start()
	assert.Nil(t, err)
	defer func() {
		_ = cmd.Process.Kill()
	}()

	// run test
	t.Run("file service", func(t *testing.T) {
		testFileService(t, func() FileService {

			fs, err := NewS3FSOnMinio(
				"http://localhost:9000",
				"test",
				"",
			)
			assert.Nil(t, err)

			return fs
		})
	})

}
