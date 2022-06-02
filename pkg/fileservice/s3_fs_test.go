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

//go:build test_s3

package fileservice

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestS3FS(t *testing.T) {
	testFileService(t, func() FileService {

		var config S3Config
		content, err := os.ReadFile("s3.json")
		assert.Nil(t, err)
		err = json.Unmarshal(content, &config)
		assert.Nil(t, err)
		config.KeyPrefix = time.Now().Format("2006-01-02T15:04:05")

		fs, err := NewS3FS(config)
		assert.Nil(t, err)

		return fs
	})
}
