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
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestS3FS(t *testing.T) {

	var sharedConfig S3Config
	content, err := os.ReadFile("s3.json")
	if os.IsNotExist(err) {
		fmt.Printf("s3.json not found, skip s3 test\n")
		return // not using t.Skip because the CI does not know SKIP cases
	}
	assert.Nil(t, err)
	err = json.Unmarshal(content, &sharedConfig)
	assert.Nil(t, err)

	os.Setenv("AWS_REGION", sharedConfig.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", sharedConfig.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", sharedConfig.APISecret)

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
}
