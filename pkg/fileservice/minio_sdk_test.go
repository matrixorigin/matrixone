// Copyright 2024 Matrix Origin
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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMinioSDK(t *testing.T) {
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)
	if config.Endpoint == "" {
		// no config
		t.Skip()
	}
	testObjectStorage(t, func(t *testing.T) *MinioSDK {
		storage, err := NewMinioSDK(context.Background(), ObjectStorageArguments{
			Name:      "test",
			KeyID:     config.APIKey,
			KeySecret: config.APISecret,
			Endpoint:  config.Endpoint,
			Region:    config.Region,
			Bucket:    config.Bucket,
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			RoleARN:   config.RoleARN,
		}, nil)
		assert.Nil(t, err)
		return storage
	})
}
