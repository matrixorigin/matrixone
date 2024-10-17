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
	"encoding/csv"
	"os"
	"strings"
	"testing"
)

func TestAwsSdkv2QCloud(t *testing.T) {

	argStr := os.Getenv("TEST_S3FS_QCLOUD")
	if argStr == "" {
		t.SkipNow()
	}
	reader := csv.NewReader(strings.NewReader(argStr))
	argStrs, err := reader.Read()
	if err != nil {
		t.Skipf("bad S3FS test spec: %v", argStr)
	}
	var args ObjectStorageArguments
	if err := args.SetFromString(argStrs); err != nil {
		t.Skipf("bad S3FS test spec: %v", argStr)
	}

	t.Run("object storage", func(t *testing.T) {
		testObjectStorage(t, func(t *testing.T) *AwsSDKv2 {
			ret, err := NewAwsSDKv2(
				context.Background(),
				args,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			return ret
		})
	})

}
