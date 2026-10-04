// Copyright 2026 Matrix Origin
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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeLegacyRemoteS3Args(t *testing.T) {
	remoteArgs := "bucket=test,endpoint=http://minio:9000,region=us-east-1,key-prefix=data,key-id=id,key-secret=secret"
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "separate legacy argument",
			args: []string{"ckp", "list", "--backend=S3", "--s3", remoteArgs},
			want: []string{"ckp", "list", "--backend=S3", "--remote-s3", remoteArgs},
		},
		{
			name: "joined legacy argument",
			args: []string{"ckp", "dump", "--s3=" + remoteArgs},
			want: []string{"ckp", "dump", "--remote-s3=" + remoteArgs},
		},
		{
			name: "bucket with default AWS settings",
			args: []string{"ckp", "list", "--s3", "bucket=test"},
			want: []string{"ckp", "list", "--remote-s3", "bucket=test"},
		},
		{
			name: "local S3FS selector",
			args: []string{"ckp", "list", "--s3", "/tmp/mo-data"},
			want: []string{"ckp", "list", "--s3", "/tmp/mo-data"},
		},
		{
			name: "current remote argument",
			args: []string{"ckp", "list", "--remote-s3", remoteArgs},
			want: []string{"ckp", "list", "--remote-s3", remoteArgs},
		},
		{
			name: "other command",
			args: []string{"object", "stat", "--s3", remoteArgs},
			want: []string{"object", "stat", "--s3", remoteArgs},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := append([]string(nil), test.args...)
			require.Equal(t, test.want, normalizeLegacyRemoteS3Args(test.args))
			require.Equal(t, original, test.args)
		})
	}
}
