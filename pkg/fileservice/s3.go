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

type S3Config struct {
	// Backend s3 backend aws s3 or minio. [S3|MINIO]
	Backend  string `toml:"backend"`
	Endpoint string `toml:"endpoint"`
	Bucket   string `toml:"bucket"`
	// KeyPrefix enables multiple fs instances in one bucket
	KeyPrefix string `toml:"key-prefix"`
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content
