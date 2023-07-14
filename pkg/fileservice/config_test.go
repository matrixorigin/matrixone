// Copyright 2023 Matrix Origin
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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetRawBackendByBackend(t *testing.T) {
	type args struct {
		src string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: memFileServiceBackend, args: args{src: memFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: diskFileServiceBackend, args: args{src: diskFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: diskETLFileServiceBackend, args: args{src: diskETLFileServiceBackend}, want: diskETLFileServiceBackend},
		{name: s3FileServiceBackend, args: args{src: s3FileServiceBackend}, want: s3FileServiceBackend},
		{name: minioFileServiceBackend, args: args{src: minioFileServiceBackend}, want: minioFileServiceBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetRawBackendByBackend(tt.args.src), "GetRawBackendByBackend(%v)", tt.args.src)
		})
	}
}
