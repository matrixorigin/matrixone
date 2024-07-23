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

package types

import (
	"reflect"
	"testing"
)

func TestParseDatalink(t *testing.T) {

	type testCase struct {
		name          string
		data          string
		wantUrlParts  []string
		wantUrlParams map[string]string
		wantFileExt   string
	}
	tests := []testCase{
		{
			name:         "Test1 - S3",
			data:         "s3://bucket/prefix/path/a.txt?offset=0&size=10",
			wantUrlParts: []string{"s3", "", "bucket", "/prefix/path/a.txt"},
			wantUrlParams: map[string]string{
				"offset": "0",
				"size":   "10",
			},
			wantFileExt: ".txt",
		},
		{
			name:         "Test2 - STAGE",
			data:         "stage://database/stage_name/a.txt?offset=0&size=10",
			wantUrlParts: []string{"stage", "", "database", "/stage_name/a.txt"},
			wantUrlParams: map[string]string{
				"offset": "0",
				"size":   "10",
			},
			wantFileExt: ".txt",
		},
		{
			name:         "Test3 - STAGE with Current Database",
			data:         "stage:///stage_name/a.txt?offset=0&size=10",
			wantUrlParts: []string{"stage", "", "", "/stage_name/a.txt"},
			wantUrlParams: map[string]string{
				"offset": "0",
				"size":   "10",
			},
			wantFileExt: ".txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, got3, err := ParseDatalink(tt.data)
			if err != nil {
				t.Errorf("ParseDatalink() error = %v", err)
			}
			if !reflect.DeepEqual(got1, tt.wantUrlParts) {
				t.Errorf("ParseDatalink() = %v, want %v", got1, tt.wantUrlParts)
			}
			if !reflect.DeepEqual(got2, tt.wantUrlParams) {
				t.Errorf("ParseDatalink() = %v, want %v", got2, tt.wantUrlParams)
			}
			if !reflect.DeepEqual(got3, tt.wantFileExt) {
				t.Errorf("ParseDatalink() = %v, want %v", got3, tt.wantFileExt)
			}
		})
	}
}

func TestConvertS3UrlToFsS3Url(t *testing.T) {
	type args struct {
		s3Url string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Test1 - S3 URL",
			args: args{
				s3Url: "s3://vector_bucket/prefix/path/img.png?region=us-east-2&key=xxx&secret=xxx&offset=0&size=-1",
			},
			want:    "s3,,us-east-2,vector_bucket,xxx,xxx,prefix/path:img.png",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertS3DatalinkToFsS3Url(tt.args.s3Url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertS3DatalinkToFsS3Url() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConvertS3DatalinkToFsS3Url() got = %v, want %v", got, tt.want)
			}
		})
	}
}
