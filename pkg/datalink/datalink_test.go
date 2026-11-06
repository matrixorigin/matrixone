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

package datalink

import (
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestParseDatalink(t *testing.T) {

	type testCase struct {
		name          string
		data          string
		wantMoUrl     string
		wantUrlParams []int64
	}
	tests := []testCase{
		{
			name:          "Test1 - File",
			data:          "file:///a/b/c/1.txt",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{0, -1},
		},
		{
			name:          "Test2 - File",
			data:          "file:///a/b/c/1.txt?offset=1&size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{1, 2},
		},
		{
			name:          "Test3 - File",
			data:          "file:///a/b/c/1.txt?offset=1",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{1, -1},
		},
		{
			name:          "Test4 - File",
			data:          "file:///a/b/c/1.txt?size=2",
			wantMoUrl:     "/a/b/c/1.txt",
			wantUrlParams: []int64{0, 2},
		},
		{
			name:          "Test5 - File",
			data:          "hdfs://localhost:8888/a/b/c/1.txt?size=2",
			wantMoUrl:     "hdfs,endpoint=localhost:8888\n:/a/b/c/1.txt",
			wantUrlParams: []int64{0, 2},
		},
		{name: "bare query", data: "file:///a/b/c/1.txt?", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{0, -1}},
		{name: "uppercase range", data: "file:///a/b/c/1.txt?OFFSET=1&SIZE=2", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{1, 2}},
		{name: "unknown query", data: "file:///a/b/c/1.txt?other=value", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{0, -1}},
		{name: "repeated key keeps first value", data: "file:///a/b/c/1.txt?offset=1&offset=2&size=0", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{1, 0}},
		{name: "unbounded range", data: "file:///a/b/c/1.txt?size=-1", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{0, -1}},
		{name: "malformed query keeps valid pair", data: "file:///a/b/c/1.txt?offset=%zz&size=2", wantMoUrl: "/a/b/c/1.txt", wantUrlParams: []int64{0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, err := ParseDatalink(tt.data, nil)
			if err != nil {
				t.Errorf("ParseDatalink() error = %v", err)
			}
			if !reflect.DeepEqual(got1, tt.wantMoUrl) {
				t.Errorf("ParseDatalink() = %v, want %v", got1, tt.wantMoUrl)
			}
			if !reflect.DeepEqual(got2, tt.wantUrlParams) {
				t.Errorf("ParseDatalink() = %v, want %v", got2, tt.wantUrlParams)
			}
			if err := ValidateDatalink(tt.data, nil); err != nil {
				t.Errorf("validation failed: %v", err)
			}
			dl, err := parseDatalink(tt.data, nil)
			if err != nil || dl.MoPath != tt.wantMoUrl || dl.Offset != tt.wantUrlParams[0] || dl.Size != tt.wantUrlParams[1] {
				t.Errorf("parsed Datalink = %+v, err=%v; want path=%s range=%v", dl, err, tt.wantMoUrl, tt.wantUrlParams)
			}
		})
	}
}

func TestParseDatalinkFailed(t *testing.T) {

	type testCase struct {
		name    string
		data    string
		wantNum string
	}
	tests := []testCase{
		{
			name: "Test1 - File",
			data: "s3://a/b/c/1.txt",
		},
		{
			name: "Test2 - File",
			data: "file:///a/b/c/1.txt?offset=-2&size=2",
		},
		{
			name: "Test3 - File",
			data: "file:///a/b/c/1.txt?offset=b",
		},
		{
			name: "Test4 - File",
			data: "file:///a/b/c/1.txt?size=c",
		},
		{
			name: "Test5 - Empty Stage",
			data: "stage:///?size=c",
		},
		{
			name: "Test6 - Without scheme",
			data: "/a/b/c/1.txt",
		},
		{name: "empty offset", data: "file:///a/b/c/1.txt?offset="},
		{name: "empty size", data: "file:///a/b/c/1.txt?size="},
		{name: "offset overflow", data: "file:///a/b/c/1.txt?offset=9223372036854775808", wantNum: "9223372036854775808"},
		{name: "size overflow", data: "file:///a/b/c/1.txt?size=9223372036854775808", wantNum: "9223372036854775808"},
		{name: "offset parse error first", data: "file:///a/b/c/1.txt?offset=bad&size=invalid", wantNum: "bad"},
		{name: "size parse before offset range", data: "file:///a/b/c/1.txt?offset=-1&size=bad", wantNum: "bad"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotRange, err := ParseDatalink(tt.data, nil)
			if gotPath != "" || gotRange != nil {
				t.Errorf("failure returned path=%q range=%v", gotPath, gotRange)
			}
			if tt.wantNum != "" {
				n, ok := err.(*strconv.NumError)
				if !ok || n.Num != tt.wantNum {
					t.Errorf("want numeric parse error for %q, got %v", tt.wantNum, err)
				}
			}
			if err == nil {
				t.Errorf("ParseDatalink() should error out. %s", tt.data)
			}
			validationErr := ValidateDatalink(tt.data, nil)
			if validationErr == nil || err == nil || validationErr.Error() != err.Error() || reflect.TypeOf(validationErr) != reflect.TypeOf(err) {
				t.Errorf("validation error=%v, want %v", validationErr, err)
			}
		})
	}
}

// Path-form Stage names reach the same resolver and range parser as host names.
func TestLogicalStageDatalink(t *testing.T) {
	proc := testutil.NewProcess(t)
	base, err := url.Parse("file:///tmp/base")
	require.NoError(t, err)
	for _, name := range []string{"two words", "part/name", "literal%2F"} {
		canonical := tree.NewCStr(name, 1).Compare()
		proc.GetStageCache().Set(canonical, stage.StageDef{Name: canonical, Url: base, Status: "enabled"})
		uri := "stage:///" + url.PathEscape(name) + "/data.csv?offset=1&size=2"
		path, bounds, err := ParseDatalink(uri, proc)
		require.NoError(t, err)
		require.Equal(t, "/tmp/base/data.csv", path)
		require.Equal(t, []int64{1, 2}, bounds)
		require.NoError(t, ValidateDatalink(uri, proc))
		_, _, err = ParseDatalink("stage:///"+url.PathEscape(name)+"/data.csv?size=bad", proc)
		var number *strconv.NumError
		require.ErrorAs(t, err, &number)
		require.Equal(t, "bad", number.Num)
	}
}
