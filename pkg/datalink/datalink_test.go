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
	"strings"
	"testing"

	"github.com/hayageek/threadsafe"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
			name: "Stage without process",
			data: "stage:///a/b/c/1.txt?size=c",
		},
		{name: "empty Stage", data: "stage:///?size=c"},
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

		})
	}
}

func TestParseDatalinkStageURL(t *testing.T) {
	cache := threadsafe.NewMap[string, stage.StageDef]()
	cache.Set("a", stage.StageDef{Name: "a", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: "/stage-fixture"}})
	proc := &process.Process{Base: &process.BaseProcess{StageCache: cache}}

	for _, tc := range []struct {
		name string
		uri  string
	}{
		{name: "host form", uri: "stage://a/b/c/1.txt?offset=1&size=2"},
		{name: "path form", uri: "stage:///a/b/c/1.txt?offset=1&size=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotPath, gotRange, err := ParseDatalink(tc.uri, proc)
			if err != nil {
				t.Fatal(err)
			}
			if gotPath != "/stage-fixture/b/c/1.txt" || !reflect.DeepEqual(gotRange, []int64{1, 2}) {
				t.Fatalf("got path %q and range %v", gotPath, gotRange)
			}
		})
	}

	gotPath, gotRange, err := ParseDatalink("stage:///a/b/c/1.txt?size=c", proc)
	if gotPath != "" || gotRange != nil {
		t.Fatalf("failure returned path=%q range=%v", gotPath, gotRange)
	}
	if numErr, ok := err.(*strconv.NumError); !ok || numErr.Num != "c" {
		t.Fatalf("want numeric parse error for size c, got %v", err)
	}
}

func TestStageDatalinkRequiresProcess(t *testing.T) {
	callers := []struct {
		name string
		call func(string) error
	}{
		{name: "parse", call: func(uri string) error { _, _, err := ParseDatalink(uri, nil); return err }},
		{name: "constructor", call: func(uri string) error { _, err := NewDatalink(uri, nil); return err }},
		{name: "async constructor", call: func(uri string) error { _, err := NewAsyncDatalink(uri, nil); return err }},
		{name: "async validation", call: func(uri string) error { return ValidateAsyncDatalinkSource(uri, nil) }},
	}
	for _, tc := range []struct {
		name string
		uri  string
	}{
		{name: "host form", uri: "stage://a/b/c"},
		{name: "path form", uri: "stage:///a/b/c"},
	} {
		for _, caller := range callers {
			t.Run(tc.name+"/"+caller.name, func(t *testing.T) {
				err := caller.call(tc.uri)
				if err == nil || !strings.Contains(err.Error(), "stage catalog lookup requires an initialized process") {
					t.Fatalf("want missing process error, got %v", err)
				}
			})
		}
	}
}
