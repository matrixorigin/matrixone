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
	"encoding/csv"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func standardServiceCSV(fields []string) (string, error) {
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	if err := w.Write(fields); err != nil {
		return "", err
	}
	w.Flush()
	return buf.String(), w.Error()
}

func checkServiceCSV(t *testing.T, fields []string) {
	t.Helper()
	want, wantErr := standardServiceCSV(fields)
	got, err := EncodeServiceCSV(fields)
	if (err == nil) != (wantErr == nil) || got != want {
		t.Fatalf("fields=%q: got %q (%v), want %q (%v)", fields, got, err, want, wantErr)
	}
}

func TestEncodeServiceCSV(t *testing.T) {
	checkServiceCSV(t, nil)
	checkServiceCSV(t, []string{})
	values := []string{"", "hdfs", "endpoint=host:9000", "key= leading", " leading", "\tleading", "\u2003leading", "\ufeffbom", "\xffinvalid", `\.`, "a,b", `a"b`, "a\rb", "a\nb", "a\r\nb", "trailing ", "世界"}
	for _, n := range []int{4096, 4097, 8192} {
		t.Run(fmt.Sprintf("length-%d", n), func(t *testing.T) {
			value := strings.Repeat("x", n)
			checkServiceCSV(t, []string{"s3-opts", value, ""})
			checkServiceCSV(t, []string{"s3-opts", value + ",", ""})
		})
	}
	for i, value := range values {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			checkServiceCSV(t, []string{value})
			checkServiceCSV(t, []string{"s3-opts", value, ""})
		})
	}
}

func FuzzEncodeServiceCSV(f *testing.F) {
	for _, seed := range []string{"", "hdfs", "key= leading", " leading", `\.`, "a,b", `a"b`, "a\r\nb", "\u2003", "\xff", "a\x00b"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		fields := strings.Split(input, "\x00")
		checkServiceCSV(t, fields)
		checkServicePath(t, fields, "/file")
		checkServicePath(t, fields, "file:part")
	})
}

func checkServicePath(t *testing.T, fields []string, file string) {
	t.Helper()
	wire := file
	if len(fields) > 0 {
		service, err := standardServiceCSV(fields)
		if err != nil {
			t.Fatal(err)
		}
		wire = JoinPath(service, file)
	}
	encoded, err := EncodeServicePath(fields, file)
	if err != nil || encoded != wire {
		t.Fatalf("encoded %q (%v), want %q", encoded, err, wire)
	}
	want, wantErr := ParsePath(wire)
	got, gotErr := ParsePathFromServiceFields(fields, file)
	if !reflect.DeepEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || reflect.TypeOf(gotErr) != reflect.TypeOf(wantErr) {
		t.Fatalf("fields=%q file=%q: got %+v (%v), want %+v (%v)", fields, file, got, gotErr, want, wantErr)
	}
}

func TestServicePathFields(t *testing.T) {
	values := []string{"", "key=caller", "secret=a,b", `secret=a"b`, " leading", "\u2003leading", "a\rb", "a\nb", "a\r\nb", "\xff", "世界"}
	fields := make([][]string, 0, 8+len(values))
	fields = append(fields, [][]string{nil, {}, {""}, {"", "arg=value"}, {"SHARED"}, {`\.`}, {"\xff"}, {"hdfs", "endpoint=host:9000"}}...)
	for _, value := range values {
		fields = append(fields, []string{"s3-opts", value, ""})
	}
	for i, record := range fields {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			for _, file := range []string{"", ".", "///", "/a/b", "SHARED:/a", "a:b", "invalid space", "a\nb", "a\x00b", "世界", "\xff"} {
				checkServicePath(t, record, file)
			}
		})
	}
	// Callers reuse their field scratch after interpretation, as they can with
	// the materialized parser. A retained Path must keep its own arguments.
	record := []string{"hdfs", "endpoint=host:9000"}
	path, err := ParsePathFromServiceFields(record, "/file")
	if err != nil {
		t.Fatal(err)
	}
	record[1] = "endpoint=other:9000"
	if path.ServiceArguments[0] != "endpoint=host:9000" {
		t.Fatal("parsed path retained caller scratch")
	}
}

func BenchmarkEncodeServiceCSV(b *testing.B) {
	for _, tc := range []struct {
		name   string
		fields []string
	}{
		{"plain", []string{"s3-opts", "endpoint=https://example.invalid", "region=region", "bucket=bucket", "key=key", "secret=secret"}},
		{"quoted", []string{"s3-opts", "secret=a,b"}},
		{"long", []string{"s3-opts", "secret=" + strings.Repeat("x", 8192)}},
	} {
		for _, encoder := range []struct {
			name   string
			encode func([]string) (string, error)
		}{{"standard", standardServiceCSV}, {"shared", EncodeServiceCSV}} {
			b.Run(tc.name+"/"+encoder.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := encoder.encode(tc.fields); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
