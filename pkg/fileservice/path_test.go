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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzParseService(f *testing.F) {
	for _, input := range []string{
		"s3", "s3,endpoint,region,bucket,key,secret,prefix", "hdfs,endpoint=host:9000\n",
		"", "\n", "\n\n", ",", "s3,,", " s3, key= value ", "\u2003s3,key=x", `\.`,
		"s3,key=x\n", "s3,key=x\n\n", "\ns3,key=x", "s3,key=x\r\n", "s3,key=x\r",
		`s3,""`, `s3,"a,b"`, `s3,"a""b"`, "s3,\"a\r\nb\"", "s3,\"a\nb\"",
		`s3,a"b`, `s3,"unclosed`, "s3,key=x\nother,key=y", "s3,key=x\nother",
		"s3,secret=" + strings.Repeat("x", 8192), "s3,key=\xff",
	} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		// The original standard-library parser is the compatibility oracle,
		// independent of stage's serializer and its fast path.
		records, wantErr := csv.NewReader(strings.NewReader(input)).ReadAll()
		service, arguments, err := parseService(input)
		if wantErr != nil {
			require.IsType(t, wantErr, err)
			require.Equal(t, wantErr, err)
			return
		}
		if len(records) != 1 {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
			require.Equal(t, moerr.NewInvalidInputNoCtxf("bad service: %v", input).Error(), err.Error())
			return
		}
		require.NoError(t, err)
		require.Equal(t, records[0][0], service)
		require.Equal(t, records[0][1:], arguments)
	})
}

func BenchmarkParseService(b *testing.B) {
	for _, input := range []struct{ name, value string }{
		{"plain", "s3-opts,endpoint=http://localhost,region=r,bucket=b,key=k,secret=s\n"},
		{"quoted", "s3-opts,\"secret=a,b\"\n"},
		{"long", "s3-opts,secret=" + strings.Repeat("x", 8192) + "\n"},
		{"quoted-long", "s3-opts,\"secret=" + strings.Repeat("x", 8192) + ",end\"\n"},
	} {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := parseService(input.value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestParseS3OrFilePath(t *testing.T) {
	// S3
	p := "s3,endpoint,region,bucket,key,secret,prefix:a/b/c.txt"
	path, err := ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "s3", path.Service)
	assert.Equal(t, "endpoint", path.ServiceArguments[0])
	assert.Equal(t, "region", path.ServiceArguments[1])
	assert.Equal(t, "bucket", path.ServiceArguments[2])
	assert.Equal(t, "key", path.ServiceArguments[3])
	assert.Equal(t, "secret", path.ServiceArguments[4])
	assert.Equal(t, "prefix", path.ServiceArguments[5])
	assert.Equal(t, "a/b/c.txt", path.File)

	// Local File
	p = "path/to/foo/bar/1.txt"
	path, err = ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "", path.Service)
}

func TestParsePath(t *testing.T) {
	p := "foo"
	path, err := ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "foo", path.File)
	assert.Equal(t, p, path.String())

	p = JoinPath("foo", "bar")
	path, err = ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)
	assert.Equal(t, p, path.String())

	p = JoinPath("foo,baz,quux", "bar")
	path, err = ParsePath(p)
	assert.Nil(t, err)
	assert.Equal(t, "bar", path.File)
	assert.Equal(t, "foo", path.Service)
	assert.Equal(t, []string{"baz", "quux"}, path.ServiceArguments)
	assert.Equal(t, p, path.String())

	path, err = ParsePath("矩阵")
	assert.Nil(t, err)
	assert.Equal(t, "矩阵", path.File)
	assert.Equal(t, "矩阵", path.String())

	path, err = ParsePath("sjrq=20200504/a.parquet")
	assert.Nil(t, err)
	assert.Equal(t, "sjrq=20200504/a.parquet", path.File)
	assert.Equal(t, "sjrq=20200504/a.parquet", path.String())
}

func TestParseFilePathAtServiceRejectsRoot(t *testing.T) {
	for _, filePath := range []string{
		"", "/", ".", "./", "dir/..", "dir/../",
		"etl:", "etl:/", "etl:.", "etl:dir/..",
	} {
		t.Run(filePath, func(t *testing.T) {
			_, err := parseFilePathAtService(filePath, "etl")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
		})
	}

	parsed, err := parseFilePathAtService("etl:/file", "etl")
	require.NoError(t, err)
	require.Equal(t, "file", parsed.File)

	_, err = parseFilePathAtService("other:file", "etl")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongService), "got %v", err)
}
