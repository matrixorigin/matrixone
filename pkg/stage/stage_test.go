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

package stage

import (
	"net/url"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ServiceProvider(t *testing.T) {
	protocol, err := getS3ServiceFromProvider("cos")
	require.Nil(t, err)
	assert.Equal(t, protocol, "s3")

	protocol, err = getS3ServiceFromProvider("amazon")
	require.Nil(t, err)
	assert.Equal(t, protocol, "s3")

	protocol, err = getS3ServiceFromProvider("minio")
	require.Nil(t, err)
	assert.Equal(t, protocol, "minio")

}

func TestStageCache(t *testing.T) {

	proc := testutil.NewProcess()
	cache := proc.GetStageCache()

	credentials := make(map[string]string)
	credentials["aws_region"] = "region"
	credentials["aws_id"] = "id"
	credentials["aws_secret"] = "secret"

	rsu, err := url.Parse("file:///tmp")
	require.Nil(t, err)
	subu, err := url.Parse("stage://rsstage/substage")
	require.Nil(t, err)

	cache.Set("rsstage", StageDef{Id: 1, Name: "rsstage", Url: rsu, Credentials: credentials})
	cache.Set("substage", StageDef{Id: 1, Name: "ftstage", Url: subu})

	// get the final URL totally based on cache value
	s, err := UrlToStageDef("stage://substage/a.csv", proc)
	require.Nil(t, err)

	require.Equal(t, s.Url.String(), "file:///tmp/substage/a.csv")
	require.Equal(t, s.Credentials["aws_region"], "region")
	require.Equal(t, s.Credentials["aws_id"], "id")
	require.Equal(t, s.Credentials["aws_secret"], "secret")

	// change the local stagedef does not change the cache
	s.Url, err = url.Parse("https://localhost/path")
	require.Nil(t, err)

	// preserve the cache
	rs, ok := cache.Get("rsstage")
	require.True(t, ok)
	require.Equal(t, rs.(StageDef).Url.String(), "file:///tmp")
	ss, ok := cache.Get("substage")
	require.True(t, ok)
	require.Equal(t, ss.(StageDef).Url.String(), "stage://rsstage/substage")
}
