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
	"fmt"
	"net/url"
	"testing"

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

func TestGetCredentials(t *testing.T) {

	c := make(map[string]string)
	c[PARAMKEY_AWS_KEY_ID] = "key"
	c[PARAMKEY_AWS_SECRET_KEY] = "secret"
	c[PARAMKEY_AWS_REGION] = "region"
	c[PARAMKEY_ENDPOINT] = "endpoint"
	c[PARAMKEY_PROVIDER] = S3_PROVIDER_AMAZON
	u, err := url.Parse("s3://bucket/path")
	require.Nil(t, err)
	s := StageDef{Id: 0,
		Name:        "mystage",
		Url:         u,
		Credentials: c,
		Status:      ""}

	// key found
	v, ok := s.GetCredentials(PARAMKEY_AWS_KEY_ID, "")
	require.True(t, ok)
	require.Equal(t, v, "key")

	// key not found and return default value
	v, ok = s.GetCredentials("nokey", "default")
	require.False(t, ok)
	require.Equal(t, v, "default")

	s = StageDef{Id: 0,
		Name:   "mystage",
		Url:    u,
		Status: ""}

	// credentials is nil and return default value
	v, ok = s.GetCredentials("nokey", "default")
	require.False(t, ok)
	require.Equal(t, v, "default")
}

func TestToPathMinio(t *testing.T) {
	c := make(map[string]string)
	c[PARAMKEY_AWS_KEY_ID] = "key"
	c[PARAMKEY_AWS_SECRET_KEY] = "secret"
	c[PARAMKEY_AWS_REGION] = "region"
	c[PARAMKEY_ENDPOINT] = "endpoint"
	c[PARAMKEY_PROVIDER] = S3_PROVIDER_MINIO

	// minio path
	u, err := url.Parse("s3://bucket/path/a.csv")
	require.Nil(t, err)
	s := StageDef{Id: 0,
		Name:        "mystage",
		Url:         u,
		Credentials: c,
		Status:      ""}

	mopath, query, err := s.ToPath()
	require.Nil(t, err)

	require.Equal(t, mopath, "s3-opts,endpoint=endpoint,region=region,bucket=bucket,key=key,secret=secret,is-minio=true\n:/path/a.csv")
	fmt.Printf("mo=%s, query = %s", mopath, query)

}

func TestToPath(t *testing.T) {
	c := make(map[string]string)
	c[PARAMKEY_AWS_KEY_ID] = "key"
	c[PARAMKEY_AWS_SECRET_KEY] = "secret"
	c[PARAMKEY_AWS_REGION] = "region"
	c[PARAMKEY_ENDPOINT] = "endpoint"
	c[PARAMKEY_PROVIDER] = S3_PROVIDER_AMAZON

	// s3 path
	u, err := url.Parse("s3://bucket/path/a.csv")
	require.Nil(t, err)
	s := StageDef{Id: 0,
		Name:        "mystage",
		Url:         u,
		Credentials: c,
		Status:      ""}

	mopath, query, err := s.ToPath()
	require.Nil(t, err)

	require.Equal(t, mopath, "s3-opts,endpoint=endpoint,region=region,bucket=bucket,key=key,secret=secret\n:/path/a.csv")
	fmt.Printf("mo=%s, query = %s", mopath, query)

	// file path
	u, err = url.Parse("file:///tmp/dir/subdir/file.pdf")
	require.Nil(t, err)
	s = StageDef{Id: 0,
		Name:   "mystage",
		Url:    u,
		Status: ""}

	mopath, query, err = s.ToPath()
	require.Nil(t, err)
	require.Equal(t, query, "")

	require.Equal(t, mopath, "/tmp/dir/subdir/file.pdf")

	// hdfs path
	u, err = url.Parse("hdfs://localhost:8080/dir/path.txt")
	require.Nil(t, err)
	s = StageDef{Id: 0,
		Name:   "mystage",
		Url:    u,
		Status: ""}
	mopath, _, err = s.ToPath()
	require.Nil(t, err)
	require.Equal(t, mopath, "hdfs,endpoint=localhost:8080\n:/dir/path.txt")

	// invalid schema
	u, err = url.Parse("https://localhost/path/file.pdf")
	require.Nil(t, err)
	s = StageDef{Id: 0,
		Name:   "mystage",
		Url:    u,
		Status: ""}

	_, _, err = s.ToPath()
	require.NotNil(t, err)
}

func TestToPathFail(t *testing.T) {
	c := make(map[string]string)

	// s3 path
	u, err := url.Parse("s3://bucket/path/a.csv")
	require.Nil(t, err)
	s := StageDef{Id: 0,
		Name:        "mystage",
		Url:         u,
		Credentials: c,
		Status:      ""}

	// no credentials
	_, _, err = s.ToPath()
	require.NotNil(t, err)

	// add key and failed with no secret key
	c[PARAMKEY_AWS_KEY_ID] = "key"
	_, _, err = s.ToPath()
	require.NotNil(t, err)

	// add secert and failed with no region
	c[PARAMKEY_AWS_SECRET_KEY] = "secret"
	_, _, err = s.ToPath()
	require.NotNil(t, err)

	// add region and failed with no endpoint
	c[PARAMKEY_AWS_REGION] = "region"
	_, _, err = s.ToPath()
	require.NotNil(t, err)

	// add endpoint and failed with unknown provider
	c[PARAMKEY_ENDPOINT] = "endpoint"
	_, _, err = s.ToPath()
	require.NotNil(t, err)

	// add unknown provider
	c[PARAMKEY_PROVIDER] = "unknown"
	_, _, err = s.ToPath()
	require.NotNil(t, err)
}

func TestCredentialsToMap(t *testing.T) {

	c := "aws_key_id=key,aws_secret_key=secret,aws_region=region,endpoint=ep,provider=minio"

	cmap, err := CredentialsToMap(c)
	require.Nil(t, err)
	require.NotNil(t, cmap)
	require.Equal(t, cmap[PARAMKEY_AWS_KEY_ID], "key")
	require.Equal(t, cmap[PARAMKEY_AWS_SECRET_KEY], "secret")
	require.Equal(t, cmap[PARAMKEY_AWS_REGION], "region")
	require.Equal(t, cmap[PARAMKEY_ENDPOINT], "ep")
	require.Equal(t, cmap[PARAMKEY_PROVIDER], "minio")

}
