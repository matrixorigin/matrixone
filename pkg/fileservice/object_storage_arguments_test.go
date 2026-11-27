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
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestObjectStorageArguments(t *testing.T) {

	t.Run("no api key", func(t *testing.T) {
		args := ObjectStorageArguments{
			KeyID: "foo",
		}
		field := zap.Any("arguments", args)

		{
			encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
			buf, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{field})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(buf.String(), "foo") {
				t.Fatal()
			}
		}

		{
			encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{})
			buf, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{field})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(buf.String(), "foo") {
				t.Fatal()
			}
		}

	})
}

func TestCSStorageArgumentsPreservePrefix(t *testing.T) {
	const endpoint = "https://myqcloud.example.invalid"
	t.Setenv("MO_CI_S3_PREFIX", "ci-ut/run-123")
	t.Setenv("TEST_S3FS_CS", "name=cs,endpoint="+endpoint+",bucket=restricted-test,is-minio=true")
	t.Setenv("TEST_S3FS_PROVIDER_CONTROL", "name=cs-s3,endpoint="+endpoint+",bucket=ordinary-test,is-minio=true")
	var sawOrdinary bool
	for _, args := range objectStorageArgumentsForTest("test", t) {
		if args.Endpoint != endpoint {
			continue
		}
		if args.Bucket == "restricted-test" {
			t.Fatal("restricted CS fixture exposed to a consumer that can replace its prefix")
		}
		sawOrdinary = sawOrdinary || args.Bucket == "ordinary-test"
	}
	if !sawOrdinary {
		t.Fatal("ordinary QCloud fixture missing from the shared provider list")
	}
	var restricted, ordinary int
	for _, spec := range prefixAwareStorageTestSpecs(t) {
		args := spec.args
		if args.Endpoint != endpoint {
			continue
		}
		switch args.Bucket {
		case "restricted-test":
			restricted++
			if !spec.scopedCS || args.Name != "cs-s3" || !strings.HasPrefix(args.KeyPrefix, "ci-ut/run-123/") || !args.IsMinio || !args.NoBucketValidation {
				t.Fatal("CS test storage is not isolated under the run prefix")
			}
		case "ordinary-test":
			ordinary++
			if spec.scopedCS || args.NoBucketValidation {
				t.Fatal("ordinary provider treated as the restricted CS fixture")
			}
		}
	}
	if restricted != 1 || ordinary != 1 {
		t.Fatalf("expected one fixture from each source, got CS=%d ordinary=%d", restricted, ordinary)
	}
}

// prepareCITestStorage refuses a pre-existing fixture prefix and only cleans
// objects underneath that exact run-specific prefix. Never operate on a bucket root.
func prepareCITestStorage(t *testing.T, args ObjectStorageArguments) {
	t.Helper()
	root := os.Getenv("MO_CI_S3_PREFIX")
	if !validCITestS3Prefix(root) || !validCITestS3Prefix(args.KeyPrefix) || !strings.HasPrefix(args.KeyPrefix, root+"/") {
		t.Fatal("unsafe CS S3 fixture prefix")
	}
	prefix := args.KeyPrefix + "/"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	sdk, err := NewMinioSDK(ctx, args, nil)
	if err != nil {
		cancel()
		t.Fatal(ciTestS3Error("initialize fixture", err))
	}
	if err := checkEmptyCITestPrefix(ctx, sdk.client, args.Bucket, prefix); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := cleanupCITestPrefix(ctx, sdk.client, args.Bucket, prefix); err != nil {
			t.Error(err)
		}
	})
}

func objectStorageArgumentsForTest(defaultName string, t *testing.T) (ret []ObjectStorageArguments) {

	// disk
	ret = append(ret, ObjectStorageArguments{
		Name:     defaultName,
		Endpoint: "disk",
		Bucket:   t.TempDir(),
	})

	// s3.json
	content, err := os.ReadFile("s3.json")
	if err == nil {
		if len(content) > 0 {
			var config struct {
				Endpoint  string `json:"s3-test-endpoint"`
				Region    string `json:"s3-test-region"`
				APIKey    string `json:"s3-test-key"`
				APISecret string `json:"s3-test-secret"`
				Bucket    string `json:"s3-test-bucket"`
				RoleARN   string `json:"role-arn"`
			}

			if err := json.Unmarshal(content, &config); err == nil {
				ret = append(ret, ObjectStorageArguments{
					Name:      "s3.json " + defaultName,
					Endpoint:  config.Endpoint,
					Region:    config.Region,
					KeyID:     config.APIKey,
					KeySecret: config.APISecret,
					Bucket:    config.Bucket,
					RoleARN:   config.RoleARN,
					KeyPrefix: fmt.Sprintf("%v", rand.Int64()),
				})
			}
		}
	}

	// s3_fs_test_new.xml
	content, err = os.ReadFile("s3_fs_test_new.xml")
	if err == nil {
		var spec struct {
			XMLName xml.Name               `xml:"Spec"`
			Cases   []S3CredentialTestCase `xml:"Case"`
		}
		if err := xml.Unmarshal(content, &spec); err == nil {
			for _, kase := range spec.Cases {
				if kase.Skip {
					continue
				}
				kase.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
				ret = append(ret, kase.ObjectStorageArguments)
			}
		}
	}

	// envs
	// examples
	//t.Setenv("TEST_S3FS_ALIYUN", "name=aliyun,endpoint=oss-cn-shenzhen.aliyuncs.com,region=oss-cn-shenzhen,bucket=reus-test,key-id=aaa,key-secret=bbb")
	//t.Setenv("TEST_S3FS_QCLOUD", "name=qcloud,endpoint=https://cos.ap-guangzhou.myqcloud.com,region=ap-guangzhou,bucket=mofstest-1251598405,key-id=aaa,key-secret=bbb")
	for _, pairs := range os.Environ() {
		name, value, ok := strings.Cut(pairs, "=")
		if !ok {
			continue
		}
		// env vars begin with TEST_S3FS_
		if !strings.HasPrefix(name, "TEST_S3FS_") || name == "TEST_S3FS_CS" {
			continue
		}

		args, err := parseStorageTestSpec(value)
		if err != nil {
			logutil.Warn("bad S3FS test spec", zap.String("env", name))
			continue
		}
		args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
		ret = append(ret, args)
	}

	return ret
}

func parseStorageTestSpec(value string) (args ObjectStorageArguments, err error) {
	fields, err := csv.NewReader(strings.NewReader(value)).Read()
	if err != nil {
		return args, err
	}
	err = args.SetFromString(fields)
	return args, err
}

type storageTestSpec struct {
	args     ObjectStorageArguments
	scopedCS bool
}

// Only consumers that keep all keys and cleanup under the assigned prefix
// may opt in here. Legacy SDK tests use objectStorageArgumentsForTest instead.
func prefixAwareStorageTestSpecs(t *testing.T) []storageTestSpec {
	t.Helper()
	ordinary := objectStorageArgumentsForTest("test", t)
	specs := make([]storageTestSpec, 0, len(ordinary)+1)
	if value, ok := os.LookupEnv("TEST_S3FS_CS"); ok {
		args, err := parseStorageTestSpec(value)
		if err != nil {
			t.Fatal("invalid TEST_S3FS_CS test specification")
		}
		prefix := os.Getenv("MO_CI_S3_PREFIX")
		if !validCITestS3Prefix(prefix) {
			t.Fatal("TEST_S3FS_CS requires a non-root, normalized MO_CI_S3_PREFIX")
		}
		if !args.IsMinio {
			t.Fatal("TEST_S3FS_CS requires is-minio=true")
		}
		args.Name = "cs-s3"
		args.KeyPrefix = path.Join(prefix, fmt.Sprintf("%v", rand.Int64()))
		// HeadBucket is not prefix-scoped. prepareCITestStorage validates the
		// assigned prefix without requiring bucket-wide access.
		args.NoBucketValidation = true
		specs = append(specs, storageTestSpec{args: args, scopedCS: true})
	}
	for _, args := range ordinary {
		specs = append(specs, storageTestSpec{args: args})
	}
	return specs
}

func TestQCloudRegion(t *testing.T) {
	args := ObjectStorageArguments{
		Endpoint: "http://cos.foobar.myqcloud.com",
	}
	args.validate()
	assert.Equal(t, "foobar", args.Region)
}

func TestAWSRegion(t *testing.T) {
	origHead := objectStorageHTTPHead
	objectStorageHTTPHead = func(url string) (*http.Response, error) {
		resp := &http.Response{Header: make(http.Header)}
		switch url {
		case "https://aws.s3.amazonaws.com":
			resp.Header.Set("x-amz-bucket-region", "us-east-1")
			return resp, nil
		case "https://fdsafdsafasdfsdafsadfsdafdsafewrewqrweqrewrwerwqrew.s3.amazonaws.com":
			return resp, nil
		default:
			return nil, assert.AnError
		}
	}
	t.Cleanup(func() {
		objectStorageHTTPHead = origHead
	})

	args := ObjectStorageArguments{
		Endpoint: "amazonaws.com",

		Bucket: "aws", // hope it will not change its region
	}
	assert.Nil(t, args.validate())
	assert.Equal(t, "us-east-1", args.Region)

	args = ObjectStorageArguments{
		Endpoint: "amazonaws.com",

		Bucket: "fdsafdsafasdfsdafsadfsdafdsafewrewqrweqrewrwerwqrew", // invalid bucket
	}
	assert.Nil(t, args.validate())
	assert.Equal(t, "", args.Region)

	args = ObjectStorageArguments{
		Endpoint: "amazonaws.com",

		Bucket: "a/b/c", // invalid bucket
	}
	assert.NotNil(t, args.validate())
}

func TestSetFromStringParallelMode(t *testing.T) {
	var args ObjectStorageArguments
	assert.NoError(t, args.SetFromString([]string{"parallel-mode=force"}))
	assert.Equal(t, ParallelForce, args.ParallelMode)

	args = ObjectStorageArguments{
		ParallelMode: ParallelAuto,
	}
	assert.Error(t, args.SetFromString([]string{"parallel-mode=unknown"}))
	assert.Equal(t, ParallelAuto, args.ParallelMode)
}

func TestObjectStorageArgumentsValidateDefaults(t *testing.T) {
	args := ObjectStorageArguments{
		Endpoint: "example.com",
	}
	assert.NoError(t, args.validate())
	assert.Equal(t, "https://example.com", args.Endpoint)
	assert.Equal(t, "mo-service", args.RoleSessionName)
}

func TestObjectStorageArgumentsShouldLoadDefaultCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "sk")
	args := ObjectStorageArguments{}
	assert.True(t, args.shouldLoadDefaultCredentials())

	args = ObjectStorageArguments{
		NoDefaultCredentials: true,
		KeyID:                "id",
		KeySecret:            "secret",
	}
	assert.False(t, args.shouldLoadDefaultCredentials())

	args = ObjectStorageArguments{
		NoDefaultCredentials: true,
		RoleARN:              "arn",
	}
	assert.False(t, args.shouldLoadDefaultCredentials())
}

func TestObjectStorageArgumentsString(t *testing.T) {
	args := ObjectStorageArguments{
		Name:        "foo",
		KeyPrefix:   "bar",
		Concurrency: 3,
	}
	s := args.String()
	var decoded ObjectStorageArguments
	assert.NoError(t, json.Unmarshal([]byte(s), &decoded))
	assert.Equal(t, args.Name, decoded.Name)
	assert.Equal(t, args.KeyPrefix, decoded.KeyPrefix)
	assert.Equal(t, args.Concurrency, decoded.Concurrency)
}

func TestParseHDFSArgs(t *testing.T) {
	var args ObjectStorageArguments
	if err := args.SetFromString([]string{
		"user=user",
		"kerberos-service-principle-name=principle-name",
		"kerberos-username=username",
		"kerberos-realm=realm",
		"kerberos-password=password",
		"kerberos-keytab-path=path",
	}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "user", args.User)
	assert.Equal(t, "principle-name", args.KerberosServicePrincipleName)
	assert.Equal(t, "username", args.KerberosUsername)
	assert.Equal(t, "realm", args.KerberosRealm)
	assert.Equal(t, "password", args.KerberosPassword)
	assert.Equal(t, "path", args.KerberosKeytabPath)
}
