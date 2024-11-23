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
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

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
		if !strings.HasPrefix(name, "TEST_S3FS_") {
			continue
		}

		// parse args
		reader := csv.NewReader(strings.NewReader(value))
		argStrs, err := reader.Read()
		if err != nil {
			logutil.Warn("bad S3FS test spec", zap.Any("spec", value))
			continue
		}
		var args ObjectStorageArguments
		if err := args.SetFromString(argStrs); err != nil {
			logutil.Warn("bad S3FS test spec", zap.Any("spec", value))
			continue
		}
		args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())

		ret = append(ret, args)
	}

	return ret
}

func TestQCloudRegion(t *testing.T) {
	args := ObjectStorageArguments{
		Endpoint: "http://cos.foobar.myqcloud.com",
	}
	args.validate()
	assert.Equal(t, "foobar", args.Region)
}

func TestAWSRegion(t *testing.T) {
	args := ObjectStorageArguments{
		Bucket: "aws", // hope it will not change its region
	}
	args.validate()
	assert.Equal(t, "us-east-1", args.Region)
}

func TestQCloudKeyIDSecretFromAwsEnv(t *testing.T) {
	args := ObjectStorageArguments{
		Endpoint: "http://cos.foobar.myqcloud.com",
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "foo")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "bar")
	args.validate()
	assert.Equal(t, "foo", args.KeyID)
	assert.Equal(t, "bar", args.KeySecret)
}
