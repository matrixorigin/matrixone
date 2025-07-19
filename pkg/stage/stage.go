// Copyright 2021 Matrix Origin
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
	"context"
	"encoding/csv"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const STAGE_PROTOCOL = "stage"
const S3_PROTOCOL = "s3"
const FILE_PROTOCOL = "file"
const HDFS_PROTOCOL = "hdfs"

const PARAMKEY_AWS_KEY_ID = "aws_key_id"
const PARAMKEY_AWS_SECRET_KEY = "aws_secret_key"
const PARAMKEY_AWS_REGION = "aws_region"
const PARAMKEY_ENDPOINT = "endpoint"
const PARAMKEY_COMPRESSION = "compression"
const PARAMKEY_PROVIDER = "provider"

const S3_PROVIDER_AMAZON = "amazon"
const S3_PROVIDER_MINIO = "minio"
const S3_PROVIDER_COS = "cos"

const S3_SERVICE = "s3"
const MINIO_SERVICE = "minio"

type StageDef struct {
	Id          uint32
	Name        string
	Url         *url.URL
	Credentials map[string]string
	Status      string
}

func (s StageDef) GetCredentials(key string, defval string) (string, bool) {
	if s.Credentials == nil {
		// no credential in this stage
		return defval, false
	}

	k := strings.ToLower(key)
	res, ok := s.Credentials[k]
	if !ok {
		return defval, false
	}
	return res, ok
}

// get stages and expand the path. stage may be a file or s3
// use the format of path  s3,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// or minio,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// expand the subpath to MO path.
// subpath is in the format like path or path with query like path?q1=v1&q2=v2...
func (s StageDef) ToPath() (mopath string, query string, err error) {

	if s.Url.Scheme == S3_PROTOCOL {
		bucket, prefix, query, err := ParseS3Url(s.Url)
		if err != nil {
			return "", "", err
		}

		// get S3 credentials
		aws_key_id, found := s.GetCredentials(PARAMKEY_AWS_KEY_ID, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_KEY_ID not found")
		}
		aws_secret_key, found := s.GetCredentials(PARAMKEY_AWS_SECRET_KEY, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_SECRET_KEY not found")
		}
		aws_region, found := s.GetCredentials(PARAMKEY_AWS_REGION, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_REGION not found")
		}
		provider, found := s.GetCredentials(PARAMKEY_PROVIDER, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: PROVIDER not found")
		}
		endpoint, found := s.GetCredentials(PARAMKEY_ENDPOINT, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: ENDPOINT not found")
		}

		service, err := getS3ServiceFromProvider(provider)
		if err != nil {
			return "", "", err
		}

		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{"s3-opts", "endpoint=" + endpoint, "region=" + aws_region, "bucket=" + bucket, "key=" + aws_key_id, "secret=" + aws_secret_key}
		if service == MINIO_SERVICE {
			opts = append(opts, "is-minio=true")
		}

		if err = w.Write(opts); err != nil {
			return "", "", err
		}
		w.Flush()
		return fileservice.JoinPath(buf.String(), prefix), query, nil
	} else if s.Url.Scheme == HDFS_PROTOCOL {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{"hdfs", "endpoint=" + s.Url.Host}

		if err = w.Write(opts); err != nil {
			return "", "", err
		}
		w.Flush()
		return fileservice.JoinPath(buf.String(), s.Url.Path), s.Url.RawQuery, nil
	} else if s.Url.Scheme == FILE_PROTOCOL {
		return s.Url.Path, s.Url.RawQuery, nil
	}
	return "", "", moerr.NewBadConfigf(context.TODO(), "URL protocol %s not supported", s.Url.Scheme)
}

func getS3ServiceFromProvider(provider string) (string, error) {
	provider = strings.ToLower(provider)
	switch provider {
	case S3_PROVIDER_COS:
		return S3_SERVICE, nil
	case S3_PROVIDER_AMAZON:
		return S3_SERVICE, nil
	case S3_PROVIDER_MINIO:
		return MINIO_SERVICE, nil
	default:
		return "", moerr.NewBadConfigf(context.TODO(), "provider %s not supported", provider)
	}
}

func CredentialsToMap(cred string) (map[string]string, error) {
	if len(cred) == 0 {
		return nil, nil
	}

	opts := strings.Split(cred, ",")
	if len(opts) == 0 {
		return nil, nil
	}

	credentials := make(map[string]string)
	for _, o := range opts {
		kv := strings.SplitN(o, "=", 2)
		if len(kv) != 2 {
			return nil, moerr.NewBadConfig(context.TODO(), "Format error: invalid stage credentials")
		}
		credentials[strings.ToLower(kv[0])] = kv[1]
	}

	return credentials, nil
}

func ParseS3Url(u *url.URL) (bucket, fpath, query string, err error) {
	bucket = u.Host
	fpath = u.Path
	query = u.RawQuery
	err = nil

	if len(bucket) == 0 {
		err = moerr.NewBadConfig(context.TODO(), "Invalid s3 URL: bucket is empty string")
		return "", "", "", err
	}

	return
}

func ParseStageUrl(u *url.URL) (stagename, prefix, query string, err error) {
	if u.Scheme != STAGE_PROTOCOL {
		return "", "", "", moerr.NewBadConfig(context.TODO(), "ParseStageUrl: URL protocol is not stage://")
	}

	stagename = u.Host
	if len(stagename) == 0 {
		return "", "", "", moerr.NewBadConfig(context.TODO(), "Invalid stage URL: stage name is empty string")
	}

	prefix = u.Path
	query = u.RawQuery

	return
}
