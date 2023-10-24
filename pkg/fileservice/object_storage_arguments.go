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
	"net/http"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ObjectStorageArguments struct {
	// misc
	Name                string `toml:"name"`
	KeyPrefix           string `toml:"key-prefix"`
	SharedConfigProfile string `toml:"shared-config-profile"`

	// s3
	Bucket   string `toml:"bucket"`
	Endpoint string `toml:"endpoint"`
	IsMinio  bool   `toml:"is-minio"`
	Region   string `toml:"region"`

	// credentials
	AssumeRoleARN     string `toml:"role-arn"`
	BearerToken       string `toml:"bearer-token"`
	ExternalID        string `toml:"external-id"`
	KeyID             string `toml:"key-id"`
	KeySecret         string `toml:"key-secret"`
	OIDCProviderARN   string `toml:"oidc-provider-arn"`
	OIDCRoleARN       string `toml:"oidc-role-arn"`
	OIDCTokenFilePath string `toml:"oidc-token-file-path"`
	RAMRole           string `toml:"ram-role"`
	RoleSessionName   string `toml:"role-session-name"`
	SecurityToken     string `toml:"security-token"`
	SessionToken      string `toml:"session-token"`
}

func (o *ObjectStorageArguments) SetFromString(arguments []string) error {
	for _, pair := range arguments {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}

		switch strings.ToLower(key) {

		case "bearer-token":
			o.BearerToken = value
		case "bucket":
			o.Bucket = value
		case "endpoint":
			o.Endpoint = value
		case "external-id":
			o.ExternalID = value
		case "is-minio", "minio":
			o.IsMinio = value != "false" && value != "0"
		case "key", "key-id":
			o.KeyID = value
		case "name":
			o.Name = value
		case "prefix", "key-prefix":
			o.KeyPrefix = value
		case "ram-role":
			o.RAMRole = value
		case "region":
			o.Region = value
		case "role-arn":
			o.AssumeRoleARN = value
		case "role-session-name":
			o.RoleSessionName = value
		case "secret", "key-secret", "secret-id":
			o.KeySecret = value
		case "security-token":
			o.SecurityToken = value
		case "shared-config-profile":
			o.SharedConfigProfile = value
		case "token", "session-token":
			o.SessionToken = value
		case "oidc-provider-arn":
			o.OIDCProviderARN = value
		case "oidc-token-file-path":
			o.OIDCTokenFilePath = value
		case "oidc-role-arn":
			o.OIDCRoleARN = value

		default:
			return moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}

	}
	return nil
}

func (o *ObjectStorageArguments) validate() error {

	// validate endpoint
	var endpointURL *url.URL
	if o.Endpoint != "" {
		var err error
		endpointURL, err = url.Parse(o.Endpoint)
		if err != nil {
			return err
		}
		if endpointURL.Scheme == "" {
			endpointURL.Scheme = "https"
		}
		o.Endpoint = endpointURL.String()
	}

	// region
	if o.Region == "" {
		// try to get region from bucket
		// only works for AWS S3
		resp, err := http.Head("https://" + o.Bucket + ".s3.amazonaws.com")
		if err == nil {
			if value := resp.Header.Get("x-amz-bucket-region"); value != "" {
				o.Region = value
			}
		}
	}

	// role session name
	if o.RoleSessionName == "" {
		o.RoleSessionName = "mo-service"
	}

	return nil
}
