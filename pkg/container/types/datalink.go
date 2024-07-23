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

package types

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	S3Datalink = "s3"
)

// ParseDatalink extracts data from a Datalink string
// and returns the url parts, offset, size, fileType etc.
// parts: [scheme, user, host, path]
// params: {offset: "0", size: "-1", secret: "secret", key: "key"}
// file type: "pdf", "txt"
func ParseDatalink(str string) ([]string, map[string]string, string, error) {
	u, err := url.Parse(str)
	if err != nil {
		return nil, nil, "", err
	}

	// check if the url scheme is supported
	switch u.Scheme {
	case "stage", "s3", "file":
	default:
		return nil, nil, "", moerr.NewNYINoCtx("unsupported url scheme %s", u.Scheme)
	}

	// create the URL parts array
	urlParts := make([]string, 0)
	urlParts = append(urlParts, u.Scheme)
	urlParts = append(urlParts, u.User.String())
	urlParts = append(urlParts, u.Host)
	urlParts = append(urlParts, u.Path)

	// get file type from the path
	extension := filepath.Ext(u.Path)
	switch extension {
	case ".txt", ".csv":
	default:
		return nil, nil, "", moerr.NewNYINoCtx("unsupported file type %s", extension)
	}

	// convert query parameters to map
	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
	}

	// set default values for offset and size if not provided
	if _, ok := urlParams["offset"]; !ok {
		urlParams["offset"] = "0"
	}
	if _, ok := urlParams["size"]; !ok {
		urlParams["size"] = "-1"
	}

	// verify offset and size are integers
	if _, err = strconv.Atoi(urlParams["offset"]); err != nil {
		return nil, nil, "", err
	}
	if _, err = strconv.Atoi(urlParams["size"]); err != nil {
		return nil, nil, "", err
	}

	return urlParts, urlParams, extension, nil
}

// ConvertFsDatalinkToFsPath converts file datalink to a file system path
// that can be used by internal File Service API.
func ConvertFsDatalinkToFsPath(fsUrl string) (string, error) {
	u, err := url.Parse(fsUrl)
	if err != nil {
		return "", err
	}

	rootFolder := u.Host
	path := u.Path

	// in: "file://a/b/c.txt"
	// out: "/a/b/c.txt"
	newUrl := strings.Join([]string{rootFolder, path}, "")

	return newUrl, nil

}

// ConvertS3DatalinkToFsS3Url converts S3 datalink to a file system S3 URL
// that can be used by internal File Service API.
// Datalink Format: "s3://vector-test-data/prefix/path/img.png?region=us-east-2&key=xxx&secret=xxx&offset=0&size=-1"
// Internal Format : "s3,endpoint,region,bucket,key,secret,prefix:a/b/c.txt"
func ConvertS3DatalinkToFsS3Url(s3Url string) (string, error) {
	u, err := url.Parse(s3Url)
	if err != nil {
		return "", err
	}

	endpoint := "" // For S3 we don't need to pass endpoint to the File Service API.
	region := u.Query()["region"][0]
	key := u.Query()["key"][0]
	secret := u.Query()["secret"][0]

	if region == "" || key == "" || secret == "" {
		return "", moerr.NewInvalidInputNoCtx("region, key, and secret are required")
	}

	bucket := u.Host
	urlPathTrimmed := strings.TrimPrefix(u.Path, "/")
	urlPathParts := strings.Split(urlPathTrimmed, "/")
	prefix := strings.Join(urlPathParts[:len(urlPathParts)-1], "/")
	path := urlPathParts[len(urlPathParts)-1]

	// in: s3://vector-test-data/prefix/path/img.png?region=us-east-2&key=xxx&secret=xxx&offset=0&size=-1
	// out: "s3,endpoint,region,bucket,key,secret,prefix:a/b/c.txt"
	newUrl := strings.Join([]string{S3Datalink, endpoint, region, bucket, key, secret, prefix}, ",")
	newUrl = strings.Join([]string{newUrl, path}, ":")

	return newUrl, nil
}

// ConvertStageDatalinkToFsUrl converts Stage datalink to a file system URL
// that can be used by internal File Service API.
// The converted URL will be in the format: "schema,endpoint,region,bucket,key,secret,prefix:a/b/c.txt"
// NOTE: schema could be "s3", "minio" etc.
// TODO: need to handle "s3-opt". Requires STAGE feature to have Database Column, Decode(Credential) etc
func ConvertStageDatalinkToFsUrl(stageUrl, stageCredential, filePath string) (string, error) {
	// stage url:= s3://vector-test-data/prefixFolder
	u, err := url.Parse(stageUrl)
	if err != nil {
		return "", err
	}

	bucket := u.Host
	prefix := u.Path

	credentialsKV, err := decodeCredentials(stageCredential)
	if err != nil {
		return "", err
	}
	endpoint := credentialsKV["endpoint"]
	key := credentialsKV["key"]
	secret := credentialsKV["secret"]
	region := credentialsKV["region"]

	newUrl := strings.Join([]string{u.Scheme, endpoint, u.Host, region, bucket, key, secret, prefix}, ",")
	newUrl = strings.Join([]string{newUrl, filePath}, ":")

	return "", nil
}

// decodeCredentials decodes the credentials string and returns a map of key value pairs
func decodeCredentials(credentials string) (map[string]string, error) {
	credentialsMap := make(map[string]string)
	credentialsMap["key"] = ""
	credentialsMap["secret"] = ""
	credentialsMap["region"] = ""
	credentialsMap["endpoint"] = ""
	return credentialsMap, nil
}
