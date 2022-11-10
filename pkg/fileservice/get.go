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
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func Get[T any](fs FileService, name string) (res T, err error) {
	lowerName := strings.ToLower(name)
	if fs, ok := fs.(*FileServices); ok {
		f, ok := fs.mappings[lowerName]
		if !ok {
			err = moerr.NewNoService(name)
			return
		}
		res, ok = f.(T)
		if !ok {
			err = moerr.NewNoService(name)
			return
		}
		return
	}
	var ok bool
	res, ok = fs.(T)
	if !ok {
		err = moerr.NewNoService(name)
		return
	}
	if !strings.EqualFold(fs.Name(), lowerName) {
		err = moerr.NewNoService(name)
		return
	}
	return
}

// GetForETL get or creates a FileService instance for ETL operations
// if service part of path is empty, a LocalETLFS will be created
// if service part of path is not empty, a ETLFileService typed instance will be extracted from fs argument
// if service part of path is argumented, a FileService instance will be created dynamically with those arguments
// supported dynamic file service:
// s3,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// s3-no-key,<endpoint>,<region>,<bucket>,<prefix>
// minio,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
func GetForETL(fs FileService, path string) (res ETLFileService, readPath string, err error) {
	fsPath, err := ParsePath(path)
	if err != nil {
		return nil, "", err
	}

	if fsPath.Service == "" {
		// no service, create local ETL fs
		dir, file := filepath.Split(path)
		res, err = NewLocalETLFS("etl", dir)
		if err != nil {
			return nil, "", err
		}
		readPath = file

	} else if len(fsPath.ServiceArguments) > 0 {
		// service with arguments, create dynamically
		switch fsPath.Service {

		case "s3":
			res, err = newS3FSFromArguments(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		case "s3-no-key":
			res, err = newS3FSFromArgumentsWithoutKey(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		case "minio":
			res, err = newMinioS3FSFromArguments(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		default:
			err = moerr.NewInvalidInput("no such service: %s", fsPath.Service)
		}

		readPath = fsPath.File

	} else {
		// get etl fs
		res, err = Get[ETLFileService](fs, fsPath.Service)
		if err != nil {
			return nil, "", err
		}
		readPath = path
	}

	return
}

func newS3FSFromArguments(arguments []string) (*S3FS, error) {
	endpoint := arguments[0]
	region := arguments[1]
	bucket := arguments[2]
	accessKey := arguments[3]
	accessSecret := arguments[4]
	keyPrefix := arguments[5]
	var name string
	if len(arguments) > 6 {
		name = arguments[6]
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	credentialProvider := credentials.NewStaticCredentialsProvider(accessKey, accessSecret, "")

	fs, err := newS3FS(
		"",
		name,
		endpoint,
		bucket,
		keyPrefix,
		0,
		[]func(*config.LoadOptions) error{
			config.WithCredentialsProvider(
				credentialProvider,
			),
		},
		[]func(*s3.Options){
			s3.WithEndpointResolver(
				s3.EndpointResolverFromURL(endpoint),
			),
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
				opt.Region = region
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func newS3FSFromArgumentsWithoutKey(arguments []string) (*S3FS, error) {
	endpoint := arguments[0]
	region := arguments[1]
	bucket := arguments[2]
	keyPrefix := arguments[3]
	var name string
	if len(arguments) > 4 {
		name = arguments[4]
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	fs, err := newS3FS(
		"",
		name,
		endpoint,
		bucket,
		keyPrefix,
		0,
		nil,
		[]func(*s3.Options){
			s3.WithEndpointResolver(
				s3.EndpointResolverFromURL(endpoint),
			),
			func(opt *s3.Options) {
				opt.Region = region
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return fs, nil
}

func newMinioS3FSFromArguments(arguments []string) (*S3FS, error) {
	endpoint := arguments[0]
	region := arguments[1]
	_ = region
	bucket := arguments[2]
	accessKey := arguments[3]
	accessSecret := arguments[4]
	keyPrefix := arguments[5]
	var name string
	if len(arguments) > 6 {
		name = arguments[6]
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	endpointResolver := s3.EndpointResolverFunc(
		func(
			region string,
			options s3.EndpointResolverOptions,
		) (
			ep aws.Endpoint,
			err error,
		) {
			_ = options
			ep.URL = endpoint
			ep.Source = aws.EndpointSourceCustom
			ep.HostnameImmutable = true
			ep.SigningRegion = region
			return
		},
	)

	credentialProvider := credentials.NewStaticCredentialsProvider(accessKey, accessSecret, "")

	fs, err := newS3FS(
		"",
		name,
		endpoint,
		bucket,
		keyPrefix,
		0,
		[]func(*config.LoadOptions) error{
			config.WithCredentialsProvider(
				credentialProvider,
			),
		},
		[]func(*s3.Options){
			s3.WithEndpointResolver(
				endpointResolver,
			),
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
				opt.Region = region
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return fs, nil
}
