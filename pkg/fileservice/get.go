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
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func Get[T any](fs FileService, name string) (res T, err error) {
	lowerName := strings.ToLower(name)
	if fs, ok := fs.(*FileServices); ok {
		f, ok := fs.mappings[lowerName]
		if !ok {
			err = moerr.NewNoServiceNoCtx(name)
			return
		}
		res, ok = f.(T)
		if !ok {
			err = moerr.NewNoServiceNoCtx(name)
			return
		}
		return
	}
	var ok bool
	res, ok = fs.(T)
	if !ok {
		err = moerr.NewNoServiceNoCtx(name)
		return
	}
	if !strings.EqualFold(fs.Name(), lowerName) {
		err = moerr.NewNoServiceNoCtx(name)
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
// s3-opts,endpoint=<endpoint>,region=<region>,bucket=<bucket>,key=<key>,secret=<secret>,prefix=<prefix>,role-arn=<role arn>,external-id=<external id>
//
//	key value pairs can be in any order
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
			arguments := fsPath.ServiceArguments
			if len(arguments) < 6 {
				return nil, "", moerr.NewInvalidInputNoCtx("invalid S3 arguments")
			}
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
			res, err = newS3FSFromArguments([]string{
				"endpoint=" + endpoint,
				"region=" + region,
				"bucket=" + bucket,
				"key=" + accessKey,
				"secret=" + accessSecret,
				"prefix=" + keyPrefix,
				"name=" + name,
			})
			if err != nil {
				return
			}
			return

		case "s3-no-key":
			arguments := fsPath.ServiceArguments
			if len(arguments) < 4 {
				return nil, "", moerr.NewInvalidInputNoCtx("invalid S3 arguments")
			}
			endpoint := arguments[0]
			region := arguments[1]
			bucket := arguments[2]
			keyPrefix := arguments[3]
			var name string
			if len(arguments) > 4 {
				name = arguments[4]
			}
			res, err = newS3FSFromArguments([]string{
				"endpoint=" + endpoint,
				"region=" + region,
				"bucket=" + bucket,
				"prefix=" + keyPrefix,
				"name=" + name,
			})
			if err != nil {
				return
			}
			return

		case "s3-opts":
			res, err = newS3FSFromArguments(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		case "minio":
			res, err = newMinioS3FSFromArguments(fsPath.ServiceArguments)
			if err != nil {
				return
			}

		default:
			err = moerr.NewInvalidInputNoCtx("no such service: %s", fsPath.Service)
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
	if len(arguments) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid S3 arguments")
	}

	var endpoint, region, bucket, apiKey, apiSecret, prefix, roleARN, externalID, name string
	for _, pair := range arguments {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}
		switch key {
		case "endpoint":
			endpoint = value
		case "region":
			region = value
		case "bucket":
			bucket = value
		case "key":
			apiKey = value
		case "secret":
			apiSecret = value
		case "prefix":
			prefix = value
		case "role-arn":
			roleARN = value
		case "external-id":
			externalID = value
		case "name":
			name = value
		default:
			return nil, moerr.NewInvalidInputNoCtx("invalid S3 argument: %s", pair)
		}
	}

	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		if u.Scheme == "" {
			u.Scheme = "https"
		}
		endpoint = u.String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var credentialProvider aws.CredentialsProvider

	if apiKey != "" && apiSecret != "" {
		// static
		credentialProvider = credentials.NewStaticCredentialsProvider(apiKey, apiSecret, "")

	} else if roleARN != "" {
		// role arn
		loadConfigOptions := []func(*config.LoadOptions) error{
			config.WithLogger(logutil.GetS3Logger()),
			config.WithClientLogMode(
				aws.LogSigning |
					aws.LogRetries |
					aws.LogRequest |
					aws.LogResponse |
					aws.LogDeprecatedUsage |
					aws.LogRequestEventMessage |
					aws.LogResponseEventMessage,
			),
		}
		config, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
		if err != nil {
			return nil, err
		}
		stsSvc := sts.NewFromConfig(config)
		credentialProvider = stscreds.NewAssumeRoleProvider(
			stsSvc,
			roleARN,
			func(opts *stscreds.AssumeRoleOptions) {
				opts.ExternalID = &externalID
			},
		)
	}

	loadConfigOptions := []func(*config.LoadOptions) error{
		config.WithLogger(logutil.GetS3Logger()),
		config.WithClientLogMode(
			aws.LogSigning |
				aws.LogRetries |
				aws.LogRequest |
				aws.LogResponse |
				aws.LogDeprecatedUsage |
				aws.LogRequestEventMessage |
				aws.LogResponseEventMessage,
		),
	}
	if credentialProvider != nil {
		loadConfigOptions = append(loadConfigOptions,
			config.WithCredentialsProvider(
				credentialProvider,
			),
		)
	}
	config, err := config.LoadDefaultConfig(ctx, loadConfigOptions...)
	if err != nil {
		return nil, err
	}

	s3Options := []func(*s3.Options){}
	if credentialProvider != nil {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
			},
		)
	}
	if endpoint != "" {
		s3Options = append(s3Options,
			s3.WithEndpointResolver(
				s3.EndpointResolverFromURL(endpoint),
			),
		)
	}
	if region != "" {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Region = region
			},
		)
	}

	client := s3.NewFromConfig(
		config,
		s3Options...,
	)

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: ptrTo(bucket),
	})
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtx("bad s3 config: %v", err)
	}

	fs := &S3FS{
		name:      name,
		client:    client,
		bucket:    bucket,
		keyPrefix: prefix,
	}

	return fs, nil

}

func newMinioS3FSFromArguments(arguments []string) (*S3FS, error) {
	if len(arguments) < 6 {
		return nil, moerr.NewInvalidInputNoCtx("invalid S3 arguments")
	}
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
