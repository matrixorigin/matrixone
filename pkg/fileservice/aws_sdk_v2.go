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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

type AwsSDKv2 struct {
	name            string
	bucket          string
	client          *s3.Client
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int32
}

func NewAwsSDKv2(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*AwsSDKv2, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// http client
	dialer := &net.Dialer{
		KeepAlive: 5 * time.Second,
	}
	transport := &stdhttp.Transport{
		Proxy:                 stdhttp.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       180 * time.Second,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       1000,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	if len(args.CertFiles) > 0 {
		// custom certs
		pool, err := x509.SystemCertPool()
		if err != nil {
			panic(err)
		}
		for _, path := range args.CertFiles {
			content, err := os.ReadFile(path)
			if err != nil {
				logutil.Info("load cert file error",
					zap.Any("err", err),
				)
				// ignore
				continue
			}
			logutil.Info("file service: load cert file",
				zap.Any("path", path),
			)
			pool.AppendCertsFromPEM(content)
		}
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            pool,
		}
		transport.TLSClientConfig = tlsConfig
	}
	httpClient := &stdhttp.Client{
		Transport: transport,
	}

	// options for loading configs
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
		config.WithHTTPClient(httpClient),
	}

	// shared config profile
	if args.SharedConfigProfile != "" {
		loadConfigOptions = append(loadConfigOptions,
			config.WithSharedConfigProfile(args.SharedConfigProfile),
		)
	}

	credentialProvider, err := args.credentialsProviderForAwsSDKv2(ctx)
	if err != nil {
		return nil, err
	}

	// validate
	if credentialProvider != nil {
		_, err := credentialProvider.Retrieve(ctx)
		if err != nil {
			return nil, err
		}
	}

	// load configs
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

	// options for s3 client
	s3Options := []func(*s3.Options){
		func(opts *s3.Options) {

			opts.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxRetryAttemps
				o.RateLimiter = noOpRateLimit{}
			})

		},
	}

	// credential provider for s3 client
	if credentialProvider != nil {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Credentials = credentialProvider
			},
		)
	}

	// endpoint for s3 client
	if args.Endpoint != "" {
		if args.IsMinio {
			// special handling for MinIO
			s3Options = append(s3Options,
				s3.WithEndpointResolver(
					s3.EndpointResolverFunc(
						func(
							region string,
							_ s3.EndpointResolverOptions,
						) (
							ep aws.Endpoint,
							err error,
						) {
							ep.URL = args.Endpoint
							ep.Source = aws.EndpointSourceCustom
							ep.HostnameImmutable = true
							ep.SigningRegion = region
							return
						},
					),
				),
			)
		} else {
			s3Options = append(s3Options,
				s3.WithEndpointResolver(
					s3.EndpointResolverFromURL(args.Endpoint),
				),
			)
		}
	}

	// region for s3 client
	if args.Region != "" {
		s3Options = append(s3Options,
			func(opt *s3.Options) {
				opt.Region = args.Region
			},
		)
	}

	// new s3 client
	client := s3.NewFromConfig(
		config,
		s3Options...,
	)

	logutil.Info("new object storage",
		zap.Any("sdk", "aws v2"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// head bucket to validate
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: ptrTo(args.Bucket),
		})
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("bad s3 config: %v", err)
		}
	}

	return &AwsSDKv2{
		name:            args.Name,
		bucket:          args.Bucket,
		client:          client,
		perfCounterSets: perfCounterSets,
	}, nil

}

var _ ObjectStorage = new(AwsSDKv2)

func (a *AwsSDKv2) List(
	ctx context.Context,
	prefix string,
	fn func(bool, string, int64) (bool, error),
) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var marker *string

loop1:
	for {
		output, err := a.listObjects(
			ctx,
			&s3.ListObjectsInput{
				Bucket:    ptrTo(a.bucket),
				Delimiter: ptrTo("/"),
				Prefix:    ptrTo(prefix),
				Marker:    marker,
				MaxKeys:   a.listMaxKeys,
			},
		)
		if err != nil {
			return err
		}

		for _, obj := range output.Contents {
			more, err := fn(false, *obj.Key, obj.Size)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		for _, prefix := range output.CommonPrefixes {
			more, err := fn(true, *prefix.Prefix, 0)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		if !output.IsTruncated {
			break
		}
		marker = output.NextMarker
	}

	return nil
}

func (a *AwsSDKv2) Stat(
	ctx context.Context,
	key string,
) (
	size int64,
	err error,
) {

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	output, err := a.headObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				err = moerr.NewFileNotFound(ctx, key)
				return
			}
		}
		return
	}

	size = output.ContentLength

	return
}

func (a *AwsSDKv2) Exists(
	ctx context.Context,
	key string,
) (
	bool,
	error,
) {
	output, err := a.headObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				return false, nil
			}
		}
		return false, err
	}
	return output != nil, nil
}

func (a *AwsSDKv2) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	size int64,
	expire *time.Time,
) (
	err error,
) {

	_, err = a.putObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(a.bucket),
			Key:           ptrTo(key),
			Body:          r,
			ContentLength: size,
			Expires:       expire,
		},
	)
	if err != nil {
		return err
	}

	return
}

func (a *AwsSDKv2) Read(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
) (
	r io.ReadCloser,
	err error,
) {

	if max == nil {
		// read to end
		r, err := a.getObject(
			ctx,
			min,
			nil,
			&s3.GetObjectInput{
				Bucket: ptrTo(a.bucket),
				Key:    ptrTo(key),
			},
		)
		err = a.mapError(err, key)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	r, err = a.getObject(
		ctx,
		min,
		max,
		&s3.GetObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	err = a.mapError(err, key)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         io.LimitReader(r, int64(*max-*min)),
		closeFunc: r.Close,
	}, nil
}

func (a *AwsSDKv2) Delete(
	ctx context.Context,
	keys ...string,
) (
	err error,
) {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return a.deleteSingle(ctx, keys[0])
	}

	objs := make([]types.ObjectIdentifier, 0, 1000)
	for _, key := range keys {
		objs = append(objs, types.ObjectIdentifier{Key: ptrTo(key)})
		if len(objs) == 1000 {
			if err := a.deleteMultiObj(ctx, objs); err != nil {
				return err
			}
			objs = objs[:0]
		}
	}
	if err := a.deleteMultiObj(ctx, objs); err != nil {
		return err
	}
	return nil
}

func (a *AwsSDKv2) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "AwsSDKv2.deleteSingle")
	defer span.End()
	_, err := a.deleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(a.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *AwsSDKv2) deleteMultiObj(ctx context.Context, objs []types.ObjectIdentifier) error {
	ctx, span := trace.Start(ctx, "AwsSDKv2.deleteMultiObj")
	defer span.End()
	output, err := a.deleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptrTo(a.bucket),
		Delete: &types.Delete{
			Objects: objs,
			// In quiet mode the response includes only keys where the delete action encountered an error.
			Quiet: true,
		},
	})
	// delete api failed
	if err != nil {
		return err
	}
	// delete api success, but with delete file failed.
	message := strings.Builder{}
	if len(output.Errors) > 0 {
		for _, Error := range output.Errors {
			if *Error.Code == (*types.NoSuchKey)(nil).ErrorCode() {
				continue
			}
			message.WriteString(fmt.Sprintf("%s: %s, %s;", *Error.Key, *Error.Code, *Error.Message))
		}
	}
	if message.Len() > 0 {
		return moerr.NewInternalErrorNoCtx("S3 Delete failed: %s", message.String())
	}
	return nil
}

func (a *AwsSDKv2) listObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.listObjects")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.List.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 list objects",
		func() (*s3.ListObjectsOutput, error) {
			return a.client.ListObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv2) headObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.headObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 head object",
		func() (*s3.HeadObjectOutput, error) {
			return a.client.HeadObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv2) putObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.putObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	// not retryable because Reader may be half consumed
	return a.client.PutObject(ctx, params, optFns...)
}

func (a *AwsSDKv2) getObject(ctx context.Context, min *int64, max *int64, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.getObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, a.perfCounterSets...)
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			var rang string
			if max != nil {
				rang = fmt.Sprintf("bytes=%d-%d", offset, *max)
			} else {
				rang = fmt.Sprintf("bytes=%d-", offset)
			}
			params.Range = &rang
			output, err := doWithRetry(
				"s3 get object",
				func() (*s3.GetObjectOutput, error) {
					return a.client.GetObject(ctx, params, optFns...)
				},
				maxRetryAttemps,
				isRetryableError,
			)
			if err != nil {
				return nil, err
			}
			return output.Body, nil
		},
		*min,
		isRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *AwsSDKv2) deleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.deleteObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 delete object",
		func() (*s3.DeleteObjectOutput, error) {
			return a.client.DeleteObject(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv2) deleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv2.deleteObjects")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 delete objects",
		func() (*s3.DeleteObjectsOutput, error) {
			return a.client.DeleteObjects(ctx, params, optFns...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv2) mapError(err error, path string) error {
	if err == nil {
		return nil
	}
	var httpError *http.ResponseError
	if errors.As(err, &httpError) {
		if httpError.Response.StatusCode == 404 {
			return moerr.NewFileNotFoundNoCtx(path)
		}
	}
	return err
}

// from https://github.com/aws/aws-sdk-go-v2/issues/543
type noOpRateLimit struct{}

func (noOpRateLimit) AddTokens(uint) error { return nil }
func (noOpRateLimit) GetToken(context.Context, uint) (func() error, error) {
	return noOpToken, nil
}
func noOpToken() error { return nil }

func (o ObjectStorageArguments) credentialsProviderForAwsSDKv2(
	ctx context.Context,
) (
	ret aws.CredentialsProvider,
	err error,
) {

	// cache
	defer func() {
		if ret != nil {
			ret = aws.NewCredentialsCache(ret)
		}
	}()

	defer func() {
		// handle assume role
		if o.RoleARN == "" {
			return
		}

		logutil.Info("setting assume role provider")
		// load default options
		awsConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			panic(err)
		}
		if ret != nil {
			logutil.Info("using upstream credential provider for assume role",
				zap.Any("type", fmt.Sprintf("%T", ret)),
			)
			awsConfig.Credentials = ret
		}

		stsSvc := sts.NewFromConfig(awsConfig, func(options *sts.Options) {
			if o.Region == "" {
				options.Region = "ap-northeast-1"
			} else {
				options.Region = o.Region
			}
		})
		provider := stscreds.NewAssumeRoleProvider(
			stsSvc,
			o.RoleARN,
			func(opts *stscreds.AssumeRoleOptions) {
				if o.ExternalID != "" {
					opts.ExternalID = &o.ExternalID
				}
			},
		)
		_, err = provider.Retrieve(ctx)
		if err != nil {
			// not good
			logutil.Info("bad assume role provider",
				zap.Any("err", err),
			)
			return
		}

		// set to assume role provider
		ret = provider
	}()

	// static credential
	if o.KeyID != "" && o.KeySecret != "" {
		// static
		logutil.Info("static credential")
		return credentials.NewStaticCredentialsProvider(o.KeyID, o.KeySecret, o.SessionToken), nil
	}

	if !o.shouldLoadDefaultCredentials() {
		return nil, moerr.NewInvalidInputNoCtx(
			"no valid credentials",
		)
	}

	return
}
