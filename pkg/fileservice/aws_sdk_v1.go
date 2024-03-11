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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

type AwsSDKv1 struct {
	name            string
	bucket          string
	client          *s3.S3
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int64
}

func NewAwsSDKv1(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*AwsSDKv1, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	// configs
	config := new(aws.Config)
	if args.Endpoint != "" {
		config.Endpoint = &args.Endpoint
	}
	if args.Region != "" {
		config.Region = &args.Region
	}

	// for 天翼云
	// from https://gitee.com/ctyun-xstore/ctyun-xstore-sdk-demo/blob/master/xos-go-demo/s3demo.go
	if strings.Contains(args.Endpoint, "ctyunapi.cn") {
		config.S3ForcePathStyle = aws.Bool(true)
		config.DisableSSL = aws.Bool(true)
	}

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
	config.HTTPClient = httpClient

	// credentials
	if args.KeyID != "" && args.KeySecret != "" {
		config.Credentials = credentials.NewStaticCredentials(
			args.KeyID,
			args.KeySecret,
			args.SessionToken,
		)
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	client := s3.New(sess, config)

	logutil.Info("new object storage",
		zap.Any("sdk", "aws v1"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// head bucket to validate
		_, err = client.HeadBucket(&s3.HeadBucketInput{
			Bucket: ptrTo(args.Bucket),
		})
		if err != nil {
			return nil, moerr.NewInternalErrorNoCtx("bad s3 config: %v", err)
		}
	}

	return &AwsSDKv1{
		name:            args.Name,
		bucket:          args.Bucket,
		client:          client,
		perfCounterSets: perfCounterSets,
	}, nil

}

var _ ObjectStorage = new(AwsSDKv1)

func (a *AwsSDKv1) List(
	ctx context.Context,
	prefix string,
	fn func(bool, string, int64) (bool, error),
) error {

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var cont *string

loop1:
	for {
		output, err := a.listObjects(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            ptrTo(a.bucket),
				Delimiter:         ptrTo("/"),
				Prefix:            ptrTo(prefix),
				ContinuationToken: cont,
				MaxKeys:           ptrTo(a.listMaxKeys),
			},
		)
		if err != nil {
			return err
		}

		for _, obj := range output.Contents {
			more, err := fn(false, *obj.Key, *obj.Size)
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

		if !*output.IsTruncated {
			break
		}
		cont = output.ContinuationToken
	}

	return nil
}

func (a *AwsSDKv1) Stat(
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
		return
	}

	size = *output.ContentLength

	return
}

func (a *AwsSDKv1) Exists(
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
		if a.is404(err) {
			return false, nil
		}
		return false, err
	}
	return output != nil, nil
}

func (a *AwsSDKv1) Write(
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
			Body:          r.(io.ReadSeeker), //TODO
			ContentLength: ptrTo(size),
			Expires:       expire,
		},
	)
	if err != nil {
		return err
	}

	return
}

func (a *AwsSDKv1) Read(
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
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         io.LimitReader(r, int64(*max-*min)),
		closeFunc: r.Close,
	}, nil
}

func (a *AwsSDKv1) Delete(
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

	objs := make([]*s3.ObjectIdentifier, 0, 1000)
	for _, key := range keys {
		objs = append(objs, &s3.ObjectIdentifier{Key: ptrTo(key)})
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

func (a *AwsSDKv1) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "AwsSDKv1.deleteSingle")
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

func (a *AwsSDKv1) deleteMultiObj(ctx context.Context, objs []*s3.ObjectIdentifier) error {
	ctx, span := trace.Start(ctx, "AwsSDKv1.deleteMultiObj")
	defer span.End()
	output, err := a.deleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptrTo(a.bucket),
		Delete: &s3.Delete{
			Objects: objs,
			// In quiet mode the response includes only keys where the delete action encountered an error.
			Quiet: ptrTo(true),
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
			if *Error.Code == s3.ErrCodeNoSuchKey {
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

func (a *AwsSDKv1) listObjects(ctx context.Context, params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.listObjects")
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
		func() (*s3.ListObjectsV2Output, error) {
			return a.client.ListObjectsV2(params)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv1) headObject(ctx context.Context, params *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.headObject")
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
			return a.client.HeadObject(params)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv1) putObject(ctx context.Context, params *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.putObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	// not retryable because Reader may be half consumed
	return a.client.PutObject(params)
}

func (a *AwsSDKv1) getObject(ctx context.Context, min *int64, max *int64, params *s3.GetObjectInput) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.getObject")
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
					return a.client.GetObject(params)
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

func (a *AwsSDKv1) deleteObject(ctx context.Context, params *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.deleteObject")
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
			return a.client.DeleteObject(params)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv1) deleteObjects(ctx context.Context, params *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	ctx, task := gotrace.NewTask(ctx, "AwsSDKv1.deleteObjects")
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
			return a.client.DeleteObjects(params)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AwsSDKv1) is404(err error) bool {
	if err == nil {
		return false
	}
	var awsErr awserr.Error
	if !errors.As(err, &awsErr) {
		return false
	}
	return awsErr.Code() == "NotFound"
}
