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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type MinioSDK struct {
	name            string
	bucket          string
	core            *minio.Core
	client          *minio.Client
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int
}

func NewMinioSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*MinioSDK, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	options := new(minio.Options)

	// credentials
	var credentialProviders []credentials.Provider
	if args.shouldLoadDefaultCredentials() {
		credentialProviders = append(credentialProviders,
			// aws env
			new(credentials.EnvAWS),
			// minio env
			new(credentials.EnvMinio),
		)
	}
	if args.KeyID != "" && args.KeySecret != "" {
		// static
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     args.KeyID,
				SecretAccessKey: args.KeySecret,
				SessionToken:    args.SessionToken,
				SignerType:      credentials.SignatureV4,
			},
		})
	}
	if args.RoleARN != "" {
		// assume role
		credentialProviders = append(credentialProviders, &credentials.STSAssumeRole{
			Options: credentials.STSAssumeRoleOptions{
				AccessKey:       args.KeyID,
				SecretKey:       args.KeySecret,
				RoleARN:         args.RoleARN,
				RoleSessionName: args.ExternalID,
			},
		})
	}

	// special treatments for 天翼云
	if strings.Contains(args.Endpoint, "ctyunapi.cn") {
		if args.KeyID == "" {
			// try to fetch one
			creds := credentials.NewChainCredentials(credentialProviders)
			value, err := creds.Get()
			if err != nil {
				return nil, err
			}
			args.KeyID = value.AccessKeyID
			args.KeySecret = value.SecretAccessKey
			args.SessionToken = value.SessionToken
		}
		credentialProviders = []credentials.Provider{
			&credentials.Static{
				Value: credentials.Value{
					AccessKeyID:     args.KeyID,
					SecretAccessKey: args.KeySecret,
					SessionToken:    args.SessionToken,
					SignerType:      credentials.SignatureV2,
				},
			},
		}
	}

	options.Creds = credentials.NewChainCredentials(credentialProviders)

	// region
	if args.Region != "" {
		options.Region = args.Region
	}

	// transport
	dialer := &net.Dialer{
		KeepAlive: 5 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
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
	options.Transport = transport

	// endpoint
	isSecure, err := minioValidateEndpoint(&args)
	if err != nil {
		return nil, err
	}
	options.Secure = isSecure

	client, err := minio.New(args.Endpoint, options)
	if err != nil {
		return nil, err
	}
	core, err := minio.NewCore(args.Endpoint, options)
	if err != nil {
		return nil, err
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "minio"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// validate
		ok, err := client.BucketExists(ctx, args.Bucket)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx(
				"bad s3 config, no such bucket or no permissions: %v",
				args.Bucket,
			)
		}
	}

	return &MinioSDK{
		name:            args.Name,
		bucket:          args.Bucket,
		client:          client,
		core:            core,
		perfCounterSets: perfCounterSets,
	}, nil
}

var _ ObjectStorage = new(MinioSDK)

func (a *MinioSDK) List(
	ctx context.Context,
	prefix string,
	fn func(bool, string, int64) (bool, error),
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	var marker string

loop1:
	for {
		result, err := a.listObjects(ctx, prefix, marker)
		if err != nil {
			return err
		}

		for _, obj := range result.Contents {
			more, err := fn(false, obj.Key, obj.Size)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		for _, prefix := range result.CommonPrefixes {
			more, err := fn(true, prefix.Prefix, 0)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		if !result.IsTruncated {
			break
		}
		marker = result.Marker
	}

	return nil
}

func (a *MinioSDK) Stat(
	ctx context.Context,
	key string,
) (
	size int64,
	err error,
) {

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	info, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		return
	}

	size = info.Size

	return
}

func (a *MinioSDK) Exists(
	ctx context.Context,
	key string,
) (
	bool,
	error,
) {

	if err := ctx.Err(); err != nil {
		return false, err
	}

	_, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		if a.is404(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (a *MinioSDK) Write(
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
		key,
		r,
		size,
		expire,
	)
	if err != nil {
		return err
	}

	return
}

func (a *MinioSDK) Read(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
) (
	r io.ReadCloser,
	err error,
) {

	defer func() {
		if a.is404(err) {
			err = moerr.NewFileNotFoundNoCtx(key)
		}
	}()

	if max == nil {
		// read to end
		r, err := a.getObject(
			ctx,
			key,
			min,
			nil,
		)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	r, err = a.getObject(
		ctx,
		key,
		min,
		max,
	)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         io.LimitReader(r, int64(*max-*min)),
		closeFunc: r.Close,
	}, nil
}

func (a *MinioSDK) Delete(
	ctx context.Context,
	keys ...string,
) (
	err error,
) {

	if err := ctx.Err(); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return a.deleteSingle(ctx, keys[0])
	}

	for i := 0; i < len(keys); i += 1000 {
		end := i + 1000
		if end > len(keys) {
			end = len(keys)
		}
		if _, err := a.deleteObjects(ctx, keys[i:end]...); err != nil {
			return err
		}
	}

	return nil
}

func (a *MinioSDK) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "MinioSDK.deleteSingle")
	defer span.End()

	_, err := a.deleteObject(
		ctx,
		key,
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *MinioSDK) listObjects(ctx context.Context, prefix string, marker string) (minio.ListBucketResult, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.listObjects")
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
		func() (minio.ListBucketResult, error) {
			return a.core.ListObjects(
				a.bucket,
				prefix,
				marker,
				"/",
				a.listMaxKeys,
			)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *MinioSDK) statObject(ctx context.Context, key string) (minio.ObjectInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.statObject")
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
		func() (minio.ObjectInfo, error) {
			return a.client.StatObject(
				ctx,
				a.bucket,
				key,
				minio.StatObjectOptions{},
			)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *MinioSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	size int64,
	expire *time.Time,
) (minio.UploadInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.putObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	// not retryable because Reader may be half consumed
	//TODO set expire
	return a.client.PutObject(
		ctx,
		a.bucket,
		key,
		r,
		size,
		minio.PutObjectOptions{},
	)
}

func (a *MinioSDK) getObject(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.getObject")
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
			obj, err := doWithRetry(
				"s3 get object",
				func() (obj *minio.Object, err error) {
					return a.client.GetObject(ctx, a.bucket, key, minio.GetObjectOptions{})
				},
				maxRetryAttemps,
				isRetryableError,
			)
			if err != nil {
				return nil, err
			}
			if offset > 0 {
				if _, err := obj.Seek(offset, 0); err != nil {
					return nil, err
				}
			}
			return obj, nil
		},
		*min,
		isRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *MinioSDK) deleteObject(ctx context.Context, key string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObject")
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
		func() (any, error) {
			if err := a.client.RemoveObject(ctx, a.bucket, key, minio.RemoveObjectOptions{}); err != nil {
				return nil, err
			}
			return nil, nil
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *MinioSDK) deleteObjects(ctx context.Context, keys ...string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObjects")
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
		func() (any, error) {
			objsCh := make(chan minio.ObjectInfo)
			errCh := a.client.RemoveObjects(ctx, a.bucket, objsCh, minio.RemoveObjectsOptions{})
			for _, key := range keys {
				objsCh <- minio.ObjectInfo{
					Key: key,
				}
			}
			for err := range errCh {
				return nil, err.Err
			}
			return nil, nil
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *MinioSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	var resp minio.ErrorResponse
	if !errors.As(err, &resp) {
		return false
	}
	return resp.Code == "NoSuchKey"
}

func minioValidateEndpoint(args *ObjectStorageArguments) (isSecure bool, err error) {
	if args.Endpoint == "" {
		return false, nil
	}

	endpointURL, err := url.Parse(args.Endpoint)
	if err != nil {
		return false, err
	}
	isSecure = endpointURL.Scheme == "https"
	endpointURL.Scheme = ""
	args.Endpoint = endpointURL.String()
	args.Endpoint = strings.TrimLeft(args.Endpoint, "/")

	return
}
