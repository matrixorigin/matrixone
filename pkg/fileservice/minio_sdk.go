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
	"bytes"
	"context"
	"io"
	"iter"
	"net/http"
	"net/url"
	gotrace "runtime/trace"
	"strings"
	"sync/atomic"
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
				SignerType:      credentials.SignatureV2,
			},
		})
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     args.KeyID,
				SecretAccessKey: args.KeySecret,
				SessionToken:    args.SessionToken,
				SignerType:      credentials.SignatureV4,
			},
		})
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     args.KeyID,
				SecretAccessKey: args.KeySecret,
				SessionToken:    args.SessionToken,
				SignerType:      credentials.SignatureDefault,
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

	// bucket lookup style
	if strings.Contains(args.Endpoint, "myqcloud") ||
		strings.Contains(args.Endpoint, "aliyuncs") {
		// 腾讯云，阿里云默认使用virtual host style
		options.BucketLookup = minio.BucketLookupDNS
	}

	// transport
	options.Transport = newHTTPClient(args).Transport

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
			return nil, moerr.NewInternalErrorNoCtxf(
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
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		var marker string

	loop1:
		for {
			result, err := a.listObjects(ctx, prefix, marker)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, obj := range result.Contents {
				if !yield(&DirEntry{
					Name: obj.Key,
					Size: obj.Size,
				}, nil) {
					break loop1
				}
			}

			for _, prefix := range result.CommonPrefixes {
				if !yield(&DirEntry{
					IsDir: true,
					Name:  prefix.Prefix,
				}, nil) {
					break loop1
				}
			}

			if !result.IsTruncated {
				break
			}
			marker = result.NextMarker
		}

	}
}

func (a *MinioSDK) Stat(
	ctx context.Context,
	key string,
) (
	size int64,
	err error,
) {

	defer func() {
		if a.is404(err) {
			err = moerr.NewFileNotFoundNoCtx(key)
		}
	}()

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
	sizeHint *int64,
	expire *time.Time,
) (
	err error,
) {
	defer wrapSizeMismatchErr(&err)

	var n atomic.Int64
	if sizeHint != nil {
		r = &countingReader{
			R: r,
			C: &n,
		}
	}

	if sizeHint != nil && *sizeHint < smallObjectThreshold {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		_, err = DoWithRetry("write", func() (minio.UploadInfo, error) {
			return a.putObject(
				ctx,
				key,
				bytes.NewReader(data),
				sizeHint,
				expire,
			)
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			return err
		}

	} else {
		_, err = a.putObject(
			ctx,
			key,
			r,
			sizeHint,
			expire,
		)
		if err != nil {
			return err
		}
	}

	if sizeHint != nil && n.Load() != *sizeHint {
		return moerr.NewSizeNotMatchNoCtx(key)
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
		// eager read to expose file not found error
		_, err = r.Read(nil)
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
	// eager read to expose file not found error
	_, err = r.Read(nil)
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

	if _, err := a.deleteObjects(ctx, keys...); err != nil {
		return err
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
	return DoWithRetry(
		"s3 list objects",
		func() (minio.ListBucketResult, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.List.Add(1)
			}, a.perfCounterSets...)
			return a.core.ListObjects(
				a.bucket,
				prefix,
				marker,
				"/",
				a.listMaxKeys,
			)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *MinioSDK) statObject(ctx context.Context, key string) (minio.ObjectInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.statObject")
	defer task.End()
	return DoWithRetry(
		"s3 head object",
		func() (minio.ObjectInfo, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Head.Add(1)
			}, a.perfCounterSets...)
			return a.client.StatObject(
				ctx,
				a.bucket,
				key,
				minio.StatObjectOptions{},
			)
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *MinioSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (minio.UploadInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.putObject")
	defer task.End()
	// not retryable because Reader may be half consumed
	//TODO set expire
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	size := int64(-1)
	if sizeHint != nil {
		size = *sizeHint
	}
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
	if min == nil {
		min = ptrTo[int64](0)
	}
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			obj, err := DoWithRetry(
				"s3 get object",
				func() (obj *minio.Object, err error) {
					perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
						counter.FileService.S3.Get.Add(1)
					}, a.perfCounterSets...)
					return a.client.GetObject(ctx, a.bucket, key, minio.GetObjectOptions{})
				},
				maxRetryAttemps,
				IsRetryableError,
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
		IsRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *MinioSDK) deleteObject(ctx context.Context, key string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObject")
	defer task.End()
	return DoWithRetry(
		"s3 delete object",
		func() (any, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Delete.Add(1)
			}, a.perfCounterSets...)
			if err := a.client.RemoveObject(ctx, a.bucket, key, minio.RemoveObjectOptions{}); err != nil {
				return nil, err
			}
			return nil, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *MinioSDK) deleteObjects(ctx context.Context, keys ...string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObjects")
	defer task.End()
	return DoWithRetry(
		"s3 delete objects",
		func() (any, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.DeleteMulti.Add(1)
			}, a.perfCounterSets...)
			objsCh := make(chan minio.ObjectInfo)
			errCh := a.client.RemoveObjects(ctx, a.bucket, objsCh, minio.RemoveObjectsOptions{})
			for _, key := range keys {
				objsCh <- minio.ObjectInfo{
					Key: key,
				}
			}
			close(objsCh)
			for err := range errCh {
				return nil, err.Err
			}
			return nil, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *MinioSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
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
