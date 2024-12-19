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

package fileservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	gotrace "runtime/trace"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/tencentyun/cos-go-sdk-v5"
	"go.uber.org/zap"
)

type QCloudSDK struct {
	name            string
	client          *cos.Client
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int
}

func NewQCloudSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (_ *QCloudSDK, err error) {
	defer catch(&err)

	// args
	if err := args.validate(); err != nil {
		return nil, err
	}

	// bucket url
	baseURL, err := url.Parse(fmt.Sprintf(
		"https://%s.cos.%s.myqcloud.com",
		args.Bucket,
		args.Region,
	))
	if err != nil {
		return nil, err
	}

	// credential arguments
	keyID := args.KeyID
	keySecret := args.KeySecret
	sessionToken := args.SessionToken
	if args.shouldLoadDefaultCredentials() {
		keyID = firstNonZero(
			args.KeyID,
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_ACCESS_KEY"),
			os.Getenv("TENCENTCLOUD_SECRETID"),
		)
		keySecret = firstNonZero(
			args.KeySecret,
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			os.Getenv("AWS_SECRET_KEY"),
			os.Getenv("TENCENTCLOUD_SECRETKEY"),
		)
		sessionToken = firstNonZero(
			args.SessionToken,
			os.Getenv("AWS_SESSION_TOKEN"),
			os.Getenv("TENCENTCLOUD_SESSIONTOKEN"),
		)
	}

	// http client
	httpClient := newHTTPClient(args)
	httpClient.Transport = &cos.AuthorizationTransport{
		SecretID:     keyID,
		SecretKey:    keySecret,
		SessionToken: sessionToken,
		Transport:    httpClient.Transport,
	}

	// client
	client := cos.NewClient(
		&cos.BaseURL{BucketURL: baseURL},
		httpClient,
	)

	logutil.Info("new object storage",
		zap.Any("sdk", "qcloud"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// validate bucket
		_, err := client.Bucket.Head(ctx, &cos.BucketHeadOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &QCloudSDK{
		name:            args.Name,
		client:          client,
		perfCounterSets: perfCounterSets,
	}, nil
}

var _ ObjectStorage = new(QCloudSDK)

func (a *QCloudSDK) List(
	ctx context.Context,
	prefix string,
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		var cont string

	loop1:
		for {
			result, err := a.listObjects(ctx, prefix, cont)
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
					Name:  prefix,
				}, nil) {
					break loop1
				}
			}

			if !result.IsTruncated {
				break
			}
			cont = result.NextMarker
		}

	}
}

func (a *QCloudSDK) Stat(
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

	header, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		return
	}

	if str := header.Get("Content-Length"); str != "" {
		size, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return
		}
	}

	return
}

func (a *QCloudSDK) Exists(
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

func (a *QCloudSDK) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (
	err error,
) {
	defer wrapSizeMismatchErr(&err)

	if sizeHint != nil && *sizeHint < smallObjectThreshold {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		_, err = DoWithRetry("write", func() (int, error) {
			return 0, a.putObject(
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
		err = a.putObject(
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

	return
}

func (a *QCloudSDK) Read(
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

func (a *QCloudSDK) Delete(
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

func (a *QCloudSDK) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "QCloudSDK.deleteSingle")
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

func (a *QCloudSDK) listObjects(ctx context.Context, prefix string, marker string) (*cos.BucketGetResult, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.listObjects")
	defer task.End()

	opts := &cos.BucketGetOptions{
		Delimiter: "/",
	}
	if prefix != "" {
		opts.Prefix = prefix
	}
	if marker != "" {
		opts.Marker = marker
	}
	if a.listMaxKeys > 0 {
		opts.MaxKeys = a.listMaxKeys
	}

	return DoWithRetry(
		"s3 list objects",
		func() (*cos.BucketGetResult, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.List.Add(1)
			}, a.perfCounterSets...)
			result, _, err := a.client.Bucket.Get(ctx, opts)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) statObject(ctx context.Context, key string) (http.Header, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.statObject")
	defer task.End()

	return DoWithRetry(
		"s3 head object",
		func() (http.Header, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Head.Add(1)
			}, a.perfCounterSets...)
			resp, err := a.client.Object.Head(ctx, key, &cos.ObjectHeadOptions{})
			if err != nil {
				return nil, err
			}
			return resp.Header, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (err error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.putObject")
	defer task.End()

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)

	// not retryable because Reader may be half consumed
	opts := &cos.ObjectPutOptions{}
	if sizeHint != nil {
		opts.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentLength: *sizeHint,
		}
	}
	_, err = a.client.Object.Put(ctx, key, r, opts)
	if err != nil {
		return err
	}
	return nil
}

func (a *QCloudSDK) getObject(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.getObject")
	defer task.End()

	if min == nil {
		min = ptrTo[int64](0)
	}

	return newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			var rang string
			if max != nil {
				rang = fmt.Sprintf("bytes=%d-%d", offset, *max)
			} else {
				rang = fmt.Sprintf("bytes=%d-", offset)
			}
			opts := &cos.ObjectGetOptions{
				Range: rang,
			}

			return DoWithRetry(
				"s3 get object",
				func() (io.ReadCloser, error) {
					perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
						counter.FileService.S3.Get.Add(1)
					}, a.perfCounterSets...)
					resp, err := a.client.Object.Get(ctx, key, opts)
					if err != nil {
						return nil, err
					}
					return &readCloser{
						r: resp.Body,
						closeFunc: func() error {
							// drain
							io.Copy(io.Discard, resp.Body)
							return resp.Body.Close()
						},
					}, nil
				},
				maxRetryAttemps,
				IsRetryableError,
			)

		},
		*min,
		IsRetryableError,
	)
}

func (a *QCloudSDK) deleteObject(ctx context.Context, key string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.deleteObject")
	defer task.End()
	return DoWithRetry(
		"s3 delete object",
		func() (bool, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Delete.Add(1)
			}, a.perfCounterSets...)
			if _, err := a.client.Object.Delete(ctx, key); err != nil {
				return false, err
			}
			return true, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) deleteObjects(ctx context.Context, keys ...string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.deleteObjects")
	defer task.End()
	return DoWithRetry(
		"s3 delete objects",
		func() (bool, error) {
			objects := make([]cos.Object, 0, len(keys))
			for _, key := range keys {
				objects = append(objects, cos.Object{
					Key: key,
				})
			}
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.DeleteMulti.Add(1)
			}, a.perfCounterSets...)
			_, _, err := a.client.Object.DeleteMulti(ctx, &cos.ObjectDeleteMultiOptions{
				Quiet:   true,
				Objects: objects,
			})
			if err != nil {
				return false, err
			}
			return true, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	var resp *cos.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Response.StatusCode == 404
	}
	return false
}
